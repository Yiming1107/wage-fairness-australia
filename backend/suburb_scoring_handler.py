import json
import logging
import pymysql.cursors

# 设置日志记录
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 数据库配置
DB_CONFIG = {
    'host': 'fairwageaustralia.ct08osmucf2b.ap-southeast-2.rds.amazonaws.com',
    'user': 'admin',
    'password': 'fairwageaustralia',
    'port': 3306,
    'database': 'fairwageaustralia',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def score_suburb(event, context):
    """
    极简suburb评分API
    """
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'POST, OPTIONS'
            },
            'body': ''
        }
    
    try:
        # 解析请求
        body = json.loads(event['body'])
        suburb_name = body.get('sub')
        industry_name = body.get('industry')
        
        if not suburb_name or not industry_name:
            return error_response(400, 'MISSING_PARAMS', 'sub和industry参数必需')
        
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # 1. 获取SAL_CODE21
        cursor.execute("SELECT SAL_CODE21 FROM epic3_mapping_suburb_postcode WHERE SAL_NAME21 = %s", (suburb_name,))
        mapping = cursor.fetchone()
        
        if not mapping:
            return error_response(404, 'SUBURB_NOT_FOUND', f'未找到地区: {suburb_name}')
        
        sal_code = mapping['SAL_CODE21']
        
        # 2. 获取房价
        cursor.execute("SELECT 2024 as price FROM epic3_house_prices WHERE SAL_CODE21 = %s", (sal_code,))
        house_data = cursor.fetchone()
        house_price = float(house_data['price']) if house_data and house_data['price'] else 0
        
        # 3. 获取交通站点
        cursor.execute("SELECT COUNT(*) as count FROM epic3_transport_stops WHERE SAL_CODE21 = %s", (sal_code,))
        transport_data = cursor.fetchone()
        transport_stops = int(transport_data['count']) if transport_data else 0
        
        # 4. 获取学校
        cursor.execute("SELECT COUNT(*) as count FROM epic3_schools WHERE SAL_CODE21 = %s", (sal_code,))
        school_data = cursor.fetchone()
        school_count = int(school_data['count']) if school_data else 0
        
        # 5. 获取儿童服务
        cursor.execute("SELECT COUNT(*) as count FROM epic3_early_childhood_services WHERE SAL_CODE21 = %s", (sal_code,))
        childcare_data = cursor.fetchone()
        childcare_count = int(childcare_data['count']) if childcare_data else 0
        
        # 6. 获取行业就业 (需要SA2映射)
        cursor.execute("SELECT SA2_CODE21 FROM epic3_mapping_sa2_sal WHERE SAL_CODE21 = %s LIMIT 1", (sal_code,))
        sa2_mapping = cursor.fetchone()
        industry_employment = 0
        
        if sa2_mapping:
            sa2_code = sa2_mapping['SA2_CODE21']
            cursor.execute("""
                SELECT `1_4 Employees`, `5_19 Employees`, `20_199 Employees`, `200 plus Employees`
                FROM epic3_industry_employment 
                WHERE SA2_CODE21 = %s AND Industry = %s
            """, (sa2_code, industry_name))
            industry_data = cursor.fetchone()
            
            if industry_data:
                # 简单估算就业人数
                industry_employment = (
                    int(industry_data['1_4 Employees'] or 0) * 2 +
                    int(industry_data['5_19 Employees'] or 0) * 10 +
                    int(industry_data['20_199 Employees'] or 0) * 100 +
                    int(industry_data['200 plus Employees'] or 0) * 300
                )
        
        # 7. 获取犯罪数据
        cursor.execute("SELECT SUM(`Incidents Recorded`) as total FROM epic3_crime WHERE suburb_name = %s", (suburb_name,))
        crime_data = cursor.fetchone()
        crime_incidents = int(crime_data['total']) if crime_data and crime_data['total'] else 0
        
        connection.close()
        
        # 8. 简单评分
        # 房价评分 (越低越好)
        if house_price < 500000:
            cost_score = 100
        elif house_price < 1000000:
            cost_score = 70
        elif house_price < 1500000:
            cost_score = 40
        else:
            cost_score = 20
        
        # 交通评分
        transport_score = min(100, transport_stops * 2)
        
        # 儿童保障评分
        child_score = min(100, school_count * 20 + childcare_count * 10)
        
        # 行业评分
        industry_score = min(100, industry_employment / 5)
        
        # 安全评分
        safety_score = max(0, 100 - crime_incidents * 0.01)
        
        # 综合评分 (简单平均)
        overall_score = (safety_score + cost_score + transport_score + child_score + industry_score) / 5
        
        # 返回结果
        result = {
            'suburb': suburb_name,
            'industry': industry_name,
            'sal_code': sal_code,
            'scores': {
                'safety': round(safety_score, 1),
                'cost_of_living': cost_score,
                'transport': transport_score,
                'child_care': child_score,
                'industry': round(industry_score, 1),
                'overall': round(overall_score, 1)
            },
            'raw_data': {
                'house_price': house_price,
                'transport_stops': transport_stops,
                'schools': school_count,
                'childcare_services': childcare_count,
                'industry_employment': industry_employment,
                'crime_incidents': crime_incidents
            }
        }
        
        return success_response(result)
        
    except Exception as e:
        return error_response(500, 'ERROR', str(e))

def success_response(data):
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({'success': True, 'data': data})
    }

def error_response(status_code, error_code, message):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({'success': False, 'error': {'code': error_code, 'message': message}})
    }