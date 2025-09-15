import json
import logging
import pymysql.cursors
import math

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

# 行业就业基准数据（基于数据分析）
INDUSTRY_BENCHMARKS = {
    'Mining': 13,
    'Manufacturing': 515,
    'Construction': 572,
    'Retail Trade': 564,
    'Health Care and Social Assistance': 525,
    'Education and Training': 400,
    'Professional, Scientific and Technical Services': 300,
    'Agriculture, Forestry and Fishing': 50,
    'Accommodation and Food Services': 200,
    'Transport, Postal and Warehousing': 250,
    'Financial and Insurance Services': 150,
    'Information Media and Telecommunications': 100,
    'Electricity, Gas, Water and Waste Services': 80,
    'Wholesale Trade': 180,
    'Rental, Hiring and Real Estate Services': 120,
    'Administrative and Support Services': 200,
    'Public Administration and Safety': 180,
    'Arts and Recreation Services': 100,
    'Other Services': 150,
    'Currently Unknown': 50
}

def score_suburb(event, context):
    """
    改进的suburb评分API - 包含透明的计算过程
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
        cursor.execute("SELECT `2024` as price FROM epic3_house_prices WHERE SAL_CODE21 = %s", (sal_code,))
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
                # 估算就业人数
                industry_employment = (
                    int(industry_data['1_4 Employees'] or 0) * 2 +
                    int(industry_data['5_19 Employees'] or 0) * 10 +
                    int(industry_data['20_199 Employees'] or 0) * 100 +
                    int(industry_data['200 plus Employees'] or 0) * 300
                )
        
        connection.close()
        
        # === 新的评分算法 ===
        
        # 1. 生活成本评分 (房价越低越好)
        if house_price == 0:
            cost_score = 50  # 无数据默认中等
            cost_calculation = "无房价数据，默认50分"
        elif house_price < 500000:
            cost_score = 100
            cost_calculation = f"房价{house_price:,.0f} < 500,000 = 100分"
        elif house_price < 750000:
            cost_score = 80
            cost_calculation = f"房价{house_price:,.0f}在500,000-750,000区间 = 80分"
        elif house_price < 1150000:
            cost_score = 60
            cost_calculation = f"房价{house_price:,.0f}在750,000-1,150,000区间 = 60分"
        elif house_price < 2000000:
            cost_score = 40
            cost_calculation = f"房价{house_price:,.0f}在1,150,000-2,000,000区间 = 40分"
        else:
            cost_score = 20
            cost_calculation = f"房价{house_price:,.0f} > 2,000,000 = 20分"
        
        # 2. 交通便利评分 (对数函数避免线性增长过快)
        if transport_stops == 0:
            transport_score = 0
            transport_calculation = "无交通站点 = 0分"
        else:
            transport_score = min(100, 30 * math.log(transport_stops + 1))
            transport_calculation = f"min(100, 30 × ln({transport_stops} + 1)) = min(100, 30 × {math.log(transport_stops + 1):.2f}) = {transport_score:.1f}分"
        
        # 3. 儿童保障评分 (学校权重更高)
        child_score = min(100, school_count * 25 + childcare_count * 8)
        child_calculation = f"min(100, {school_count}所学校 × 25 + {childcare_count}个托儿所 × 8) = min(100, {school_count * 25 + childcare_count * 8}) = {child_score}分"
        
        # 4. 行业就业评分 (基于行业基准)
        industry_benchmark = INDUSTRY_BENCHMARKS.get(industry_name, 100)
        if industry_employment == 0:
            industry_score = 0
            industry_calculation = f"该地区{industry_name}行业无就业数据 = 0分"
        else:
            # 计算相对于基准的百分比，再转换为评分
            relative_ratio = industry_employment / industry_benchmark
            industry_score = min(100, relative_ratio * 50)
            industry_calculation = f"({industry_employment}人 ÷ 行业基准{industry_benchmark}人) × 50 = {relative_ratio:.2f} × 50 = {industry_score:.1f}分"
        
        # 5. 综合评分 (等权重平均)
        overall_score = (cost_score + transport_score + child_score + industry_score) / 4
        overall_calculation = f"({cost_score} + {transport_score:.1f} + {child_score} + {industry_score:.1f}) ÷ 4 = {overall_score:.1f}分"
        
        # 构建返回结果
        result = {
            'suburb': suburb_name,
            'industry': industry_name,
            'sal_code': sal_code,
            'scores': {
                'cost_of_living': round(cost_score, 1),
                'transport': round(transport_score, 1),
                'child_care': round(child_score, 1),
                'industry': round(industry_score, 1),
                'overall': round(overall_score, 1)
            },
            'raw_data': {
                'house_price': house_price,
                'transport_stops': transport_stops,
                'schools': school_count,
                'childcare_services': childcare_count,
                'industry_employment': industry_employment
            },
            'calculation': {
                'cost_of_living': cost_calculation,
                'transport': transport_calculation,
                'child_care': child_calculation,
                'industry': industry_calculation,
                'overall': overall_calculation
            }
        }
        
        return success_response(result)
        
    except Exception as e:
        logger.error(f"评分计算错误: {str(e)}")
        return error_response(500, 'CALCULATION_ERROR', str(e))

def success_response(data):
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({'success': True, 'data': data}, ensure_ascii=False)
    }

def error_response(status_code, error_code, message):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({'success': False, 'error': {'code': error_code, 'message': message}}, ensure_ascii=False)
    }