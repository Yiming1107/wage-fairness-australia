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

# 基于实际数据分析的行业密度系数
INDUSTRY_DENSITY_COEFFICIENTS = {
    # 高系数行业 (中位就业人数低，需要更高系数)
    'Mining': 20.0,                                         # 中位6人
    'Agriculture, Forestry and Fishing': 20.0,              # 中位44人
    'Electricity, Gas, Water and Waste Services': 20.0,     # 中位14人
    'Information Media and Telecommunications': 20.0,        # 中位10人
    'Public Administration and Safety': 20.0,               # 中位30人
    'Arts and Recreation Services': 20.0,                   # 中位40人
    'Financial and Insurance Services': 20.0,               # 中位38人
    'Transport, Postal and Warehousing': 20.0,              # 中位62人
    'Wholesale Trade': 20.0,                                # 中位58人
    'Rental, Hiring and Real Estate Services': 20.0,        # 中位52人
    'Education and Training': 20.0,                         # 中位52人
    'Currently Unknown': 20.0,                              # 中位6人
    
    # 中等系数行业
    'Other Services': 14.4,                                 # 中位104人
    'Administrative and Support Services': 10.3,            # 中位146人
    'Professional, Scientific and Technical Services': 8.0, # 中位188人
    
    # 低系数行业 (中位就业人数高，系数较低)
    'Manufacturing': 4.7,                                   # 中位316人
    'Construction': 3.9,                                    # 中位388人
    'Retail Trade': 3.7,                                    # 中位402人
    'Health Care and Social Assistance': 3.6,               # 中位418人
    'Accommodation and Food Services': 3.3                  # 中位460人
}

def score_suburb(event, context):
    """
    基于数据驱动的密度调整suburb评分API
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
        
        # 2. 获取地区面积
        cursor.execute("SELECT AREASQKM21 FROM epic3_Victoria_suburb_2021 WHERE SAL_CODE21 = %s", (sal_code,))
        area_data = cursor.fetchone()
        area_sqkm = float(area_data['AREASQKM21']) if area_data and area_data['AREASQKM21'] else 50.0
        
        # 3. 获取房价
        cursor.execute("SELECT `2024` as price FROM epic3_house_prices WHERE SAL_CODE21 = %s", (sal_code,))
        house_data = cursor.fetchone()
        house_price = float(house_data['price']) if house_data and house_data['price'] else 0
        
        # 4. 获取交通站点数量
        cursor.execute("SELECT COUNT(*) as count FROM epic3_transport_stops WHERE SAL_CODE21 = %s", (sal_code,))
        transport_data = cursor.fetchone()
        transport_stops = int(transport_data['count']) if transport_data else 0
        
        # 5. 获取学校数量
        cursor.execute("SELECT COUNT(*) as count FROM epic3_schools WHERE SAL_CODE21 = %s", (sal_code,))
        school_data = cursor.fetchone()
        school_count = int(school_data['count']) if school_data else 0
        
        # 6. 获取儿童服务数量
        cursor.execute("SELECT COUNT(*) as count FROM epic3_early_childhood_services WHERE SAL_CODE21 = %s", (sal_code,))
        childcare_data = cursor.fetchone()
        childcare_count = int(childcare_data['count']) if childcare_data else 0
        
        # 7. 获取行业就业数据
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
                industry_employment = (
                    int(industry_data['1_4 Employees'] or 0) * 2 +
                    int(industry_data['5_19 Employees'] or 0) * 10 +
                    int(industry_data['20_199 Employees'] or 0) * 100 +
                    int(industry_data['200 plus Employees'] or 0) * 300
                )
        
        connection.close()
        
        # === 基于数据的密度评分算法 ===
        
        # 1. 生活成本评分 (房价，不受面积影响)
        if house_price == 0:
            cost_score = 50
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
        
        # 2. 交通便利评分 (简单线性，直接反映便利程度)
        if transport_stops == 0:
            transport_score = 0
            transport_calculation = "无交通站点 = 0分"
        else:
            transport_density = transport_stops / area_sqkm
            transport_score = min(100, transport_density * 8)
            transport_calculation = f"min(100, {transport_density:.4f}站点/km² × 8) = min(100, {transport_density * 8:.1f}) = {transport_score:.1f}分"
        
        # 3. 儿童保障评分 (基于数据调整的密度系数)
        school_density = school_count / area_sqkm
        childcare_density = childcare_count / area_sqkm
        child_score = min(100, school_density * 58 + childcare_density * 18)
        child_calculation = f"min(100, {school_density:.4f}所/km² × 58 + {childcare_density:.4f}个/km² × 18) = min(100, {school_density * 58:.1f} + {childcare_density * 18:.1f}) = {child_score:.1f}分"
        
        # 4. 行业就业评分 (纯密度，基于数据的行业系数)
        industry_coefficient = INDUSTRY_DENSITY_COEFFICIENTS.get(industry_name, 10.0)
        
        if industry_employment == 0:
            industry_score = 0
            industry_calculation = f"该地区{industry_name}行业无就业数据 = 0分"
        else:
            industry_density = industry_employment / area_sqkm
            industry_score = min(100, industry_density * industry_coefficient)
            industry_calculation = f"min(100, {industry_density:.3f}人/km² × {industry_coefficient}) = min(100, {industry_density * industry_coefficient:.1f}) = {industry_score:.1f}分"
        
        # 5. 综合评分 (等权重平均)
        overall_score = (cost_score + transport_score + child_score + industry_score) / 4
        overall_calculation = f"({cost_score:.1f} + {transport_score:.1f} + {child_score:.1f} + {industry_score:.1f}) ÷ 4 = {overall_score:.1f}分"
        
        # 构建返回结果
        result = {
            'suburb': suburb_name,
            'industry': industry_name,
            'sal_code': sal_code,
            'area_sqkm': area_sqkm,
            'algorithm_version': '2.2_simple_linear_transport',
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
                'industry_employment': industry_employment,
                'area_sqkm': area_sqkm
            },
            'density_data': {
                'transport_density': round(transport_stops / area_sqkm, 4) if area_sqkm > 0 else 0,
                'school_density': round(school_count / area_sqkm, 4) if area_sqkm > 0 else 0,
                'childcare_density': round(childcare_count / area_sqkm, 4) if area_sqkm > 0 else 0,
                'industry_density': round(industry_employment / area_sqkm, 4) if area_sqkm > 0 else 0
            },
            'calculation': {
                'cost_of_living': cost_calculation,
                'transport': transport_calculation,
                'child_care': child_calculation,
                'industry': industry_calculation,
                'overall': overall_calculation
            },
            'coefficients_used': {
                'transport': 100,
                'school': 58,
                'childcare': 18,
                'industry': industry_coefficient
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