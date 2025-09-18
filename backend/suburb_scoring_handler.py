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

# 行业映射 - 标准化输入到正确名称
INDUSTRY_MAPPING = {
    'currentlyunknown': 'Currently Unknown',
    'mining': 'Mining',
    'informationmediaandtelecommunications': 'Information Media and Telecommunications',
    'publicadministrationandsafety': 'Public Administration and Safety',
    'electricitygaswaterandwasteservices': 'Electricity, Gas, Water and Waste Services',
    'financialandinsuranceservices': 'Financial and Insurance Services',
    'agricultureforestryandfishing': 'Agriculture, Forestry and Fishing',
    'artsandrecreationservices': 'Arts and Recreation Services',
    'rentalhiringandrealestateservices': 'Rental, Hiring and Real Estate Services',
    'transportpostalandwarehousing': 'Transport, Postal and Warehousing',
    'wholesaletrade': 'Wholesale Trade',
    'educationandtraining': 'Education and Training',
    'otherservices': 'Other Services',
    'administrativeandsupportservices': 'Administrative and Support Services',
    'manufacturing': 'Manufacturing',
    'professionalscientificandtechnicalservices': 'Professional, Scientific and Technical Services',
    'retailtrade': 'Retail Trade',
    'accommodationandfoodservices': 'Accommodation and Food Services',
    'healthcareandsocialassistance': 'Health Care and Social Assistance',
    'construction': 'Construction'
}

def normalize_string(text):
    """
    标准化字符串：移除空格、符号，转换为小写
    """
    if not text:
        return ""
    return ''.join(c.lower() for c in text if c.isalnum())

def find_suburb_match(input_suburb, cursor):
    """
    在数据库中查找匹配的郊区名称
    """
    # 先尝试精确匹配
    cursor.execute("SELECT SAL_NAME21 FROM epic3_mapping_suburb_postcode WHERE SAL_NAME21 = %s", (input_suburb,))
    result = cursor.fetchone()
    if result:
        return result['SAL_NAME21']
    
    # 获取所有郊区名称进行标准化匹配
    cursor.execute("SELECT DISTINCT SAL_NAME21 FROM epic3_mapping_suburb_postcode WHERE SAL_NAME21 IS NOT NULL")
    all_suburbs = cursor.fetchall()
    
    normalized_input = normalize_string(input_suburb)
    
    for suburb in all_suburbs:
        suburb_name = suburb['SAL_NAME21']
        if normalize_string(suburb_name) == normalized_input:
            return suburb_name
    
    return None

# 基于实际数据分析的行业密度系数
INDUSTRY_DENSITY_COEFFICIENTS = {
    'Currently Unknown': 104.7,                             # 大幅提高
    'Mining': 83.4,                                         # 大幅提高
    'Information Media and Telecommunications': 65.9,        # 大幅提高
    'Public Administration and Safety': 59.0,               # 大幅提高
    'Electricity, Gas, Water and Waste Services': 56.1,     # 大幅提高
    'Financial and Insurance Services': 26.0,               # 适度提高
    'Agriculture, Forestry and Fishing': 25.1,              # 适度提高
    'Arts and Recreation Services': 23.1,                   # 适度提高
    'Rental, Hiring and Real Estate Services': 17.9,        # 适度降低
    'Transport, Postal and Warehousing': 15.3,              # 适度降低
    'Wholesale Trade': 11.6,                                # 明显降低
    'Education and Training': 11.2,                         # 明显降低
    'Other Services': 8.3,                                  # 明显降低
    'Administrative and Support Services': 6.3,             # 明显降低
    'Manufacturing': 6.0,                                   # 适度提高
    'Professional, Scientific and Technical Services': 5.0, # 明显降低
    'Retail Trade': 3.5,                                    # 微调
    'Accommodation and Food Services': 2.9,                 # 微调
    'Health Care and Social Assistance': 2.9,               # 微调
    'Construction': 2.6                                      # 微调
}

def score_suburb(event, context):
    """
    基于数据驱动的密度调整suburb评分API（支持简单标准化匹配）
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
    
    connection = None
    try:
        # 解析请求
        body = json.loads(event['body'])
        input_suburb = body.get('sub', '').strip()
        input_industry = body.get('industry', '').strip()
        input_population = body.get('population')  # 可选的人口输入
        
        if not input_suburb or not input_industry:
            return error_response(400, 'MISSING_PARAMS', 'sub和industry参数必需')
        
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # 查找匹配的郊区和行业
        suburb_name = find_suburb_match(input_suburb, cursor)
        industry_name = INDUSTRY_MAPPING.get(normalize_string(input_industry))
        
        if not suburb_name:
            return error_response(404, 'SUBURB_NOT_FOUND', f'未找到地区: {input_suburb}')
        
        if not industry_name:
            available_industries = list(INDUSTRY_MAPPING.values())
            return error_response(404, 'INDUSTRY_NOT_FOUND', f'未找到行业: {input_industry}. 可用行业包括: {", ".join(available_industries[:5])}...')
        
        
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
        
        # 8. 获取人口和犯罪数据（支持用户输入人口数据）
        if input_population:
            population = int(input_population)
        else:
            cursor.execute("SELECT Tot_P_P as population FROM epic3_p WHERE _CODE_2021 = %s", (sal_code,))
            pop_result = cursor.fetchone()
            population = int(pop_result['population']) if pop_result and pop_result['population'] else 0
        
        # 获取2025年犯罪数据
        cursor.execute("""
            SELECT SUM(CAST(`Incidents Recorded` AS UNSIGNED)) as total_crimes
            FROM epic3_crime 
            WHERE SAL_CODE21 = %s AND `Year` = 2025
        """, (sal_code,))
        crime_result = cursor.fetchone()
        total_crimes = int(crime_result['total_crimes']) if crime_result and crime_result['total_crimes'] else 0
        
        # === 保持原有的评分算法 ===
        
        # 1. 生活成本评分 (房价，线性函数)
        if house_price == 0:
            cost_score = 50
            cost_calculation = "No house price data = 50 points"
        elif house_price <= 300000:
            cost_score = 100
            cost_calculation = f"House price ${house_price:,.0f} ≤ $300k = 100 points"
        elif house_price >= 2000000:
            cost_score = 10
            cost_calculation = f"House price ${house_price:,.0f} ≥ $2M = 10 points"
        else:
            # 30万到200万之间，从100分线性下降到10分
            cost_score = 100 - ((house_price - 300000) / (2000000 - 300000)) * 90
            cost_calculation = f"House price ${house_price:,.0f}: 100 - (({house_price:,.0f} - 300k) / 1.7M) × 90 = {cost_score:.1f} points"
        
        # 2. 交通便利评分
        if transport_stops == 0:
            transport_score = 0
            transport_calculation = "No transport stops = 0 points"
        else:
            transport_density = transport_stops / area_sqkm
            transport_score = min(100, transport_density * 8)
            if transport_score >= 100:
                transport_calculation = f"Transport density: {transport_density:.4f} stops/km² × 8 = {transport_density * 8:.1f}, capped at 100 points"
            else:
                transport_calculation = f"Transport density: {transport_density:.4f} stops/km² × 8 = {transport_score:.1f} points"
        
        # 3. 儿童保障评分
        school_density = school_count / area_sqkm
        childcare_density = childcare_count / area_sqkm
        child_score = min(100, school_density * 58 + childcare_density * 18)
        school_points = school_density * 58
        childcare_points = childcare_density * 18
        total_points = school_points + childcare_points
        
        if child_score >= 100:
            child_calculation = f"Child care score: {school_density:.4f} schools/km² × 58 + {childcare_density:.4f} childcare/km² × 18 = {school_points:.1f} + {childcare_points:.1f} = {total_points:.1f}, capped at 100 points"
        else:
            child_calculation = f"Child care score: {school_density:.4f} schools/km² × 58 + {childcare_density:.4f} childcare/km² × 18 = {school_points:.1f} + {childcare_points:.1f} = {child_score:.1f} points"
        
        # 4. 行业就业评分
        industry_coefficient = INDUSTRY_DENSITY_COEFFICIENTS.get(industry_name, 10.0)
        
        if industry_employment == 0:
            industry_score = 0
            industry_calculation = f"No employment data for {industry_name} industry = 0 points"
        else:
            industry_density = industry_employment / area_sqkm
            raw_score = industry_density * industry_coefficient
            industry_score = min(100, raw_score)
            
            if industry_score >= 100:
                industry_calculation = f"Industry score: {industry_density:.3f} employees/km² × {industry_coefficient} = {raw_score:.1f}, capped at 100 points"
            else:
                industry_calculation = f"Industry score: {industry_density:.3f} employees/km² × {industry_coefficient} = {industry_score:.1f} points"
        
        # 5. 犯罪安全评分（线性函数）
        if population < 50:
            safety_score = 60
            crime_rate = 0
            safety_calculation = f"Population {population} too small for reliable crime rate = 60 points"
        elif population == 0:
            safety_score = 50
            crime_rate = 0
            safety_calculation = "No population data = 50 points"
        else:
            crime_rate = (total_crimes / population) * 1000
            
            if crime_rate <= 0:
                safety_score = 100
                safety_calculation = "Crime rate 0.00/1000 people = 100 points"
            elif crime_rate >= 200:
                safety_score = 5
                safety_calculation = f"Crime rate {crime_rate:.2f}/1000 people ≥ 200 = 5 points"
            else:
                # 0到200犯罪率之间，从100分线性下降到5分
                safety_score = 100 - (crime_rate / 200) * 95
                safety_calculation = f"Crime rate {crime_rate:.2f}/1000 people: 100 - ({crime_rate:.2f} / 200) × 95 = {safety_score:.1f} points"
        
        # 6. 综合评分
        overall_score = (cost_score + transport_score + child_score + industry_score + safety_score) / 5
        overall_calculation = f"({cost_score:.1f} + {transport_score:.1f} + {child_score:.1f} + {industry_score:.1f} + {safety_score:.1f}) ÷ 5 = {overall_score:.1f} points"
        
        # 构建返回结果
        result = {
            'suburb': suburb_name,
            'industry': industry_name,
            'sal_code': sal_code,
            'area_sqkm': area_sqkm,
            'population': population,

            'scores': {
                'cost_of_living': round(cost_score, 1),
                'transport': round(transport_score, 1),
                'child_care': round(child_score, 1),
                'industry': round(industry_score, 1),
                'safety': round(safety_score, 1),
                'overall': round(overall_score, 1)
            },
            'raw_data': {
                'house_price': house_price,
                'transport_stops': transport_stops,
                'schools': school_count,
                'childcare_services': childcare_count,
                'industry_employment': industry_employment,
                'population': population,
                'total_crimes_2025': total_crimes,
                'area_sqkm': area_sqkm
            },
            'density_data': {
                'transport_density': round(transport_stops / area_sqkm, 4) if area_sqkm > 0 else 0,
                'school_density': round(school_count / area_sqkm, 4) if area_sqkm > 0 else 0,
                'childcare_density': round(childcare_count / area_sqkm, 4) if area_sqkm > 0 else 0,
                'industry_density': round(industry_employment / area_sqkm, 4) if area_sqkm > 0 else 0,
                'crime_rate_per_1000': round(crime_rate, 2)
            },
            'calculation': {
                'cost_of_living': cost_calculation,
                'transport': transport_calculation,
                'child_care': child_calculation,
                'industry': industry_calculation,
                'safety': safety_calculation,
                'overall': overall_calculation
            },
            'coefficients_used': {
                'transport': 8,
                'school': 58,
                'childcare': 18,
                'industry': industry_coefficient
            }
        }
        
        return success_response(result)
        
    except Exception as e:
        logger.error(f"评分计算错误: {str(e)}")
        return error_response(500, 'CALCULATION_ERROR', str(e))
    finally:
        if connection:
            connection.close()

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