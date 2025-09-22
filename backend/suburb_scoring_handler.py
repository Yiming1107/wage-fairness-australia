import json
import logging
import pymysql.cursors
import math
import os
import time
import re

# 日志配置 | Logging configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 数据库配置，支持环境变量覆盖 | Database configuration with environment variable override
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'fairwageaustralia.ct08osmucf2b.ap-southeast-2.rds.amazonaws.com'),
    'user': os.environ.get('DB_USER', 'admin'),
    'password': os.environ.get('DB_PASSWORD', 'fairwageaustralia'),
    'port': int(os.environ.get('DB_PORT', 3306)),
    'database': os.environ.get('DB_NAME', 'fairwageaustralia'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'connect_timeout': 10,
    'read_timeout': 10,
    'write_timeout': 10,
    'autocommit': True
}

# 行业标准化映射表 | Industry standardization mapping
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
    """字符串标准化：仅保留字母数字 | String normalization: letters and numbers only"""
    if not text:
        return ""
    return ''.join(c.lower() for c in text if c.isalnum())

def log_api_usage(suburb, industry, execution_time, status="SUCCESS"):
    """记录API调用信息 | Log API usage information"""
    logger.info(f"API_USAGE: suburb={suburb}, industry={industry}, time={execution_time:.2f}s, status={status}")

def log_security_event(event_type, details):
    """记录安全相关事件 | Log security events"""
    logger.warning(f"SECURITY_EVENT: {event_type} - {details}")

def enhanced_validate_input(suburb, industry):
    """输入验证：长度检查和字符过滤 | Input validation: length check and character filtering"""
    if len(suburb) > 150:
        raise ValueError("Suburb name too long")
    if len(industry) > 150:
        raise ValueError("Industry name too long")
    
    # 允许澳洲地名常见字符：字母、数字、标点符号 | Allow common Australian place name characters
    if not re.match(r'^[a-zA-Z0-9\s\-\'\.\/&,_]+$', suburb):
        log_security_event("INVALID_SUBURB_CHARS", f"suburb: {suburb}")
        raise ValueError("Suburb contains invalid characters")
    
    # 检测可疑的恶意输入模式 | Detect suspicious malicious input patterns
    suspicious_patterns = ['<script', 'javascript:', 'eval(', 'exec(', 'onclick=', 'onload=', 'onerror=', 'data:text', 'vbscript:', 'expression(']
    input_lower = (suburb + industry).lower()
    for pattern in suspicious_patterns:
        if pattern in input_lower:
            log_security_event("SUSPICIOUS_INPUT", f"Pattern '{pattern}' found in: {suburb}, {industry}")
            raise ValueError("Suspicious input detected")

def find_suburb_match(input_suburb, cursor):
    """查找匹配的郊区：先精确匹配，再标准化匹配 | Find matching suburb: exact match first, then normalized match"""
    cursor.execute("SELECT SAL_NAME21 FROM epic3_mapping_suburb_postcode WHERE SAL_NAME21 = %s", (input_suburb,))
    result = cursor.fetchone()
    if result:
        return result['SAL_NAME21']
    
    # 获取所有郊区进行模糊匹配 | Get all suburbs for fuzzy matching
    cursor.execute("SELECT DISTINCT SAL_NAME21 FROM epic3_mapping_suburb_postcode WHERE SAL_NAME21 IS NOT NULL")
    all_suburbs = cursor.fetchall()
    
    normalized_input = normalize_string(input_suburb)
    for suburb in all_suburbs:
        suburb_name = suburb['SAL_NAME21']
        if normalize_string(suburb_name) == normalized_input:
            return suburb_name
    
    return None

# 基于数据分析的行业密度调整系数 | Industry density adjustment coefficients based on data analysis
INDUSTRY_DENSITY_COEFFICIENTS = {
    'Currently Unknown': 104.7,
    'Mining': 83.4,
    'Information Media and Telecommunications': 65.9,
    'Public Administration and Safety': 59.0,
    'Electricity, Gas, Water and Waste Services': 56.1,
    'Financial and Insurance Services': 26.0,
    'Agriculture, Forestry and Fishing': 25.1,
    'Arts and Recreation Services': 23.1,
    'Rental, Hiring and Real Estate Services': 17.9,
    'Transport, Postal and Warehousing': 15.3,
    'Wholesale Trade': 11.6,
    'Education and Training': 11.2,
    'Other Services': 8.3,
    'Administrative and Support Services': 6.3,
    'Manufacturing': 6.0,
    'Professional, Scientific and Technical Services': 5.0,
    'Retail Trade': 3.5,
    'Accommodation and Food Services': 2.9,
    'Health Care and Social Assistance': 2.9,
    'Construction': 2.6
}

def score_suburb(event, context):
    """主要API函数：计算郊区各项评分 | Main API function: calculate suburb scores"""
    start_time = time.time()
    
    # 处理CORS预检请求 | Handle CORS preflight requests
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
    suburb_name = "unknown"
    industry_name = "unknown"
    
    try:
        # 解析JSON请求体 | Parse JSON request body
        body = json.loads(event['body'])
        input_suburb = body.get('sub', '').strip()
        input_industry = body.get('industry', '').strip()
        input_population = body.get('population')
        
        # 验证必需参数 | Validate required parameters
        if not input_suburb or not input_industry:
            execution_time = time.time() - start_time
            log_api_usage("missing", "missing", execution_time, "ERROR_MISSING_PARAMS")
            return error_response(400, 'MISSING_PARAMS', 'Both sub and industry parameters are required')
        
        # 输入安全验证 | Input security validation
        try:
            enhanced_validate_input(input_suburb, input_industry)
        except ValueError as ve:
            execution_time = time.time() - start_time
            log_api_usage(input_suburb, input_industry, execution_time, "ERROR_VALIDATION")
            return error_response(400, 'INVALID_INPUT', str(ve))
        
        # 建立数据库连接 | Establish database connection
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # 查找匹配的郊区和行业 | Find matching suburb and industry
        suburb_name = find_suburb_match(input_suburb, cursor)
        industry_name = INDUSTRY_MAPPING.get(normalize_string(input_industry))
        
        if not suburb_name:
            execution_time = time.time() - start_time
            log_api_usage(input_suburb, input_industry, execution_time, "ERROR_SUBURB_NOT_FOUND")
            return error_response(404, 'SUBURB_NOT_FOUND', f'Suburb not found: {input_suburb}')
        
        if not industry_name:
            available_industries = list(INDUSTRY_MAPPING.values())
            execution_time = time.time() - start_time
            log_api_usage(suburb_name, input_industry, execution_time, "ERROR_INDUSTRY_NOT_FOUND")
            return error_response(404, 'INDUSTRY_NOT_FOUND', f'Industry not found: {input_industry}. Available industries include: {", ".join(available_industries[:5])}...')
        
        # 获取郊区代码 | Get suburb code
        cursor.execute("SELECT SAL_CODE21 FROM epic3_mapping_suburb_postcode WHERE SAL_NAME21 = %s", (suburb_name,))
        mapping = cursor.fetchone()
        
        if not mapping:
            execution_time = time.time() - start_time
            log_api_usage(suburb_name, industry_name, execution_time, "ERROR_SAL_CODE")
            return error_response(404, 'SUBURB_NOT_FOUND', f'Suburb mapping not found: {suburb_name}')
        
        sal_code = mapping['SAL_CODE21']
        
        # 查询基础数据：面积、房价、基础设施 | Query basic data: area, house price, infrastructure
        cursor.execute("SELECT AREASQKM21 FROM epic3_Victoria_suburb_2021 WHERE SAL_CODE21 = %s", (sal_code,))
        area_data = cursor.fetchone()
        area_sqkm = float(area_data['AREASQKM21']) if area_data and area_data['AREASQKM21'] else 50.0
        
        cursor.execute("SELECT `2024` as price FROM epic3_house_prices WHERE SAL_CODE21 = %s", (sal_code,))
        house_data = cursor.fetchone()
        house_price = float(house_data['price']) if house_data and house_data['price'] else 0
        
        cursor.execute("SELECT COUNT(*) as count FROM epic3_transport_stops WHERE SAL_CODE21 = %s", (sal_code,))
        transport_data = cursor.fetchone()
        transport_stops = int(transport_data['count']) if transport_data else 0
        
        cursor.execute("SELECT COUNT(*) as count FROM epic3_schools WHERE SAL_CODE21 = %s", (sal_code,))
        school_data = cursor.fetchone()
        school_count = int(school_data['count']) if school_data else 0
        
        cursor.execute("SELECT COUNT(*) as count FROM epic3_early_childhood_services WHERE SAL_CODE21 = %s", (sal_code,))
        childcare_data = cursor.fetchone()
        childcare_count = int(childcare_data['count']) if childcare_data else 0
        
        # 查询行业就业数据 | Query industry employment data
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
            
            # 按企业规模加权计算就业人数 | Calculate weighted employment by company size
            if industry_data:
                industry_employment = (
                    int(industry_data['1_4 Employees'] or 0) * 2 +
                    int(industry_data['5_19 Employees'] or 0) * 10 +
                    int(industry_data['20_199 Employees'] or 0) * 100 +
                    int(industry_data['200 plus Employees'] or 0) * 300
                )
        
        # 查询人口和犯罪数据 | Query population and crime data
        if input_population:
            population = int(input_population)
        else:
            cursor.execute("SELECT Tot_P_P as population FROM epic3_p WHERE _CODE_2021 = %s", (sal_code,))
            pop_result = cursor.fetchone()
            population = int(pop_result['population']) if pop_result and pop_result['population'] else 0
        
        cursor.execute("""
            SELECT SUM(CAST(`Incidents Recorded` AS UNSIGNED)) as total_crimes
            FROM epic3_crime 
            WHERE SAL_CODE21 = %s AND `Year` = 2025
        """, (sal_code,))
        crime_result = cursor.fetchone()
        total_crimes = int(crime_result['total_crimes']) if crime_result and crime_result['total_crimes'] else 0
        
        # 计算各项评分 | Calculate scores
        
        # 生活成本评分：基于房价的线性函数 | Cost of living score: linear function based on house price
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
            cost_score = 100 - ((house_price - 300000) / (2000000 - 300000)) * 90
            cost_calculation = f"House price ${house_price:,.0f}: 100 - (({house_price:,.0f} - 300k) / 1.7M) × 90 = {cost_score:.1f} points"
        
        # 交通便利评分：基于交通站点密度 | Transport convenience score: based on transport stop density
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
        
        # 儿童保障评分：学校和托儿所密度加权 | Child care score: weighted density of schools and childcare
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
        
        # 行业就业评分：就业密度乘以行业系数 | Industry employment score: employment density × industry coefficient
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
        
        # 安全评分：基于犯罪率的线性函数 | Safety score: linear function based on crime rate
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
                safety_score = 100 - (crime_rate / 200) * 95
                safety_calculation = f"Crime rate {crime_rate:.2f}/1000 people: 100 - ({crime_rate:.2f} / 200) × 95 = {safety_score:.1f} points"
        
        # 综合评分：五项指标平均值 | Overall score: average of five indicators
        overall_score = (cost_score + transport_score + child_score + industry_score + safety_score) / 5
        overall_calculation = f"({cost_score:.1f} + {transport_score:.1f} + {child_score:.1f} + {industry_score:.1f} + {safety_score:.1f}) ÷ 5 = {overall_score:.1f} points"
        
        # 构建返回结果 | Build response result
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
        
        # 记录成功调用 | Log successful call
        execution_time = time.time() - start_time
        log_api_usage(suburb_name, industry_name, execution_time, "SUCCESS")
        
        return success_response(result)
        
    except Exception as e:
        execution_time = time.time() - start_time
        log_api_usage(suburb_name, industry_name, execution_time, f"ERROR_{type(e).__name__}")
        logger.error(f"Suburb scoring calculation error: {str(e)}")
        return error_response(500, 'CALCULATION_ERROR', str(e))
    finally:
        if connection:
            connection.close()

def success_response(data):
    """构建成功响应 | Build success response"""
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({'success': True, 'data': data}, ensure_ascii=False)
    }

def error_response(status_code, error_code, message):
    """构建错误响应 | Build error response"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({'success': False, 'error': {'code': error_code, 'message': message}}, ensure_ascii=False)
    }