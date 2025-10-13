import json
import logging
import math
from datetime import datetime
import pymysql.cursors
import re
import os

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def normalize_for_comparison(text):
    """标准化文本用于比较：去除空格、符号，转小写"""
    if not text:
        return ""
    return re.sub(r'[^\w]', '', str(text).lower())
# Industry mapping - hardcoded for performance
INDUSTRY_MAPPING = {
    'A': 'Agriculture, forestry and fishing',
    'B': 'Mining', 
    'C': 'Manufacturing',
    'D': 'Electricity, gas, water and waste services',
    'E': 'Construction',
    'F': 'Wholesale trade',
    'G': 'Retail trade',
    'H': 'Accommodation and food services',
    'I': 'Transport, postal and warehousing',
    'J': 'Information media and telecommunications',
    'K': 'Financial and insurance services',
    'L': 'Rental, hiring and real estate services',
    'M': 'Professional, scientific and technical services',
    'N': 'Administrative and support services',
    'O': 'Public administration and safety',
    'P': 'Education and training',
    'Q': 'Health care and social assistance',
    'R': 'Arts and recreation services'
}

# Global data cache
OCCUPATION_DATA = {}
EMPLOYEES_DATA = {}
WEEKLY_EARNINGS_DATA = {}
HOURLY_EARNINGS_DATA = {}
DATA_LOADED = False

# Database configuration
# Database configuration - using environment variables
DB_CONFIG = {
    'host': os.environ['DB_HOST'],
    'port': int(os.environ.get('DB_PORT', '3306')),
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'database': os.environ['DB_NAME'],
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'connect_timeout': 10,
    'read_timeout': 30,
    'write_timeout': 30
}

def normalize_industry(user_input):
    """Convert user input to industry code for database queries"""
    user_input = user_input.strip().upper()
    
    # If it's already a single letter code
    if len(user_input) == 1 and user_input in INDUSTRY_MAPPING:
        return user_input
    
    # Normalize user input for comparison
    normalized_input = normalize_for_comparison(user_input)
    
    # Exact match with normalized comparison
    for code, full_name in INDUSTRY_MAPPING.items():
        if normalized_input == normalize_for_comparison(full_name):
            return code
    
    # If no match found, return original (will cause error in database query)
    return user_input

def get_db_connection():
    """Create MySQL database connection using PyMySQL"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise Exception(f"Failed to connect to database: {str(e)}")

def load_all_data():
    """Load all required data from database"""
    global DATA_LOADED
    
    if DATA_LOADED:
        return
    
    try:
        conn = get_db_connection()
        
        load_occupation_data(conn)
        load_employees_data(conn)
        load_weekly_earnings_data(conn)
        load_hourly_earnings_data(conn)
        
        conn.close()
        
        DATA_LOADED = True
        logger.info("All data loaded successfully")
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def load_occupation_data(connection):
    """Load occupation salary data"""
    global OCCUPATION_DATA
    
    query = """
        SELECT anzsco_code, occupation, 
               share_fulltime, avg_fulltime_hours,
               median_fulltime_earnings, median_fulltime_hourly_earnings
        FROM occup_fulltime_earnings
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        for row in cursor.fetchall():
            code = str(row['anzsco_code'])
            OCCUPATION_DATA[code] = {
                'occupation': row['occupation'],
                'full_time_hours': float(row['avg_fulltime_hours'] or 0),
                'weekly_earnings': float(row['median_fulltime_earnings']) if row['median_fulltime_earnings'] else None,
                'hourly_earnings': float(row['median_fulltime_hourly_earnings']) if row['median_fulltime_hourly_earnings'] else None
            }

def load_employees_data(connection):
    """Load employee count data"""
    global EMPLOYEES_DATA
    
    query = """
        SELECT `Survey month`, `State and territory`, `industry_code`,
               `Postgraduate Degree`, `Graduate Diploma or Certificate`, `Bachelor Degree`,
               `Advanced Diploma or Diploma`, `Certificate III or IV`, 
               `Other qualification`, `Without qualification`
        FROM `6_Education_Employees_State_Gender_Industry`
        WHERE `Parameter` = 'Employees' 
        AND `Sex` = 'Persons' 
        AND `Leave entitlements` = 'Total employees'
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        education_fields = [
            'Postgraduate Degree', 'Graduate Diploma or Certificate', 'Bachelor Degree',
            'Advanced Diploma or Diploma', 'Certificate III or IV', 
            'Other qualification', 'Without qualification'
        ]
        
        for row in cursor.fetchall():
            year = str(row['Survey month'])
            if year not in EMPLOYEES_DATA:
                EMPLOYEES_DATA[year] = {}
            
            state = row['State and territory']
            industry_code = row['industry_code']
            
            for education in education_fields:
                count = row[education]
                if count and count > 0:
                    key = (state, industry_code, education)
                    EMPLOYEES_DATA[year][key] = float(count)

def load_weekly_earnings_data(connection):
    """Load weekly earnings data"""
    global WEEKLY_EARNINGS_DATA
    
    query = """
        SELECT `Survey month`, `State and territory`, `industry_code`,
               `Postgraduate Degree`, `Postgraduate Degree_RSE`,
               `Graduate Diploma or Certificate`, `Graduate Diploma or Certificate_RSE`,
               `Bachelor Degree`, `Bachelor Degree_RSE`,
               `Advanced Diploma or Diploma`, `Advanced Diploma or Diploma_RSE`,
               `Certificate III or IV`, `Certificate III or IV_RSE`,
               `Other qualification`, `Other qualification_RSE`,
               `Without qualification`, `Without qualification_RSE`
        FROM `6_Education_Weekly_State_Gender_Industry`
        WHERE `Parameter` = 'Median weekly earnings'
        AND `Sex` = 'Persons'
        AND `Leave entitlements` = 'Total employees'
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        education_fields = [
            'Postgraduate Degree', 'Graduate Diploma or Certificate', 'Bachelor Degree',
            'Advanced Diploma or Diploma', 'Certificate III or IV', 
            'Other qualification', 'Without qualification'
        ]
        
        for row in cursor.fetchall():
            year = str(row['Survey month'])
            if year not in WEEKLY_EARNINGS_DATA:
                WEEKLY_EARNINGS_DATA[year] = {}
            
            state = row['State and territory']
            industry_code = row['industry_code']
            
            for education in education_fields:
                value = row[education]
                rse = row[f'{education}_RSE']
                
                if value and value > 0:
                    key = (state, industry_code, education)
                    WEEKLY_EARNINGS_DATA[year][key] = {
                        'value': float(value),
                        'rse': float(rse) if rse else 50.0
                    }

def load_hourly_earnings_data(connection):
    """Load hourly earnings data"""
    global HOURLY_EARNINGS_DATA
    
    query = """
        SELECT `Survey month`, `State and territory`, `industry_code`,
               `Postgraduate Degree`, `Postgraduate Degree_RSE`,
               `Graduate Diploma or Certificate`, `Graduate Diploma or Certificate_RSE`,
               `Bachelor Degree`, `Bachelor Degree_RSE`,
               `Advanced Diploma or Diploma`, `Advanced Diploma or Diploma_RSE`,
               `Certificate III or IV`, `Certificate III or IV_RSE`,
               `Other qualification`, `Other qualification_RSE`,
               `Without qualification`, `Without qualification_RSE`
        FROM `6_Education_Hourly_State_Gender_Industry`
        WHERE `Parameter` = 'Median hourly earnings'
        AND `Sex` = 'Persons'
        AND `Leave entitlements` = 'Total employees'
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        education_fields = [
            'Postgraduate Degree', 'Graduate Diploma or Certificate', 'Bachelor Degree',
            'Advanced Diploma or Diploma', 'Certificate III or IV', 
            'Other qualification', 'Without qualification'
        ]
        
        for row in cursor.fetchall():
            year = str(row['Survey month'])
            if year not in HOURLY_EARNINGS_DATA:
                HOURLY_EARNINGS_DATA[year] = {}
            
            state = row['State and territory']
            industry_code = row['industry_code']
            
            for education in education_fields:
                value = row[education]
                rse = row[f'{education}_RSE']
                
                if value and value > 0:
                    key = (state, industry_code, education)
                    HOURLY_EARNINGS_DATA[year][key] = {
                        'value': float(value),
                        'rse': float(rse) if rse else 50.0
                    }

def get_anchor_education(industry_code):
    """Find education level with most employees in latest year for given industry code"""
    latest_year = max(EMPLOYEES_DATA.keys())
    year_data = EMPLOYEES_DATA[latest_year]
    
    education_counts = {}
    for (state, ind_code, education), count in year_data.items():
        if state == "Australia" and ind_code == industry_code:
            education_counts[education] = count
    
    if not education_counts:
        raise ValueError(f"No employee data found for industry code '{industry_code}'")
    
    anchor_education = max(education_counts.items(), key=lambda x: x[1])[0]
    logger.info(f"Anchor education for industry '{industry_code}': {anchor_education}")
    return anchor_education

def get_occupation_base_salary(occupation, earnings_type):
    """Get base salary for occupation based on earnings type"""
    # Normalize user input for comparison
    normalized_input = normalize_for_comparison(occupation)
    
    for code, data in OCCUPATION_DATA.items():
        if normalized_input == normalize_for_comparison(data['occupation']):
            if earnings_type == 'hourly':
                if data['hourly_earnings']:
                    return data['hourly_earnings']
                else:
                    raise ValueError(f"No hourly earnings data for '{occupation}'")
            else:  # weekly
                if data['weekly_earnings']:
                    return data['weekly_earnings']
                else:
                    raise ValueError(f"No weekly earnings data for '{occupation}'")
    
    available = [data['occupation'] for data in OCCUPATION_DATA.values()][:10]
    raise ValueError(f"Occupation '{occupation}' not found. Available: {available}")

def calculate_10_year_factors(industry_code, user_state, user_education, earnings_type):
    """Calculate salary factors for 10 years using industry codes"""
    
    # Choose data source based on earnings_type
    earnings_data = HOURLY_EARNINGS_DATA if earnings_type == 'hourly' else WEEKLY_EARNINGS_DATA
    
    # Get anchor education from latest year using industry code
    anchor_education = get_anchor_education(industry_code)
    
    # Get latest year for baseline
    latest_year = max(earnings_data.keys())
    
    # Get baseline salary (latest year Australia anchor education)
    baseline_key = ("Australia", industry_code, anchor_education)
    baseline_data = earnings_data[latest_year].get(baseline_key)
    
    if not baseline_data:
        raise ValueError(f"No baseline data for {latest_year} Australia industry code '{industry_code}' {anchor_education}")
    
    baseline_salary = baseline_data['value']
    
    # Calculate factors for all years
    yearly_factors = []
    
    for year in sorted(earnings_data.keys()):
        # 1. 首先尝试用户指定的州
        user_key = (user_state, industry_code, user_education)
        user_data = earnings_data[year].get(user_key)
        
        # 2. 如果没有数据，降级到全澳洲数据
        if not user_data or user_data['value'] <= 0:
            fallback_key = ("Australia", industry_code, user_education)
            user_data = earnings_data[year].get(fallback_key)
            used_location = "Australia (fallback)"
        else:
            used_location = user_state
        
        if user_data and user_data['value'] > 0:
            factor = user_data['value'] / baseline_salary
            yearly_factors.append({
                'year': year,
                'factor': factor,
                'user_salary': user_data['value'],
                'baseline_salary': baseline_salary,
                'rse': user_data['rse'],
                'anchor_education': anchor_education,
                'used_location': used_location,  # 记录实际使用的地区
                'source': f"{year} {used_location} {user_education} vs {latest_year} Australia {anchor_education} ({earnings_type}) [Industry: {industry_code}]"
            })
    
    return yearly_factors

def get_experience_factor(industry_code, years):
    """
    根据ANZSIC行业分类和工作年限计算经验因子
    
    参数:
        industry_code: str - ANZSIC行业代码 (A-R)
        years: float - 工作经验年限
    
    返回:
        float - 经验加权因子 (0.75-1.5范围)
    
    设计基于:
    - 标准Mincer方程的经验项系数通常为3-7%/年
    - 不同行业的技能折旧率、学习曲线差异
    - 国际劳动经济学文献的典型参数范围
    """
    
    # 行业参数配置
    # base: 新手基础因子 (0年经验)
    # peak_years: 达到峰值的年限
    # growth_rate: 早期增长速率
    # max_factor: 峰值后保持的最大因子（避免年龄歧视）
    
    industry_profiles = {
        # A: 农业、林业和渔业
        # 特征: 经验积累慢但稳定，技能持久有效
        'A': {
            'base': 0.82,
            'peak_years': 20,
            'growth_rate': 0.028,
            'max_factor': 1.35
        },
        
        # B: 采矿业
        # 特征: 高技能要求，早期快速增长，安全经验关键
        'B': {
            'base': 0.85,
            'peak_years': 15,
            'growth_rate': 0.045,
            'max_factor': 1.48
        },
        
        # C: 制造业
        # 特征: 中等学习曲线，工艺经验持续有价值
        'C': {
            'base': 0.83,
            'peak_years': 18,
            'growth_rate': 0.032,
            'max_factor': 1.38
        },
        
        # D: 电力、燃气、水和废物处理服务
        # 特征: 技术要求高，经验价值持久
        'D': {
            'base': 0.86,
            'peak_years': 16,
            'growth_rate': 0.038,
            'max_factor': 1.42
        },
        
        # E: 建筑业
        # 特征: 实操经验关键，管理经验长期有效
        'E': {
            'base': 0.80,
            'peak_years': 14,
            'growth_rate': 0.042,
            'max_factor': 1.40
        },
        
        # F: 批发贸易
        # 特征: 人际技能+产品知识，客户网络持久
        'F': {
            'base': 0.84,
            'peak_years': 16,
            'growth_rate': 0.034,
            'max_factor': 1.36
        },
        
        # G: 零售贸易
        # 特征: 入门快但管理经验有价值
        'G': {
            'base': 0.88,
            'peak_years': 10,
            'growth_rate': 0.025,
            'max_factor': 1.22
        },
        
        # H: 住宿和餐饮服务
        # 特征: 快速上手，服务经验持续有用
        'H': {
            'base': 0.89,
            'peak_years': 9,
            'growth_rate': 0.022,
            'max_factor': 1.18
        },
        
        # I: 运输、邮政和仓储
        # 特征: 操作技能为主，安全记录重要
        'I': {
            'base': 0.83,
            'peak_years': 15,
            'growth_rate': 0.030,
            'max_factor': 1.32
        },
        
        # J: 信息媒体和电信
        # 特征: 虽然技术迭代快，但架构思维和问题解决能力持久有效
        'J': {
            'base': 0.90,
            'peak_years': 8,
            'growth_rate': 0.055,
            'max_factor': 1.38
        },
        
        # K: 金融和保险服务
        # 特征: 知识密集，经验价值高
        'K': {
            'base': 0.87,
            'peak_years': 14,
            'growth_rate': 0.040,
            'max_factor': 1.42
        },
        
        # L: 租赁、招聘和房地产服务
        # 特征: 市场知识+关系网络长期有效
        'L': {
            'base': 0.85,
            'peak_years': 13,
            'growth_rate': 0.036,
            'max_factor': 1.35
        },
        
        # M: 专业、科学和技术服务
        # 特征: 高技能行业，专业判断力持续增值
        'M': {
            'base': 0.82,
            'peak_years': 18,
            'growth_rate': 0.048,
            'max_factor': 1.52
        },
        
        # N: 行政和支持服务
        # 特征: 组织经验和流程知识有价值
        'N': {
            'base': 0.86,
            'peak_years': 12,
            'growth_rate': 0.028,
            'max_factor': 1.26
        },
        
        # O: 公共管理和安全
        # 特征: 制度知识重要，经验价值稳定持久
        'O': {
            'base': 0.84,
            'peak_years': 20,
            'growth_rate': 0.032,
            'max_factor': 1.40
        },
        
        # P: 教育和培训
        # 特征: 教学经验持续积累，永不过时
        'P': {
            'base': 0.85,
            'peak_years': 22,
            'growth_rate': 0.030,
            'max_factor': 1.42
        },
        
        # Q: 医疗保健和社会援助
        # 特征: 临床经验关键，判断力终身有效
        'Q': {
            'base': 0.84,
            'peak_years': 20,
            'growth_rate': 0.038,
            'max_factor': 1.48
        },
        
        # R: 艺术和娱乐服务
        # 特征: 创意+技能结合，成熟度提升价值
        'R': {
            'base': 0.87,
            'peak_years': 12,
            'growth_rate': 0.033,
            'max_factor': 1.30
        }
    }
    
    # 默认配置（如果行业代码无效）
    default_profile = {
        'base': 0.85,
        'peak_years': 15,
        'growth_rate': 0.035,
        'max_factor': 1.35
    }
    
    # 获取行业配置
    profile = industry_profiles.get(industry_code, default_profile)
    
    # 处理边界情况
    if years <= 0:
        return profile['base']
    
    # 计算经验因子
    if years <= profile['peak_years']:
        # 峰值前：对数增长模型（符合人力资本理论）
        # 使用修正的对数函数：避免log(0)问题
        factor = profile['base'] + (math.log(years + 1) * profile['growth_rate'] * 3.5)
        # 确保不超过最大值
        factor = min(factor, profile['max_factor'])
    else:
        # 峰值后：保持平台期（避免年龄歧视）
        # 经验是资产而非负担，达到峰值后维持最高水平
        factor = profile['max_factor']
    
    return round(factor, 3)

def calculate_intensity_factor(work_intensity):
    """Calculate work intensity factor"""
    return 0.005 * work_intensity + 0.7

def get_verdict(fairness_ratio):
    """Get verdict based on fairness ratio"""
    if fairness_ratio >= 1.2:
        return "Above Average"
    elif fairness_ratio >= 0.8:
        return "Average"
    else:
        return "Below Average"

def validate_input(data):
    """Validate input data"""
    required_fields = ['occupation', 'industry', 'education', 'location', 'currentHourlyRate', 'yearsExperience', 'workIntensity']
    
    for field in required_fields:
        if field not in data:
            return {'valid': False, 'message': f'Missing required field: {field}'}
    
    # Set default earningsType if not provided
    if 'earningsType' not in data:
        data['earningsType'] = 'hourly'
    
    if not isinstance(data['currentHourlyRate'], (int, float)) or data['currentHourlyRate'] <= 0:
        return {'valid': False, 'message': 'currentHourlyRate must be a positive number'}
    
    if not isinstance(data['yearsExperience'], int) or data['yearsExperience'] < 0 or data['yearsExperience'] > 50:
        return {'valid': False, 'message': 'yearsExperience must be an integer between 0 and 50'}
    
    if not isinstance(data['workIntensity'], (int, float)) or data['workIntensity'] < 0 or data['workIntensity'] > 100:
        return {'valid': False, 'message': 'workIntensity must be a number between 0 and 100'}
    
    valid_education_levels = [
        'Postgraduate Degree', 'Bachelor Degree', 'Advanced Diploma or Diploma',
        'Certificate III or IV', 'Other qualification', 'Without qualification'
    ]
    if data['education'] not in valid_education_levels:
        return {'valid': False, 'message': f'Invalid education level. Must be one of: {valid_education_levels}'}
    
    if data['earningsType'] not in ['hourly', 'weekly']:
        return {'valid': False, 'message': 'earningsType must be either "hourly" or "weekly"'}
    
    return {'valid': True, 'message': 'Valid input'}

def calculate_fairness_score(input_data):
    """Main calculation function"""
    occupation = input_data['occupation']
    industry_input = input_data['industry']
    education = input_data['education']
    location = input_data['location']
    hourly_rate = float(input_data['currentHourlyRate'])
    years_exp = input_data['yearsExperience']
    work_intensity = input_data['workIntensity']
    earnings_type = input_data['earningsType']
    
    # Normalize industry input to industry code
    industry_code = normalize_industry(industry_input)
    
    # Use state code directly
    user_state = location
    
    # Get base salary from occupation data (matching earnings type)
    base_salary = get_occupation_base_salary(occupation, earnings_type)
    
    # Get experience and intensity factors
    experience_factor = get_experience_factor(industry_input, years_exp)
    intensity_factor = calculate_intensity_factor(work_intensity)
    
    # Calculate 10-year factors with chosen earnings type using industry code
    yearly_factors = calculate_10_year_factors(industry_code, user_state, education, earnings_type)
    
    if not yearly_factors:
        raise Exception("No historical data available for calculation")
    
    # Build historical data with complete salary calculations
    historical_data = []
    for factor_data in yearly_factors:
        complete_salary = base_salary * factor_data['factor'] * experience_factor * intensity_factor
        
        historical_data.append({
            'year': factor_data['year'],
            'salary': round(complete_salary, 2),
            'rse': factor_data['rse'],
            'source': factor_data['source'],
            'anchorEducation': factor_data['anchor_education'],
            'factors': {
                'base': round(base_salary, 2),
                'regional': round(factor_data['factor'], 3),
                'experience': round(experience_factor, 3),
                'intensity': round(intensity_factor, 3)
            }
        })
    
    # Use latest year for current comparison
    current_data = historical_data[-1]
    expected_hourly_rate = current_data['salary']
    
    # Calculate fairness metrics
    fairness_ratio = hourly_rate / expected_hourly_rate
    fairness_score = min(100, max(0, fairness_ratio * 75))
    
    # Calculate trend
    salaries = [item['salary'] for item in historical_data]
    if len(salaries) > 1:
        total_growth = ((salaries[-1] - salaries[0]) / salaries[0] * 100)
        trend_direction = 'increasing' if total_growth > 2 else 'decreasing' if total_growth < -2 else 'stable'
    else:
        total_growth = 0
        trend_direction = 'stable'
    
    # Build response
    return {
        "fairnessScore": round(fairness_score, 1),
        "verdict": get_verdict(fairness_ratio),
        "comparison": {
            "yourRate": hourly_rate,
            "expectedRate": round(expected_hourly_rate, 2),
            "difference": round(hourly_rate - expected_hourly_rate, 2)
        },
        "calculation": current_data['factors'],
        "dataSource": current_data['source'],
        "anchorEducation": current_data['anchorEducation'],
        "industryCode": industry_code,
        "industryName": INDUSTRY_MAPPING.get(industry_code, industry_input),
        "earningsType": earnings_type,
        "generatedAt": datetime.now().strftime('%Y-%m-%d %H:%M'),
        "historicalTrend": {
            'yearlyData': historical_data,
            'totalGrowth': f"{total_growth:.1f}%",
            'trendDirection': trend_direction,
            'yearsWithData': len(historical_data)
        }
    }

def lambda_handler(event, context):
    """Main Lambda handler"""
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type, Accept, Authorization',
                'Access-Control-Allow-Methods': 'POST, OPTIONS',
                'Access-Control-Max-Age': '86400'
            },
            'body': ''
        }
    
    try:
        load_all_data()
        
        if 'body' not in event:
            return error_response(400, 'MISSING_BODY', 'Request body is required')
        
        body = json.loads(event['body'])
        logger.info(f"Received request: {body}")
        
        validation_result = validate_input(body)
        if not validation_result['valid']:
            return error_response(400, 'INVALID_INPUT', validation_result['message'])
        
        fairness_data = calculate_fairness_score(body)
        return success_response(fairness_data)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return error_response(500, 'INTERNAL_ERROR', f'Internal server error: {str(e)}')

def success_response(data):
    """Return success response"""
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type, Accept, Authorization',
            'Access-Control-Allow-Methods': 'POST, OPTIONS'
        },
        'body': json.dumps({
            'success': True,
            'statusCode': 200,
            'data': data,
            'message': 'Fairness score calculated successfully'
        })
    }

def error_response(status_code, error_code, message):
    """Return error response"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type, Accept, Authorization',
            'Access-Control-Allow-Methods': 'POST, OPTIONS'
        },
        'body': json.dumps({
            'success': False,
            'statusCode': status_code,
            'error': {
                'code': error_code,
                'message': message
            }
        })
    }