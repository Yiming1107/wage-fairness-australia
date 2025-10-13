import json
import csv
import os
import logging
import pymysql.cursors
import re

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global data cache
INDUSTRY_DATA = {}
DATA_LOADED = False

# Industry code mapping
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

# Database configuration
# Database configuration - using environment variables
DB_CONFIG = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'port': int(os.environ.get('DB_PORT', '3306')),
    'database': os.environ['DB_NAME'],
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'connect_timeout': 10,
    'read_timeout': 30,
    'write_timeout': 30
}

def normalize_text(text):
    """去除标点、空格、转小写"""
    return re.sub(r'[^a-zA-Z0-9]', '', text.lower())

def find_industry_code_by_name(industry_name):
    """通过行业名称匹配找到对应的行业代码（去除符号、大小写、空格）"""
    normalized_input = normalize_text(industry_name)
    
    for code, name in INDUSTRY_MAPPING.items():
        normalized_name = normalize_text(name)
        if normalized_input == normalized_name:
            return code
    return None

def load_industry_data():
    """Load industry data from gender1.csv"""
    global INDUSTRY_DATA, DATA_LOADED
    if DATA_LOADED:
        return
    
    try:
        # 尝试多个可能的路径
        possible_paths = [
            'gender1.csv',
            'data/gender1.csv',
            os.path.join(os.path.dirname(__file__), 'gender1.csv'),
            os.path.join(os.path.dirname(__file__), 'data', 'gender1.csv')
        ]
        
        csv_path = None
        for path in possible_paths:
            if os.path.exists(path):
                csv_path = path
                break
        
        if not csv_path:
            logger.error("CSV file not found in any of the expected paths")
            return
            
        logger.info(f"Loading CSV from: {csv_path}")
        
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                industry_name = row['Industry'].strip()
                # 用标准化的名称作为键，这样可以无视逗号空格大小写
                normalized_key = normalize_text(industry_name)
                
                INDUSTRY_DATA[normalized_key] = {
                    'original_name': industry_name,
                    'average_midpoint': float(row['Average GPG Mid-point (%)']) if row['Average GPG Mid-point (%)'] else 0,
                    'median_midpoint': float(row['Median GPG Mid-point (%)']) if row['Median GPG Mid-point (%)'] else 0,
                    'total_women_percentage': float(row['Total Women (%)']) if row['Total Women (%)'] else 0,
                    'women_by_quartile': {
                        'upper_quartile': float(row['Upper Quartile Women (%)']) if row['Upper Quartile Women (%)'] else 0,
                        'upper_middle_quartile': float(row['Upper Middle Quartile Women (%)']) if row['Upper Middle Quartile Women (%)'] else 0,
                        'lower_middle_quartile': float(row['Lower Middle Quartile Women (%)']) if row['Lower Middle Quartile Women (%)'] else 0,
                        'lower_quartile': float(row['Lower Quartile Women (%)']) if row['Lower Quartile Women (%)'] else 0
                    }
                }
                logger.info(f"Loaded: '{industry_name}' -> normalized key: '{normalized_key}'")
        
        logger.info(f"Successfully loaded {len(INDUSTRY_DATA)} industries from CSV")
        DATA_LOADED = True
        
    except Exception as e:
        logger.error(f"Error loading industry data: {str(e)}")

def get_historical_earnings_data(state, industry_code):
    """从数据库获取历史薪资数据"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            sql = """
            SELECT 
                `Survey month` as year,
                `State and territory` as state,
                `Industry` as industry,
                AVG(NULLIF(`Males Weekly Earnings`, 0)) as male_weekly,
                AVG(NULLIF(`Males Weekly Earnings_RSE`, 0)) as male_rse,
                AVG(NULLIF(`Females Weekly Earnings`, 0)) as female_weekly,
                AVG(NULLIF(`Females Weekly Earnings_RSE`, 0)) as female_rse
            FROM 3_Industry_FullPart_Gender_State_Employee_Weekly_Hourly 
            WHERE `State and territory` = %s AND `Industry_Code` = %s AND `Category` = 'Full-time'
            GROUP BY `Survey month`, `State and territory`, `Industry`
            ORDER BY `Survey month` ASC
            """
            
            cursor.execute(sql, (state, industry_code))
            results = cursor.fetchall()
            
            processed_data = []
            for row in results:
                male_weekly = float(row['male_weekly'] or 0)
                female_weekly = float(row['female_weekly'] or 0)
                
                if male_weekly == 0 and female_weekly == 0:
                    continue
                
                gap = ((male_weekly - female_weekly) / male_weekly * 100) if male_weekly > 0 else 0
                
                processed_data.append({
                    'year': str(row['year']),
                    'state': row['state'],
                    'industry': row['industry'],
                    'male_weekly_earnings': male_weekly,
                    'male_weekly_earnings_rse': float(row['male_rse'] or 0),
                    'female_weekly_earnings': female_weekly,
                    'female_weekly_earnings_rse': float(row['female_rse'] or 0),
                    'weekly_pay_gap_percentage': round(gap, 2)
                })
            
            return processed_data
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        return None
    finally:
        if 'connection' in locals():
            connection.close()

def calculate_gender_gap(event, context):
    """主要的API处理函数"""
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
        # 加载CSV数据
        load_industry_data()
        
        body = json.loads(event['body'])
        state = body.get('state')
        industry_name = body.get('industry')
        
        if not state or not industry_name:
            return error_response(400, 'MISSING_PARAMS', 'State and industry parameters are required')
        
        # 1. 根据用户输入找到行业代码
        industry_code = find_industry_code_by_name(industry_name)
        if not industry_code:
            return error_response(400, 'INVALID_INDUSTRY', f'No matching industry found for: {industry_name}')
        
        # 2. 用行业代码从数据库获取历史数据
        historical_data = get_historical_earnings_data(state, industry_code)
        if not historical_data:
            return error_response(404, 'NO_DATA', f'No data found for {state} - {industry_name}')
        
        # 3. 构建基础结果
        result = {
            'state': state,
            'industry_name': INDUSTRY_MAPPING[industry_code],
            'matched_from_input': industry_name,
            'historical_earnings': {
                'yearly_data': historical_data,
                'latest_year_data': historical_data[-1] if historical_data else None
            },
            'industry_statistics': None
        }
        
        # 4. 从CSV获取行业统计数据
        # 使用INDUSTRY_MAPPING中的名称进行标准化匹配
        mapping_industry_name = INDUSTRY_MAPPING[industry_code]
        normalized_mapping_name = normalize_text(mapping_industry_name)
        
        logger.info(f"Looking for CSV data with normalized key: '{normalized_mapping_name}'")
        logger.info(f"Available CSV keys: {list(INDUSTRY_DATA.keys())}")
        
        if normalized_mapping_name in INDUSTRY_DATA:
            result['industry_statistics'] = INDUSTRY_DATA[normalized_mapping_name]
            logger.info(f"Found CSV data for: {mapping_industry_name}")
        else:
            logger.warning(f"No CSV data found for: {mapping_industry_name} (normalized: {normalized_mapping_name})")
        
        return success_response(result)
        
    except Exception as e:
        logger.error(f"Error in calculate_gender_gap: {str(e)}")
        return error_response(500, 'INTERNAL_ERROR', str(e))

def success_response(data):
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'success': True,
            'data': data
        })
    }

def error_response(status_code, error_code, message):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'success': False,
            'error': {
                'code': error_code,
                'message': message
            }
        })
    }