import json
import csv
import os
import logging
import pymysql.cursors
from collections import defaultdict

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

# CSV file paths (只保留gender1.csv)
GENDER1_CSV_PATH = os.path.join(os.path.dirname(__file__), 'data', 'gender1.csv')

# Database configuration
DB_CONFIG = {
    'host': 'fairwageaustralia.ct08osmucf2b.ap-southeast-2.rds.amazonaws.com',
    'user': 'admin',
    'password': 'fairwageaustralia',
    'port': 3306,
    'database': 'fairwageaustralia',  # 请根据实际数据库名称调整
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def get_db_connection():
    """获取数据库连接"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise Exception(f"Failed to connect to database: {str(e)}")

def parse_earnings_value(value):
    """Parse earnings value, handling strings with commas"""
    if not value or value == '':
        return 0
    try:
        # Remove commas and convert to float
        if isinstance(value, str):
            value = value.replace(',', '')
        return float(value)
    except (ValueError, TypeError):
        return 0

def load_industry_data():
    """Load industry data from gender1.csv"""
    global INDUSTRY_DATA, DATA_LOADED
    
    if DATA_LOADED:
        return
    
    try:
        # Load industry data (gender1.csv)
        with open(GENDER1_CSV_PATH, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                industry_name = row['Industry'].strip()
                INDUSTRY_DATA[industry_name] = {
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
        
        DATA_LOADED = True
        logger.info(f"Loaded {len(INDUSTRY_DATA)} industries from gender1.csv")
        
    except Exception as e:
        logger.error(f"Error loading industry data: {str(e)}")
        raise Exception(f"Failed to load industry data: {str(e)}")

def get_historical_earnings_data(state, industry_code):
    """从数据库获取指定州和行业代码的历史薪资数据"""
    connection = None
    try:
        connection = get_db_connection()
        
        with connection.cursor() as cursor:
            # 使用正确的表名和字段名
            sql = """
            SELECT 
                `Survey month` as survey_year,
                `State and territory` as state_territory,
                `Industry` as industry_name,
                `Industry_Code` as industry_code,
                `Males Weekly Earnings` as male_weekly,
                `Males Weekly Earnings_RSE` as male_weekly_rse,
                `Females Weekly Earnings` as female_weekly,
                `Females Weekly Earnings_RSE` as female_weekly_rse
            FROM 3_Industry_FullPart_Gender_State_Employee_Weekly_Hourly 
            WHERE `State and territory` = %s AND `Industry_Code` = %s 
            ORDER BY `Survey month` ASC
            """
            
            cursor.execute(sql, (state, industry_code))
            results = cursor.fetchall()
            
            if not results:
                return None, f"No data found for state '{state}' and industry_code '{industry_code}'"
            
            # 处理数据并计算薪资差距
            processed_data = []
            for row in results:
                year_data = {
                    'year': str(row['survey_year']),
                    'state': row['state_territory'],
                    'industry': row['industry_name'],
                    'industry_code': row['industry_code'],
                    'male_weekly_earnings': parse_earnings_value(row['male_weekly']),
                    'male_weekly_earnings_rse': parse_earnings_value(row['male_weekly_rse']),
                    'female_weekly_earnings': parse_earnings_value(row['female_weekly']),
                    'female_weekly_earnings_rse': parse_earnings_value(row['female_weekly_rse'])
                }
                
                # 计算周薪性别差距百分比
                if year_data['male_weekly_earnings'] > 0:
                    weekly_gap = ((year_data['male_weekly_earnings'] - year_data['female_weekly_earnings']) / year_data['male_weekly_earnings']) * 100
                    year_data['weekly_pay_gap_percentage'] = round(weekly_gap, 2)
                else:
                    year_data['weekly_pay_gap_percentage'] = 0
                
                processed_data.append(year_data)
            
            return processed_data, None
            
    except Exception as e:
        logger.error(f"Database query error: {str(e)}")
        return None, f"Database error: {str(e)}"
    
    finally:
        if connection:
            connection.close()

def calculate_trend_change(historical_data, field):
    """Calculate percentage change from first to last year for a given field"""
    if len(historical_data) < 2:
        return None
    
    first_value = historical_data[0].get(field, 0)
    last_value = historical_data[-1].get(field, 0)
    
    if first_value == 0:
        return None
    
    percentage_change = ((last_value - first_value) / first_value) * 100
    return {
        'first_year': historical_data[0]['year'],
        'first_value': first_value,
        'last_year': historical_data[-1]['year'],
        'last_value': last_value,
        'percentage_change': round(percentage_change, 2)
    }

def calculate_gender_gap(event, context):
    """主要的API处理函数 - 现在支持按州和行业代码查询"""
    # Handle CORS
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
        # 加载本地行业数据
        load_industry_data()
        
        body = json.loads(event['body'])
        state = body.get('state')
        industry = body.get('industry')
        
        # 验证必需参数
        if not state:
            return error_response(400, 'MISSING_STATE', 'State parameter is required')
        
        if not industry:
            return error_response(400, 'MISSING_INDUSTRY_CODE', 'Industry_Code parameter is required')
        
        # 验证行业代码是否有效
        if industry not in INDUSTRY_MAPPING:
            return error_response(400, 'INVALID_INDUSTRY_CODE', {
                'message': f'Invalid industry code: {industry}',
                'available_codes': list(INDUSTRY_MAPPING.keys()),
                'code_mapping': INDUSTRY_MAPPING
            })
        
        # 从数据库获取历史薪资数据
        historical_data, db_error = get_historical_earnings_data(state, industry)
        
        if db_error:
            # 如果查询失败，提供可用选项信息
            available_states = ["Australia", "NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
            available_industries = [
                {'code': code, 'name': name} 
                for code, name in INDUSTRY_MAPPING.items()
            ]
            error_details = {
                'message': db_error,
                'available_states': available_states,
                'available_industries': available_industries
            }
            return error_response(404, 'DATA_NOT_FOUND', error_details)
        
        # 构建返回结果
        result = {
            'state': state,
            'industry_code': industry,
            'industry_name': INDUSTRY_MAPPING[industry],
            'historical_earnings': {
                'data_years': [d['year'] for d in historical_data],
                'yearly_data': historical_data,
                'latest_year_data': historical_data[-1] if historical_data else None,
                'earnings_trend': {
                    'male_weekly_change': calculate_trend_change(historical_data, 'male_weekly_earnings'),
                    'female_weekly_change': calculate_trend_change(historical_data, 'female_weekly_earnings'),
                    'male_weekly_rse_change': calculate_trend_change(historical_data, 'male_weekly_earnings_rse'),
                    'female_weekly_rse_change': calculate_trend_change(historical_data, 'female_weekly_earnings_rse'),
                    'pay_gap_trend': calculate_trend_change(historical_data, 'weekly_pay_gap_percentage')
                }
            }
        }
        
        # 如果本地行业数据中有匹配的行业，添加行业统计信息
        industry_name = INDUSTRY_MAPPING[industry]
        if industry_name in INDUSTRY_DATA:
            industry_data = INDUSTRY_DATA[industry_name]
            result['industry_statistics'] = {
                'average_midpoint': industry_data['average_midpoint'],
                'median_midpoint': industry_data['median_midpoint'],
                'women_representation': {
                    'total_women_percentage': industry_data['total_women_percentage'],
                    'upper_quartile_women_percentage': industry_data['women_by_quartile']['upper_quartile'],
                    'upper_middle_quartile_women_percentage': industry_data['women_by_quartile']['upper_middle_quartile'],
                    'lower_middle_quartile_women_percentage': industry_data['women_by_quartile']['lower_middle_quartile'],
                    'lower_quartile_women_percentage': industry_data['women_by_quartile']['lower_quartile']
                }
            }
        
        return success_response(result)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return error_response(500, 'INTERNAL_ERROR', str(e))

def get_available_options(event, context):
    """获取可用的州和行业代码列表"""
    # Handle CORS
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'GET, OPTIONS'
            },
            'body': ''
        }
    
    try:
        # 可用的州列表（基于数据观察）
        available_states = ["Australia", "NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
        
        # 使用本地映射而不需要查询数据库
        available_industries = [
            {'code': code, 'name': name} 
            for code, name in INDUSTRY_MAPPING.items()
        ]
        
        result = {
            'available_states': available_states,
            'available_industries': available_industries,
            'total_states': len(available_states),
            'total_industries': len(available_industries)
        }
        
        return success_response(result)
        
    except Exception as e:
        logger.error(f"Error getting available options: {str(e)}")
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