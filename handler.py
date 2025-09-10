import json
import logging
import csv
import os
from decimal import Decimal
import math
from datetime import datetime
import uuid

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global data cache
OCCUPATION_DATA = {}         # Occupation data dictionary {ANZSCO code: occupation info}
REGIONAL_INDUSTRY_DATA = {}  # Regional industry education data dictionary {year: {(state, industry, education): {value, rse}}}
DATA_LOADED = False          # Data loaded flag

# CSV file paths in Lambda package
OCCUPATION_CSV_PATH = os.path.join(os.path.dirname(__file__), 'data', 'occupation_salary_data.csv')
REGIONAL_CSV_PATH = os.path.join(os.path.dirname(__file__), 'data', 'regional_industry_education_data.csv')

def load_csv_data():
    """
    Load data from CSV files in Lambda package into memory with caching
    """
    global OCCUPATION_DATA, REGIONAL_INDUSTRY_DATA, DATA_LOADED
    
    if DATA_LOADED:
        return
    
    try:
        if not os.path.exists(OCCUPATION_CSV_PATH):
            raise FileNotFoundError(f"Occupation data file not found: {OCCUPATION_CSV_PATH}")
        if not os.path.exists(REGIONAL_CSV_PATH):
            raise FileNotFoundError(f"Regional data file not found: {REGIONAL_CSV_PATH}")
            
        logger.info(f"Loading occupation data from {OCCUPATION_CSV_PATH}")
        load_occupation_data()
        
        logger.info(f"Loading regional data from {REGIONAL_CSV_PATH}")
        load_regional_data()
        
        DATA_LOADED = True
        
        years_loaded = sorted(REGIONAL_INDUSTRY_DATA.keys())
        logger.info(f"Successfully loaded {len(OCCUPATION_DATA)} occupations and {len(years_loaded)} years of regional data: {years_loaded}")
        
    except Exception as e:
        logger.error(f"Error loading CSV data: {str(e)}")
        raise Exception(f"Failed to load required data files: {str(e)}")

def load_occupation_data():
    """
    Load occupation data using built-in csv module
    """
    global OCCUPATION_DATA
    
    with open(OCCUPATION_CSV_PATH, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            anzsco_code = str(row['ANZSCO Code'])
            OCCUPATION_DATA[anzsco_code] = {
                "occupation": row['Occupation'],
                "full_time_percentage": float(row['Share of workers who work full-time hours (%)']) if row['Share of workers who work full-time hours (%)'] else 0,
                "avg_hours_per_week": float(row['Average full-time hours worked per week']) if row['Average full-time hours worked per week'] else 0,
                "median_weekly_earnings": parse_currency(row['Median full-time earnings per week']),
                "median_hourly_earnings": parse_currency(row['Median full-time hourly earnings'])
            }

def load_regional_data():
    """
    Load regional industry data, keeping 6 years of data from 2018-2023
    """
    global REGIONAL_INDUSTRY_DATA
    
    # Keep 6 years of data: 2018-2023
    target_years = ['18', '19', '20', '21', '22', '23']
    
    with open(REGIONAL_CSV_PATH, 'r', encoding='utf-8') as file:
        # Skip first 3 formatting header rows
        for _ in range(3):
            next(file, None)
            
        reader = csv.DictReader(file)
        
        education_columns = [
            'Postgraduate Degree',
            'Graduate Diploma or Certificate', 
            'Bachelor Degree',
            'Advanced Diploma or Diploma',
            'Certificate III or IV',
            'Other non-school qualification',
            'Without non-school qualification'
        ]
        
        for row in reader:
            # Extract year
            survey_month = row.get('Survey month', '')
            year = survey_month
            
            # Only keep target years data
            if year not in target_years:
                continue
            
            # Initialize year dictionary
            if year not in REGIONAL_INDUSTRY_DATA:
                REGIONAL_INDUSTRY_DATA[year] = {}
            
            # Only process median hourly earnings data rows
            if row.get('Parameter') == 'Median hourly earnings':
                state = str(row['State and territory']).strip()
                industry = str(row['Category']).strip()
                
                for edu_col in education_columns:
                    if edu_col in row and row[edu_col]:
                        try:
                            value = float(row[edu_col])
                            if value > 0:
                                key = (state, industry, edu_col)
                                
                                # Get RSE value
                                rse_col = f"{edu_col} RSE"
                                rse_value = 50  # Default RSE
                                if rse_col in row and row[rse_col]:
                                    try:
                                        rse_value = float(row[rse_col])
                                    except (ValueError, TypeError):
                                        pass
                                
                                REGIONAL_INDUSTRY_DATA[year][key] = {
                                    'value': value,
                                    'rse': rse_value
                                }
                        except (ValueError, TypeError):
                            continue

def parse_currency(value):
    """
    Parse currency string like '$1,944' to float
    """
    if not value or value in ['N/A', 'n/a', '']:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace('$', '').replace(',', '').strip()
        if cleaned and cleaned not in ['N/A', 'n/a']:
            try:
                return float(cleaned)
            except ValueError:
                return None
    return None

def get_occupation_base_salary(occupation):
    """
    Get occupation base hourly wage from loaded CSV data
    """
    if not OCCUPATION_DATA:
        raise Exception("Occupation data not loaded")
    
    # Exact match
    for anzsco_code, data in OCCUPATION_DATA.items():
        if data['occupation'].lower() == occupation.lower():
            if data['median_hourly_earnings']:
                return data['median_hourly_earnings']
            elif data['median_weekly_earnings'] and data['avg_hours_per_week'] > 0:
                return data['median_weekly_earnings'] / data['avg_hours_per_week']
    
    
    available_occupations = [data['occupation'] for data in OCCUPATION_DATA.values()]
    raise ValueError(f"Occupation '{occupation}' not found in data. Available occupations include: {', '.join(available_occupations[:10])}{'...' if len(available_occupations) > 10 else ''}")

def calculate_regional_factor_for_year(year, location, industry, education):
    """
    计算特定年份的regional factor，使用2023年数据作为固定锚点
    """
    location_mapping = {
        'VIC': 'Victoria',
        'NSW': 'New South Wales',
        'QLD': 'Queensland',
        'SA': 'South Australia',
        'WA': 'Western Australia',
        'TAS': 'Tasmania',
        'NT': 'Northern Territory',
        'ACT': 'Australian Capital Territory'
    }
    
    region = location_mapping.get(location, location)
    
    # 获取该年份数据（分子用）
    if year not in REGIONAL_INDUSTRY_DATA:
        return {'factor': 1.0, 'rse': 0, 'source': 'No data available for this year'}
        
    year_data = REGIONAL_INDUSTRY_DATA[year]
    
    # 【关键修改】：固定使用23年数据作为锚点（分母用）
    if '23' not in REGIONAL_INDUSTRY_DATA:
        return {'factor': 1.0, 'rse': 0, 'source': 'No 2023 anchor data available'}
    
    anchor_year_data = REGIONAL_INDUSTRY_DATA['23']  # 锚点数据
    bachelor_national_key = ("Australia", industry, "Bachelor Degree")
    bachelor_national = anchor_year_data.get(bachelor_national_key)  # 分母：23年全国本科
    
    if not bachelor_national or bachelor_national['value'] <= 0:
        return {'factor': 1.0, 'rse': 0, 'source': 'No 2023 national bachelor degree anchor data available'}
    
    # 第一优先级：该年份目标地区目标学历数据
    regional_key = (region, industry, education)
    regional_data = year_data.get(regional_key)  # 分子：该年份实际数据
    
    if regional_data and regional_data['value'] > 0:
        factor = regional_data['value'] / bachelor_national['value']  # 该年份数据 ÷ 23年锚点
        rse = regional_data.get('rse', 50)
        
        source = f"20{year} {region} {education} vs 2023 national bachelor baseline"
        if rse >= 25:
            source += f", RSE={rse:.1f}%, less reliable"
            
        return {'factor': factor, 'rse': rse, 'source': source}
    
    # 第二优先级：该年份地区本科 + 全国教育比例估算
    bachelor_regional_key = (region, industry, "Bachelor Degree")
    bachelor_regional = year_data.get(bachelor_regional_key)  # 该年份地区本科
    
    if bachelor_regional and bachelor_regional['value'] > 0:
        target_national_key = ("Australia", industry, education)
        target_national = year_data.get(target_national_key)  # 该年份全国该教育水平
        
        if target_national and target_national['value'] > 0:
            # 用该年份的教育比例
            education_ratio = target_national['value'] / anchor_year_data.get(("Australia", industry, "Bachelor Degree"), {'value': 1})['value']
            estimated_regional = bachelor_regional['value'] * education_ratio
            factor = estimated_regional / bachelor_national['value']  # 估算值 ÷ 23年锚点
            
            avg_rse = (bachelor_regional.get('rse', 50) + target_national.get('rse', 50)) / 2
            source = f"Estimated: 20{year} regional bachelor + education ratio vs 2023 baseline"
            
            return {'factor': factor, 'rse': avg_rse, 'source': source}
    
    # 第三优先级：该年份全国同教育水平数据
    target_national_key = ("Australia", industry, education)
    target_national = year_data.get(target_national_key)  # 该年份全国数据
    
    if target_national and target_national['value'] > 0:
        factor = target_national['value'] / bachelor_national['value']  # 该年份全国 ÷ 23年锚点
        rse = target_national.get('rse', 50)
        source = f"20{year} national {education} vs 2023 national bachelor baseline"
        
        return {'factor': factor, 'rse': rse, 'source': source}
    
    # 最终fallback
    return {'factor': 1.0, 'rse': 0, 'source': 'No data available, using 1.0 factor'}
def get_experience_factor(industry, years):
    """
    Calculate experience adjustment factor based on industry and years
    """
    industry_profiles = {
        "Agriculture, forestry and fishing": {
            "base": 0.85, "growth_rate": 0.035, "plateau_years": 15
        },
        "Information media and telecommunications": {
            "base": 0.90, "growth_rate": 0.08, "plateau_years": 10
        },
        "Professional, scientific and technical services": {
            "base": 0.80, "growth_rate": 0.055, "plateau_years": 20
        },
        "Health care and social assistance": {
            "base": 0.85, "growth_rate": 0.045, "plateau_years": 18
        }
    }
    
    profile = industry_profiles.get(industry, {
        "base": 0.85, "growth_rate": 0.04, "plateau_years": 15
    })
    
    if years <= 1:
        return profile["base"]
    
    effective_years = min(years, profile["plateau_years"])
    return profile["base"] + (math.log(effective_years) * profile["growth_rate"] * 2.5)

def calculate_intensity_adjustment(work_intensity):
    """
    Calculate work intensity adjustment factor: 0.005 * intensity + 0.7
    """
    return 0.005 * work_intensity + 0.7

def calculate_complete_salary_for_years(occupation, location, industry, education, years_exp, work_intensity):
    """
    计算6年的完整薪资数据 - 统一计算逻辑
    """
    # 获取occupation base salary
    try:
        occupation_base_salary = get_occupation_base_salary(occupation)
    except:
        return None
    
    # 获取experience和intensity factors（这两个不随年份变化）
    experience_factor = get_experience_factor(industry, years_exp)
    intensity_factor = calculate_intensity_adjustment(work_intensity)
    
    historical_data = []
    available_years = sorted(REGIONAL_INDUSTRY_DATA.keys())
    
    for year in available_years:
        # 计算该年份的regional factor
        regional_result = calculate_regional_factor_for_year(year, location, industry, education)
        
        if regional_result['factor'] > 0:
            # 完整计算：base × regional × experience × intensity
            complete_salary = occupation_base_salary * regional_result['factor'] * experience_factor * intensity_factor
            
            historical_data.append({
                'year': f"20{year}",
                'salary': round(complete_salary, 2),
                'rse': regional_result['rse'],
                'source': regional_result['source'],
                'factors': {
                    'base': round(occupation_base_salary, 2),
                    'regional': round(regional_result['factor'], 3),
                    'experience': round(experience_factor, 3),
                    'intensity': round(intensity_factor, 3)
                }
            })
    
    return historical_data

def get_verdict(fairness_ratio):
    """
    Simplified evaluation result
    """
    if fairness_ratio >= 1.2:
        return "Above Average"
    elif fairness_ratio >= 0.8:
        return "Average" 
    else:
        return "Below Average"

def validate_input(data):
    """
    Validate required input fields
    """
    required_fields = ['occupation', 'industry', 'education', 'location', 'currentHourlyRate', 'yearsExperience', 'workIntensity']
    
    for field in required_fields:
        if field not in data:
            return {'valid': False, 'message': f'Missing required field: {field}'}
    
    if not isinstance(data['currentHourlyRate'], (int, float)) or data['currentHourlyRate'] <= 0:
        return {'valid': False, 'message': 'currentHourlyRate must be a positive number'}
    
    if not isinstance(data['yearsExperience'], int) or data['yearsExperience'] < 0 or data['yearsExperience'] > 50:
        return {'valid': False, 'message': 'yearsExperience must be an integer between 0 and 50'}
    
    if not isinstance(data['workIntensity'], (int, float)) or data['workIntensity'] < 0 or data['workIntensity'] > 100:
        return {'valid': False, 'message': 'workIntensity must be a number between 0 and 100'}
    
    valid_education_levels = [
        'Postgraduate Degree', 'Bachelor Degree', 'Advanced Diploma or Diploma',
        'Certificate III or IV', 'Other non-school qualification', 'Without non-school qualification'
    ]
    if data['education'] not in valid_education_levels:
        return {'valid': False, 'message': f'Invalid education level. Must be one of: {valid_education_levels}'}
    
    return {'valid': True, 'message': 'Valid input'}

def calculate_fairness_score(input_data):
    """
    重写版：统一计算逻辑，只计算一次
    """
    occupation = input_data['occupation']
    industry = input_data['industry']
    education = input_data['education']
    location = input_data['location']
    hourly_rate = float(input_data['currentHourlyRate'])
    years_exp = input_data['yearsExperience']
    work_intensity = input_data['workIntensity']
    
    # 计算6年完整数据（包含2023年）
    historical_data = calculate_complete_salary_for_years(
        occupation, location, industry, education, years_exp, work_intensity
    )
    
    if not historical_data:
        return error_response(500, 'CALCULATION_ERROR', 'Unable to calculate salary data')
    
    # 使用2023年（最后一年）数据作为当前计算基础
    current_year_data = historical_data[-1]  # 2023年数据
    expected_hourly_rate = current_year_data['salary']
    
    # 计算公平性指标
    fairness_ratio = hourly_rate / expected_hourly_rate
    fairness_score = min(100, max(0, fairness_ratio * 75))
    
    # 计算历史趋势
    salaries = [item['salary'] for item in historical_data]
    total_growth = ((salaries[-1] - salaries[0]) / salaries[0] * 100) if salaries[0] > 0 else 0
    trend_direction = 'increasing' if total_growth > 2 else 'decreasing' if total_growth < -2 else 'stable'
    
    # 构建响应
    response = {
        "fairnessScore": round(fairness_score, 1),
        "verdict": get_verdict(fairness_ratio),
        "comparison": {
            "yourRate": hourly_rate,
            "expectedRate": round(expected_hourly_rate, 2),
            "difference": round(hourly_rate - expected_hourly_rate, 2)
        },
        "calculation": {
            "baseRate": current_year_data['factors']['base'],
            "regionalFactor": current_year_data['factors']['regional'],
            "experienceFactor": current_year_data['factors']['experience'],
            "intensityFactor": current_year_data['factors']['intensity']
        },
        "dataSource": current_year_data['source'],
        "generatedAt": datetime.now().strftime('%Y-%m-%d %H:%M'),
        "historicalTrend": {
            'yearlyData': [{
                'year': item['year'],
                'salary': item['salary'],
                'rse': item['rse'],
                'source': item['source']
            } for item in historical_data],
            'totalGrowth': f"{total_growth:.1f}%",
            'trendDirection': trend_direction
        }
    }
    
    return response

def lambda_handler(event, context):
    """
    Main Lambda handler function
    """
    # Handle CORS preflight requests
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
    
    # Handle POST requests
    try:
        load_csv_data()
        
        if 'body' not in event:
            return error_response(400, 'MISSING_BODY', 'Request body is required')
            
        body = json.loads(event['body'])
        logger.info(f"Received request: {body}")
        
        validation_result = validate_input(body)
        if not validation_result['valid']:
            return error_response(400, 'INVALID_INPUT', validation_result['message'])
            
        fairness_data = calculate_fairness_score(body)
        return success_response(fairness_data)
        
    except FileNotFoundError as e:
        logger.error(f"Data file not found: {str(e)}")
        return error_response(500, 'DATA_FILES_MISSING', f'Required CSV data files missing from Lambda package: {str(e)}')
    except json.JSONDecodeError:
        return error_response(400, 'INVALID_JSON', 'Invalid JSON format')
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return error_response(500, 'INTERNAL_ERROR', f'Internal server error: {str(e)}')

def success_response(data):
    """
    Return success response
    """
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
    """
    Return error response
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': 'https://www.fairwage.click/',
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