import json
import logging
import math
from datetime import datetime
import mysql.connector
from mysql.connector import Error

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
DB_CONFIG = {
    'host': 'fairwageaustralia.ct08osmucf2b.ap-southeast-2.rds.amazonaws.com',
    'port': 3306,
    'user': 'admin',
    'password': 'fairwageaustralia',
    'database': 'fairwageaustralia'
}

def normalize_industry(user_input):
    """Convert user input to industry code for database queries"""
    user_input = user_input.strip().upper()
    
    # If it's already a single letter code
    if len(user_input) == 1 and user_input in INDUSTRY_MAPPING:
        return user_input
    
    # If it's a full name, find the corresponding code
    user_lower = user_input.lower()
    for code, full_name in INDUSTRY_MAPPING.items():
        if user_lower == full_name.lower():
            return code
        # Partial match for convenience
        if user_lower in full_name.lower():
            return code
    
    # If no match found, return original (will cause error in database query)
    return user_input

def get_db_connection():
    """Create MySQL database connection"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        logger.error(f"Database connection error: {str(e)}")
        raise Exception(f"Failed to connect to database: {str(e)}")

def load_all_data():
    """Load all required data from database"""
    global DATA_LOADED
    
    if DATA_LOADED:
        return
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        load_occupation_data(cursor)
        load_employees_data(cursor)
        load_weekly_earnings_data(cursor)
        load_hourly_earnings_data(cursor)
        
        cursor.close()
        conn.close()
        
        DATA_LOADED = True
        logger.info("All data loaded successfully")
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def load_occupation_data(cursor):
    """Load occupation salary data"""
    global OCCUPATION_DATA
    
    query = """
        SELECT anzsco_code, occupation, 
               share_fulltime, avg_fulltime_hours,
               median_fulltime_earnings, median_fulltime_hourly_earnings
        FROM occup_fulltime_earnings
    """
    
    cursor.execute(query)
    for row in cursor.fetchall():
        code = str(row['anzsco_code'])
        OCCUPATION_DATA[code] = {
            'occupation': row['occupation'],
            'full_time_hours': float(row['avg_fulltime_hours'] or 0),
            'weekly_earnings': float(row['median_fulltime_earnings']) if row['median_fulltime_earnings'] else None,
            'hourly_earnings': float(row['median_fulltime_hourly_earnings']) if row['median_fulltime_hourly_earnings'] else None
        }

def load_employees_data(cursor):
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

def load_weekly_earnings_data(cursor):
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

def load_hourly_earnings_data(cursor):
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
    for code, data in OCCUPATION_DATA.items():
        if data['occupation'].lower() == occupation.lower():
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
        user_key = (user_state, industry_code, user_education)
        user_data = earnings_data[year].get(user_key)
        
        if user_data and user_data['value'] > 0:
            factor = user_data['value'] / baseline_salary
            
            yearly_factors.append({
                'year': year,
                'factor': factor,
                'user_salary': user_data['value'],
                'baseline_salary': baseline_salary,
                'rse': user_data['rse'],
                'anchor_education': anchor_education,
                'source': f"{year} {user_state} {user_education} vs {latest_year} Australia {anchor_education} ({earnings_type}) [Industry: {industry_code}]"
            })
        else:
            # Skip years without data as requested
            logger.info(f"Skipping year {year} - no data for {user_state} industry code {industry_code} {user_education}")
    
    return yearly_factors

def get_experience_factor(industry, years):
    """Calculate experience factor"""
    industry_profiles = {
        "Agriculture, forestry and fishing": {"base": 0.85, "growth_rate": 0.035, "plateau_years": 15},
        "Information media and telecommunications": {"base": 0.90, "growth_rate": 0.08, "plateau_years": 10},
        "Professional, scientific and technical services": {"base": 0.80, "growth_rate": 0.055, "plateau_years": 20},
        "Health care and social assistance": {"base": 0.85, "growth_rate": 0.045, "plateau_years": 18}
    }
    
    profile = industry_profiles.get(industry, {"base": 0.85, "growth_rate": 0.04, "plateau_years": 15})
    
    if years <= 1:
        return profile["base"]
    
    effective_years = min(years, profile["plateau_years"])
    return profile["base"] + (math.log(effective_years) * profile["growth_rate"] * 2.5)

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
    required_fields = ['occupation', 'industry', 'education', 'location', 'currentHourlyRate', 'yearsExperience', 'workIntensity', 'earningsType']
    
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