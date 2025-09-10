import json
import csv
import os
import logging
from collections import defaultdict

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global data cache
INDUSTRY_DATA = {}
COMPANY_SIZE_DATA = {}
HISTORICAL_EARNINGS_DATA = {}
DATA_LOADED = False

# CSV file paths
GENDER1_CSV_PATH = os.path.join(os.path.dirname(__file__), 'data', 'gender1.csv')
GENDER2_CSV_PATH = os.path.join(os.path.dirname(__file__), 'data', 'gender2.csv')
GENDER3_CSV_PATH = os.path.join(os.path.dirname(__file__), 'data', 'gender3.csv')

# Company size mapping - based on actual gender2.csv data
COMPANY_SIZE_MAPPING = {
    (1, 249): "<250",
    (250, 499): "250-499",
    (500, 999): "500-999", 
    (1000, 4999): "1000-4999",
    (5000, float('inf')): "5000+"
}

def get_company_size_category(employee_count):
    """Convert employee count to company size category"""
    try:
        employee_count = int(employee_count)
        if employee_count <= 0:
            return None, "Employee count must be greater than 0"
            
        for (min_size, max_size), category in COMPANY_SIZE_MAPPING.items():
            if min_size <= employee_count <= max_size:
                return category, None
        
        return None, f"Employee count {employee_count} is outside valid range"
        
    except (ValueError, TypeError):
        return None, "Employee count must be a valid number"

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

def load_gender_data():
    """Load gender pay gap data from CSV files"""
    global INDUSTRY_DATA, COMPANY_SIZE_DATA, HISTORICAL_EARNINGS_DATA, DATA_LOADED
    
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
        
        # Load company size data (gender2.csv)
        with open(GENDER2_CSV_PATH, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                size_category = row['Company Size (Employees)'].strip()
                COMPANY_SIZE_DATA[size_category] = {
                    'average_midpoint': float(row['Average GPG Mid-point (%)']) if row['Average GPG Mid-point (%)'] else 0
                }
        
        # Load historical earnings data (gender3.csv)
        HISTORICAL_EARNINGS_DATA = defaultdict(list)
        with open(GENDER3_CSV_PATH, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                industry_name = row['Industry'].strip()
                survey_year = row['Survey month'].strip()
                
                # Convert year (e.g., "14" -> "2014")
                year = "20" + survey_year if len(survey_year) == 2 else survey_year
                
                earnings_data = {
                    'year': year,
                    'total_weekly_earnings': parse_earnings_value(row['Median weekly earnings']),
                    'total_hourly_earnings': parse_earnings_value(row['Median hourly earnings']),
                    'male_weekly_earnings': parse_earnings_value(row['Males Median weekly earnings']),
                    'male_hourly_earnings': parse_earnings_value(row['Males Median hourly earnings']),
                    'female_weekly_earnings': parse_earnings_value(row['Females Median weekly earnings']),
                    'female_hourly_earnings': parse_earnings_value(row['Females Median hourly earnings'])
                }
                
                # Calculate gender pay gap for each year
                if earnings_data['male_weekly_earnings'] > 0:
                    weekly_gap = ((earnings_data['male_weekly_earnings'] - earnings_data['female_weekly_earnings']) / earnings_data['male_weekly_earnings']) * 100
                    earnings_data['weekly_pay_gap_percentage'] = round(weekly_gap, 2)
                else:
                    earnings_data['weekly_pay_gap_percentage'] = 0
                
                if earnings_data['male_hourly_earnings'] > 0:
                    hourly_gap = ((earnings_data['male_hourly_earnings'] - earnings_data['female_hourly_earnings']) / earnings_data['male_hourly_earnings']) * 100
                    earnings_data['hourly_pay_gap_percentage'] = round(hourly_gap, 2)
                else:
                    earnings_data['hourly_pay_gap_percentage'] = 0
                
                HISTORICAL_EARNINGS_DATA[industry_name].append(earnings_data)
        
        # Sort historical data by year for each industry
        for industry in HISTORICAL_EARNINGS_DATA:
            HISTORICAL_EARNINGS_DATA[industry].sort(key=lambda x: x['year'])
        
        DATA_LOADED = True
        logger.info(f"Loaded {len(INDUSTRY_DATA)} industries, {len(COMPANY_SIZE_DATA)} size categories, and {len(HISTORICAL_EARNINGS_DATA)} industries with historical data")
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise Exception(f"Failed to load data files: {str(e)}")

def calculate_gender_gap(event, context):
    """Enhanced lookup function with historical earnings data"""
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
        load_gender_data()
        
        body = json.loads(event['body'])
        industry = body.get('industry')
        employee_count = body.get('employee_count')
        
        # Validate industry
        if not industry or industry not in INDUSTRY_DATA:
            return error_response(400, 'INVALID_INDUSTRY', f'Available industries: {list(INDUSTRY_DATA.keys())}')
        
        # Get industry data
        industry_data = INDUSTRY_DATA[industry]
        
        # Build result with industry data
        result = {
            'industry': industry,
            'industry_midpoints': {
                'average': industry_data['average_midpoint'],
                'median': industry_data['median_midpoint']
            },
            'women_representation': {
                'total_women_percentage': industry_data['total_women_percentage'],
                'upper_quartile_women_percentage': industry_data['women_by_quartile']['upper_quartile'],
                'upper_middle_quartile_women_percentage': industry_data['women_by_quartile']['upper_middle_quartile'],
                'lower_middle_quartile_women_percentage': industry_data['women_by_quartile']['lower_middle_quartile'],
                'lower_quartile_women_percentage': industry_data['women_by_quartile']['lower_quartile']
            }
        }
        
        # Add historical earnings data if available
        if industry in HISTORICAL_EARNINGS_DATA:
            historical_data = HISTORICAL_EARNINGS_DATA[industry]
            result['historical_earnings'] = {
                'data_years': [d['year'] for d in historical_data],
                'yearly_data': historical_data,
                'latest_year_data': historical_data[-1] if historical_data else None,
                'earnings_trend': {
                    'total_weekly_change': calculate_trend_change(historical_data, 'total_weekly_earnings'),
                    'male_weekly_change': calculate_trend_change(historical_data, 'male_weekly_earnings'),
                    'female_weekly_change': calculate_trend_change(historical_data, 'female_weekly_earnings'),
                    'pay_gap_trend': calculate_trend_change(historical_data, 'weekly_pay_gap_percentage')
                }
            }
        
        # Handle employee count if provided
        if employee_count is not None:
            try:
                employee_count_int = int(employee_count)
                result['employee_count'] = employee_count_int
                
                # Map to company size category
                company_size_category, mapping_error = get_company_size_category(employee_count)
                if mapping_error:
                    return error_response(400, 'EMPLOYEE_COUNT_ERROR', mapping_error)
                
                result['company_size_category'] = company_size_category
                
                # Add company size data if available
                if company_size_category in COMPANY_SIZE_DATA:
                    size_data = COMPANY_SIZE_DATA[company_size_category]
                    result['company_size_midpoint'] = {
                        'average': size_data['average_midpoint']
                    }
                
            except (ValueError, TypeError):
                return error_response(400, 'INVALID_EMPLOYEE_COUNT', 'Employee count must be a valid number')
        
        return success_response(result)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return error_response(500, 'INTERNAL_ERROR', str(e))

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