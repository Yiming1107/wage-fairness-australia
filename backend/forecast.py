"""
Wage Forecast Lambda Handler - SECURE VERSION
Filename: forecast.py

Provides two endpoints:
- POST /forecast/predict - Single group wage forecast
- POST /forecast/compare - Compare two demographic groups

Security improvements:
- Database credentials from environment variables
- Input validation and size limits
- Enhanced error logging
- SQL injection protection
"""

import json
import logging
import math
import os
from datetime import datetime
import pymysql.cursors

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 安全配置：从环境变量读取
MAX_REQUEST_SIZE = int(os.environ.get('MAX_REQUEST_SIZE', 1048576))  # 1MB
MAX_STRING_LENGTH = 200  # 防止恶意超长字符串


def get_db_config():
    """
    从环境变量获取数据库配置
    如果环境变量不存在则抛出错误
    """
    required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    return {
        'host': os.environ.get('DB_HOST'),
        'port': int(os.environ.get('DB_PORT', '3306')),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'database': os.environ.get('DB_NAME'),
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
        'connect_timeout': 10,
        'read_timeout': 30,
        'write_timeout': 30
    }


def get_db_connection():
    """Create MySQL database connection"""
    try:
        config = get_db_config()
        conn = pymysql.connect(**config)
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise Exception(f"Failed to connect to database: {str(e)}")


def validate_string_input(value, field_name, max_length=MAX_STRING_LENGTH):
    """
    验证字符串输入
    防止超长字符串攻击
    """
    if value is None:
        return None
    
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    
    if len(value) > max_length:
        raise ValueError(f"{field_name} exceeds maximum length of {max_length}")
    
    return value


def normalize_string(text):
    """Normalize string for fuzzy matching"""
    if not text:
        return ""
    return text.lower().strip()


def fuzzy_match(target, choices, cutoff=0.6):
    """
    Simple fuzzy string matching without difflib
    安全增强：限制输入长度
    """
    if not target or not choices:
        return None
    
    # 安全检查：防止超长字符串
    if len(target) > MAX_STRING_LENGTH:
        logger.warning(f"Fuzzy match target too long: {len(target)}")
        target = target[:MAX_STRING_LENGTH]
    
    target_norm = normalize_string(target)
    best_match = None
    best_score = 0
    
    for choice in choices:
        choice_norm = normalize_string(choice)
        
        # Exact match
        if target_norm == choice_norm:
            return choice
        
        # Contains match
        if target_norm in choice_norm or choice_norm in target_norm:
            score = min(len(target_norm), len(choice_norm)) / max(len(target_norm), len(choice_norm))
            if score > best_score and score >= cutoff:
                best_score = score
                best_match = choice
    
    return best_match


def load_industry_education_history(connection, parameter, gender=None, industry=None, education=None, state=None):
    """Load data from Industry_Education_History table"""
    
    # 安全检查：验证 parameter（白名单）
    valid_parameters = ['weekly', 'hourly']
    if parameter.lower() not in valid_parameters:
        raise ValueError(f"Invalid parameter. Must be one of: {', '.join(valid_parameters)}")
    
    # Build WHERE clause - 使用参数化查询防止SQL注入
    where_conditions = ["`Parameter` = %s"]
    params = [parameter.lower()]
    
    if gender:
        where_conditions.append("`Sex` = %s")
        params.append(gender)
    
    if industry:
        where_conditions.append("`Industry` = %s")
        params.append(industry)
    
    if state:
        where_conditions.append("`State and territory` = %s")
        params.append(state)
    
    where_clause = " AND ".join(where_conditions)
    
    # First, get all possible education columns
    query_columns = "SHOW COLUMNS FROM `Industry_Education_History`"
    
    with connection.cursor() as cursor:
        cursor.execute(query_columns)
        all_columns = [row['Field'] for row in cursor.fetchall()]
    
    # Filter education columns (exclude metadata columns)
    exclude_cols = ['Survey month', 'Parameter', 'State and territory', 'Sex', 'Gender', 
                    'Industry', 'industry_code', 'Total', 'year']
    education_cols = [col for col in all_columns 
                     if col not in exclude_cols 
                     and not col.endswith('_RSE')]
    
    if not education_cols:
        raise ValueError("No education columns found in Industry_Education_History")
    
    # If specific education requested, find best match
    if education:
        education_match = fuzzy_match(education, education_cols, cutoff=0.3)
        if not education_match:
            raise ValueError(f"Education '{education}' not found. Available: {education_cols[:5]}")
        education_cols = [education_match]
    
    # Build SELECT with all education columns
    select_cols = ['`Survey month` as year'] + [f'`{col}` as `{col}`' for col in education_cols]
    
    query = f"""
        SELECT {', '.join(select_cols)}
        FROM `Industry_Education_History`
        WHERE {where_clause}
        ORDER BY `Survey month`
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    
    # Transform to time series format
    result = []
    for row in rows:
        year = int(row['year'])
        for edu_col in education_cols:
            value = row.get(edu_col)
            if value and float(value) > 0:
                result.append({
                    'year': year,
                    'education': edu_col,
                    'value': float(value)
                })
    
    return result


def load_household_data(connection, parameter, gender=None, category=None, state=None):
    """Load data from household table"""
    
    # 安全检查：验证 parameter（白名单）
    valid_parameters = ['weekly', 'hourly']
    if parameter.lower() not in valid_parameters:
        raise ValueError(f"Invalid parameter. Must be one of: {', '.join(valid_parameters)}")
    
    where_conditions = ["`Parameter` = %s"]
    params = [parameter.lower()]
    
    if gender:
        where_conditions.append("`Sex` = %s")
        params.append(gender)
    
    if category:
        where_conditions.append("`Category` = %s")
        params.append(category)
    
    if state:
        where_conditions.append("`State and territory` = %s")
        params.append(state)
    
    where_clause = " AND ".join(where_conditions)
    
    query = f"""
        SELECT `Survey month` as year, `Total` as value
        FROM `household`
        WHERE {where_clause}
        ORDER BY `Survey month`
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    
    result = []
    for row in rows:
        value = row.get('value')
        if value and float(value) > 0:
            result.append({
                'year': int(row['year']),
                'value': float(value)
            })
    
    return result


def get_available_options(connection, table_name, column_name):
    """Get unique values from a column"""
    # 安全检查：表名和列名白名单
    valid_tables = ['Industry_Education_History', 'household']
    if table_name not in valid_tables:
        raise ValueError(f"Invalid table name: {table_name}")
    
    query = f"SELECT DISTINCT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL"
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        return [row[column_name] for row in cursor.fetchall() if row[column_name]]


def aggregate_by_year(data):
    """Group data by year and calculate median"""
    year_groups = {}
    
    for item in data:
        year = item['year']
        if year not in year_groups:
            year_groups[year] = []
        year_groups[year].append(item['value'])
    
    result = []
    for year in sorted(year_groups.keys()):
        values = sorted(year_groups[year])
        n = len(values)
        if n == 0:
            continue
        
        # Calculate median
        if n % 2 == 0:
            median = (values[n//2 - 1] + values[n//2]) / 2
        else:
            median = values[n//2]
        
        result.append({'year': year, 'value': median})
    
    return result


def linear_regression(x_data, y_data):
    """Simple linear regression implementation"""
    n = len(x_data)
    if n < 2:
        raise ValueError("Need at least 2 data points for regression")
    
    sum_x = sum(x_data)
    sum_y = sum(y_data)
    sum_xy = sum(x * y for x, y in zip(x_data, y_data))
    sum_x2 = sum(x * x for x in x_data)
    
    # Calculate slope and intercept
    slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
    intercept = (sum_y - slope * sum_x) / n
    
    return slope, intercept


def loglinear_forecast(series, years_ahead=5):
    """Perform log-linear forecast on time series"""
    if len(series) < 3:
        raise ValueError("Need at least 3 years of data for forecasting")
    
    # Extract data
    years = [item['year'] for item in series]
    values = [item['value'] for item in series]
    
    # Log transform
    log_values = [math.log(v) for v in values]
    
    # Fit linear model to log values
    slope, intercept = linear_regression(years, log_values)
    
    # Define forecast starting year
    FORECAST_START_YEAR = 2025
    last_year = max(years)
    
    result = []
    
    # Add all historical data (actual values from database)
    for item in series:
        result.append({
            'year': item['year'],
            'median_forecast': round(item['value'], 2),
            'is_future': False
        })
    
    # Generate forecasts starting from FORECAST_START_YEAR
    forecast_start = max(FORECAST_START_YEAR, last_year + 1)
    forecast_years = list(range(forecast_start, forecast_start + years_ahead))
    
    for year in forecast_years:
        log_pred = intercept + slope * year
        pred = math.exp(log_pred)
        result.append({
            'year': year,
            'median_forecast': round(pred, 2),
            'is_future': True
        })
    
    # Calculate annual growth rate
    growth_rate = (math.exp(slope) - 1.0) * 100.0
    
    return result, round(growth_rate, 2)


def fetch_time_series(connection, industry=None, gender=None, parameter='weekly', 
                     education=None, category=None, state=None):
    """Fetch and prepare time series data"""
    
    # 安全检查：验证输入
    industry = validate_string_input(industry, 'industry')
    gender = validate_string_input(gender, 'gender')
    parameter = validate_string_input(parameter, 'parameter', max_length=20)
    education = validate_string_input(education, 'education')
    category = validate_string_input(category, 'category')
    state = validate_string_input(state, 'state')
    
    parameter = normalize_string(parameter)
    if parameter not in ['weekly', 'hourly']:
        parameter = 'weekly'
    
    # Try Industry_Education_History first if industry or education specified
    if industry or education:
        # Get available options for fuzzy matching
        industries = get_available_options(connection, 'Industry_Education_History', 'Industry')
        genders = get_available_options(connection, 'Industry_Education_History', 'Sex')
        states = get_available_options(connection, 'Industry_Education_History', 'State and territory')
        
        # Fuzzy match inputs
        industry_match = fuzzy_match(industry, industries, cutoff=0.3) if industry else None
        gender_match = fuzzy_match(gender, genders, cutoff=0.3) if gender else 'Persons'
        state_match = fuzzy_match(state, states, cutoff=0.2) if state else None
        
        data = load_industry_education_history(
            connection, parameter, gender_match, industry_match, education, state_match
        )
        
        dataset_used = 'industry_education'
        matched = {
            'parameter': parameter,
            'gender': gender_match,
            'industry': industry_match,
            'education': education,
            'state': state_match
        }
    else:
        # Use household data
        categories = get_available_options(connection, 'household', 'Category')
        genders = get_available_options(connection, 'household', 'Sex')
        states = get_available_options(connection, 'household', 'State and territory')
        
        category_match = fuzzy_match(category, categories, cutoff=0.3) if category else None
        gender_match = fuzzy_match(gender, genders, cutoff=0.3) if gender else 'Persons'
        state_match = fuzzy_match(state, states, cutoff=0.2) if state else None
        
        data = load_household_data(connection, parameter, gender_match, category_match, state_match)
        
        dataset_used = 'household'
        matched = {
            'parameter': parameter,
            'gender': gender_match,
            'category': category_match,
            'state': state_match
        }
    
    if not data:
        raise ValueError("No matching data found with given filters")
    
    # Aggregate by year (median)
    series = aggregate_by_year(data)
    
    return series, dataset_used, matched


def forecast_5y(connection, industry=None, gender=None, parameter='weekly',
                education=None, category=None, state=None, years_ahead=5):
    """Generate 5-year forecast for given profile"""
    
    series, dataset_used, matched = fetch_time_series(
        connection, industry, gender, parameter, education, category, state
    )
    
    forecast_data, growth_rate = loglinear_forecast(series, years_ahead)
    
    unit = "Weekly" if matched['parameter'] == 'weekly' else "Hourly"
    
    return {
        'forecast': forecast_data,
        'metadata': {
            'dataset_used': dataset_used,
            'filters_used': matched,
            'annual_growth_pct': growth_rate,
            'unit': unit,
            'data_points': len(series)
        }
    }


def compare_with_peers(connection, target, peer, years_ahead=5):
    """Compare two forecast profiles"""
    
    # Generate forecasts for both profiles
    result_target = forecast_5y(connection, years_ahead=years_ahead, **target)
    result_peer = forecast_5y(connection, years_ahead=years_ahead, **peer)
    
    fc_target = result_target['forecast']
    fc_peer = result_peer['forecast']
    
    # Merge forecasts by year
    merged = []
    target_dict = {item['year']: item for item in fc_target}
    peer_dict = {item['year']: item for item in fc_peer}
    
    all_years = sorted(set(target_dict.keys()) | set(peer_dict.keys()))
    
    for year in all_years:
        if year in target_dict and year in peer_dict:
            emily_val = target_dict[year]['median_forecast']
            peer_val = peer_dict[year]['median_forecast']
            gap_dollar = emily_val - peer_val
            gap_pct = (gap_dollar / peer_val * 100) if peer_val != 0 else 0
            
            merged.append({
                'year': year,
                'target_forecast': emily_val,
                'peer_forecast': peer_val,
                'gap_dollar': round(gap_dollar, 2),
                'gap_percent': round(gap_pct, 2),
                'is_future': target_dict[year]['is_future']
            })
    
    # Calculate gap trend
    gap_trend = (result_target['metadata']['annual_growth_pct'] - 
                 result_peer['metadata']['annual_growth_pct'])
    
    summary = {
        'target_filters': result_target['metadata']['filters_used'],
        'peer_filters': result_peer['metadata']['filters_used'],
        'target_growth_pct_per_year': result_target['metadata']['annual_growth_pct'],
        'peer_growth_pct_per_year': result_peer['metadata']['annual_growth_pct'],
        'gap_trend_pct_per_year': round(gap_trend, 2),
        'unit': result_target['metadata']['unit']
    }
    
    return {
        'comparison': merged,
        'summary': summary
    }


def validate_forecast_input(data):
    """Validate forecast endpoint input"""
    
    # At least one of industry/category should be specified
    if not data.get('industry') and not data.get('category'):
        return {'valid': False, 'message': 'Either industry or category must be specified'}
    
    # Validate years_ahead
    years_ahead = data.get('years_ahead', 5)
    if not isinstance(years_ahead, int) or years_ahead < 1 or years_ahead > 20:
        return {'valid': False, 'message': 'years_ahead must be integer between 1 and 20'}
    
    # Validate parameter
    parameter = data.get('parameter', 'weekly')
    if parameter not in ['weekly', 'hourly']:
        return {'valid': False, 'message': 'parameter must be either "weekly" or "hourly"'}
    
    return {'valid': True, 'message': 'Valid input'}


def validate_compare_input(data):
    """Validate compare endpoint input"""
    
    if 'target' not in data or 'peer' not in data:
        return {'valid': False, 'message': 'Both target and peer profiles required'}
    
    # Validate each profile
    for key in ['target', 'peer']:
        profile = data[key]
        if not isinstance(profile, dict):
            return {'valid': False, 'message': f'{key} must be an object'}
        
        if not profile.get('industry') and not profile.get('category'):
            return {'valid': False, 'message': f'{key} must specify either industry or category'}
    
    years_ahead = data.get('years_ahead', 5)
    if not isinstance(years_ahead, int) or years_ahead < 1 or years_ahead > 20:
        return {'valid': False, 'message': 'years_ahead must be integer between 1 and 20'}
    
    return {'valid': True, 'message': 'Valid input'}


def forecast_handler(event, context):
    """Handler for /forecast/predict endpoint"""
    
    # Handle CORS preflight
    if event.get('httpMethod') == 'OPTIONS':
        return cors_response()
    
    try:
        # 安全检查：验证请求体大小
        body_str = event.get('body', '{}')
        if len(body_str.encode('utf-8')) > MAX_REQUEST_SIZE:
            logger.warning(f"Request body too large: {len(body_str.encode('utf-8'))} bytes")
            return error_response(413, 'REQUEST_TOO_LARGE', 'Request body exceeds 1MB limit')
        
        # Parse request
        if not body_str:
            return error_response(400, 'MISSING_BODY', 'Request body is required')
        
        try:
            body = json.loads(body_str)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {str(e)}")
            return error_response(400, 'INVALID_JSON', 'Request body must be valid JSON')
        
        logger.info(f"Forecast request: {json.dumps(body, ensure_ascii=False)}")
        
        # Validate input
        validation = validate_forecast_input(body)
        if not validation['valid']:
            return error_response(400, 'INVALID_INPUT', validation['message'])
        
        # Get database connection
        connection = get_db_connection()
        
        try:
            result = forecast_5y(
                connection,
                industry=body.get('industry'),
                gender=body.get('gender'),
                parameter=body.get('parameter', 'weekly'),
                education=body.get('education'),
                category=body.get('category'),
                state=body.get('state'),
                years_ahead=body.get('years_ahead', 5)
            )
            
            logger.info("Forecast generated successfully")
            return success_response(result, 'Forecast generated successfully')
        
        finally:
            connection.close()
    
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return error_response(400, 'VALIDATION_ERROR', str(e))
    
    except Exception as e:
        logger.error(f"Internal error: {str(e)}", exc_info=True)
        return error_response(500, 'INTERNAL_ERROR', f'Internal server error: {str(e)}')


def compare_handler(event, context):
    """Handler for /forecast/compare endpoint"""
    
    # Handle CORS preflight
    if event.get('httpMethod') == 'OPTIONS':
        return cors_response()
    
    try:
        # 安全检查：验证请求体大小
        body_str = event.get('body', '{}')
        if len(body_str.encode('utf-8')) > MAX_REQUEST_SIZE:
            logger.warning(f"Request body too large: {len(body_str.encode('utf-8'))} bytes")
            return error_response(413, 'REQUEST_TOO_LARGE', 'Request body exceeds 1MB limit')
        
        # Parse request
        if not body_str:
            return error_response(400, 'MISSING_BODY', 'Request body is required')
        
        try:
            body = json.loads(body_str)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {str(e)}")
            return error_response(400, 'INVALID_JSON', 'Request body must be valid JSON')
        
        logger.info(f"Compare request: {json.dumps(body, ensure_ascii=False)}")
        
        # Validate input
        validation = validate_compare_input(body)
        if not validation['valid']:
            return error_response(400, 'INVALID_INPUT', validation['message'])
        
        # Get database connection
        connection = get_db_connection()
        
        try:
            result = compare_with_peers(
                connection,
                target=body['target'],
                peer=body['peer'],
                years_ahead=body.get('years_ahead', 5)
            )
            
            logger.info("Comparison generated successfully")
            return success_response(result, 'Comparison generated successfully')
        
        finally:
            connection.close()
    
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return error_response(400, 'VALIDATION_ERROR', str(e))
    
    except Exception as e:
        logger.error(f"Internal error: {str(e)}", exc_info=True)
        return error_response(500, 'INTERNAL_ERROR', f'Internal server error: {str(e)}')


def cors_response():
    """Return CORS preflight response"""
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


def success_response(data, message='Success'):
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
            'message': message,
            'generatedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }, ensure_ascii=False)
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
        }, ensure_ascii=False)
    }