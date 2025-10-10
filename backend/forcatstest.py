import json
from forecast import forecast_handler, compare_handler

print("=" * 60)
print("Testing Wage Forecast API")
print("=" * 60)

# Test 1: Single Forecast
print("\n[TEST 1] Single Forecast - Retail Trade Female")
print("-" * 60)

event_forecast = {
    "httpMethod": "POST",
    "body": json.dumps({
        "industry": "Retail Trade",
        "gender": "Female",
        "parameter": "hourly",
        "education": "Bachelor Degree",
        "state": "NSW",
        "years_ahead": 5
    })
}

try:
    result = forecast_handler(event_forecast, None)
    print("Status Code:", result['statusCode'])
    print("Response:")
    print(json.dumps(json.loads(result['body']), indent=2))
except Exception as e:
    print("Error:", str(e))

# Test 2: Compare Two Groups
print("\n" + "=" * 60)
print("[TEST 2] Compare - Female vs Male in Retail")
print("-" * 60)

event_compare = {
    "httpMethod": "POST",
    "body": json.dumps({
        "target": {
            "industry": "Retail Trade",
            "gender": "Female",
            "parameter": "hourly",
            "state": "NSW"
        },
        "peer": {
            "industry": "Retail Trade",
            "gender": "Male",
            "parameter": "hourly",
            "state": "NSW"
        },
        "years_ahead": 5
    })
}

try:
    result = compare_handler(event_compare, None)
    print("Status Code:", result['statusCode'])
    print("Response:")
    print(json.dumps(json.loads(result['body']), indent=2))
except Exception as e:
    print("Error:", str(e))

# Test 3: Weekly Earnings
print("\n" + "=" * 60)
print("[TEST 3] Weekly Earnings - Mining Industry")
print("-" * 60)

event_weekly = {
    "httpMethod": "POST",
    "body": json.dumps({
        "industry": "Mining",
        "gender": "Persons",
        "parameter": "weekly",
        "education": "Certificate III or IV",
        "state": "VIC",
        "years_ahead": 3
    })
}

try:
    result = forecast_handler(event_weekly, None)
    print("Status Code:", result['statusCode'])
    body = json.loads(result['body'])
    if body['success']:
        print("✓ Success!")
        print(f"Growth Rate: {body['data']['metadata']['annual_growth_pct']}% per year")
        print(f"Data Points: {body['data']['metadata']['data_points']}")
        print(f"Latest Year Forecast: {body['data']['forecast'][-1]}")
    else:
        print("Response:", json.dumps(body, indent=2))
except Exception as e:
    print("Error:", str(e))

print("\n" + "=" * 60)
print("Tests Complete")
print("=" * 60)