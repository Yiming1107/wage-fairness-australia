import json
from handler import lambda_handler

# 模拟API Gateway事件 - 正确格式
event = {
    "httpMethod": "POST",
    "body": json.dumps({
        "occupation": "Engineering Managers",
        "industry": "Mining",
        "education": "Bachelor Degree",
        "location": "VIC",
        "currentHourlyRate": 45.50,
        "yearsExperience": 5,
        "workIntensity": 75,
        "earningsType": "weekly"
    })
}

# 运行测试
try:
    result = lambda_handler(event, None)
    print("Status Code:", result['statusCode'])
    print("Response:")
    print(json.dumps(json.loads(result['body']), indent=2))
except Exception as e:
    print("Error:", str(e))