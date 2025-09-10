import json
from handler import lambda_handler

# 模拟API Gateway事件
event = {
    'body': json.dumps({
        "occupation": "Software Engineer",
        "currentHourlyRate": 45.50,
        "location": "Melbourne, VIC",
        "yearsExperience": 5
    })
}

# 运行测试
result = lambda_handler(event, None)
print("Status Code:", result['statusCode'])
print("Response:")
print(json.dumps(json.loads(result['body']), indent=2))