import json
from gender_gap_handler import calculate_gender_gap

# 模拟API Gateway事件 - 测试新的参数格式
event = {
    "httpMethod": "POST",
    "body": json.dumps({
        "state": "NSW",
        "industry": "K"
        ""
    })
}

# 运行测试
try:
    result = calculate_gender_gap(event, None)
    print("Status Code:", result['statusCode'])
    print("Response:")
    print(json.dumps(json.loads(result['body']), indent=2))
except Exception as e:
    print("Error:", str(e))