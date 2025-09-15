import json
from suburb_scoring_handler import score_suburb

# 模拟API Gateway事件 - 测试suburb评分
event = {
    "httpMethod": "POST",
    "body": json.dumps({
        "sub": "ABBOTSFORD",
        "industry": "Mining"
    })
}

# 运行测试
try:
    result = score_suburb(event, None)
    print("Status Code:", result['statusCode'])
    print("Response:")
    print(json.dumps(json.loads(result['body']), indent=2))
except Exception as e:
    print("Error:", str(e))