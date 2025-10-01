import json
from AI_chat import lambda_handler

# 模拟API Gateway事件
event = {
    "httpMethod": "POST",
    "body": json.dumps({
        "boss_type": "skeptical",
        "message": "您好，我想和您谈谈关于薪资调整的事情。",
        "conversation_history": [],
        "user_data": {
            "current_salary": 8000,
            "market_average": 12000,
            "experience": 3,
            "position": "前端开发工程师"
        }
    })
}

# 运行测试
try:
    result = lambda_handler(event, None)
    print("Status Code:", result['statusCode'])
    print("\nResponse:")
    print(json.dumps(json.loads(result['body']), indent=2, ensure_ascii=False))
except Exception as e:
    print("Error:", str(e))