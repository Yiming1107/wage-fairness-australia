import json
from AI_chat import lambda_handler

def print_result(test_name, result):
    """格式化输出测试结果"""
    print("\n" + "="*60)
    print(f"测试: {test_name}")
    print("="*60)
    print("Status Code:", result['statusCode'])
    print("\nResponse:")
    body = json.loads(result['body'])
    print(json.dumps(body, indent=2, ensure_ascii=False))
    print("="*60 + "\n")


def test_chat():
    """测试1: 正常对话功能"""
    event = {
        "httpMethod": "POST",
        "body": json.dumps({
            "action_type": "chat",  # 可省略，默认就是chat
            "boss_type": "skeptical",
            "message": "我想和您谈一下薪资调整的事情",
            "conversation_history": [],
            "user_data": {
                "current_salary": 80000,
                "market_average": 120000,
                "experience": 3,
                "position": "前端开发工程师",
                "industry": "Technology"
            }
        })
    }
    
    result = lambda_handler(event, None)
    print_result("Chat - 开始对话", result)
    return result





def test_analyze():
    """测试3: 分析评价功能"""
    event = {
        "httpMethod": "POST",
        "body": json.dumps({
            "action_type": "analyze",
            "conversation_history": [
                {
                    "role": "user",
                    "content": "我想和您谈一下薪资调整的事情"
                },
                {
                    "role": "assistant",
                    "content": "好的，我们可以聊聊。你觉得应该调整到什么水平？"
                },
                {
                    "role": "user",
                    "content": "市场上同等经验的前端开发平均薪资是12万，而我目前只有8万"
                },
                {
                    "role": "assistant",
                    "content": "市场数据不代表你个人。你交付了什么？"
                },
                {
                    "role": "user",
                    "content": "我在过去一年里主导了3个重要项目，包括重构了整个前端架构，使页面加载速度提升了60%，用户留存率提高了25%"
                },
                {
                    "role": "assistant",
                    "content": "这些成果确实不错。给我具体的数据，比如影响了多少用户？带来多少收益？"
                },
                {
                    "role": "user",
                    "content": "影响了50万日活用户，根据产品团队估算，留存率提升为公司每月节省了约20万的获客成本"
                },
                {
                    "role": "assistant",
                    "content": "好的，这个数据很有说服力。让我看看能做什么。我可以批准调整到10.5万，你觉得如何？[CONVERSATION_END][RESULT:SUCCESS]"
                }
            ]
        })
    }
    
    result = lambda_handler(event, None)
    print_result("Analyze - 分析对话", result)
    return result


def test_hint():
    """测试3: 获取提示功能"""
    event = {
        "httpMethod": "POST",
        "body": json.dumps({
            "action_type": "hint",
            "conversation_history": [
                {
                    "role": "user",
                    "content": "我想和您谈一下薪资调整的事情"
                },
                {
                    "role": "assistant",
                    "content": "好的，我们可以聊聊。你觉得应该调整到什么水平？"
                },
                {
                    "role": "user",
                    "content": "市场上同等经验的前端开发平均薪资是12万，而我目前只有8万"
                },
                {
                    "role": "assistant",
                    "content": "市场数据不代表你个人。你交付了什么？"
                }
            ]
        })
    }
    
    result = lambda_handler(event, None)
    print_result("Hint - 获取提示", result)
    return result


# 运行所有测试
if __name__ == "__main__":
    print("\n" + "#"*60)
    print("# 开始测试 Lambda 函数 - 三种功能")
    print("#"*60)
    
    try:
        # 测试1: 对话功能
        print("\n【测试 1: Chat - 对话功能】")
        test_chat()
        
        # 测试2: 分析功能
        print("\n【测试 2: Analyze - 分析功能】")
        test_analyze()
        
        # 测试3: 提示功能
        print("\n【测试 3: Hint - 提示功能】")
        test_hint()
        
        print("\n" + "#"*60)
        print("# 所有测试完成！")
        print("#"*60 + "\n")
        
    except Exception as e:
        print("\n" + "!"*60)
        print("测试出错:", str(e))
        print("!"*60 + "\n")
        import traceback
        traceback.print_exc()