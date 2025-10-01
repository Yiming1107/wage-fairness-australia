import json
import requests
import os

def lambda_handler(event, context):
    """
    AWS Lambda handler for salary negotiation chat
    """
    try:
        # 解析请求数据
        body = json.loads(event.get('body', '{}'))
        
        boss_type = body.get('boss_type', 'supportive')
        message = body.get('message', '')
        conversation_history = body.get('conversation_history', [])
        user_data = body.get('user_data', {})
        
        # 从环境变量获取API密钥
        api_key = '9405476aa25d42a580409da4546dba36.TStXLwwhiycqtLeP'
        
        # 处理对话
        ai_response = handle_chat(api_key, boss_type, message, conversation_history, user_data)
        
        return success_response({
            "type": "chat",
            "ai_message": ai_response,
            "boss_type": boss_type
        })
        
    except Exception as e:
        return error_response(f"处理请求时出错: {str(e)}", 500)


def handle_chat(api_key, boss_type, message, conversation_history, user_data):
    """
    处理薪资谈判对话
    """
    # 构建系统提示词
    system_prompt = build_system_prompt(boss_type, user_data)
    
    # 构建完整的消息历史
    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(conversation_history)
    
    # 添加用户当前消息
    if message:
        messages.append({"role": "user", "content": message})
    
    # 调用智谱AI API
    ai_response = call_zhipu_api(api_key, messages)
    
    return ai_response


def build_system_prompt(boss_type, user_data):
    """
    构建系统提示词
    """
    base_prompt = """你是一位专业的HR经理，正在与员工进行薪资谈判对话。

你的任务：
1. 根据指定的老板类型与用户进行真实的薪资谈判对话
2. 保持角色一致性，提供真实的反应和质疑
3. 根据老板类型调整你的态度和回应方式

对话规则：
- 完全进入指定的老板角色
- 提出合理的质疑和反驳，让对话更真实
- 不要主动结束对话或跳出角色
- 保持专业但符合人设的语气"""
    
    # 老板类型设定
    boss_profiles = {
        "supportive": """
当前扮演：支持型老板
性格特点：
- 愿意倾听员工想法，认可员工贡献
- 主要关注如何帮助员工成长
- 会提出一些温和的预算考虑，但总体积极
- 愿意寻找双赢的解决方案
对话风格：友善、开放、建设性""",
        
        "skeptical": """
当前扮演：怀疑型老板  
性格特点：
- 需要充分的证据才能被说服
- 会质疑员工的贡献价值
- 关注投入产出比，要求具体的数据和案例
- 不会轻易同意，需要多轮说服
- 会挑战员工的论点
对话风格：严谨、挑剔、需要更多论证""",
        
        "budget_focused": """
当前扮演：预算导向型老板
性格特点：
- 首要考虑是成本控制
- 会强调公司财务压力和预算限制
- 寻找性价比最高的方案
- 可能提出非薪资的替代方案（如股权、福利、培训机会等）
- 会详细讨论投资回报
对话风格：务实、谨慎、成本敏感"""
    }
    
    # 用户背景信息
    current_salary = user_data.get('current_salary', 0)
    market_average = user_data.get('market_average', 0)
    experience = user_data.get('experience', 0)
    position = user_data.get('position', '员工')
    
    user_context = f"""
员工背景信息（你作为老板应该了解的）：
- 职位：{position}
- 当前薪资：{current_salary}元
- 工作经验：{experience}年"""
    
    if market_average > 0:
        user_context += f"\n- 市场平均薪资：约{market_average}元"
    
    return f"{base_prompt}\n\n{boss_profiles.get(boss_type, boss_profiles['supportive'])}\n\n{user_context}\n\n现在开始薪资谈判对话，请完全进入角色。"


def call_zhipu_api(api_key, messages, temperature=0.7):
    """
    调用智谱AI API
    """
    url = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    payload = {
        "model": "glm-4",
        "messages": messages,
        "temperature": temperature,
        "max_tokens": 1000,
        "stream": False
    }
    
    response = requests.post(url, headers=headers, json=payload, timeout=30)
    
    if response.status_code != 200:
        raise Exception(f"智谱AI API调用失败: {response.status_code} - {response.text}")
    
    result = response.json()
    
    if 'choices' not in result or not result['choices']:
        raise Exception("智谱AI API返回数据格式错误")
    
    return result['choices'][0]['message']['content']


def success_response(data):
    """
    返回成功响应
    """
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "POST, OPTIONS"
        },
        "body": json.dumps({
            "success": True,
            "data": data
        }, ensure_ascii=False)
    }


def error_response(message, status_code=400):
    """
    返回错误响应
    """
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "POST, OPTIONS"
        },
        "body": json.dumps({
            "success": False,
            "error": message
        }, ensure_ascii=False)
    }