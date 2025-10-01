import json
import urllib.request
import urllib.error

def lambda_handler(event, context):
    """
    AWS Lambda handler for salary negotiation chat
    """
    # 处理OPTIONS请求（CORS预检）
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
        # 解析请求数据
        body = json.loads(event.get('body', '{}'))
        
        boss_type = body.get('boss_type', 'supportive')
        message = body.get('message', '')
        conversation_history = body.get('conversation_history', [])
        user_data = body.get('user_data', {})
        
        # API密钥
        api_key = "9405476aa25d42a580409da4546dba36.TStXLwwhiycqtLeP"  # 替换成你的智谱AI密钥
        
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
    base_prompt = """You are an HR manager having a salary negotiation with an employee.

CRITICAL RULES:
- Keep responses SHORT (2-4 sentences maximum)
- Speak naturally like a real manager would
- Stay in character based on your personality type
- Ask ONE question at a time
- Don't give long explanations or multiple bullet points
- Be direct and conversational"""
    
    # 老板类型设定
    boss_profiles = {
        "supportive": """
Character: Supportive Boss
Personality: Friendly and open to discussion, but still needs to see value
Style: Warm but professional. Ask about their contributions briefly.""",
        
        "skeptical": """
Character: Skeptical Boss  
Personality: Needs proof and concrete examples. Questions everything.
Style: Direct and challenging. Push back on claims without clear evidence.""",
        
        "budget_focused": """
Character: Budget-Focused Boss
Personality: Worried about costs. Always thinking about ROI and alternatives.
Style: Practical and cautious. Mention budget constraints and explore cheaper options."""
    }
    
    # 用户背景信息
    current_salary = user_data.get('current_salary', 0)
    market_average = user_data.get('market_average', 0)
    experience = user_data.get('experience', 0)
    position = user_data.get('position', '员工')
    
    user_context = f"""
Employee Background:
- Position: {position}
- Current Salary: ${current_salary}
- Experience: {experience} years"""
    
    if market_average > 0:
        user_context += f"\n- Market Average: ${market_average}"
    
    return f"{base_prompt}\n\n{boss_profiles.get(boss_type, boss_profiles['supportive'])}\n\n{user_context}\n\nRemember: Keep responses SHORT (2-4 sentences). Speak naturally."


def call_zhipu_api(api_key, messages, temperature=0.7):
    """
    调用智谱AI API（使用urllib，不依赖第三方库）
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
    
    # 将payload转换为JSON字符串并编码为bytes
    data = json.dumps(payload).encode('utf-8')
    
    # 创建请求对象
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')
    
    try:
        # 发送请求
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            
            if 'choices' not in result or not result['choices']:
                raise Exception("智谱AI API返回数据格式错误")
            
            return result['choices'][0]['message']['content']
            
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8')
        raise Exception(f"智谱AI API调用失败: {e.code} - {error_body}")
    except urllib.error.URLError as e:
        raise Exception(f"网络请求失败: {str(e)}")


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