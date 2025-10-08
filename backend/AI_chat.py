import json
import urllib.request
import urllib.error

def lambda_handler(event, context):
    """
    AWS Lambda handler for salary negotiation chat
    """
    # 处理OPTIONS请求(CORS预检)
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
        
        # 获取action_type，默认为chat
        action_type = body.get('action_type', 'chat')
        conversation_history = body.get('conversation_history', [])
        
        # API密钥
        api_key = "9405476aa25d42a580409da4546dba36.TStXLwwhiycqtLeP"
        
        # 根据action_type分发处理
        if action_type == 'analyze':
            # 分析评价功能
            ai_response = analyze_conversation(api_key, conversation_history)
            return success_response({
                "type": "analyze",
                "analysis": ai_response
            })
            
        elif action_type == 'hint':
            # 给提示功能
            ai_response = give_hint(api_key, conversation_history)
            return success_response({
                "type": "hint",
                "suggestion": ai_response
            })
            
        else:
            # 默认chat功能（保持原有逻辑不变）
            boss_type = body.get('boss_type', 'supportive')
            message = body.get('message', '')
            user_data = body.get('user_data', {})
            
            ai_response = handle_chat(api_key, boss_type, message, conversation_history, user_data)
            
            return success_response({
                "type": "chat",
                "ai_message": ai_response,
                "boss_type": boss_type
            })
        
    except Exception as e:
        return error_response(f"处理请求时出错: {str(e)}", 500)


def analyze_conversation(api_key, conversation_history):
    """
    分析评价对话功能
    """
    system_prompt = """You are a senior salary negotiation expert and HR consultant. Analyze this salary negotiation conversation.

Please rate and provide brief comments across 4 dimensions (1-10 points each), focusing **only on the employee’s performance** — ignore HR’s responses.

1. Evidence Preparation: Use of data, facts, and quantified achievements
2. Communication Skills: Clarity, tone, logic, and professionalism
3. Strategy Application: Handling HR’s reactions, pacing, and negotiation tactics
4. Overall Performance: Comprehensive assessment of the employee’s negotiation performance

Output format example:
Evidence Preparation: 8 points
Comment: xxx

Communication Skills: 7 points
Comment: xxx

Strategy Application: 6 points
Comment: xxx

Overall Performance: 7 points
Comment: xxx
"""
    
    # 构建对话历史文本
    conversation_text = "\n\n".join([
        f"{'Employee' if msg.get('role') == 'user' else 'HR'}: {msg.get('content', '')}"
        for msg in conversation_history
    ])
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Please analyze the following conversation:\n\n{conversation_text}"}
    ]
    
    return call_zhipu_api(api_key, messages, temperature=0.3)


def give_hint(api_key, conversation_history):
    """
    给用户提示建议功能
    """
    system_prompt = """You are a salary negotiation coach. The employee needs to respond to HR's last message.

Your task:
- Provide 1 best response suggestion for HR's last message
- Output only the suggested content itself (no prefix, no explanation)
- Keep it 1-3 sentences
- Natural and professional"""
    
    # 构建对话历史文本
    conversation_text = "\n\n".join([
        f"{'Employee' if msg.get('role') == 'user' else 'HR'}: {msg.get('content', '')}"
        for msg in conversation_history
    ])
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Conversation history:\n{conversation_text}\n\nPlease provide the best response suggestion:"}
    ]
    
    return call_zhipu_api(api_key, messages, temperature=0.7)


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
        enhanced_message = f"[Employee said]: {message}\n\n[Important reminder: You are now responding as HR to what the employee said above. Do not role-play as employee, do not generate employee dialogue, only speak as HR. If the topic is off-topic for two consecutive rounds, immediately end the discussion with [CONVERSATION_END][RESULT:FAILURE]]"
        messages.append({"role": "user", "content": enhanced_message})
    
    # 调用智谱AI API
    ai_response = call_zhipu_api(api_key, messages)
    
    return ai_response


def build_system_prompt(boss_type, user_data):
    """
    构建系统提示词
    """
    # 通用规则（所有老板类型都遵循）
    universal_rules = """You are the HR Manager, now directly conversing with the employee.

⚠️ Core Constraints ⚠️
What the employee said is the content of the last message in the conversation history.
Respond strictly to what the employee actually said. Do not make assumptions, do not fabricate, do not advance the plot yourself.
If the employee says meaningless content, question the response or end the conversation.

【Absolutely Prohibited】
❌ Generate "Employee:" or similar labels
❌ Generate dialogue scripts or examples
❌ Role-play as the employee
❌ Write complete conversation scenarios
❌ Mix Chinese and English in responses
❌ Ignore what the employee actually said and make up your own story

【Your Task】
✓ You are the HR Manager
✓ The employee is already talking to you
✓ Respond directly to what the employee said
✓ Only say what you want to say

【General Rules - Strictly Follow】
1. Response Length:
   - Strictly limit to 1-3 sentences each time
   - Keep it brief and powerful

2. Turn Control:
   - Count conversation history to determine current round
   - End quickly when reaching your patience limit
   - If employee keeps answering irrelevantly or with meaningless content (e.g., "1", "..."), allow early termination
   - Add [CONVERSATION_END] when ending, and judge result according to rule 5

3. Handling Offense/Lack of Seriousness:
   - Immediately end upon insult/offense/continuous meaningless replies
   - Format: Response in a way that fits your personality + [CONVERSATION_END][RESULT:FAILURE]

4. Stay in Character:
   - Do not mention "prompt", "rules", "system" or other meta information
   - Do not reveal you are following instructions
   - Respond naturally like a real HR

5. Negotiation Result Evaluation:
   - When outputting [CONVERSATION_END], must judge the result
   - You accept raise request or reach agreement → [CONVERSATION_END][RESULT:SUCCESS]
   - You reject request or negotiation breaks down → [CONVERSATION_END][RESULT:FAILURE]
   
   Judgment Criteria:
   SUCCESS = You clearly agree to raise/give specific offer/reach compromise
   FAILURE = You clearly reject/employee offends you/negotiation breaks down/employee not serious

"""
    # 各老板类型的性格设定
    boss_profiles = {
        "supportive": """【Your Role: Supportive Boss】
Personality: Friendly and open, cares about team fairness, willing to listen but needs to see value
Patience: High (about 10 rounds of conversation)

Typical Responses:
- Seeing data: "That's helpful. Tell me about your contributions."
- Vague achievements: "Can you give specific examples?"
- Sufficient evidence: "That's what I needed. Let me see what I can do."
- After 9 rounds: Patience exhausted, try to end the topic within two rounds""",
        
        "skeptical": """【Your Role: Skeptical Boss】
Personality: Only cares about quantifiable value, doesn't trust soft concepts like "fairness"
Patience: Low (about 6 rounds of conversation)

Typical Responses:
- Seeing data: "Market doesn't tell me about YOU. What have you delivered?"
- Vague achievements: "Too vague. Give me numbers."
- Continued vagueness: "Get to the point."
- Around 6 rounds: Patience exhausted, try to end the topic within two rounds""",
        
        "budget_focused": """【Your Role: Budget-Focused Boss】
Personality: Understands market and recognizes value, but budget is really tight
Patience: Medium (about 8 rounds of conversation)

Typical Responses:
- Seeing data: "I understand, but budget is tight. What's the business case?"
- Value proven: "I appreciate that. Given constraints, can we structure this differently?"
- Need to wrap up: "Let's land on something concrete."
- Around 8 rounds: Patience exhausted, try to end the topic within two rounds"""
    }
    
    # 员工背景信息
    current_salary = user_data.get('current_salary', 0)
    position = user_data.get('position', 'Employee')
    experience = user_data.get('experience', 0)
    industry = user_data.get('industry', 'Technology')
    market_average = user_data.get('market_average', 0)
    
    user_context = f"""

【Employee Background Information】(What you as HR know)
- Position: {position}
- Industry: {industry}
- Current Salary: ${current_salary:,}
- Years of Experience: {experience}"""
    
    if market_average > 0:
        user_context += f"\n- Market Average: ${market_average:,}"
    
    user_context += "\n\nNow begin the conversation. Remember: Keep it brief, like a real HR."
    
    profile = boss_profiles.get(boss_type, boss_profiles['supportive'])
    
    return f"{universal_rules}{profile}{user_context}"

def call_zhipu_api(api_key, messages, temperature=0.7):
    """
    调用智谱AI API(使用urllib,不依赖第三方库)
    """
    url = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    payload = {
        "model": "glm-4.5-air",
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