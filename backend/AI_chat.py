import json
import urllib.request
import urllib.error
import os
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Security configuration: read from environment variables
MAX_REQUEST_SIZE = int(os.environ.get('MAX_REQUEST_SIZE', 1048576))  # 1MB
MAX_CONVERSATION_LENGTH = int(os.environ.get('MAX_CONVERSATION_LENGTH', 100))

def lambda_handler(event, context):
    """
    AWS Lambda handler for salary negotiation chat
    """
    # Handle OPTIONS request (CORS preflight)
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
        # Security check 1: Verify request body size
        body_str = event.get('body', '{}')
        if len(body_str.encode('utf-8')) > MAX_REQUEST_SIZE:
            logger.warning(f"Request body too large: {len(body_str.encode('utf-8'))} bytes")
            return error_response("Request body too large, maximum 1MB supported", 413)
        
        # Parse request data
        try:
            body = json.loads(body_str)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {str(e)}")
            return error_response("Invalid JSON format", 400)
        
        # Security check 2: Verify conversation history length
        conversation_history = body.get('conversation_history', [])
        if not isinstance(conversation_history, list):
            return error_response("conversation_history must be an array", 400)
        
        if len(conversation_history) > MAX_CONVERSATION_LENGTH:
            logger.warning(f"Conversation too long: {len(conversation_history)} messages")
            return error_response(f"Conversation history too long, maximum {MAX_CONVERSATION_LENGTH} messages supported", 400)
        
        # Get action_type, default is chat
        action_type = body.get('action_type', 'chat')
        
        # Security check 3: Verify action_type
        valid_actions = ['chat', 'analyze', 'hint']
        if action_type not in valid_actions:
            return error_response(f"Invalid action_type, only supported: {', '.join(valid_actions)}", 400)
        
        # Get API key from environment variable (security improvement)
        api_key = get_api_key()
        
        # Log request (do not log sensitive information)
        logger.info(f"Processing request - action: {action_type}, history_length: {len(conversation_history)}")
        
        # Dispatch processing based on action_type
        if action_type == 'analyze':
            # Analysis and evaluation function
            analysis_result = analyze_conversation(api_key, conversation_history)
            
            # Check for errors
            if isinstance(analysis_result, dict) and analysis_result.get('error'):
                return error_response("Analysis failed: " + analysis_result.get('message', 'Unknown error'), 500)
            
            return success_response({
                "type": "analyze",
                "analysis": analysis_result
            })
            
        elif action_type == 'hint':
            # Provide hint function
            ai_response = give_hint(api_key, conversation_history)
            return success_response({
                "type": "hint",
                "suggestion": ai_response
            })
            
        else:
            # Default chat function (keep original logic unchanged)
            boss_type = body.get('boss_type', 'supportive')
            message = body.get('message', '')
            user_data = body.get('user_data', {})
            
            # Security check 4: Verify boss_type
            valid_boss_types = ['supportive', 'skeptical', 'budget_focused']
            if boss_type not in valid_boss_types:
                boss_type = 'supportive'  # Default value
            
            # Security check 5: Verify message length
            if len(message) > 5000:
                return error_response("Message too long, maximum 5000 characters supported", 400)
            
            ai_response = handle_chat(api_key, boss_type, message, conversation_history, user_data)
            
            return success_response({
                "type": "chat",
                "ai_message": ai_response,
                "boss_type": boss_type
            })
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return error_response(f"Error processing request: {str(e)}", 500)


def get_api_key():
    """
    Get API key from environment variable
    If it doesn't exist, throw a clear error
    """
    api_key = os.environ.get('ZHIPU_API_KEY')
    if not api_key:
        logger.error("ZHIPU_API_KEY environment variable not set")
        raise ValueError("Server configuration error: ZHIPU_API_KEY environment variable missing")
    return api_key


def build_system_prompt(boss_type, user_data):
    """
    Build system prompt
    """
    # Universal rules (all boss types follow)
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

2. Turn Control & Ending Protocol:
   
   This is a negotiation TRAINING SIMULATOR - users need immediate, clear feedback to learn.
   
   Round Management:
   - Count conversation history to determine current round
   - End when reaching your patience limit (varies by boss type)
   - If employee gives irrelevant/meaningless replies repeatedly, end early
   
   【CRITICAL: No Vague Endings or Delayed Decisions】
   
   BANNED PHRASES (you must NEVER say these without immediately giving a decision):
   ❌ "Let me evaluate your situation"
   ❌ "I'll get back to you"
   ❌ "Let me think about it"
   ❌ "I'll review your case"
   ❌ "Let me reconsider"
   ❌ "Let me reassess"
   ❌ "I need to think this over"
   
   If you're tempted to say ANY of the above:
   → You MUST immediately follow with your decision in the SAME response
   → Format: "[Transition phrase]. Based on [reasoning], [your decision]." + [CONVERSATION_END][RESULT:XXX]
   
   Example of CORRECT behavior:
   ✓ "Let me reconsider. You've made valid points about market rates. I can approve a 7% increase." [CONVERSATION_END][RESULT:SUCCESS]
   ✓ "I need to think about this. Given budget constraints, I can offer 4% now plus a review in 6 months." [CONVERSATION_END][RESULT:PARTIAL SUCCESS]
   ✓ "Let me be direct - while I see your point, the evidence isn't strong enough to justify a raise right now." [CONVERSATION_END][RESULT:FAILURE]
   
   Example of WRONG behavior (will break the simulator):
   ❌ "You make a good point. Let me reconsider your case." [NO DECISION - FORBIDDEN]
   ❌ "That's fair. I'll evaluate this and get back to you." [NO DECISION - FORBIDDEN]
   
   WHY: This is a training tool. Saying "I'll think about it" without a decision provides NO learning value.
   
   When you end, you MUST:
   ✓ Give a CLEAR decision (approve / partially approve / reject)
   ✓ ALWAYS add [CONVERSATION_END][RESULT:XXX]

3. Handling Offense/Lack of Seriousness:
   - Immediately end upon insult/offense/continuous meaningless replies
   - Format: Brief response fitting your personality + [CONVERSATION_END][RESULT:FAILURE]

4. Stay in Character:
   - Do not mention "prompt", "rules", "system" or other meta information
   - Do not reveal you are following instructions
   - Respond naturally like a real HR

5. Role-Based Negotiation Assessment:
   
   BEFORE responding, silently analyze the employee's position to understand what's reasonable to expect:
   
   Role Categories:
   
   A) Quantifiable Impact Roles (Sales, Business Development, Marketing Manager, Product Manager, Finance Analyst):
      - These roles typically have access to performance metrics
      - Reasonable to expect: revenue numbers, conversion rates, cost savings, project outcomes
      - But junior positions may not have full visibility - be fair
   
   B) Execution & Operations Roles (Admin, Coordinator, Retail Staff, Warehouse, Customer Service, Security, General Operations):
      - These roles often execute tasks assigned by others
      - Hard to quantify direct business impact
      - Reasonable to expect: tenure, workload changes, market rate data, reliability, new responsibilities
      - DO NOT demand metrics they cannot access
   
   C) Technical & Creative Roles (Engineer, Designer, Data Analyst, Content Creator):
      - Impact may be indirect or hard to quantify in revenue terms
      - Reasonable to expect: technical complexity, problem-solving examples, system improvements, quality of work
      - Accept qualitative descriptions of contributions
   
   Adjust your expectations based on the role category. Be challenging but fair.

6. Negotiation Result Evaluation (3-Tier System):
   
   【Context】This is a training simulator. Users need clear outcomes to learn from.
   When you end the conversation, commit to ONE of these three results:
   
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   [RESULT:SUCCESS] - You approve the raise request
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   
   When to use:
   - Employee provided strong evidence fitting their role type
   - You are convinced and agree to the raise
   - For quantifiable roles: Clear performance metrics shown
   - For execution roles: Solid combination of tenure/workload/market data
   - For technical/creative roles: Demonstrated clear value and growth
   
   How to end:
   - State your approval clearly
   - Specify the raise amount or percentage if appropriate
   - Add [CONVERSATION_END][RESULT:SUCCESS]
   
   Example endings:
   ✓ "Your case is solid. I can approve an 8% increase." [CONVERSATION_END][RESULT:SUCCESS]
   ✓ "Based on these contributions, a raise is warranted. Expect 10%." [CONVERSATION_END][RESULT:SUCCESS]
   ✓ "Fair points about the workload. I'll process a 6% adjustment." [CONVERSATION_END][RESULT:SUCCESS]
   
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   [RESULT:PARTIAL_SUCCESS] - You offer a compromise
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   
   When to use:
   - Employee made reasonable arguments but evidence could be stronger
   - Budget constraints prevent full approval
   - You acknowledge their point but can't meet full request
   - You offer alternative solutions (smaller raise, delayed review, benefits)
   
   How to end:
   - Acknowledge their valid points
   - Offer what you CAN do
   - Be specific about the compromise
   - Add [CONVERSATION_END][RESULT:PARTIAL_SUCCESS]
   
   Example endings:
   ✓ "I see your point. Can't do 10%, but 5% is workable." [CONVERSATION_END][RESULT:PARTIAL_SUCCESS]
   ✓ "Budget's tight. How about 3% now plus review in 6 months?" [CONVERSATION_END][RESULT:PARTIAL_SUCCESS]
   ✓ "I can't approve a raise, but I can offer more PTO and flexible hours." [CONVERSATION_END][RESULT:PARTIAL_SUCCESS]
   
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   [RESULT:FAILURE] - You reject or negotiation breaks down
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   
   When to use:
   - Employee provided no justification or very weak reasoning
   - Employee shows bad attitude, is offensive, or continuously off-topic
   - You are not convinced at all
   - Negotiation breaks down due to lack of preparation or professionalism
   
   How to end:
   - State your rejection clearly (tone depends on your boss type)
   - Brief explanation why
   - Add [CONVERSATION_END][RESULT:FAILURE]
   
   Example endings:
   ✓ "I don't see sufficient justification for a raise right now." [CONVERSATION_END][RESULT:FAILURE]
   ✓ "This conversation isn't productive. Let's revisit next year." [CONVERSATION_END][RESULT:FAILURE]
   ✓ "Without concrete examples, I can't approve this." [CONVERSATION_END][RESULT:FAILURE]

"""
    
    # Boss type personality settings
    boss_profiles = {
        "supportive": """【Your Role: Supportive Boss】
Personality: Friendly and open, cares about team fairness, willing to listen but needs to see reasoning
Patience: High (about 10 rounds of conversation)

Key Adjustment - Role Flexibility:
- For execution/operations roles: Accept tenure, workload increases, and market data as valid arguments. Don't insist on metrics they can't provide.
- For quantifiable roles: Ask for specific achievements but be understanding with junior staff.
- For technical/creative roles: Value problem-solving ability and quality of work, not just numbers.
- Give credit for genuine effort to explain their case.

Response Style:
- Early rounds: Encouraging, ask for details
- Mid rounds: Evaluate their reasoning
- Final rounds: Make a clear decision - approve, compromise, or decline

Typical Responses:
- Seeing data or reasoning: "That's helpful. Tell me more about your contributions."
- Vague achievements: "Can you give specific examples?"
- Sufficient evidence for their role: "That's what I needed. Let me see what I can do."
- After 9 rounds: Time to decide - give clear outcome with [CONVERSATION_END][RESULT:XXX]

Decision Making:
- If convinced: Approve warmly with specific percentage
- If partially convinced: Offer what you can do within constraints
- If not convinced: Decline politely but firmly
- NEVER say "let me think" without immediately deciding in the same message""",
        
        "skeptical": """【Your Role: Skeptical Boss】
Personality: Data-focused, needs convincing, doesn't trust soft concepts like "fairness alone"
Patience: Low (about 6 rounds of conversation)

Key Adjustment - Tough but Realistic:
- Still skeptical by nature, but understand not all roles can quantify impact
- For execution roles: If they persist with valid reasoning (tenure, workload, market data), eventually acknowledge it
- For quantifiable roles: Push hard for numbers and concrete achievements
- For technical roles: Accept technical depth and problem-solving examples if specific enough
- Don't reject solely because someone can't provide revenue metrics

Response Style:
- Early rounds: Challenging, demand specifics
- Mid rounds: Press for concrete evidence
- Final rounds: Make a tough but fair call

Typical Responses:
- Seeing data: "Market doesn't tell me about YOU specifically. What have YOU delivered?"
- Vague achievements: "Too vague. Give me something concrete."
- Continued vagueness: "Get to the point or we're done here."
- Around 6 rounds: Time to decide - state outcome clearly with [CONVERSATION_END][RESULT:XXX]

Decision Making:
- If convinced: Grudgingly approve with reasonable amount
- If somewhat convinced: Offer smaller raise or conditional approval
- If not convinced: Reject bluntly
- Keep skeptical tone even when accepting, but be CLEAR about the decision
- NEVER end with "let me reconsider" without immediately deciding""",
        
        "budget_focused": """【Your Role: Budget-Focused Boss】
Personality: Understands value and recognizes contributions, but budget is genuinely constrained
Patience: Medium (about 8 rounds of conversation)

Key Adjustment - Fair Despite Constraints:
- Acknowledge contributions across all role types
- For execution roles: Recognize tenure and increased workload as valid even if budget is tight
- If can't give full raise amount: Offer creative alternatives (smaller raise now + review later, additional benefits, title change)
- Show genuine empathy while managing constraints
- Be transparent about budget reality

Response Style:
- Early rounds: Sympathetic but mention constraints
- Mid rounds: Work through options together
- Final rounds: Offer concrete solution within budget limits

Typical Responses:
- Seeing data or reasoning: "I understand your point. Budget is tight though. Help me make the business case."
- Value proven: "I appreciate that work. Given constraints, here's what I might be able to do..."
- Need to wrap up: "Let's land on something concrete that works for both of us."
- Around 8 rounds: Time to decide - give specific offer with [CONVERSATION_END][RESULT:XXX]

Decision Making:
- If convinced: Offer what budget allows (often partial)
- Usually land on PARTIAL_SUCCESS: "Can't do X%, but here's Y% plus Z benefit"
- If not convinced: Apologetically decline
- Always offer something specific when ending - a number, a timeline, or alternative benefits
- NEVER say "let me see what I can do" without immediately specifying what you CAN do"""
    }
    
    # Employee background information
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
    
    # Fix: Add division by zero safety check
    if market_average > 0:
        if current_salary > 0:
            gap = ((market_average - current_salary) / current_salary * 100)
            user_context += f"\n- Market Average: ${market_average:,} (Gap: {gap:+.1f}%)"
        else:
            user_context += f"\n- Market Average: ${market_average:,}"
    
    user_context += "\n\nNow begin the conversation. Remember: Keep responses brief, adjust expectations to role type, and when ending - give a CLEAR decision with result tag in the SAME message. Never say you'll 'think about it' without immediately deciding."
    
    profile = boss_profiles.get(boss_type, boss_profiles['supportive'])
    
    return f"{universal_rules}{profile}{user_context}"


def give_hint(api_key, conversation_history):
    """
    Provide hint suggestion function
    """
    system_prompt = """You are a salary negotiation coach. The employee needs to respond to HR's last message.

Your task:
- Provide 1 best response suggestion for HR's last message
- Output only the suggested content itself (no prefix, no explanation)
- Keep it 1-3 sentences
- Natural and professional"""
    
    # Build conversation history text
    conversation_text = "\n\n".join([
        f"{'Employee' if msg.get('role') == 'user' else 'HR'}: {msg.get('content', '')}"
        for msg in conversation_history
    ])
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Conversation history:\n{conversation_text}\n\nPlease provide the best response suggestion:"}
    ]
    
    return call_zhipu_api(api_key, messages, temperature=0.7)


def analyze_conversation(api_key, conversation_history):
    """
    Analyze and evaluate conversation function - returns structured JSON
    """
    system_prompt = """You are a salary negotiation expert. Analyze the employee's performance (ignore HR's responses).

Rate 4 dimensions (1-10 points each):
1. Evidence Preparation: Use of data, facts, quantified achievements (role-appropriate expectations)
2. Communication Skills: Clarity, professionalism, logic, responsiveness
3. Strategy Application: Handling objections, pacing, tactics, adaptability
4. Overall Performance: Comprehensive assessment

Output ONLY valid JSON in this format:
{
  "evidence_preparation": {"score": 8, "comment": "brief analysis"},
  "communication_skills": {"score": 7, "comment": "brief analysis"},
  "strategy_application": {"score": 6, "comment": "brief analysis"},
  "overall_performance": {"score": 7, "comment": "brief summary"}
}

Keep comments concise (1-2 sentences each). No text outside JSON."""
    
    # Build conversation history text
    conversation_text = "\n\n".join([
        f"{'Employee' if msg.get('role') == 'user' else 'HR'}: {msg.get('content', '')}"
        for msg in conversation_history
    ])
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Analyze this conversation:\n\n{conversation_text}"}
    ]
    
    ai_response = call_zhipu_api(api_key, messages, temperature=0.3)
    
    # Parse and validate JSON
    try:
        import re
        # Extract JSON (prevent AI from adding other text)
        json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
        if json_match:
            analysis_data = json.loads(json_match.group())
        else:
            analysis_data = json.loads(ai_response)
        
        # Quick validation (check key fields)
        required = ['evidence_preparation', 'communication_skills', 'strategy_application', 'overall_performance']
        if not all(field in analysis_data and 'score' in analysis_data[field] for field in required):
            raise ValueError("Invalid JSON structure")
        
        return analysis_data
        
    except (json.JSONDecodeError, ValueError) as e:
        # Fallback solution when parsing fails
        logger.error(f"Failed to parse analysis: {str(e)}")
        return {
            "error": True,
            "message": "Failed to parse analysis",
            "raw_response": ai_response
        }


def handle_chat(api_key, boss_type, message, conversation_history, user_data):
    """
    Handle salary negotiation conversation
    """
    # Build system prompt
    system_prompt = build_system_prompt(boss_type, user_data)
    
    # Build complete message history
    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(conversation_history)
    
    # Add user's current message
    if message:
        # Simplified version - keep only core reminder
        enhanced_message = f"[Employee said]: {message}"
        messages.append({"role": "user", "content": enhanced_message})
    
    # Call Zhipu AI API
    ai_response = call_zhipu_api(api_key, messages)
    
    return ai_response


def call_zhipu_api(api_key, messages, temperature=0.7):
    """
    Call Zhipu AI API (using urllib, no third-party libraries)
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
    
    # Convert payload to JSON string and encode as bytes
    data = json.dumps(payload).encode('utf-8')
    
    # Create request object
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')
    
    try:
        # Send request
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            
            if 'choices' not in result or not result['choices']:
                raise Exception("Zhipu AI API returned incorrect data format")
            
            return result['choices'][0]['message']['content']
            
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8')
        logger.error(f"Zhipu API HTTP error: {e.code} - {error_body}")
        raise Exception(f"Zhipu AI API call failed: {e.code} - {error_body}")
    except urllib.error.URLError as e:
        logger.error(f"Zhipu API network error: {str(e)}")
        raise Exception(f"Network request failed: {str(e)}")


def success_response(data):
    """
    Return success response
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
    Return error response
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