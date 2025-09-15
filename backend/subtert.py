import json
from suburb_scoring_handler import score_suburb

# 模拟API Gateway事件 - suburb评分测试
event = {
    "httpMethod": "POST",
    "body": json.dumps({
        "sub": "ABBOTSFORD",
        "industry": "Mining"
    })
}

# 运行测试
print("=== Suburb评分API测试 ===")
print(f"测试地区: ABBOTSFORD")
print(f"测试行业: Mining")
print("-" * 50)

try:
    result = score_suburb(event, None)
    print("Status Code:", result['statusCode'])
    print("Response:")
    
    response_body = json.loads(result['body'])
    
    if response_body.get('success'):
        data = response_body['data']
        print(f"地区: {data['suburb']}")
        print(f"行业: {data['industry']}")
        print(f"SAL编码: {data['sal_code']}")
        print("\n📊 评分结果:")
        scores = data['scores']
        print(f"🛡️  安全评分: {scores['safety']}/100")
        print(f"💰 生活成本: {scores['cost_of_living']}/100")
        print(f"🚌 交通评分: {scores['transport']}/100")
        print(f"👶 孩子保障: {scores['child_care']}/100")
        print(f"💼 行业评分: {scores['industry']}/100")
        print(f"🏆 综合评分: {scores['overall']}/100")
        
        print("\n📋 原始数据:")
        raw_data = data['raw_data']
        print(f"房价: ${raw_data['house_price']:,.0f}")
        print(f"交通站点: {raw_data['transport_stops']}个")
        print(f"学校数量: {raw_data['schools']}所")
        print(f"儿童服务: {raw_data['childcare_services']}个")
        print(f"行业就业: {raw_data['industry_employment']}人")
        print(f"犯罪事件: {raw_data['crime_incidents']}起")
        
    else:
        print("❌ API返回错误:")
        error = response_body['error']
        print(f"错误代码: {error['code']}")
        print(f"错误信息: {error['message']}")
        
except Exception as e:
    print("❌ 测试执行错误:", str(e))
    import traceback
    traceback.print_exc()

print("\n" + "=" * 50)

# 测试不同的地区和行业组合
test_cases = [
    {"sub": "MELBOURNE", "industry": "Construction"},
    {"sub": "ABERFELDIE", "industry": "Education and Training"},
    {"sub": "INVALID_SUBURB", "industry": "Mining"},  # 测试错误情况
    {"sub": "ABBOTSFORD", "industry": ""}  # 测试缺失参数
]

print("=== 多个测试用例 ===")
for i, test_case in enumerate(test_cases, 1):
    print(f"\n测试用例 {i}: {test_case}")
    test_event = {
        "httpMethod": "POST",
        "body": json.dumps(test_case)
    }
    
    try:
        result = score_suburb(test_event, None)
        response_body = json.loads(result['body'])
        
        if response_body.get('success'):
            scores = response_body['data']['scores']
            print(f"✅ 成功 - 综合评分: {scores['overall']}/100")
        else:
            error = response_body['error']
            print(f"❌ 失败 - {error['code']}: {error['message']}")
            
    except Exception as e:
        print(f"❌ 异常 - {str(e)}")

print("\n=== 测试完成 ===")