import json
from handler import lambda_handler, load_all_data, EMPLOYEES_DATA, WEEKLY_EARNINGS_DATA, HOURLY_EARNINGS_DATA

# 强制重新加载数据
import handler
handler.DATA_LOADED = False

load_all_data()

print("=== 调试信息 ===")
print(f"员工数据: {len(EMPLOYEES_DATA)} 年")
print(f"周薪数据: {len(WEEKLY_EARNINGS_DATA)} 年") 
print(f"时薪数据: {len(HOURLY_EARNINGS_DATA)} 年")

# 查看员工数据
if EMPLOYEES_DATA:
    latest_year = max(EMPLOYEES_DATA.keys())
    print(f"\n最新年份: {latest_year}")
    print("员工数据样本 (前5条):")
    for i, (key, value) in enumerate(list(EMPLOYEES_DATA[latest_year].items())[:5]):
        state, industry, education = key
        print(f"  {state} | {industry} | {education} = {value}")

# 查看时薪数据
if HOURLY_EARNINGS_DATA:
    print(f"\n时薪数据年份: {list(HOURLY_EARNINGS_DATA.keys())}")
    sample_year = list(HOURLY_EARNINGS_DATA.keys())[0]
    print(f"时薪数据样本 ({sample_year}年前5条):")
    for i, (key, value) in enumerate(list(HOURLY_EARNINGS_DATA[sample_year].items())[:5]):
        state, industry, education = key
        print(f"  {state} | {industry} | {education} = {value['value']}")

# 专门检查Victoria和Information相关数据
print(f"\n=== 查找Victoria相关数据 ===")
vic_found = False
for year, year_data in HOURLY_EARNINGS_DATA.items():
    for (state, industry, education), data in year_data.items():
        if "Victoria" in state and "Information" in industry:
            print(f"找到: {year} | {state} | {industry} | {education}")
            vic_found = True
            break
    if vic_found:
        break

if not vic_found:
    print("未找到Victoria + Information相关数据")
    # 显示所有州名
    all_states = set()
    all_industries = set()
    for year_data in HOURLY_EARNINGS_DATA.values():
        for (state, industry, education), data in year_data.items():
            all_states.add(state)
            all_industries.add(industry)
    print(f"所有州名: {sorted(all_states)}")
    print(f"包含'Information'的行业: {[ind for ind in sorted(all_industries) if 'Information' in ind]}")