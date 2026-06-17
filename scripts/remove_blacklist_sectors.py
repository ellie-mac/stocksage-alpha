"""查看所有板块并删除机器人/AI/元宇宙"""
import json

# 读取
with open('copilot/data/resonance_audit_picks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 统计板块
print('=== 删除前板块统计 ===')
sectors = {}
for rec in data['records']:
    for pick in rec['picks']:
        sector = pick.get('sector', '未分类')
        if sector not in sectors:
            sectors[sector] = []
        sectors[sector].append(f"{pick['name']}({pick['code']})")

for sector in sorted(sectors.keys()):
    print(f'{sector}: {len(sectors[sector])}只')

# 黑名单关键词
BLACKLIST = ['机器人', 'robot', 'AI', '元宇宙', 'telecom', '通信', '3D_vision', '视觉']

# 删除黑名单板块
removed = []
for rec in data['records']:
    original_count = len(rec['picks'])
    new_picks = []
    
    for pick in rec['picks']:
        sector = pick.get('sector', '').lower()
        should_remove = any(keyword.lower() in sector for keyword in BLACKLIST)
        
        if should_remove:
            removed.append(f"{pick['name']}({pick['code']}) - {pick.get('sector', '')}")
        else:
            new_picks.append(pick)
    
    rec['picks'] = new_picks
    removed_count = original_count - len(new_picks)
    if removed_count > 0:
        print(f"\n{rec.get('audit_date', rec.get('date', ''))}: 删除{removed_count}只")

# 保存
with open('copilot/data/resonance_audit_picks.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f'\n=== 共删除 {len(removed)} 只 ===')
for stock in removed:
    print(f'  ❌ {stock}')

# 统计删除后
print('\n=== 删除后板块统计 ===')
sectors_after = {}
for rec in data['records']:
    for pick in rec['picks']:
        sector = pick.get('sector', '未分类')
        if sector not in sectors_after:
            sectors_after[sector] = 0
        sectors_after[sector] += 1

for sector in sorted(sectors_after.keys()):
    print(f'{sector}: {sectors_after[sector]}只')
