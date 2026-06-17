"""删除黑名单行业的票"""
import json

# 新增黑名单关键词
BLACKLIST = [
    '光伏', '逆变器', '风电', '储能', '锂电', '充电桩', '新能源',
    '环保', '污水', '水务', '燃气', '电力',
    '轨交', '轨道', '铁路',
    '钢铁', '煤炭', '化工', '水泥', '建材',
    '整车', '汽车',
]

with open('copilot/data/resonance_audit_picks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

removed = []
for rec in data['records']:
    original_count = len(rec['picks'])
    new_picks = []
    
    for pick in rec['picks']:
        sector = pick.get('sector', '').lower()
        should_remove = any(kw.lower() in sector for kw in BLACKLIST)
        
        if should_remove:
            removed.append(f"{pick['name']}({pick['code']}) - {pick.get('sector', '')}")
        else:
            new_picks.append(pick)
    
    rec['picks'] = new_picks
    removed_count = original_count - len(new_picks)
    if removed_count > 0:
        date = rec.get('audit_date') or rec.get('date', '')
        print(f"{date}: 删除{removed_count}只")

# 保存
with open('copilot/data/resonance_audit_picks.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f'\n=== 共删除 {len(removed)} 只 ===')
for stock in removed:
    print(f'  ❌ {stock}')

# 统计删除后
total = sum(len(rec['picks']) for rec in data['records'])
print(f'\n删除后剩余 {total} 只')
