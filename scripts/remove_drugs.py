"""删除创新药板块的票"""
import json

with open('copilot/data/resonance_audit_picks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 黑名单关键词（新增创新药）
BLACKLIST = ['创新药', '医药', '生物医药', '中药']

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
        print(f"{rec.get('audit_date', rec.get('date', ''))}: 删除{removed_count}只")

# 保存
with open('copilot/data/resonance_audit_picks.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f'\n=== 共删除 {len(removed)} 只 ===')
for stock in removed:
    print(f'  ❌ {stock}')

# 统计删除后
print('\n=== 删除后统计 ===')
total = sum(len(rec['picks']) for rec in data['records'])
print(f'剩余 {total} 只')
