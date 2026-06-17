"""查找sector为空的票"""
import json

with open('copilot/data/resonance_audit_picks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print('=== sector为空的票 ===')
empty_list = []
for rec in data['records']:
    date = rec.get('audit_date') or rec.get('date', '')
    for pick in rec['picks']:
        sector = pick.get('sector', '')
        if not sector or sector.strip() == '':
            empty_list.append({
                'date': date,
                'name': pick['name'],
                'code': pick['code'],
            })
            print(f"{date}: {pick['name']}({pick['code']})")

print(f'\n共 {len(empty_list)} 只票缺少sector')
