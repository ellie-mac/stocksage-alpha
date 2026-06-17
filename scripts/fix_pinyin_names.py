"""修复JSON里的拼音名字为中文"""
import json

# 读取
with open('copilot/data/resonance_audit_picks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 拼音 -> 中文映射
name_fix = {
    'chaoshengdz': '超声电子',
    'lddixieBO': '绿的谐波',
    'zhongxingtx': '中兴通讯',
    'haozhijidian': '昊志机电',
    'aobizhongguang': '奥比中光',
}

fixed_count = 0
for rec in data['records']:
    for pick in rec['picks']:
        old_name = pick.get('name', '')
        if old_name in name_fix:
            pick['name'] = name_fix[old_name]
            fixed_count += 1
            print(f"✅ {old_name} -> {pick['name']} ({pick['code']})")

# 保存
with open('copilot/data/resonance_audit_picks.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f'\n共修复 {fixed_count} 处拼音名字')
