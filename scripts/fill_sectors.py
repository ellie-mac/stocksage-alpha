"""为缺失sector的票补充行业信息"""
import json

# 手动整理的sector映射（根据公开资料）
SECTOR_MAP = {
    '300334': '环保/膜分离',
    '002700': '光伏逆变器',
    '300331': '光学膜/光刻掩模',
    '300398': '光刻胶/封装材料',  # 已有
    '300440': '轨交信号',
    '300604': '半导体测试设备',
    '301182': '半导体CMP设备',
    '301526': '玻纤复合材料',
    '603139': '金刚线',
    '603150': '磁性材料',
    '603186': '覆铜板',
    '603213': '精密制造',
    '688403': '半导体特气阀门',
    '688733': '陶瓷材料/CMP',
    '301387': '半导体设备零部件',
}

with open('copilot/data/resonance_audit_picks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

updated = 0
for rec in data['records']:
    for pick in rec['picks']:
        code = pick['code']
        sector = pick.get('sector', '')
        
        # 如果sector为空且在映射表里
        if (not sector or sector.strip() == '') and code in SECTOR_MAP:
            pick['sector'] = SECTOR_MAP[code]
            updated += 1
            print(f"✅ {pick['name']}({code}): {SECTOR_MAP[code]}")

# 保存
with open('copilot/data/resonance_audit_picks.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f'\n共补充 {updated} 只票的sector')
