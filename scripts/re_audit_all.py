"""基于新黑名单重新审核所有票"""
import json

# 新黑名单（精细版）
BLACKLIST_STRICT = [
    '光伏组件', '逆变器', '光伏电站', '风电', '风机',
    '动力电池', '新能源车', '整车', '汽车', '充电桩',
    '环保', '污水', '水务', '燃气',
    '轨交', '轨道', '铁路',
    '钢铁', '煤炭', '水泥', '建材',
]

# 应该保留的关键词（即使包含敏感词）
WHITELIST = ['六氟', '电解液', '隔膜', '储能', '电力设备', '特高压', '锂盐', '碳酸锂']

with open('copilot/data/resonance_audit_picks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print('=== 按新黑名单重新审核 ===\n')
total_picks = 0
for rec in data['records']:
    for pick in rec['picks']:
        total_picks += 1
        sector = pick.get('sector', '')
        
        # 检查是否命中白名单（保留）
        is_whitelist = any(kw in sector for kw in WHITELIST)
        
        # 检查是否命中黑名单
        is_blacklist = any(kw in sector for kw in BLACKLIST_STRICT)
        
        if is_blacklist and not is_whitelist:
            print(f"❌ 应删除: {pick['name']}({pick['code']}) - {sector}")
        elif is_whitelist:
            print(f"✅ 保留(白名单): {pick['name']}({pick['code']}) - {sector}")

print(f'\n当前picks总数: {total_picks}')
