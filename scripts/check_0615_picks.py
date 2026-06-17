"""检查6-15推送的票T+2数据"""
import json

with open('copilot/data/resonance_audit_picks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 找6-15推送的
for rec in data['records']:
    if rec.get('audit_date') == '2026-06-15' or rec.get('date') == '2026-06-15':
        print(f"找到6-15推送，共{len(rec['picks'])}只\n")
        for p in rec['picks']:
            name = p['name']
            code = p['code']
            has_t0 = 'T0_close' in p
            has_t1 = 'T1_close' in p
            has_t2 = 'T2_close' in p
            
            t0_val = p.get('T0_close', 'N/A')
            t1_val = p.get('T1_close', 'N/A')
            
            print(f"  {name}({code}):")
            print(f"    T0_close (6/16): {t0_val}")
            print(f"    T1_close (6/17): {t1_val}")
            print(f"    T2_close: {p.get('T2_close', 'N/A')}")
            print()
