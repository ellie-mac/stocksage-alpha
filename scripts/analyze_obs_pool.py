"""分析观察池票 vs 其他票的表现"""
import json

with open('copilot/data/resonance_audit_picks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 观察池股票代码
obs_pool = ['603931', '688396', '600237', '300570', '002876', '688368', '688106', '300666', '688099', '688005', '603078', '002886']

obs_picks = []
other_picks = []

for rec in data['records']:
    for pick in rec['picks']:
        if not pick.get('entry_open'):
            continue
        
        code = pick['code']
        entry = pick['entry_open']
        
        # 获取最新收益
        latest_pnl = None
        for i in range(20, 0, -1):
            key = f'T{i}_close'
            if key in pick:
                latest_pnl = (pick[key] / entry - 1) * 100
                break
        
        if latest_pnl is not None:
            info = {
                'name': pick['name'],
                'code': code,
                'sector': pick.get('sector', ''),
                'pnl': latest_pnl
            }
            
            if code in obs_pool:
                obs_picks.append(info)
            else:
                other_picks.append(info)

print('=== 观察池股票表现 ===')
print(f'数量: {len(obs_picks)}只')
if obs_picks:
    avg_obs = sum(p['pnl'] for p in obs_picks) / len(obs_picks)
    wins_obs = len([p for p in obs_picks if p['pnl'] > 0])
    print(f'胜率: {wins_obs}/{len(obs_picks)} ({wins_obs/len(obs_picks)*100:.1f}%)')
    print(f'平均收益: {avg_obs:+.2f}%')
    print()
    print('前5名:')
    for p in sorted(obs_picks, key=lambda x: x['pnl'], reverse=True)[:5]:
        print(f"  {p['name']} ({p['code']}) {p['sector']}: {p['pnl']:+.2f}%")

print()
print('=== 其他科技股表现 ===')
print(f'数量: {len(other_picks)}只')
if other_picks:
    avg_other = sum(p['pnl'] for p in other_picks) / len(other_picks)
    wins_other = len([p for p in other_picks if p['pnl'] > 0])
    print(f'胜率: {wins_other}/{len(other_picks)} ({wins_other/len(other_picks)*100:.1f}%)')
    print(f'平均收益: {avg_other:+.2f}%')
    print()
    print('机器人板块:')
    robot_picks = [p for p in other_picks if 'robot' in p['sector'].lower() or '机器人' in p['sector']]
    if robot_picks:
        for p in robot_picks:
            print(f"  {p['name']} ({p['code']}) {p['sector']}: {p['pnl']:+.2f}%")
        avg_robot = sum(p['pnl'] for p in robot_picks) / len(robot_picks)
        print(f'  机器人平均: {avg_robot:+.2f}%')
    
    print()
    print('后5名（亏损最多）:')
    for p in sorted(other_picks, key=lambda x: x['pnl'])[:5]:
        print(f"  {p['name']} ({p['code']}) {p['sector']}: {p['pnl']:+.2f}%")

print()
if obs_picks and other_picks:
    print(f'💡 差距: 观察池 vs 其他科技 = {avg_obs - avg_other:+.2f}%')
