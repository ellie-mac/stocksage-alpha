"""按行业分类分析表现"""
import json
from collections import defaultdict

with open('copilot/data/resonance_audit_picks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 定义行业分类
CORE_TRACKS = ['半导体', 'PCB', '覆铜板', '电子布', '靶材', '光掩模', 'CMP', '光刻胶', '前驱体', '测试', '封装', '连接器', '光模块', 'CPO', '存储']
ROBOT_TRACKS = ['机器人', 'robot']
OTHER_TRACKS = []

sector_stats = defaultdict(list)

for rec in data['records']:
    for pick in rec['picks']:
        if not pick.get('entry_open'):
            continue
        
        entry = pick['entry_open']
        sector = pick.get('sector', '未分类')
        
        # 获取最新收益
        latest_pnl = None
        for i in range(20, 0, -1):
            key = f'T{i}_close'
            if key in pick:
                latest_pnl = (pick[key] / entry - 1) * 100
                break
        
        if latest_pnl is not None:
            sector_stats[sector].append({
                'name': pick['name'],
                'code': pick['code'],
                'pnl': latest_pnl
            })

# 分类统计
core_pnls = []
robot_pnls = []
other_pnls = []

for sector, picks in sector_stats.items():
    is_core = any(track in sector for track in CORE_TRACKS)
    is_robot = any(track in sector.lower() for track in ROBOT_TRACKS)
    
    for p in picks:
        if is_robot:
            robot_pnls.append((sector, p))
        elif is_core:
            core_pnls.append((sector, p))
        else:
            other_pnls.append((sector, p))

print('=== 半导体产业链（材料/设备/封测） ===')
print(f'数量: {len(core_pnls)}只')
if core_pnls:
    avg = sum(p[1]['pnl'] for p in core_pnls) / len(core_pnls)
    wins = len([p for p in core_pnls if p[1]['pnl'] > 0])
    print(f'胜率: {wins}/{len(core_pnls)} ({wins/len(core_pnls)*100:.1f}%)')
    print(f'平均收益: {avg:+.2f}%')
    print()
    print('前5名:')
    for sector, p in sorted(core_pnls, key=lambda x: x[1]['pnl'], reverse=True)[:5]:
        print(f"  {p['name']} ({p['code']}) {sector}: {p['pnl']:+.2f}%")

print()
print('=== 机器人板块 ===')
print(f'数量: {len(robot_pnls)}只')
if robot_pnls:
    avg = sum(p[1]['pnl'] for p in robot_pnls) / len(robot_pnls)
    wins = len([p for p in robot_pnls if p[1]['pnl'] > 0])
    print(f'胜率: {wins}/{len(robot_pnls)} ({wins/len(robot_pnls)*100:.1f}%)')
    print(f'平均收益: {avg:+.2f}%')
    print()
    for sector, p in robot_pnls:
        print(f"  {p['name']} ({p['code']}) {sector}: {p['pnl']:+.2f}%")

print()
print('=== 其他板块 ===')
print(f'数量: {len(other_pnls)}只')
if other_pnls:
    avg = sum(p[1]['pnl'] for p in other_pnls) / len(other_pnls)
    wins = len([p for p in other_pnls if p[1]['pnl'] > 0])
    print(f'胜率: {wins}/{len(other_pnls)} ({wins/len(other_pnls)*100:.1f}%)')
    print(f'平均收益: {avg:+.2f}%')
    print()
    print('代表:')
    for sector, p in sorted(other_pnls, key=lambda x: x[1]['pnl'], reverse=True)[:3]:
        print(f"  {p['name']} ({p['code']}) {sector}: {p['pnl']:+.2f}%")

print()
if core_pnls and robot_pnls:
    core_avg = sum(p[1]['pnl'] for p in core_pnls) / len(core_pnls)
    robot_avg = sum(p[1]['pnl'] for p in robot_pnls) / len(robot_pnls)
    print(f'💡 半导体产业链 vs 机器人 = {core_avg:+.2f}% vs {robot_avg:+.2f}% (差{core_avg - robot_avg:+.2f}%)')
