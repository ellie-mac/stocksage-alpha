"""
策略共振选股跟踪器
- 记录每次共振选出的股票及入选价格
- T+5/T+10 后自动回测收益
- 汇总统计胜率/平均收益

用法:
  python -X utf8 src/strategy_tracker.py --log       # 记录今日共振结果(由unified_monitor自动调用)
  python -X utf8 src/strategy_tracker.py --check     # 检查历史picks的T+5/T+10表现
  python -X utf8 src/strategy_tracker.py --report    # 输出胜率报告
"""
import json, os, sys, urllib.request
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent / 'data'
LOG_FILE = DATA_DIR / 'resonance_picks_log.json'


def load_log():
    if LOG_FILE.exists():
        return json.loads(LOG_FILE.read_text(encoding='utf-8'))
    return []


def save_log(data):
    LOG_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


def get_price(market, code):
    """获取当前价格"""
    prefix = 'sh' if market == 'sh' else 'sz'
    url = f'http://qt.gtimg.cn/q={prefix}{code}'
    try:
        resp = urllib.request.urlopen(url, timeout=10).read().decode('gbk')
        parts = resp.split('~')
        if len(parts) > 3 and parts[3]:
            return float(parts[3])
    except:
        pass
    return None


def log_picks(picks, date_str=None):
    """记录今日共振选出的picks"""
    log = load_log()
    date_str = date_str or datetime.now().strftime('%Y-%m-%d')
    
    # 避免重复记录同一天
    existing_dates = {entry['date'] for entry in log}
    if date_str in existing_dates:
        # 更新当天记录
        log = [e for e in log if e['date'] != date_str]
    
    for pick in picks:
        code = pick['code']
        # 判断市场
        market = 'sh' if code.startswith('6') else 'sz'
        price = get_price(market, code)
        
        entry = {
            'date': date_str,
            'code': code,
            'name': pick['name'],
            'score': pick['score'],
            'strategies': pick.get('strategies', []),
            'tiers': pick.get('tiers', []),
            'entry_price': price,
            'in_portfolio': pick.get('in_portfolio', False),
            't5_price': None,
            't5_return': None,
            't5_date': None,
            't10_price': None,
            't10_return': None,
            't10_date': None,
        }
        log.append(entry)
    
    save_log(log)
    print(f'[OK] 记录{len(picks)}只共振picks ({date_str})')


def check_performance():
    """检查历史picks的T+5/T+10表现"""
    log = load_log()
    today = datetime.now()
    updated = 0
    
    for entry in log:
        if not entry['entry_price']:
            continue
        
        pick_date = datetime.strptime(entry['date'], '%Y-%m-%d')
        days_elapsed = (today - pick_date).days
        code = entry['code']
        market = 'sh' if code.startswith('6') else 'sz'
        
        # T+5 检查 (5个交易日 ≈ 7自然日)
        if days_elapsed >= 7 and entry['t5_price'] is None:
            price = get_price(market, code)
            if price:
                entry['t5_price'] = price
                entry['t5_return'] = round((price - entry['entry_price']) / entry['entry_price'] * 100, 2)
                entry['t5_date'] = today.strftime('%Y-%m-%d')
                updated += 1
        
        # T+10 检查 (10个交易日 ≈ 14自然日)
        if days_elapsed >= 14 and entry['t10_price'] is None:
            price = get_price(market, code)
            if price:
                entry['t10_price'] = price
                entry['t10_return'] = round((price - entry['entry_price']) / entry['entry_price'] * 100, 2)
                entry['t10_date'] = today.strftime('%Y-%m-%d')
                updated += 1
    
    save_log(log)
    print(f'[OK] 更新了{updated}条记录的收益数据')


def report():
    """输出胜率报告"""
    log = load_log()
    if not log:
        print('暂无数据')
        return
    
    # T+5 统计
    t5_data = [e for e in log if e['t5_return'] is not None]
    t10_data = [e for e in log if e['t10_return'] is not None]
    
    print('=' * 50)
    print('📊 策略共振选股跟踪报告')
    print('=' * 50)
    print(f'总记录: {len(log)}只')
    print()
    
    if t5_data:
        wins = sum(1 for e in t5_data if e['t5_return'] > 0)
        avg_ret = sum(e['t5_return'] for e in t5_data) / len(t5_data)
        print(f'T+5 统计 ({len(t5_data)}只已到期):')
        print(f'  胜率: {wins}/{len(t5_data)} = {wins/len(t5_data)*100:.1f}%')
        print(f'  平均收益: {avg_ret:+.2f}%')
        print(f'  最大盈利: {max(e["t5_return"] for e in t5_data):+.2f}%')
        print(f'  最大亏损: {min(e["t5_return"] for e in t5_data):+.2f}%')
        
        # 按score分组
        for score_min in [4, 3, 2]:
            group = [e for e in t5_data if e['score'] >= score_min]
            if group:
                g_wins = sum(1 for e in group if e['t5_return'] > 0)
                g_avg = sum(e['t5_return'] for e in group) / len(group)
                print(f'  score>={score_min}: 胜率{g_wins}/{len(group)}={g_wins/len(group)*100:.0f}% 均收益{g_avg:+.1f}%')
    else:
        print('T+5: 暂无到期数据（需等7天）')
    
    print()
    
    if t10_data:
        wins = sum(1 for e in t10_data if e['t10_return'] > 0)
        avg_ret = sum(e['t10_return'] for e in t10_data) / len(t10_data)
        print(f'T+10 统计 ({len(t10_data)}只已到期):')
        print(f'  胜率: {wins}/{len(t10_data)} = {wins/len(t10_data)*100:.1f}%')
        print(f'  平均收益: {avg_ret:+.2f}%')
    else:
        print('T+10: 暂无到期数据（需等14天）')
    
    # 最近picks
    print()
    print('最近选股:')
    recent = sorted(log, key=lambda x: x['date'], reverse=True)[:10]
    for e in recent:
        ret_str = f'T5:{e["t5_return"]:+.1f}%' if e['t5_return'] is not None else 'T5:待验'
        portfolio_tag = ' 💰持仓' if e.get('in_portfolio') else ''
        print(f'  {e["date"]} {e["name"]}({e["code"]}) score={e["score"]} 入选价{e["entry_price"]:.2f} {ret_str}{portfolio_tag}')


if __name__ == '__main__':
    if '--log' in sys.argv:
        # 从strategy_pool.json读取今日picks并记录
        pool_file = DATA_DIR / 'strategy_pool.json'
        if pool_file.exists():
            pool = json.loads(pool_file.read_text(encoding='utf-8'))
            if pool:
                log_picks(pool)
            else:
                print('[INFO] strategy_pool为空')
        else:
            print('[WARN] strategy_pool.json不存在')
    elif '--check' in sys.argv:
        check_performance()
    elif '--report' in sys.argv:
        report()
    else:
        print(__doc__)
