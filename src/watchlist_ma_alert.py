"""
Watchlist MA金叉/死叉提醒
规则：
- 5日线上穿20日线(金叉) → 提醒关注买入
- 价格破20日线(收盘价<MA20) → 提醒是否清仓
数据源：腾讯日K线接口
"""
import sys
import os
import urllib.request
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('http_proxy', 'http://127.0.0.1:7890')
os.environ.setdefault('https_proxy', 'http://127.0.0.1:7890')

# ========== 配置 ==========
# Watchlist: (market_prefix, code, name)
WATCHLIST = [
    # #9 CPO小市值弹性
    ('sz', '301205', '联特科技'),
    ('sz', '301486', '致尚科技'),
    # #10 CPO光学/光器件
    ('sh', '688025', '杰普特'),
    ('sh', '688127', '蓝特光学'),
    ('sz', '301183', '东田微'),
    # #11 存储/先进封装
    ('sh', '688123', '聚辰股份'),
    # #12 高速连接器
    ('sz', '002897', '意华股份'),
    # #13 半导体材料
    ('sz', '300666', '江丰电子'),
    ('sh', '688515', '裕太微'),
    # #14 AI电源
    ('sz', '002364', '中恒电气'),
    ('sz', '002851', '麦格米特'),
]

MA_SHORT = 5
MA_LONG = 20
KLINE_DAYS = 30  # 获取30天K线够算MA20


def get_kline(market, code):
    """获取日K线数据(腾讯接口)"""
    # market: sh/sz -> 1/0 for tencent kline api
    mkt = '1' if market == 'sh' else '0'
    url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={market}{code},day,,,{KLINE_DAYS},qfq'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urllib.request.urlopen(req, timeout=10).read().decode('utf-8')
        data = json.loads(resp)
        # 解析路径: data -> data -> {market}{code} -> day/qfqday
        stock_key = f'{market}{code}'
        stock_data = data.get('data', {}).get(stock_key, {})
        # 尝试qfqday(前复权)，否则day
        klines = stock_data.get('qfqday') or stock_data.get('day') or []
        # 每条: [date, open, close, high, low, volume]
        closes = [float(k[2]) for k in klines]
        return closes
    except Exception as e:
        print(f'  [{code}] K线获取失败: {e}')
        return []


def calc_ma(closes, period):
    """计算最近两天的MA值"""
    if len(closes) < period + 1:
        return None, None
    ma_today = sum(closes[-period:]) / period
    ma_yesterday = sum(closes[-period-1:-1]) / period
    return ma_today, ma_yesterday


def check_signals():
    """检查所有watchlist股票的MA信号"""
    now = datetime.now()
    # 非交易日跳过
    if now.weekday() >= 5:
        print('周末，跳过')
        return []

    print(f'📊 Watchlist MA信号检测 ({now.strftime("%Y-%m-%d %H:%M")})')
    print(f'   规则: MA{MA_SHORT}上穿MA{MA_LONG}=金叉买入 | 收盘<MA{MA_LONG}=破位清仓')
    print(f'   监控{len(WATCHLIST)}只标的')
    print('-' * 60)

    alerts = []

    for market, code, name in WATCHLIST:
        closes = get_kline(market, code)
        if len(closes) < MA_LONG + 1:
            print(f'  [{name}] 数据不足({len(closes)}天)，跳过')
            continue

        # 计算MA5和MA20
        ma5_today, ma5_yesterday = calc_ma(closes, MA_SHORT)
        ma20_today, ma20_yesterday = calc_ma(closes, MA_LONG)

        if ma5_today is None or ma20_today is None:
            continue

        current_price = closes[-1]
        yesterday_price = closes[-2]

        # 信号1: MA5金叉MA20 (昨天MA5<MA20, 今天MA5>=MA20)
        golden_cross = (ma5_yesterday < ma20_yesterday) and (ma5_today >= ma20_today)

        # 信号2: 价格破MA20 (昨天收盘>=MA20, 今天收盘<MA20)
        break_ma20 = (yesterday_price >= ma20_yesterday) and (current_price < ma20_today)

        # 状态显示
        status = ''
        if golden_cross:
            status = '🟢 金叉！关注买入'
            alerts.append(f'🟢 {name}({code}) MA5上穿MA20金叉！现价{current_price:.2f} MA5={ma5_today:.2f} MA20={ma20_today:.2f}')
        elif break_ma20:
            status = '🔴 破MA20！考虑清仓'
            alerts.append(f'🔴 {name}({code}) 跌破MA20！现价{current_price:.2f} MA20={ma20_today:.2f}')
        elif current_price < ma20_today:
            status = '⚠️ 在MA20下方'
        elif ma5_today > ma20_today:
            status = '✅ MA5>MA20多头'
        else:
            status = '— MA5<MA20等待'

        print(f'  {name:6s} 现价{current_price:>8.2f} | MA5={ma5_today:.2f} MA20={ma20_today:.2f} | {status}')

    print('-' * 60)

    if alerts:
        print(f'\n🚨 触发{len(alerts)}个MA信号!')
        for a in alerts:
            print(f'  {a}')

        # 飞书通知
        try:
            from notify.notify import push_feishu_card
            title = '📈 Watchlist MA信号提醒'
            push_feishu_card(title, alerts + ['', '检查K线确认是否操作！'])
            print('[OK] 飞书已通知')
        except Exception as e:
            print(f'[WARN] 飞书通知失败: {e}')
    else:
        print('\n✅ 无MA信号触发，继续观察')

    return alerts


if __name__ == '__main__':
    check_signals()
