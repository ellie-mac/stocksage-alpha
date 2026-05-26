"""
stock_fetcher.py - 通用行情/基本面数据获取模块
支持：实时行情、财务指标、主力资金、近期新闻
"""
import requests
import json
import time
from typing import Optional

PROXIES = {'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'}
TIMEOUT = 12

def _market_prefix(code: str) -> str:
    """股票代码 -> 东财secid前缀 (0=深 1=沪)"""
    if code.startswith(('6', '688')):
        return f'1.{code}'
    else:
        return f'0.{code}'

def _tencent_prefix(code: str) -> str:
    """股票代码 -> 腾讯接口前缀"""
    if code.startswith(('6', '688')):
        return f'sh{code}'
    else:
        return f'sz{code}'


def fetch_realtime_batch(codes: list[str]) -> dict:
    """
    批量拉取实时行情(腾讯接口，稳定)
    返回: {code: {name, price, chg_pct, high, low, open, volume, amount, turnover}}
    """
    tencent_codes = ','.join([_tencent_prefix(c) for c in codes])
    r = requests.get(f'https://qt.gtimg.cn/q={tencent_codes}', timeout=TIMEOUT, proxies=PROXIES)
    r.encoding = 'gbk'
    results = {}
    for line in r.text.strip().split(';'):
        if '~' not in line:
            continue
        p = line.split('~')
        if len(p) < 45:
            continue
        code = p[2]
        results[code] = {
            'name': p[1],
            'price': float(p[3]) if p[3] else 0,
            'chg_pct': float(p[32]) if p[32] else 0,
            'high': float(p[33]) if p[33] else 0,
            'low': float(p[34]) if p[34] else 0,
            'open': float(p[5]) if p[5] else 0,
            'volume': float(p[36]) if p[36] else 0,  # 成交量(手)
            'amount': float(p[37]) if p[37] else 0,  # 成交额(万)
            'turnover': float(p[38]) if p[38] else 0,  # 换手率%
            'pe': float(p[39]) if p[39] and p[39] != '' else 0,
            'pb': float(p[46]) if len(p) > 46 and p[46] else 0,
        }
    return results


def fetch_realtime_hk(codes: list[str]) -> dict:
    """
    批量拉取港股实时行情(腾讯接口)
    codes: ['09660', '00696', ...]
    """
    tencent_codes = ','.join([f'hk{c}' for c in codes])
    r = requests.get(f'https://qt.gtimg.cn/q={tencent_codes}', timeout=TIMEOUT, proxies=PROXIES)
    r.encoding = 'gbk'
    results = {}
    for line in r.text.strip().split(';'):
        if '~' not in line:
            continue
        p = line.split('~')
        if len(p) < 40:
            continue
        code = p[2]
        results[code] = {
            'name': p[1],
            'price': float(p[3]) if p[3] else 0,
            'chg_pct': float(p[32]) if p[32] else 0,
            'high': float(p[33]) if p[33] else 0,
            'low': float(p[34]) if p[34] else 0,
            'open': float(p[5]) if p[5] else 0,
            'volume': float(p[36]) if p[36] else 0,
            'amount': float(p[37]) if p[37] else 0,
        }
    return results


def fetch_eastmoney_multi(codes: list[str]) -> dict:
    """
    东财批量接口 - 行情+换手+量比+涨幅等
    比腾讯多: 量比、5日/60日涨幅、年涨幅、总市值
    注意: 该接口0:00-6:00及午间可能502
    """
    secids = ','.join([_market_prefix(c) for c in codes])
    url = (f'https://push2.eastmoney.com/api/qt/ulist.np/get?'
           f'fields=f2,f3,f8,f10,f12,f14,f15,f16,f17,f20,f24,f25,f116,f117'
           f'&secids={secids}')
    try:
        r = requests.get(url, timeout=TIMEOUT, proxies=PROXIES)
        if r.status_code != 200 or not r.text.strip().startswith('{'):
            return {}
        data = r.json()
    except:
        return {}
    results = {}
    if data.get('data') and data['data'].get('diff'):
        for item in data['data']['diff']:
            code = str(item.get('f12', ''))
            results[code] = {
                'name': item.get('f14', ''),
                'price': item.get('f2', 0) / 100 if isinstance(item.get('f2'), (int, float)) else 0,
                'chg_pct': item.get('f3', 0) / 100 if isinstance(item.get('f3'), (int, float)) else 0,
                'turnover': item.get('f8', 0) / 100 if isinstance(item.get('f8'), (int, float)) else 0,
                'vol_ratio': item.get('f10', 0) / 100 if isinstance(item.get('f10'), (int, float)) else 0,
                'chg_60d': item.get('f24', 0) / 100 if isinstance(item.get('f24'), (int, float)) else 0,
                'chg_ytd': item.get('f25', 0) / 100 if isinstance(item.get('f25'), (int, float)) else 0,
                'mcap': item.get('f116', 0) if isinstance(item.get('f116'), (int, float)) else 0,
            }
    return results


def fetch_finance(code: str) -> dict:
    """
    拉取单只股票财务指标(东财datacenter接口)
    返回: {roe, rev_growth, profit_growth, gross_margin, net_margin, eps, bps, debt_ratio, report}
    """
    # 判断市场后缀
    suffix = 'SH' if code.startswith(('6', '688')) else 'SZ'
    secucode = f'{code}.{suffix}'
    url = (f'https://datacenter.eastmoney.com/securities/api/data/get?'
           f'type=RPT_F10_FINANCE_MAINFINADATA&sty=ALL'
           f'&filter=(SECUCODE=%22{secucode}%22)&p=1&ps=1&sr=-1&st=REPORT_DATE')
    try:
        r = requests.get(url, timeout=TIMEOUT, proxies=PROXIES)
        d = r.json()
        if not d.get('result') or not d['result'].get('data'):
            return {'error': 'no data'}
        item = d['result']['data'][0]
        return {
            'roe': item.get('ROEJQ'),
            'rev_growth': item.get('TOTALOPERATEREVETZ'),
            'profit_growth': item.get('PARENTNETPROFITTZ'),
            'gross_margin': item.get('XSMLL'),
            'net_margin': item.get('XSJLL'),
            'eps': item.get('EPSJB'),
            'bps': item.get('BPS'),
            'debt_ratio': item.get('ZCFZL'),
            'report': item.get('REPORT_DATE_NAME', ''),
            'revenue': item.get('TOTALOPERATEREVE', 0),
            'net_profit': item.get('PARENTNETPROFIT', item.get('MLR', 0)),
        }
    except Exception as e:
        return {'error': str(e)}


def fetch_main_flow(code: str, days: int = 5) -> list[dict]:
    """
    主力资金流向(近N日)
    返回: [{date, main_net, main_pct}, ...]
    """
    secid = _market_prefix(code)
    url = (f'https://push2.eastmoney.com/api/qt/stock/fflow/kline/get?'
           f'secid={secid}&fields1=f1,f2,f3&fields2=f51,f52,f53,f54,f55,f56&klt=101&lmt={days}')
    try:
        r = requests.get(url, timeout=TIMEOUT, proxies=PROXIES)
        if r.status_code != 200 or not r.text.strip().startswith('{'):
            return []
        d = r.json().get('data', {})
        klines = d.get('klines', [])
        results = []
        for kl in klines:
            parts = kl.split(',')
            if len(parts) >= 3:
                results.append({
                    'date': parts[0],
                    'main_net': float(parts[1]) if parts[1] != '-' else 0,
                    'retail_net': float(parts[2]) if parts[2] != '-' else 0,
                })
        return results
    except:
        return []


def fetch_news(code: str, count: int = 5) -> list[dict]:
    """
    获取个股最新资讯(东财搜索)
    返回: [{title, date, url}, ...]
    """
    url = (f'https://search-api-web.eastmoney.com/search/jsonp?cb=x&param='
           f'{{"uid":"","keyword":"{code}","type":["cmsArticleWebOld"],'
           f'"client":"web","clientType":"web","clientVersion":"curr",'
           f'"param":{{"cmsArticleWebOld":{{"searchScope":"default","sort":"default",'
           f'"pageIndex":1,"pageSize":{count},"preTag":"","postTag":""}}}}}}')
    try:
        r = requests.get(url, timeout=TIMEOUT, proxies=PROXIES)
        text = r.text
        start = text.index('(') + 1
        end = text.rindex(')')
        data = json.loads(text[start:end])
        articles = data.get('result', {}).get('cmsArticleWebOld', {}).get('list', [])
        return [{'title': a.get('title', ''), 'date': a.get('date', ''),
                 'url': a.get('url', '')} for a in articles[:count]]
    except:
        return []


def fetch_concept_tags(code: str) -> list[str]:
    """获取个股所属概念板块"""
    secid = _market_prefix(code)
    url = f'https://push2.eastmoney.com/api/qt/slist/get?secid={secid}&spt=3&fields=f12,f14'
    try:
        r = requests.get(url, timeout=TIMEOUT, proxies=PROXIES)
        d = r.json().get('data', {})
        if d and d.get('diff'):
            return [item.get('f14', '') for item in d['diff']]
        return []
    except:
        return []


if __name__ == '__main__':
    # 测试
    print("测试 fetch_realtime_batch:")
    data = fetch_realtime_batch(['300505', '601069', '688205'])
    for code, d in data.items():
        print(f"  {d['name']} {code}: {d['price']} ({d['chg_pct']}%)")
    
    print("\n测试 fetch_finance:")
    fin = fetch_finance('300505')
    print(f"  川金诺: {fin}")
    
    print("\n测试 fetch_main_flow:")
    flow = fetch_main_flow('300505', 5)
    for f in flow:
        print(f"  {f['date']}: 主力净流入 {f['main_net']/10000:.0f}万")
