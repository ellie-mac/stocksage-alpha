"""
portfolio_analyzer.py - 持仓深度分析
综合：行情、基本面、资金面、消息面，给出风险评估
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from stock_fetcher import (
    fetch_realtime_batch, fetch_realtime_hk, fetch_eastmoney_multi,
    fetch_finance, fetch_main_flow, fetch_news, fetch_concept_tags
)
import time


def analyze_stock(code: str, name: str, cost: float, realtime: dict, em_data: dict) -> dict:
    """
    单只股票综合分析
    返回结构化的分析结果
    """
    result = {
        'code': code, 'name': name, 'cost': cost,
        'price': 0, 'pnl': 0, 'risk_level': 'unknown',
        'fundamentals': {}, 'flow': [], 'news': [], 'concepts': [],
        'diagnosis': ''
    }
    
    # 实时行情
    rt = realtime.get(code, {})
    em = em_data.get(code, {})
    price = rt.get('price', 0) or em.get('price', 0)
    result['price'] = price
    if cost > 0 and price > 0:
        result['pnl'] = (price - cost) / cost * 100
    
    result['chg_today'] = rt.get('chg_pct', 0) or em.get('chg_pct', 0)
    result['chg_60d'] = em.get('chg_60d', 0)
    result['chg_ytd'] = em.get('chg_ytd', 0)
    result['turnover'] = rt.get('turnover', 0) or em.get('turnover', 0)
    result['vol_ratio'] = em.get('vol_ratio', 0)
    
    # 基本面
    result['fundamentals'] = fetch_finance(code)
    time.sleep(0.2)
    
    # 主力资金
    result['flow'] = fetch_main_flow(code, 5)
    time.sleep(0.2)
    
    # 新闻
    result['news'] = fetch_news(code, 3)
    time.sleep(0.2)
    
    # 概念标签
    result['concepts'] = fetch_concept_tags(code)
    time.sleep(0.1)
    
    return result


def print_analysis(r: dict):
    """格式化输出单只股票分析"""
    print(f"\n{'='*75}")
    pnl_str = f"{r['pnl']:+.1f}%" if r['cost'] > 0 else "无成本"
    print(f"  [{r['code']}] {r['name']}  成本:{r['cost']:.3f} → 现价:{r['price']:.2f}  盈亏:{pnl_str}")
    print(f"  今日:{r['chg_today']:+.1f}% | 60日:{r['chg_60d']:+.1f}% | 年初至今:{r['chg_ytd']:+.1f}% | 换手:{r['turnover']:.1f}% | 量比:{r['vol_ratio']:.2f}")
    print(f"{'='*75}")
    
    # 基本面
    fin = r['fundamentals']
    if fin and 'error' not in fin:
        roe_s = f"{fin['roe']:.2f}%" if fin.get('roe') is not None else '-'
        rev_s = f"{fin['rev_growth']:.1f}%" if fin.get('rev_growth') is not None else '-'
        prof_s = f"{fin['profit_growth']:.1f}%" if fin.get('profit_growth') is not None else '-'
        gm_s = f"{fin['gross_margin']:.1f}%" if fin.get('gross_margin') is not None else '-'
        nm_s = f"{fin['net_margin']:.1f}%" if fin.get('net_margin') is not None else '-'
        eps_s = f"{fin['eps']:.3f}" if fin.get('eps') is not None else '-'
        debt_s = f"{fin['debt_ratio']:.1f}%" if fin.get('debt_ratio') is not None else '-'
        report = fin.get('report', '')
        print(f"  📊 基本面 ({report}): ROE={roe_s} EPS={eps_s} 负债率={debt_s}")
        print(f"            营收增速={rev_s} 净利增速={prof_s} 毛利率={gm_s} 净利率={nm_s}")
    else:
        print(f"  📊 基本面: 数据未获取 {fin.get('error','')}")
    
    # 主力资金
    flows = r['flow']
    if flows:
        total = sum(f['main_net'] for f in flows)
        recent = flows[-1] if flows else {}
        direction = '流入' if total > 0 else '流出'
        print(f"  💰 主力资金: 近{len(flows)}日累计净{direction} {abs(total)/1e4:.0f}万")
        for f in flows[-3:]:
            d = '入' if f['main_net'] > 0 else '出'
            print(f"       {f['date']}: 主力净{d} {abs(f['main_net'])/1e4:.0f}万")
    else:
        print(f"  💰 主力资金: 数据未获取")
    
    # 新闻
    if r['news']:
        print(f"  📰 近期消息:")
        for n in r['news'][:3]:
            title = n['title'].replace('<em>', '').replace('</em>', '')
            print(f"       [{n['date'][:10]}] {title[:55]}")
    
    # 概念标签
    if r['concepts']:
        print(f"  🏷️ 所属概念: {', '.join(r['concepts'][:8])}")


def run_portfolio_analysis(portfolio: list[tuple], hk_portfolio: list[tuple] = None):
    """
    运行完整持仓分析
    portfolio: [(code, name, cost), ...]
    hk_portfolio: [(hk_code, name, cost), ...]
    """
    codes = [p[0] for p in portfolio]
    
    print("正在获取批量行情...")
    realtime = fetch_realtime_batch(codes)
    em_data = fetch_eastmoney_multi(codes)
    
    # 按亏损排序
    ranked = []
    for code, name, cost in portfolio:
        rt = realtime.get(code, {})
        em = em_data.get(code, {})
        price = rt.get('price', 0) or em.get('price', 0)
        pnl = (price - cost) / cost * 100 if cost > 0 and price > 0 else 0
        ranked.append((code, name, cost, pnl))
    ranked.sort(key=lambda x: x[3])
    
    print(f"\n持仓共 {len(portfolio)} 只A股，按浮亏排序分析：\n")
    
    for code, name, cost, pnl in ranked:
        r = analyze_stock(code, name, cost, realtime, em_data)
        print_analysis(r)
    
    # 港股
    if hk_portfolio:
        print(f"\n\n{'#'*75}")
        print(f"  港股持仓分析 ({len(hk_portfolio)}只)")
        print(f"{'#'*75}")
        hk_codes = [p[0] for p in hk_portfolio]
        hk_rt = fetch_realtime_hk(hk_codes)
        for hk_code, name, cost in hk_portfolio:
            rt = {}
            for k, v in hk_rt.items():
                if hk_code in k:
                    rt = v
                    break
            price = rt.get('price', 0)
            pnl = (price - cost) / cost * 100 if cost > 0 and price > 0 else 0
            print(f"\n  [{hk_code}] {name}  成本:{cost:.3f} → 现价:{price:.3f}  盈亏:{pnl:+.1f}%")
            print(f"  今日: {rt.get('chg_pct', 0):+.2f}%")


if __name__ == '__main__':
    # 当前持仓
    portfolio = [
        ('601069', '西部黄金', 33.213),
        ('600801', '华新建材', 22.75),
        ('603993', '洛阳钼业', 23.39),
        ('000933', '神火股份', 33.454),
        ('002842', '翔鹭钨业', 38.723),
        ('600711', '盛屯矿业', 14.184),
        ('605599', '菜百股份', 0),
        ('002240', '盛新锂能', 54.454),
        ('600036', '招商银行', 40.56),
        ('688205', '德科立', 240.18),
        ('002170', '芭田股份', 13.161),
        ('002033', '丽江股份', 9.884),
        ('600309', '万华化学', 79.007),
        ('301219', '腾远钴业', 83.507),
        ('000792', '盐湖股份', 33.712),
        ('002468', '申通快递', 18.06),
        ('301358', '湖南裕能', 97.935),
        ('603444', '吉比特', 370.133),
        ('688114', '华大智造', 60.094),
        ('603893', '瑞芯微', 191.627),
        ('300505', '川金诺', 35.359),
        ('002810', '山东赫达', 26.48),
        ('000833', '粤桂股份', 24.987),
        ('605228', '神通科技', 14.736),
        ('300570', '太辰光', 161.893),
        ('300970', '华绿生物', 35.503),
        ('300657', '弘信电子', 48.764),
        ('300102', '乾照光电', 33.476),
        ('002213', '大为股份', 38.923),
        ('600522', '中天科技', 42.169),
        ('300475', '香农芯创', 159.955),
        ('688059', '华锐精密', 122.116),
        ('688257', '新锐股份', 75.092),
        ('688525', '佰维存储', 260.614),
        ('300953', '震裕科技', 222.216),
        ('603588', '高能环境', 10.236),
    ]
    
    hk_portfolio = [
        ('09660', '地平线机器人', 9.332),
        ('00696', '中国民航信息网络', 10.29),
        ('02498', '速腾聚创', 35.086),
        ('06049', '保利物业', 33.377),
        ('00506', '中国食品', 3.873),
        ('06862', '海底捞', 15.796),
    ]
    
    run_portfolio_analysis(portfolio, hk_portfolio)
