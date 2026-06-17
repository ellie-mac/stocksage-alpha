"""
止盈/止损信号模块
- 被 unified_monitor.py 导入使用
- 根据持仓成本、当前价格、技术指标生成止盈/止损建议

规则:
1. 盈利>20% + 破MA5 + MACD日线死叉 → 建议减仓锁利
2. 盈利>30% + 连跌3天 → 提示回落风险
3. 亏损>15% + 破MA20 + 周线空头 → 建议止损
4. 从最高点回撤>10% → 提示回撤警报
"""


def check_take_profit_signals(portfolio_data):
    """
    检查持仓的止盈/止损信号
    
    Args:
        portfolio_data: list of dict, each containing:
            - name: 股票名称
            - code: 代码
            - cost: 成本价
            - price: 当前价格
            - chg: 今日涨跌幅%
            - ma5: MA5值
            - ma20: MA20值
            - dif: MACD DIF值
            - dea: MACD DEA值
            - weekly_ok: 周线是否多头
            - consecutive_down: 连跌天数
            - high_since_buy: 买入后最高价(可选)
    
    Returns:
        list of alert strings
    """
    alerts = []
    
    for stock in portfolio_data:
        name = stock['name']
        code = stock['code']
        cost = stock.get('cost', 0)
        price = stock.get('price', 0)
        ma5 = stock.get('ma5', 0)
        ma20 = stock.get('ma20', 0)
        dif = stock.get('dif', 0)
        dea = stock.get('dea', 0)
        weekly_ok = stock.get('weekly_ok', True)
        consecutive_down = stock.get('consecutive_down', 0)
        
        if not cost or not price or cost == 0:
            continue
        
        pnl_pct = (price - cost) / cost * 100  # 盈亏百分比
        above_ma5 = price > ma5 if ma5 else True
        above_ma20 = price > ma20 if ma20 else True
        macd_death = dif < dea if (dif and dea) else False
        
        signals = []
        
        # 规则1: 盈利>20% + 破MA5 + MACD死叉 → 建议减仓
        if pnl_pct > 20 and not above_ma5 and macd_death:
            signals.append('🔔 盈利%.0f%%+破MA5+MACD死叉→建议减仓锁利' % pnl_pct)
        
        # 规则2: 盈利>30% + 连跌3天 → 回落风险
        elif pnl_pct > 30 and consecutive_down >= 3:
            signals.append('⚠️ 盈利%.0f%%+连跌%d天→注意回落风险' % (pnl_pct, consecutive_down))
        
        # 规则3: 亏损>15% + 破MA20 + 周线空头 → 止损
        if pnl_pct < -15 and not above_ma20 and not weekly_ok:
            signals.append('🚨 亏损%.0f%%+破MA20+周线空头→考虑止损' % pnl_pct)
        
        # 规则4: 从高点回撤>10% (需要high_since_buy数据)
        high = stock.get('high_since_buy')
        if high and high > 0 and price < high * 0.9:
            drawdown = (high - price) / high * 100
            signals.append('📉 从高点%.1f回撤%.0f%%→注意趋势' % (high, drawdown))
        
        for sig in signals:
            alerts.append(f'  {name}({code}) {sig}')
    
    if alerts:
        header = ['', '💰 止盈/止损信号', '━' * 20]
        return header + alerts
    return []
