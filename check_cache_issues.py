#!/usr/bin/env python3
"""检查所有缓存函数是否存在同样的历史数据重拉问题"""

print('检查 fetcher.py 中的历史数据函数')
print('=' * 70)
print()

findings = [
    {
        'func': 'get_valuation_history',
        'line': '1090-1164',
        'ttl': 'smart_valuation_ttl (24h)',
        'data_type': '历史PE/PB估值',
        'issue': '🔴 严重',
        'detail': '每天拉取550天历史PE/PB，其中549天不变',
        'fix': '应该增量追加（与 price_history 同样逻辑）'
    },
    {
        'func': 'get_fund_flow',
        'line': '1303-1343',
        'ttl': 'smart_price_ttl',
        'data_type': '资金流向历史（20天）',
        'issue': '🟡 中等',
        'detail': '每天重拉20天历史，其中19天不变',
        'fix': '应该增量追加最新1天'
    },
    {
        'func': 'get_margin_data',
        'line': '1346-1370',
        'ttl': 'TTL_VALUATION (24h)',
        'data_type': '融资融券历史',
        'issue': '🟡 中等',
        'detail': '每天重拉全部历史，数据量不大但效率低',
        'fix': '可以增量追加'
    },
    {
        'func': 'get_northbound_holdings',
        'line': '1611-1630',
        'ttl': 'TTL_VALUATION (24h)',
        'data_type': '北向资金持仓历史',
        'issue': '🟡 中等',
        'detail': '每天重拉历史持仓，历史不变',
        'fix': '可以增量追加'
    },
    {
        'func': 'get_cyq',
        'line': '1372-1395',
        'ttl': '14400秒 (4h)',
        'data_type': '筹码分布',
        'issue': '🟠 特殊',
        'detail': '筹码算法可能调整，短TTL合理',
        'fix': '不需要修改'
    },
]

print('函数名                         | TTL策略                   | 问题级别')
print('-' * 70)
for f in findings:
    print(f'{f["func"]:30s} {f["ttl"]:25s} {f["issue"]}')
    print(f'  数据: {f["data_type"]}')
    print(f'  问题: {f["detail"]}')
    print(f'  建议: {f["fix"]}')
    print()

print()
print('=' * 70)
print('优先级排序')
print('=' * 70)
print()

print('🔴 P0 - 必须修复（性能影响大）:')
print('  1. get_price_history → ✅ 已修复')
print('  2. get_valuation_history')
print('     • 每天拉 5258只×550天 PE/PB')
print('     • 估计耗时 20-30 分钟')
print('     • 修复方案：与 price_history 同样逻辑')
print()

print('🟡 P1 - 应该修复（效率优化）:')
print('  1. get_fund_flow')
print('     • 每天拉 5258只×20天')
print('     • 估计耗时 5-10 分钟')
print('  2. get_northbound_holdings')
print('     • 北向资金历史持仓')
print('     • 数据量中等')
print()

print('🟢 P2 - 可选优化（影响较小）:')
print('  1. get_margin_data')
print('  2. prefetch_price 的 skip 检查')
print()

print('🟠 不需要修改:')
print('  1. get_cyq (筹码分布算法可能调整)')
print('  2. get_financial_indicators (14天TTL已经合理)')
print('  3. prefetch_fundflow (只拉2天，不是历史累积)')
print()

print('=' * 70)
print('总结')
print('=' * 70)
print()
print('核心问题：使用 smart_*_ttl 判断缓存是否过期')
print('正确做法：按数据最后日期判断，缺哪天补哪天')
print()
print('最高优先级：get_valuation_history（估计耗时20-30分钟）')
