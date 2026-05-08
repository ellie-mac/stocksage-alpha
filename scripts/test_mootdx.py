import sys, time
sys.path.insert(0, r'C:\Users\jiapeichen\repos\stocksage-alpha\scripts')

print('=== mootdx 直连测试 ===', flush=True)
try:
    from mootdx.quotes import Quotes
    t0 = time.time()
    tdx = Quotes.factory(market='std')
    df = tdx.bars(symbol='000001', frequency=9, offset=600)
    elapsed = time.time() - t0
    if df is not None and not df.empty:
        print(f'mootdx OK  {len(df)} rows  elapsed={elapsed:.2f}s', flush=True)
        print('columns:', df.columns.tolist(), flush=True)
        print(df.tail(2).to_string(), flush=True)
    else:
        print(f'mootdx FAIL (empty)  elapsed={elapsed:.2f}s', flush=True)
except Exception as e:
    import traceback
    print(f'mootdx ERROR: {e}', flush=True)
    traceback.print_exc()

print(flush=True)
print('=== fetcher.get_price_history 测试（001979 招商蛇口）===', flush=True)
try:
    import fetcher
    code = '001979'
    t0 = time.time()
    df = fetcher.get_price_history(code, days=365)
    elapsed = time.time() - t0
    if df is not None and not df.empty:
        last_date = df.iloc[-1]['date']
        print(f'OK  {len(df)} rows  last={last_date}  elapsed={elapsed:.2f}s', flush=True)
    else:
        print(f'FAIL  elapsed={elapsed:.2f}s', flush=True)
except Exception as e:
    import traceback
    print(f'fetcher ERROR: {e}', flush=True)
    traceback.print_exc()

print(flush=True)
print('=== concept_map 强制重建测试 ===', flush=True)
try:
    import cache as _cache
    import fetcher

    # 检查缓存状态（小 TTL 让已有缓存看起来像过期）
    old = _cache.get('concept_reverse_map', 10)  # 10s TTL — 必定过期
    print(f'10s TTL 缓存命中: {old is not None}', flush=True)

    valid = _cache.get('concept_reverse_map', 7*24*3600)
    if valid:
        print(f'正式缓存（7天）命中: True  {len(valid)} 只股票', flush=True)
    else:
        print('正式缓存未命中，触发重建...', flush=True)
        t0 = time.time()
        m = fetcher._build_concept_reverse_map()
        elapsed = time.time() - t0
        print(f'重建完成  {len(m)} 只股票  elapsed={elapsed:.1f}s', flush=True)
except Exception as e:
    import traceback
    print(f'concept ERROR: {e}', flush=True)
    traceback.print_exc()
