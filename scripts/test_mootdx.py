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
print('=== mootdx 共享连接 100只批量测试 ===', flush=True)
try:
    import json
    from pathlib import Path
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from mootdx.quotes import Quotes

    universe = json.loads(Path(r'C:\Users\jiapeichen\repos\stocksage-alpha\data\universe_main.json').read_text())
    # 过滤掉北交所（bj/43/92开头），只测 SH/SZ
    szsh = [c for c in universe if c[:2] not in ('bj', '43', '92') and not c.startswith('9')]
    sample = szsh[:100]
    print(f'SH/SZ universe: {len(szsh)}  sample: {sample[:5]}...', flush=True)

    # 每线程新建连接（当前实现）
    errors = {}

    def fetch_one(code):
        try:
            tdx = Quotes.factory(market='std')
            df = tdx.bars(symbol=code, frequency=9, offset=600)
            return 'ok' if (df is not None and not df.empty) else 'empty'
        except Exception as e:
            return f'err:{type(e).__name__}:{e}'

    t0 = time.time()
    ok = fail = 0
    err_samples = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(fetch_one, c): c for c in sample}
        for fut in as_completed(futs):
            r = fut.result()
            if r == 'ok':
                ok += 1
            else:
                fail += 1
                if len(err_samples) < 3:
                    err_samples.append(f'{futs[fut]}: {r}')
    elapsed = time.time() - t0
    rate = 100 / elapsed
    eta_full = len(universe) / rate / 60
    print(f'100只  OK:{ok}  FAIL:{fail}  elapsed:{elapsed:.1f}s  速率:{rate:.1f}只/s  预计全量:{eta_full:.0f}min', flush=True)
    for s in err_samples:
        print(f'  样例错误: {s}', flush=True)

except Exception as e:
    import traceback
    print(f'批量测试 ERROR: {e}', flush=True)
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

    old = _cache.get('concept_reverse_map', 10)
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
