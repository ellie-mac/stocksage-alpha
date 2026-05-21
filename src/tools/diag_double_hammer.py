"""诊断双锤策略 — 按 filter 维度统计每层 reject 数，找瓶颈。"""
import json, random, time, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import pandas as pd
import numpy as np
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed

random.seed(42)
SAMPLE_N = 50  # 小样本快速
START = "20260101"
END   = "20260520"

# 同步 v4 参数
RANGE_PCT_MIN        = 0.5
BODY_OVER_RANGE_MAX  = 0.4
LOWER_OVER_RANGE_MIN = 0.55
UPPER_OVER_RANGE_MAX = 0.15
HAMMER_WINDOW        = 5
HAMMER_MIN_COUNT     = 2
VOL_RATIO_MIN        = 0.8
AMOUNT_MIN_YI        = 0.3


def is_hammer(o, c, h, l):
    if o <= 0 or h <= l: return False
    rng = h - l
    if rng / o * 100 < RANGE_PCT_MIN: return False
    body = abs(c - o)
    if body / rng > BODY_OVER_RANGE_MAX: return False
    lower = min(o, c) - l
    upper = h - max(o, c)
    if lower / rng < LOWER_OVER_RANGE_MIN: return False
    if upper / rng > UPPER_OVER_RANGE_MAX: return False
    return True


def diag_one(code, counters):
    try:
        df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                start_date=START, end_date=END, adjust="qfq")
    except Exception:
        counters['fetch_fail'] += 1
        return
    if df is None or len(df) < 30:
        counters['too_short'] += 1
        return
    df = df.rename(columns={"日期":"date","开盘":"open","收盘":"close",
                            "最高":"high","最低":"low","成交量":"vol","成交额":"amt"})
    df = df.sort_values("date").reset_index(drop=True)
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma5v"] = df["vol"].rolling(5).mean()
    n = len(df)
    for i in range(25, n - 1):
        counters['total_bars'] += 1
        d2 = df.iloc[i]
        # 今天是锤子
        if not is_hammer(d2["open"], d2["close"], d2["high"], d2["low"]):
            counters['fail_today_not_hammer'] += 1
            continue
        counters['pass_today_hammer'] += 1
        # 窗口内 ≥2 锤
        hammers = []
        for offset in range(HAMMER_WINDOW - 1, -1, -1):
            if i - offset < 0:
                continue
            row = df.iloc[i - offset]
            if is_hammer(row["open"], row["close"], row["high"], row["low"]):
                hammers.append(i - offset)
        if len(hammers) < HAMMER_MIN_COUNT:
            counters['fail_not_enough_hammers'] += 1
            continue
        counters['pass_2_hammers'] += 1
        # 不破窗口第一个锤子的 low
        d1 = df.iloc[hammers[0]]
        if d2["low"] < d1["low"] * 0.995:
            counters['fail_break_low'] += 1
            continue
        counters['pass_no_break'] += 1
        # MA20 above
        if pd.isna(d2["ma20"]) or d2["close"] < d2["ma20"]:
            counters['fail_ma20_below'] += 1
            continue
        counters['pass_ma20_above'] += 1
        # MA20 rising
        if pd.isna(df["ma20"].iloc[i-5]):
            counters['fail_ma20_nan_5d'] += 1
            continue
        if d2["ma20"] <= df["ma20"].iloc[i-5]:
            counters['fail_ma20_not_rising'] += 1
            continue
        counters['pass_ma20_rising'] += 1
        # 量能
        if pd.isna(d2["ma5v"]) or d2["vol"] < d2["ma5v"] * VOL_RATIO_MIN:
            counters['fail_vol'] += 1
            continue
        counters['pass_vol'] += 1
        # 金额
        if (d2["amt"] / 1e8) < AMOUNT_MIN_YI:
            counters['fail_amt'] += 1
            continue
        counters['pass_amt'] += 1
        counters['TRIGGER'] += 1


def main():
    print("[1/3] 加载...")
    raw = json.load(open('data/universe_main.json', encoding='utf-8'))
    codes = []
    for x in raw:
        c = x[2:] if x[:2] in ('sh','sz','bj') else x
        if c.startswith(('8','4','9')): continue
        codes.append(c)
    random.shuffle(codes)
    codes = codes[:SAMPLE_N]
    print(f"  样本: {len(codes)}")

    from collections import defaultdict
    counters = defaultdict(int)

    print("[2/3] 诊断中...")
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(diag_one, c, counters) for c in codes]
        for f in as_completed(futs):
            try: f.result()
            except Exception: pass

    print("\n[3/3] Filter 漏斗（按顺序）：\n")
    order = [
        'total_bars',
        'pass_today_hammer', 'fail_today_not_hammer',
        'pass_2_hammers', 'fail_not_enough_hammers',
        'pass_no_break', 'fail_break_low',
        'pass_ma20_above', 'fail_ma20_below',
        'pass_ma20_rising', 'fail_ma20_not_rising', 'fail_ma20_nan_5d',
        'pass_vol', 'fail_vol',
        'pass_amt', 'fail_amt',
        'TRIGGER',
    ]
    for k in order:
        print(f"  {k:<30} {counters[k]}")


if __name__ == "__main__":
    main()
