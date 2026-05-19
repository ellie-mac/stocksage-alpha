#!/usr/bin/env python3
"""模拟 sideways_scan 严格档（HX0-3）判定，可选剔除最后一天，看是否入选。"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

TARGET = sys.argv[1] if len(sys.argv) > 1 else "603163"
EXCLUDE_TODAY = "--keep-today" not in sys.argv

import fetcher

df = fetcher.get_price_history(TARGET, days=40)
if df is None or df.empty:
    print(f"{TARGET}: 无价格数据")
    sys.exit(1)

close_col = next((c for c in df.columns if c.lower() in ("close", "收盘", "收盘价")), None)
date_col  = next((c for c in df.columns if c.lower() in ("date", "trade_date", "日期")), df.columns[0])
closes = df[close_col].astype(float).values

print(f"{TARGET}  最近 12 个交易日:")
for _, row in df.tail(12).iterrows():
    print(f"  {row[date_col]}  close={float(row[close_col]):.2f}")

if EXCLUDE_TODAY:
    closes = closes[:-1]
    print(f"\n[已剔除今天 {df.iloc[-1][date_col]}]")
else:
    print(f"\n[包含今天 {df.iloc[-1][date_col]}]")

# 严格档判定
_TIER_SPEC = {
    "HX0": (30, 0.05), "HX1": (20, 0.04),
    "HX2": (10, 0.03), "HX3": (5,  0.02),
}

print(f"\n{'tier':<6}{'win':>5}{'pct':>7}  {'hi':>7}{'lo':>7}{'mid':>7}  {'hi/mid-1':>10}{'1-lo/mid':>10}  {'range_pct':>10}  pass?")
print("-" * 95)
for tier, (n, pct) in _TIER_SPEC.items():
    if len(closes) < n:
        print(f"{tier:<6}{n:>5}{pct*100:>6.1f}%  not enough bars")
        continue
    window = closes[-n:]
    hi, lo = float(np.max(window)), float(np.min(window))
    if lo <= 0:
        continue
    mid = (hi + lo) / 2.0
    up = hi / mid - 1
    dn = 1 - lo / mid
    range_pct = (hi - lo) / mid * 100
    ok = up <= pct and dn <= pct
    mark = "✓ PASS" if ok else "✗"
    print(f"{tier:<6}{n:>5}{pct*100:>6.1f}%  {hi:>7.2f}{lo:>7.2f}{mid:>7.2f}  "
          f"{up*100:>9.2f}%{dn*100:>9.2f}%  {range_pct:>9.2f}%  {mark}")
