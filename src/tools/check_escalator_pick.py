#!/usr/bin/env python3
"""验证某只票是否被 escalator 策略收入，看它进哪一档。"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

TARGET = sys.argv[1] if len(sys.argv) > 1 else "603163"

from strategies.escalator_scan import _classify, _ma_bullish
import fetcher

df = fetcher.get_price_history(TARGET, days=40)
if df is None or df.empty:
    print(f"{TARGET}: 无价格数据")
    sys.exit(1)

closes = df["close"].astype(float).values
highs  = df["high"].astype(float).values
lows   = df["low"].astype(float).values

print(f"{TARGET}  最近 {len(closes)} 个交易日")
print(f"  close 区间: {closes.min():.2f} ~ {closes.max():.2f}")
print(f"  today: {closes[-1]:.2f}")

# MA bullish check
ma_ok = _ma_bullish(closes)
ma5  = float(np.mean(closes[-5:]))
ma10 = float(np.mean(closes[-10:]))
ma20 = float(np.mean(closes[-20:]))
print(f"  MA: close={closes[-1]:.2f}  MA5={ma5:.2f}  MA10={ma10:.2f}  MA20={ma20:.2f}  bullish={ma_ok}")

if not ma_ok:
    print("\n  → MA 预筛不通过，直接淘汰")
    sys.exit(0)

result = _classify(closes, highs, lows)
if result:
    print(f"\n  → 入选档位 {result['tier']}!")
    for k, v in result.items():
        print(f"      {k}: {v}")
else:
    print(f"\n  → 未入选任何档（不符合 E0/E1/E2 条件）")
    # 显示各档为什么不过
    from strategies.escalator_scan import _TIER_ORDER, _TIER_SPEC, _AMP_MIN, _DRAWDOWN_FLOOR
    for tier in _TIER_ORDER:
        spec = _TIER_SPEC[tier]
        n = spec["window"]
        if len(closes) < n:
            print(f"    {tier} (n={n}): bars 不够")
            continue
        win_c = closes[-n:]
        win_h = highs[-n:]
        win_l = lows[-n:]
        mean_c = float(np.mean(win_c))
        t = np.arange(n, dtype=float)
        slope, intercept = np.polyfit(t, win_c, 1)
        slope_pct = float(slope * (n - 1) / mean_c * 100)
        fit = slope * t + intercept
        ss_res = float(np.sum((win_c - fit) ** 2))
        ss_tot = float(np.sum((win_c - mean_c) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
        amp = float(np.mean((win_h - win_l) / win_c) * 100)
        rets = np.diff(win_c) / win_c[:-1] * 100
        dd = float(np.min(rets))
        print(f"    {tier} (n={n}): slope={slope_pct:+.2f}% [{spec['slope_lo']:.0f}~{spec['slope_hi']:.0f}]  "
              f"R²={r2:.3f} [≥{spec['r2_min']:.2f}]  amp={amp:.2f}% [≥{_AMP_MIN}]  dd={dd:+.2f}% [≥{_DRAWDOWN_FLOOR}]")
