#!/usr/bin/env python3
"""
位置分析 — 快速判断股票当前处于高位/低位

用法:
    python -X utf8 src/pos_check.py 000001
    python -X utf8 src/pos_check.py 600519
"""
from __future__ import annotations
import re
import sys
from pathlib import Path

import numpy as np

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))


def pos_report(code: str) -> str:
    import fetcher as _f

    code = re.sub(r'\D', '', code)[:6].zfill(6) or code.strip().zfill(6)
    if not code.isdigit() or len(code) != 6:
        return f"❌ 无效代码: {code}"

    df = _f.get_price_history(code, days=260)
    if df is None or df.empty or len(df) < 20:
        return f"❌ {code} 无价格数据"

    close = float(df["close"].iloc[-1])
    c = df["close"].values
    h = df["high"].values
    lo = df["low"].values

    # 52-week position
    n252 = min(252, len(c))
    high52 = float(np.max(h[-n252:]))
    low52  = float(np.min(lo[-n252:]))
    pos52  = (close - low52) / (high52 - low52) * 100 if high52 > low52 else 50.0

    # MA distances
    ma20 = float(np.mean(c[-20:])) if len(c) >= 20 else close
    n60  = min(60, len(c))
    ma60 = float(np.mean(c[-n60:])) if n60 >= 10 else close
    dist20 = (close - ma20) / ma20 * 100
    dist60 = (close - ma60) / ma60 * 100

    # Bollinger band position (20-day, ±2σ)
    if len(c) >= 20:
        std20   = float(np.std(c[-20:], ddof=1))
        boll_up  = ma20 + 2 * std20
        boll_low = ma20 - 2 * std20
        boll_pos = (close - boll_low) / (boll_up - boll_low) * 100 if boll_up > boll_low else 50.0
        boll_pos = round(max(0.0, min(120.0, boll_pos)), 0)
    else:
        boll_pos = 50.0

    # Distance from 6-month high
    n120 = min(120, len(h))
    high6m = float(np.max(h[-n120:]))
    dist_high6m = (close - high6m) / high6m * 100  # ≤0

    # Verdict
    if pos52 >= 80:
        verdict = "🔴 高位"
    elif pos52 >= 60:
        verdict = "🟠 中高位"
    elif pos52 >= 40:
        verdict = "🟡 中位"
    elif pos52 >= 20:
        verdict = "🟢 中低位"
    else:
        verdict = "🟢 低位"

    # Trend context (MA alignment)
    if ma20 > ma60 and close > ma20:
        trend = "↑ 多头排列"
    elif ma20 < ma60 and close < ma20:
        trend = "↓ 空头排列"
    else:
        trend = "→ 震荡"

    # Stock name
    name = code
    try:
        q = _f.get_realtime_quote(code)
        if q and q.get("name"):
            name = q["name"]
    except Exception:
        pass

    lines = [
        f"{code} {name}  ¥{close:.2f}  {verdict}",
        f"52周区间: {low52:.2f}~{high52:.2f}  位置 {pos52:.0f}%  距6月高 {dist_high6m:+.1f}%",
        f"趋势: {trend}",
        f"MA20偏离: {dist20:+.1f}%  MA60偏离: {dist60:+.1f}%",
        f"布林位置: {int(boll_pos)}%（0=下轨  100=上轨）",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python -X utf8 src/pos_check.py 000001")
        sys.exit(1)
    print(pos_report(sys.argv[1]))
