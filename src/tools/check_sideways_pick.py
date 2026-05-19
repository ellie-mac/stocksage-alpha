#!/usr/bin/env python3
"""检查横盘策略某只票为什么入选 + 看实际价格走势。"""
from __future__ import annotations
import json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

TARGET_CODE = sys.argv[1] if len(sys.argv) > 1 else "002407"

d = json.loads((ROOT / "data" / "sideways_latest.json").read_text(encoding="utf-8"))
print(f"sideways_latest.json date={d.get('date')} total={len(d.get('all_picks', []))}")
hit = None
for p in d.get("all_picks", []):
    if str(p.get("code", "")).endswith(TARGET_CODE) or TARGET_CODE in str(p.get("code", "")):
        hit = p
        break
if not hit:
    print(f"{TARGET_CODE} 不在 sideways_latest.json")
    sys.exit(0)

print(f"\n=== {hit.get('name', '?')} ({hit.get('code')}) ===")
for k, v in hit.items():
    print(f"  {k}: {v}")

# 真实价格走势
print(f"\n=== 价格历史（最近 40 天）===")
import fetcher
df = fetcher.get_price_history(TARGET_CODE, days=40)
if df is None or df.empty:
    print("  无价格数据")
    sys.exit(0)

# 找列名（中英文都可能）
date_col = next((c for c in df.columns if c.lower() in ("date", "trade_date", "日期")), df.columns[0])
close_col = next((c for c in df.columns if c.lower() in ("close", "收盘", "收盘价")), None)
high_col = next((c for c in df.columns if c.lower() in ("high", "最高")), None)
low_col = next((c for c in df.columns if c.lower() in ("low", "最低")), None)
pct_col = next((c for c in df.columns if c.lower() in ("change_pct", "pct_chg", "涨跌幅")), None)

print(f"  columns: {list(df.columns)}")
recent = df.tail(40)
if close_col:
    closes = recent[close_col].astype(float)
    cmin, cmax = closes.min(), closes.max()
    print(f"  close range last 40d: {cmin:.2f} ~ {cmax:.2f}  ({(cmax-cmin)/cmin*100:+.1f}%)")
    closes20 = closes.tail(20)
    c20_min, c20_max = closes20.min(), closes20.max()
    print(f"  close range last 20d: {c20_min:.2f} ~ {c20_max:.2f}  ({(c20_max-c20_min)/c20_min*100:+.1f}%)")
    closes5 = closes.tail(5)
    c5_min, c5_max = closes5.min(), closes5.max()
    print(f"  close range last  5d: {c5_min:.2f} ~ {c5_max:.2f}  ({(c5_max-c5_min)/c5_min*100:+.1f}%)")

print(f"\n  最近 10 个交易日:")
for _, row in recent.tail(10).iterrows():
    cells = [str(row[c])[:12] for c in [date_col, close_col, high_col, low_col, pct_col] if c]
    print(f"    {' | '.join(cells)}")
