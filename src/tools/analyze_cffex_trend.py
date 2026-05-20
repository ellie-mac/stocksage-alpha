#!/usr/bin/env python3
"""分析中信期货 IF/IH/IC/IM 主席持仓最近 N 天的趋势"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
N = int(sys.argv[1]) if len(sys.argv) > 1 else 10

hist_path = ROOT / "data" / "cffex_citic_history.json"
if not hist_path.exists():
    print("history file not found")
    sys.exit(1)

raw = json.loads(hist_path.read_text(encoding="utf-8"))
# history is a list of daily snapshots, each containing `items` (4 stock indices)
snapshots = raw if isinstance(raw, list) else [raw]

by_sym: dict[str, list] = {}
for snap in snapshots:
    for e in snap.get("items", []) if isinstance(snap, dict) else []:
        s = e.get("symbol")
        if s:
            by_sym.setdefault(s, []).append(e)

for sym in ("IF", "IH", "IC", "IM"):
    if sym not in by_sym:
        continue
    rows = sorted(by_sym[sym], key=lambda r: r.get("trade_date", ""))
    rows = rows[-N:]
    print(f"\n=== {sym} ({len(rows)} days, latest first) ===")
    print(f"{'date':<10} {'short':>8} {'Δshort':>8} {'long':>8} {'Δlong':>8} {'net':>8}")
    for r in rows[::-1]:
        d = r.get("trade_date", "?")
        sq = r.get("short_qty", 0)
        sc = r.get("short_change", 0)
        lq = r.get("long_qty", 0)
        lc = r.get("long_change", 0)
        net = sq - lq
        print(f"{d:<10} {sq:>8} {sc:>+8} {lq:>8} {lc:>+8} {net:>+8}")
