#!/usr/bin/env python3
import sys, json
from pathlib import Path
ROOT = Path("C:/Users/jiapeichen/repos/stocksage-alpha")

# Check latest ETF picks - handle various formats
etf_files = sorted(ROOT.glob("data/etf_picks_*.json"), reverse=True)
print(f"ETF picks files: {[f.name for f in etf_files[:5]]}")

for f in etf_files[:3]:
    raw = json.loads(f.read_text(encoding="utf-8"))
    print(f"\n=== {f.name} ===")
    print(f"  type={type(raw).__name__}  len={len(raw)}")
    if raw:
        first = raw[0] if isinstance(raw, list) else raw
        if isinstance(first, dict):
            keys = list(first.keys())[:8]
            print(f"  keys={keys}")
            for item in (raw[:5] if isinstance(raw, list) else [raw]):
                print(f"  {item}")
        elif isinstance(first, str):
            # might be list of code strings, or list of formatted strings
            print(f"  first item: {repr(first)}")
            print(f"  all: {raw[:10]}")

print()
# etf_scan_latest
etf_latest = ROOT / "data" / "etf_scan_latest.json"
if etf_latest.exists():
    raw = json.loads(etf_latest.read_text(encoding="utf-8"))
    print(f"=== etf_scan_latest.json ===")
    print(f"  type={type(raw).__name__}  len={len(raw)}")
    if raw:
        first = raw[0] if isinstance(raw, list) else None
        if first and isinstance(first, dict):
            print(f"  keys={list(first.keys())[:8]}")
            buys = [x for x in raw if x.get("action") == "买入" or x.get("signal") == "buy" or x.get("buy")]
            print(f"  buy candidates: {len(buys)}")
            for b in buys[:5]:
                print(f"  {b}")
        else:
            print(f"  sample: {raw[:3]}")
