#!/usr/bin/env python3
import sys, json
from pathlib import Path
ROOT = Path("C:/Users/jiapeichen/repos/stocksage-alpha")
sys.path.insert(0, str(ROOT / "src"))

# Check latest_picks.json
picks_path = ROOT / "data" / "latest_picks.json"
if picks_path.exists():
    data = json.loads(picks_path.read_text(encoding="utf-8"))
    print(f"=== latest_picks.json ===")
    print(f"timestamp: {data.get('timestamp')}")
    print(f"regime: {data.get('regime')}")
    print(f"results (main): {len(data.get('results', []))} items")
    print(f"smallcap: {len(data.get('smallcap', []))} items")
    for r in data.get("results", []):
        print(f"  {r.get('code')} {r.get('name')} score={r.get('buy_score')}")
else:
    print("latest_picks.json not found")

print()

# Check latest ETF picks
etf_files = sorted(ROOT.glob("data/etf_picks_*.json"), reverse=True)
for f in etf_files[:3]:
    data = json.loads(f.read_text(encoding="utf-8"))
    buys = [x for x in data if x.get("signal") == "buy"]
    print(f"=== {f.name} ===")
    print(f"  total={len(data)}  buys={len(buys)}")
    for x in buys[:5]:
        print(f"  {x.get('code')} {x.get('name')} score={x.get('score', x.get('buy_score'))}")

print()

# Check etf_scan_latest.json
etf_latest = ROOT / "data" / "etf_scan_latest.json"
if etf_latest.exists():
    data = json.loads(etf_latest.read_text(encoding="utf-8"))
    if isinstance(data, list):
        buys = [x for x in data if x.get("signal") == "buy"]
        print(f"=== etf_scan_latest.json ===  total={len(data)}  buys={len(buys)}")
    else:
        print(f"=== etf_scan_latest.json === {type(data)}")
