#!/usr/bin/env python3
"""一次性：找出 signals_log.json 今天写入的所有条目，按时间排列。"""
import json
import os
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent.parent
TODAY = datetime.now().strftime("%Y-%m-%d")

with open(ROOT / "data" / "signals_log.json", encoding="utf-8") as f:
    d = json.load(f)

recent = []
for e in d:
    ts = e.get("run_time", "") or ""
    dt = e.get("date", "") or ""
    if dt == TODAY or ts[:10] == TODAY:
        recent.append(e)

print(f"today={TODAY}  found={len(recent)} entries\n")
for e in recent[-10:]:
    buys = e.get("buy_signals") or []
    print(f"  date={e.get('date')}  run_time={e.get('run_time')}  source={e.get('source')}  buys={len(buys)}")
    if buys:
        for b in buys[:12]:
            print(f"    - {b.get('code')} {b.get('name')}  buy={b.get('buy_score')}")
