#!/usr/bin/env python3
import sys
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import _conn

today = datetime.now().strftime("%Y-%m-%d")
print(f"today = {today}")

with _conn() as c:
    print("\n=== runs (last 15) ===")
    for r in c.execute(
        "SELECT id, job_name, status, trade_date, started_at, artifacts FROM runs ORDER BY id DESC LIMIT 15"
    ).fetchall():
        d = dict(r)
        print(f"id={d['id']} [{d['trade_date']}] {d['job_name']} → {d['status']}")
        if d.get('artifacts'):
            print(f"  artifacts={d['artifacts']}")
