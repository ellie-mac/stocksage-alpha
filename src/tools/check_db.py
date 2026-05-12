#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import _conn

with _conn() as c:
    print("=== signal_runs (last 10) ===")
    for r in c.execute(
        "SELECT date, source, run_time, length(buy_signals) bs FROM signal_runs ORDER BY id DESC LIMIT 10"
    ).fetchall():
        print(dict(r))

    print()
    print("=== snapshots by date+source ===")
    for r in c.execute(
        "SELECT date, source, count(*) cnt FROM snapshots GROUP BY date, source ORDER BY date DESC LIMIT 15"
    ).fetchall():
        print(dict(r))

    print()
    print("=== runs (last 10) ===")
    for r in c.execute(
        "SELECT id, job_name, status, started_at, finished_at FROM runs ORDER BY id DESC LIMIT 10"
    ).fetchall():
        print(dict(r))
