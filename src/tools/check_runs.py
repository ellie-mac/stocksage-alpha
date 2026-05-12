#!/usr/bin/env python3
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import _conn

with _conn() as c:
    print("=== all runs today ===")
    for r in c.execute(
        "SELECT id, job_name, status, started_at, finished_at, artifacts, error FROM runs WHERE trade_date='2026-05-11' ORDER BY id"
    ).fetchall():
        d = dict(r)
        print(f"id={d['id']} job={d['job_name']} status={d['status']}")
        print(f"  started={d['started_at']} finished={d['finished_at']}")
        if d['artifacts']:
            print(f"  artifacts={d['artifacts']}")
        if d['error']:
            print(f"  error={d['error'][:200]}")
        print()
