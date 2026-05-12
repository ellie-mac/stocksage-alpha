#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import _conn

with _conn() as c:
    print("=== snapshots - all rows ===")
    rows = c.execute("SELECT date, source, count(*) cnt FROM snapshots GROUP BY date, source ORDER BY date DESC").fetchall()
    if rows:
        for r in rows:
            print(dict(r))
    else:
        print("(empty)")

    print()
    print("=== signal_runs - all rows ===")
    rows2 = c.execute("SELECT date, source, run_time FROM signal_runs ORDER BY id DESC LIMIT 20").fetchall()
    if rows2:
        for r in rows2:
            print(dict(r))
    else:
        print("(empty)")

    print()
    print("=== db file ===")
    from db import DB_PATH
    print(f"DB path: {DB_PATH}")
    print(f"DB size: {Path(str(DB_PATH)).stat().st_size} bytes")
