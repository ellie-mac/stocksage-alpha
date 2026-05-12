#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import _conn

with _conn() as c:
    print("=== tables ===")
    for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall():
        print(r[0])

    print()
    print("=== indexes ===")
    for r in c.execute("SELECT name, tbl_name FROM sqlite_master WHERE type='index' ORDER BY tbl_name, name").fetchall():
        print(dict(r))

    print()
    print("=== snapshots schema ===")
    for r in c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='snapshots'").fetchall():
        print(r[0])
