"""
event_log.py — 统一事件日志 SQLite

所有策略的买卖信号、推送事件写入同一 SQLite 数据库，exactly-once 语义。

event_id = SHA-256(date + strategy + code + signal_type)[:16]
重复插入相同 event_id 会被静默忽略（INSERT OR IGNORE）。

用法：
    from event_log import log_event, query_events

    # 写入一条事件
    log_event(
        date="2026-04-29",
        strategy="main",
        code="000001",
        signal_type="buy",
        price=12.5,
        score=78.0,
        details={"buy_score": 78, "regime": "BULL"},
    )

    # 查询
    df = query_events(strategy="main", signal_type="buy", limit=50)
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

sys.stdout.reconfigure(encoding="utf-8")

_ROOT   = Path(__file__).resolve().parent.parent
DB_PATH = _ROOT / "data" / "event_log.db"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,
    ts           TEXT NOT NULL,        -- ISO-8601 timestamp of log() call
    date         TEXT NOT NULL,        -- trade date (YYYYMMDD or YYYY-MM-DD)
    strategy     TEXT NOT NULL,        -- 'main' / 'chip' / 'golden_cross' / 'hot_scan'
    code         TEXT NOT NULL,        -- 6-digit stock code
    signal_type  TEXT NOT NULL,        -- 'buy' / 'sell' / 'stall' / 'fast_surge' / 'fast_drop'
    price        REAL,
    score        REAL,
    details      TEXT                  -- JSON blob for extra fields
);
CREATE INDEX IF NOT EXISTS idx_date     ON events(date);
CREATE INDEX IF NOT EXISTS idx_strategy ON events(strategy);
CREATE INDEX IF NOT EXISTS idx_code     ON events(code);
"""


def _make_event_id(date: str, strategy: str, code: str, signal_type: str) -> str:
    raw = f"{date}|{strategy}|{code}|{signal_type}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@contextmanager
def _conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_PATH), timeout=10)
    try:
        con.executescript(_CREATE_TABLE)
        yield con
        con.commit()
    finally:
        con.close()


def log_event(
    date: str,
    strategy: str,
    code: str,
    signal_type: str,
    price: Optional[float] = None,
    score: Optional[float] = None,
    details: Optional[dict] = None,
) -> str:
    """
    Insert one event. Returns event_id. Silently skips duplicates (exactly-once).
    date: trade date string, e.g. '2026-04-29' or '20260429'.
    """
    event_id = _make_event_id(date, strategy, code, signal_type)
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    details_json = json.dumps(details, ensure_ascii=False) if details else None
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO events "
            "(event_id, ts, date, strategy, code, signal_type, price, score, details) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (event_id, ts, date, strategy, code, signal_type, price, score, details_json),
        )
    return event_id


def log_events(rows: list[dict]) -> list[str]:
    """Batch insert; each dict must have keys: date, strategy, code, signal_type.
    Optional keys: price, score, details."""
    ids: list[str] = []
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    with _conn() as con:
        for row in rows:
            event_id = _make_event_id(
                row["date"], row["strategy"], row["code"], row["signal_type"]
            )
            details_json = (json.dumps(row.get("details"), ensure_ascii=False)
                            if row.get("details") else None)
            con.execute(
                "INSERT OR IGNORE INTO events "
                "(event_id, ts, date, strategy, code, signal_type, price, score, details) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (event_id, ts, row["date"], row["strategy"], row["code"],
                 row["signal_type"], row.get("price"), row.get("score"), details_json),
            )
            ids.append(event_id)
    return ids


def query_events(
    strategy: Optional[str] = None,
    code: Optional[str] = None,
    signal_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 200,
) -> list[dict]:
    """Return matching events as list of dicts, newest first."""
    if not DB_PATH.exists():
        return []
    clauses: list[str] = []
    params: list[Any]  = []
    if strategy:
        clauses.append("strategy = ?"); params.append(strategy)
    if code:
        clauses.append("code = ?");     params.append(code)
    if signal_type:
        clauses.append("signal_type = ?"); params.append(signal_type)
    if date_from:
        clauses.append("date >= ?");    params.append(date_from)
    if date_to:
        clauses.append("date <= ?");    params.append(date_to)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"SELECT * FROM events {where} ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    with sqlite3.connect(str(DB_PATH), timeout=10) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute(sql, params).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        if d.get("details"):
            try:
                d["details"] = json.loads(d["details"])
            except Exception:
                pass
        result.append(d)
    return result


def event_exists(date: str, strategy: str, code: str, signal_type: str) -> bool:
    """Check if an event already exists (without inserting)."""
    if not DB_PATH.exists():
        return False
    event_id = _make_event_id(date, strategy, code, signal_type)
    with sqlite3.connect(str(DB_PATH), timeout=10) as con:
        row = con.execute("SELECT 1 FROM events WHERE event_id=?", (event_id,)).fetchone()
    return row is not None


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Query or inspect event log")
    parser.add_argument("--strategy",    default="")
    parser.add_argument("--code",        default="")
    parser.add_argument("--signal-type", default="")
    parser.add_argument("--date-from",   default="")
    parser.add_argument("--date-to",     default="")
    parser.add_argument("--limit",       type=int, default=50)
    args = parser.parse_args()

    rows = query_events(
        strategy=args.strategy or None,
        code=args.code or None,
        signal_type=args.signal_type or None,
        date_from=args.date_from or None,
        date_to=args.date_to or None,
        limit=args.limit,
    )
    if not rows:
        print("(no events found)")
    else:
        for r in rows:
            print(f"{r['date']} [{r['strategy']}] {r['code']} {r['signal_type']}"
                  f"  price={r.get('price')}  score={r.get('score')}"
                  f"  id={r['event_id']}")
