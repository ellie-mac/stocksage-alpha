"""
signals_store — 信号日志的统一存储接口（SQLite 主存 + JSON 兼容导出）

写入：append(entry, run_id=None) — 有run_id用IGNORE幂等；无run_id用REPLACE覆盖当日 + 原子刷新 json
读取：load(n=500, source=None)   — 优先 SQLite，fallback json
查询：query(source, start_date, end_date, n) — SQLite 精确查询
迁移：migrate_from_json()        — 一次性导入历史 json 数据

SQLite 为唯一事实来源；signals_log.json 是兼容性导出缓存。
DB 文件由 db.py 统一管理（stocksage.db），与 run_manifest 共库，支持跨表事务。
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from db import _conn, DB_PATH as _DB

_JSON = Path(__file__).resolve().parent.parent / "data" / "signals_log.json"


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["buy_signals"]  = json.loads(d.get("buy_signals",  "[]"))
    d["sell_signals"] = json.loads(d.get("sell_signals", "[]"))
    d.pop("id", None)
    d.pop("created_at", None)
    d.pop("run_id", None)
    return d


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def append(entry: dict, run_id: int | None = None) -> bool:
    """
    写入 SQLite + 原子刷新 signals_log.json。
    有 run_id：INSERT OR IGNORE，同一批次不重复写。
    无 run_id：INSERT … ON CONFLICT DO UPDATE，覆盖当日同 source 的旧记录，
               保留原始 created_at，更新 updated_at。

    run_id 可以不显式传入：若当前上下文已通过 logger.bind_run_id() 绑定，
    则自动补填，确保夜跑路径的信号写入走幂等路径而非覆盖路径。
    """
    if run_id is None:
        from logger import get_run_id as _get_run_id
        run_id = _get_run_id()

    inserted = False
    date     = entry.get("date", "")
    run_time = entry.get("run_time", "")
    score    = entry.get("regime_score")
    source   = entry.get("source", "unknown")
    buys     = json.dumps(entry.get("buy_signals",  []), ensure_ascii=False)
    sells    = json.dumps(entry.get("sell_signals", []), ensure_ascii=False)

    with _conn() as conn:
        if run_id is not None:
            cur = conn.execute(
                """INSERT OR IGNORE INTO signal_runs
                   (date, run_time, regime_score, source, buy_signals, sell_signals, run_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (date, run_time, score, source, buys, sells, run_id),
            )
        else:
            cur = conn.execute(
                """INSERT INTO signal_runs
                   (date, run_time, regime_score, source, buy_signals, sell_signals, run_id)
                   VALUES (?, ?, ?, ?, ?, ?, NULL)
                   ON CONFLICT(date, source) WHERE run_id IS NULL
                   DO UPDATE SET
                       run_time     = excluded.run_time,
                       regime_score = excluded.regime_score,
                       buy_signals  = excluded.buy_signals,
                       sell_signals = excluded.sell_signals,
                       updated_at   = datetime('now','localtime')""",
                (date, run_time, score, source, buys, sells),
            )
        inserted = cur.rowcount > 0

    if inserted:
        _json_append(entry)

    return inserted


def _json_append(entry: dict) -> None:
    existing: list[dict] = []
    if _JSON.exists():
        try:
            existing = json.loads(_JSON.read_text(encoding="utf-8"))
        except Exception:
            existing = []
    existing.append(entry)
    tmp = _JSON.with_suffix(".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(_JSON)


def load(n: int = 500, source: str | None = None) -> list[dict]:
    """返回最近 n 条记录，与 signals_log.json 格式完全兼容。"""
    if not _DB.exists():
        if _JSON.exists():
            return json.loads(_JSON.read_text(encoding="utf-8"))[-n:]
        return []

    where  = "WHERE source = ?" if source else ""
    params = ([source] if source else []) + [n]

    with _conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM signal_runs {where} ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()

    return [_row_to_dict(r) for r in reversed(rows)]


def query(
    source: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    n: int = 500,
) -> list[dict]:
    """灵活查询，所有参数可选。"""
    clauses: list[str] = []
    params:  list      = []
    if source:
        clauses.append("source = ?")
        params.append(source)
    if start_date:
        clauses.append("date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("date <= ?")
        params.append(end_date)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(n)

    with _conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM signal_runs {where} ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()

    return [_row_to_dict(r) for r in reversed(rows)]


def migrate_from_json() -> int:
    """一次性把现有 signals_log.json 数据导入 SQLite（幂等）。"""
    if not _JSON.exists():
        return 0
    data = json.loads(_JSON.read_text(encoding="utf-8"))
    if not data:
        return 0

    inserted = 0
    with _conn() as conn:
        for entry in data:
            cur = conn.execute(
                """INSERT OR IGNORE INTO signal_runs
                   (date, run_time, regime_score, source, buy_signals, sell_signals)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    entry.get("date", ""),
                    entry.get("run_time", ""),
                    entry.get("regime_score"),
                    entry.get("source", "unknown"),
                    json.dumps(entry.get("buy_signals",  []), ensure_ascii=False),
                    json.dumps(entry.get("sell_signals", []), ensure_ascii=False),
                ),
            )
            inserted += cur.rowcount

    return inserted


if __name__ == "__main__":
    n = migrate_from_json()
    print(f"Migrated {n} entries from signals_log.json → signals_store.db")
