#!/usr/bin/env python3
"""
SQLite-backed job state tracker with state-machine semantics.

Pattern A — two-phase (recommended for in-process jobs):
    run_id = start_run(job_name, trade_date)
    try:
        ... do work ...
        finish_run(run_id, success=True, duration_sec=elapsed)
    except Exception as e:
        finish_run(run_id, success=False, error=str(e))

Pattern B — one-shot (for subprocess wrappers):
    log_run(job_name, trade_date, success=ok, duration_sec=elapsed)

Public API:
    start_run(job_name, trade_date, params=None) -> int
    finish_run(run_id, success, duration_sec=None, artifacts=None, error=None)
    log_run(job_name, trade_date, *, params, success, duration_sec, artifacts, error) -> int
    get_last_run(job_name, trade_date) -> dict | None
    was_successful_for_trade_date(job_name, trade_date) -> bool
    cleanup_old_runs(keep_days=30)
    get_failed_runs(days=7) -> list[dict]
"""
from __future__ import annotations

import json
import os
import sys
import sqlite3
from datetime import datetime, timedelta

# src/ 在 sys.path 里才能 import db
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import _conn      # noqa: E402
from logger import get_logger  # noqa: E402

log = get_logger("run_manifest")

STATUS_STARTED   = "started"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED    = "failed"
STATUS_CRASHED   = "crashed"   # started but process died before finish_run


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    if d.get("params"):
        d["params"] = json.loads(d["params"])
    if d.get("artifacts"):
        d["artifacts"] = json.loads(d["artifacts"])
    return d


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def start_run(
    job_name: str,
    trade_date: str,
    params: dict | None = None,
) -> int | None:
    """
    原子 claim 当天该 job 的执行权，返回 run_id。
    若当天同 job 已有 started/succeeded 记录则返回 None（调用方应跳过执行）。
    failed/crashed 记录不阻塞重试。
    """
    with _conn() as conn:
        # 孤儿扫描：started > 2h 视为 crashed，释放 claim
        conn.execute(
            """UPDATE runs
               SET status = ?, finished_at = datetime('now','localtime')
               WHERE status = ? AND started_at < datetime('now','localtime','-2 hours')""",
            (STATUS_CRASHED, STATUS_STARTED),
        )
        # 原子 claim：依赖 uq_run_claim 偏唯一索引
        # (job_name, trade_date) WHERE status IN ('started','succeeded')
        cur = conn.execute(
            """INSERT OR IGNORE INTO runs (job_name, trade_date, status, params)
               VALUES (?, ?, ?, ?)""",
            (
                job_name,
                trade_date,
                STATUS_STARTED,
                json.dumps(params, ensure_ascii=False) if params else None,
            ),
        )
        if cur.rowcount == 0:
            log.info("run_claim_blocked", extra={"job": job_name, "trade_date": trade_date})
            return None  # already claimed or succeeded today
        run_id = cur.lastrowid
        log.info("run_started", extra={"job": job_name, "trade_date": trade_date, "run_id": run_id})
        return run_id  # type: ignore[return-value]


def finish_run(
    run_id: int,
    success: bool,
    *,
    duration_sec: float | None = None,
    artifacts: list[str] | None = None,
    error: str | None = None,
) -> None:
    """Update an existing 'started' record to succeeded/failed."""
    status = STATUS_SUCCEEDED if success else STATUS_FAILED
    log.info("run_finished", extra={"run_id": run_id, "status": status, "duration_sec": duration_sec})
    with _conn() as conn:
        conn.execute(
            """UPDATE runs
               SET status=?, success=?, duration_sec=?, artifacts=?, error=?,
                   finished_at=datetime('now','localtime')
               WHERE id=?""",
            (
                status,
                1 if success else 0,
                duration_sec,
                json.dumps(artifacts, ensure_ascii=False) if artifacts else None,
                error,
                run_id,
            ),
        )


def log_run(
    job_name: str,
    trade_date: str,
    *,
    params: dict | None = None,
    success: bool = True,
    duration_sec: float | None = None,
    artifacts: list[str] | None = None,
    error: str | None = None,
) -> int:
    """One-shot convenience: write a completed record atomically."""
    status = STATUS_SUCCEEDED if success else STATUS_FAILED
    with _conn() as conn:
        cur = conn.execute(
            """INSERT OR IGNORE INTO runs
               (job_name, trade_date, status, params, success, duration_sec, artifacts, error,
                finished_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))""",
            (
                job_name,
                trade_date,
                status,
                json.dumps(params, ensure_ascii=False) if params else None,
                1 if success else 0,
                duration_sec,
                json.dumps(artifacts, ensure_ascii=False) if artifacts else None,
                error,
            ),
        )
        return cur.lastrowid  # type: ignore[return-value]


def get_last_run(job_name: str, trade_date: str) -> dict | None:
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM runs WHERE job_name=? AND trade_date=? ORDER BY id DESC LIMIT 1",
            (job_name, trade_date),
        ).fetchone()
    return _row_to_dict(row) if row else None


def was_successful_for_trade_date(job_name: str, trade_date: str) -> bool:
    """Return True only if the last run for this job+date has status='succeeded'."""
    row = get_last_run(job_name, trade_date)
    return row is not None and row.get("status") == STATUS_SUCCEEDED


# Backward-compat alias
was_successful_today = was_successful_for_trade_date


def cleanup_old_runs(keep_days: int = 30) -> int:
    """Delete runs older than keep_days. Returns number of rows deleted."""
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    with _conn() as conn:
        cur = conn.execute(
            "DELETE FROM runs WHERE trade_date < ?", (cutoff,)
        )
        deleted = cur.rowcount
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    return deleted


def get_failed_runs(days: int = 7) -> list[dict]:
    """Return all failed/crashed/stuck runs from the last N days."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _conn() as conn:
        rows = conn.execute(
            """SELECT * FROM runs
               WHERE trade_date >= ? AND status IN (?, ?, ?)
               ORDER BY id DESC""",
            (cutoff, STATUS_FAILED, STATUS_CRASHED, STATUS_STARTED),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]
