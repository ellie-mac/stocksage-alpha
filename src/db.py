"""
共享 SQLite 连接工厂 — data/stocksage.db 是唯一入口。

run_manifest 和 signals_store 共享同一 DB 文件，支持跨表事务。
两个模块均 from db import _conn 使用；不直接操作 DB_PATH。

Schema 版本化：PRAGMA user_version 追踪，_MIGRATIONS 列表顺序执行。
迁移旧数据：python src/db.py
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

_ROOT   = Path(__file__).resolve().parent.parent
DB_PATH = _ROOT / "data" / "stocksage.db"

_OLD_MANIFEST_DB = _ROOT / "data" / "run_manifest.db"
_OLD_SIGNALS_DB  = _ROOT / "data" / "signals_store.db"


# ── Schema 迁移清单 ───────────────────────────────────────────────────────────
# 每项：(version_after, description, sql)
# version 0 = CREATE TABLE IF NOT EXISTS 建立的基础 schema
_MIGRATIONS: list[tuple[int, str, str]] = [
    (
        1,
        "signal_runs: add updated_at",
        "ALTER TABLE signal_runs ADD COLUMN updated_at TEXT",
    ),
    (
        2,
        "runs: partial unique index for atomic claim (started/succeeded)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_run_claim "
        "ON runs(job_name, trade_date) "
        "WHERE status IN ('started', 'succeeded')",
    ),
    (
        3,
        "alerts_sent: failure-alert dedup table keyed on run_id",
        "CREATE TABLE IF NOT EXISTS alerts_sent ("
        "run_id INTEGER PRIMARY KEY, "
        "alerted_at TEXT NOT NULL"
        ")",
    ),
    (
        4,
        "snapshots: EOD per-signal snapshot for IC backtesting",
        "CREATE TABLE IF NOT EXISTS snapshots ("
        "id           INTEGER PRIMARY KEY AUTOINCREMENT, "
        "date         TEXT NOT NULL, "
        "source       TEXT NOT NULL, "
        "run_id       INTEGER, "
        "code         TEXT NOT NULL, "
        "name         TEXT, "
        "score        REAL NOT NULL, "
        "sell_score   REAL NOT NULL DEFAULT 0.0, "
        "rank         INTEGER, "
        "regime_score REAL, "
        "regime_label TEXT, "
        "factor_scores TEXT, "
        "created_at   TEXT NOT NULL DEFAULT (datetime('now','localtime')), "
        "UNIQUE(date, source, code)"
        ")",
    ),
]


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """顺序执行 _MIGRATIONS 中尚未应用的迁移，更新 user_version。"""
    current = conn.execute("PRAGMA user_version").fetchone()[0]
    for version, desc, sql in _MIGRATIONS:
        if version <= current:
            continue
        try:
            conn.execute(sql)
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            # 幂等：列/索引已存在时视为成功
            if "duplicate column" not in msg and "already exists" not in msg:
                raise
        conn.execute(f"PRAGMA user_version = {version}")
        conn.commit()


# ── 连接工厂 ──────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")

    # ── runs (run_manifest) — base schema ───────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name     TEXT NOT NULL,
            trade_date   TEXT NOT NULL,
            status       TEXT NOT NULL DEFAULT 'succeeded',
            params       TEXT,
            success      INTEGER,
            duration_sec REAL,
            artifacts    TEXT,
            error        TEXT,
            started_at   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            finished_at  TEXT
        )
    """)

    # ── signal_runs (signals_store) — base schema ────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_runs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            date         TEXT NOT NULL,
            run_time     TEXT NOT NULL,
            regime_score REAL,
            source       TEXT NOT NULL DEFAULT 'unknown',
            buy_signals  TEXT NOT NULL DEFAULT '[]',
            sell_signals TEXT NOT NULL DEFAULT '[]',
            run_id       INTEGER,
            created_at   TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sig_date   ON signal_runs(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sig_source ON signal_runs(source)")
    conn.execute("DROP INDEX IF EXISTS uq_sig_no_run")
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_sig_with_run
        ON signal_runs(date, source, run_id) WHERE run_id IS NOT NULL
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_sig_no_run
        ON signal_runs(date, source) WHERE run_id IS NULL
    """)

    conn.commit()

    # 执行尚未应用的迁移（幂等），migration 4 会建 snapshots 表
    _apply_migrations(conn)

    # snapshots 索引：表由 migration 4 创建，索引在此幂等补建
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_snap_date_src  ON snapshots(date, source)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_snap_code_date ON snapshots(code, date)"
    )
    conn.commit()

    return conn


# ── 旧库迁移 ──────────────────────────────────────────────────────────────────

def migrate_legacy_dbs() -> dict[str, object]:
    """
    一次性把旧 run_manifest.db / signals_store.db 数据导入 stocksage.db。
    迁移后把旧文件重命名为 *.migrated（保留，不删除）。
    """
    result: dict[str, object] = {}
    with _conn() as conn:
        for old_path, table in [
            (_OLD_MANIFEST_DB, "runs"),
            (_OLD_SIGNALS_DB,  "signal_runs"),
        ]:
            if not old_path.exists():
                result[table] = "skip (not found)"
                continue
            try:
                conn.execute(f"ATTACH DATABASE '{old_path}' AS legacy")
                cur = conn.execute(
                    f"INSERT OR IGNORE INTO {table} SELECT * FROM legacy.{table}"
                )
                rows = cur.rowcount
                conn.execute("DETACH DATABASE legacy")
                old_path.rename(old_path.with_suffix(".migrated"))
                result[table] = f"imported {rows} rows"
            except Exception as exc:
                result[f"{table}_error"] = str(exc)
                try:
                    conn.execute("DETACH DATABASE legacy")
                except Exception:
                    pass
    return result


if __name__ == "__main__":
    r = migrate_legacy_dbs()
    for k, v in r.items():
        print(f"  {k}: {v}")
