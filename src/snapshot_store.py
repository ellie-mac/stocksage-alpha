"""
EOD 快照存储 — 每日把策略信号结果写入 snapshots 表，供 IC 回测使用。

每行 = 一个股票在某天某策略下的得分 + 因子明细。
UNIQUE(date, source, code) + INSERT OR REPLACE：同一天重跑会更新记录。
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from db import _conn

if TYPE_CHECKING:
    from strategies.schemas import Signal


def save_snapshot(
    date: str,
    source: str,
    signals: list["Signal"],
    *,
    run_id: int | None = None,
    regime_score: float | None = None,
    regime_label: str | None = None,
) -> int:
    """写入当日策略信号快照，返回写入行数。"""
    if not signals:
        return 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        (
            date, source, run_id,
            s.code, s.name,
            float(s.score), float(s.sell_score),
            rank,
            regime_score, regime_label,
            json.dumps(s.factor_scores, ensure_ascii=False) if s.factor_scores else None,
            now,
        )
        for rank, s in enumerate(signals, 1)
    ]
    with _conn() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO snapshots
                (date, source, run_id, code, name, score, sell_score, rank,
                 regime_score, regime_label, factor_scores, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
    return len(rows)


def get_snapshot(date: str, source: str) -> list[dict]:
    """读取指定日期+策略的快照，按 rank 升序返回。"""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM snapshots WHERE date=? AND source=? ORDER BY rank",
            (date, source),
        ).fetchall()
    return [dict(r) for r in rows]
