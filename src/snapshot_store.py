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
            float(s.price) if s.price is not None else None,
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
                 price, regime_score, regime_label, factor_scores, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
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


def get_ic_series(
    source: str,
    factor: str = "score",
    horizon: int = 5,
    start_date: str | None = None,
    end_date: str | None = None,
    min_count: int = 5,
) -> list[tuple[str, float]]:
    """计算每日因子 IC（Spearman 秩相关），返回 [(date, IC), ...]。

    factor: 'score' 或 factor_scores JSON 里的键名（如 'value', 'technical'）。
    horizon: 5 或 20（对应 ret_5d / ret_20d）。
    min_count: 当日有效数据点少于此值时跳过。
    """
    if horizon not in (5, 20):
        raise ValueError("horizon must be 5 or 20")
    ret_col = f"ret_{horizon}d"

    params: list = [source]
    clauses = [f"{ret_col} IS NOT NULL", "price IS NOT NULL"]
    if start_date:
        clauses.append("date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("date <= ?")
        params.append(end_date)

    with _conn() as conn:
        rows = conn.execute(
            f"SELECT date, score, factor_scores, {ret_col} as ret "
            f"FROM snapshots WHERE source=? AND {' AND '.join(clauses)} ORDER BY date",
            params,
        ).fetchall()

    # Group by date, compute Spearman IC
    from itertools import groupby
    result: list[tuple[str, float]] = []
    for date_val, group in groupby(rows, key=lambda r: r["date"]):
        group_list = list(group)

        if factor == "score":
            x = [r["score"] for r in group_list]
        else:
            x = []
            for r in group_list:
                fs = json.loads(r["factor_scores"] or "{}")
                v = fs.get(factor)
                x.append(v)
            if any(v is None for v in x):
                valid = [(xi, r["ret"]) for xi, r in zip(x, group_list) if xi is not None]
                if len(valid) < min_count:
                    continue
                x, rets = zip(*valid)
                x, rets = list(x), list(rets)
            else:
                rets = [r["ret"] for r in group_list]

        if factor == "score":
            rets = [r["ret"] for r in group_list]

        if len(x) < min_count:
            continue

        ic = _spearmanr(x, rets)
        if ic == ic:  # not NaN
            result.append((date_val, round(ic, 4)))

    return result


def _spearmanr(x: list[float], y: list[float]) -> float:
    """Spearman 秩相关（无 scipy 依赖）。"""
    import numpy as np
    a, b = np.array(x, dtype=float), np.array(y, dtype=float)

    def _rank(v: "np.ndarray") -> "np.ndarray":
        order = v.argsort()
        ranks = np.empty_like(order, dtype=float)
        ranks[order] = np.arange(1, len(v) + 1, dtype=float)
        for val in np.unique(v):
            mask = v == val
            ranks[mask] = ranks[mask].mean()
        return ranks

    ra, rb = _rank(a), _rank(b)
    n = len(ra)
    if n < 3:
        return float("nan")
    d2 = float(((ra - rb) ** 2).sum())
    return 1.0 - 6.0 * d2 / (n * (n * n - 1))
