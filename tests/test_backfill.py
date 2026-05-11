"""
R15 测试：forward return 回填 + IC 分析
  - save_snapshot 存 price
  - backfill dry-run 不写 DB
  - backfill 正常更新 ret_5d / ret_20d
  - 无快照需要回填时 graceful 返回
  - get_ic_series 基础 IC 计算
  - get_ic_series 数据点不足时跳过
"""
from __future__ import annotations

import importlib
import json
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock


def _make_signal(code: str, score: float, price: float = 10.0):
    from strategies.schemas import Signal
    return Signal(code=code, name=f"股票{code}", score=score, price=price,
                  factor_scores={"value": score / 100})


# ── save_snapshot 存 price ────────────────────────────────────────────────────

def test_save_snapshot_stores_price(tmp_path):
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        importlib.reload(ss)
        signals = [_make_signal("000001", 60.0, price=15.5)]
        ss.save_snapshot("2026-05-11", "main", signals, run_id=1)
        rows = ss.get_snapshot("2026-05-11", "main")
    assert rows[0]["price"] == 15.5


# ── backfill dry-run ──────────────────────────────────────────────────────────

def test_backfill_dry_run_does_not_write(tmp_path):
    """dry-run 不应写入 ret_5d。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        import jobs.backfill_returns as br
        import run_manifest as rm
        importlib.reload(rm); importlib.reload(ss); importlib.reload(br)

        date_5d_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        signals = [_make_signal("000001", 60.0, price=10.0)]
        ss.save_snapshot(date_5d_ago, "main", signals, run_id=1)

        with patch("jobs.backfill_returns._nth_trading_day_before",
                   side_effect=lambda n: date_5d_ago if n == 5 else None), \
             patch("jobs.backfill_returns._fetch_today_close", return_value={"000001": 11.0}):
            result = br.run_backfill(dry_run=True)

        rows = ss.get_snapshot(date_5d_ago, "main")
    assert rows[0]["ret_5d"] is None
    assert result["dry_run"] is True


# ── backfill 正常更新 ret_5d ──────────────────────────────────────────────────

def test_backfill_updates_ret_5d(tmp_path):
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        import jobs.backfill_returns as br
        importlib.reload(ss); importlib.reload(br)

        date_5d_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        signals = [
            _make_signal("000001", 60.0, price=10.0),
            _make_signal("000002", 50.0, price=20.0),
        ]
        ss.save_snapshot(date_5d_ago, "main", signals, run_id=1)

        with patch("jobs.backfill_returns._nth_trading_day_before",
                   side_effect=lambda n: date_5d_ago if n == 5 else None), \
             patch("jobs.backfill_returns._fetch_today_close",
                   return_value={"000001": 11.0, "000002": 19.0}):
            result = br.run_backfill(dry_run=False)

        rows = {r["code"]: r for r in ss.get_snapshot(date_5d_ago, "main")}

    assert result["updated_5d"] == 2
    assert abs(rows["000001"]["ret_5d"] - 0.1) < 1e-5   # (11-10)/10 = 0.1
    assert abs(rows["000002"]["ret_5d"] - (-0.05)) < 1e-5  # (19-20)/20 = -0.05


def test_backfill_skips_missing_price_in_snapshot(tmp_path):
    """快照 price 为 NULL 时跳过（无法计算 return）。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        import jobs.backfill_returns as br
        importlib.reload(ss); importlib.reload(br)

        date_5d_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        from strategies.schemas import Signal
        sig = Signal(code="000001", name="测试", score=60.0, price=None)
        ss.save_snapshot(date_5d_ago, "main", [sig], run_id=1)

        with patch("jobs.backfill_returns._nth_trading_day_before",
                   side_effect=lambda n: date_5d_ago if n == 5 else None), \
             patch("jobs.backfill_returns._fetch_today_close", return_value={"000001": 11.0}):
            result = br.run_backfill(dry_run=False)

    # rows with NULL price are excluded from tasks, so updated_5d == 0
    assert result["updated_5d"] == 0


def test_backfill_nothing_to_do(tmp_path):
    """没有快照需要回填时 graceful 返回。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import jobs.backfill_returns as br
        importlib.reload(br)
        with patch("jobs.backfill_returns._nth_trading_day_before", return_value=None):
            result = br.run_backfill(dry_run=False)
    assert result["updated_5d"] == 0
    assert result["updated_20d"] == 0


# ── get_ic_series ─────────────────────────────────────────────────────────────

def _seed_ic_data(ss, date: str, source: str, n: int, ret_5d_values: list[float]):
    """写 n 条 snapshot + 手动填入 ret_5d（绕过回填 job）。"""
    from strategies.schemas import Signal
    signals = [
        Signal(code=f"00{i:04d}", name=f"股票{i}", score=float(i * 10), price=10.0)
        for i in range(1, n + 1)
    ]
    ss.save_snapshot(date, source, signals, run_id=1)
    from db import _conn
    with _conn() as conn:
        for i, ret in enumerate(ret_5d_values, 1):
            conn.execute(
                "UPDATE snapshots SET ret_5d=? WHERE date=? AND source=? AND code=?",
                (ret, date, source, f"00{i:04d}"),
            )


def test_get_ic_series_basic(tmp_path):
    """IC 应与信号方向一致（强正相关信号 → IC > 0）。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        importlib.reload(ss)
        # score 越高、return 越高 → 强正相关 IC
        rets = [0.01 * i for i in range(1, 8)]  # 0.01 ~ 0.07
        _seed_ic_data(ss, "2026-05-01", "main", 7, rets)
        series = ss.get_ic_series("main", factor="score", horizon=5)
    assert len(series) == 1
    date_val, ic = series[0]
    assert date_val == "2026-05-01"
    assert ic > 0.9  # 近乎完美正相关


def test_get_ic_series_insufficient_data_skipped(tmp_path):
    """当日数据点 < min_count 时跳过，不报错。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        importlib.reload(ss)
        _seed_ic_data(ss, "2026-05-01", "main", 3, [0.01, 0.02, 0.03])
        series = ss.get_ic_series("main", factor="score", horizon=5, min_count=5)
    assert series == []


def test_get_ic_series_date_filter(tmp_path):
    """start_date / end_date 过滤正常工作。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        importlib.reload(ss)
        for d, rets in [
            ("2026-04-01", [0.01 * i for i in range(1, 7)]),
            ("2026-05-01", [0.01 * i for i in range(1, 7)]),
        ]:
            _seed_ic_data(ss, d, "main", 6, rets)
        series = ss.get_ic_series("main", start_date="2026-05-01")
    assert len(series) == 1
    assert series[0][0] == "2026-05-01"
