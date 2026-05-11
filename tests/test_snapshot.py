"""
R14 测试：EOD 快照层 (snapshot_store + nightly_scan 集成)
  - save_snapshot 写入行数正确
  - INSERT OR REPLACE：同一 (date, source, code) 第二次写覆盖第一次
  - 空 signals 返回 0、不写行
  - get_snapshot 按 rank 升序返回
  - nightly_scan 非 dry-run 时写入快照
  - nightly_scan dry-run 时跳过快照
"""
from __future__ import annotations

import importlib
import json
from datetime import datetime
from unittest.mock import patch, MagicMock


def _make_signal(code: str, score: float, factors: dict | None = None):
    from strategies.schemas import Signal
    return Signal(
        code=code,
        name=f"股票{code}",
        score=score,
        sell_score=0.0,
        factor_scores=factors or {},
    )


# ── save_snapshot ─────────────────────────────────────────────────────────────

def test_save_snapshot_writes_correct_count(tmp_path):
    """3 个信号应写入 3 行。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        importlib.reload(ss)
        signals = [_make_signal(f"00000{i}", float(i * 10)) for i in range(1, 4)]
        count = ss.save_snapshot("2026-05-11", "main", signals, run_id=1)
    assert count == 3


def test_save_snapshot_empty_returns_zero(tmp_path):
    """空信号列表应返回 0 且不写行。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        importlib.reload(ss)
        count = ss.save_snapshot("2026-05-11", "main", [], run_id=1)
    assert count == 0


def test_save_snapshot_idempotent_replace(tmp_path):
    """同 (date, source, code) 第二次写应覆盖（分数更新），总行数不增加。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        importlib.reload(ss)
        sig = [_make_signal("000001", 50.0)]
        ss.save_snapshot("2026-05-11", "main", sig, run_id=1)
        sig2 = [_make_signal("000001", 75.0)]
        ss.save_snapshot("2026-05-11", "main", sig2, run_id=2)
        rows = ss.get_snapshot("2026-05-11", "main")
    assert len(rows) == 1
    assert rows[0]["score"] == 75.0


def test_get_snapshot_ordered_by_rank(tmp_path):
    """get_snapshot 应按 rank 升序返回，rank 从 1 开始。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        importlib.reload(ss)
        signals = [
            _make_signal("000001", 90.0),
            _make_signal("000002", 80.0),
            _make_signal("000003", 70.0),
        ]
        ss.save_snapshot("2026-05-11", "main", signals, run_id=5,
                         regime_score=0.7, regime_label="bull")
        rows = ss.get_snapshot("2026-05-11", "main")
    assert [r["rank"] for r in rows] == [1, 2, 3]
    assert rows[0]["code"] == "000001"
    assert rows[0]["regime_label"] == "bull"


def test_save_snapshot_stores_factor_scores(tmp_path):
    """factor_scores 应序列化为 JSON 字符串存入 DB。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import snapshot_store as ss
        importlib.reload(ss)
        sig = [_make_signal("000001", 60.0, factors={"value": 0.8, "tech": 0.5})]
        ss.save_snapshot("2026-05-11", "main", sig, run_id=1)
        rows = ss.get_snapshot("2026-05-11", "main")
    fs = json.loads(rows[0]["factor_scores"])
    assert fs["value"] == 0.8


# ── nightly_scan 集成 ─────────────────────────────────────────────────────────

def _make_mock_strategy(signal_count: int = 2):
    mock_strategy = MagicMock()
    mock_result = MagicMock()
    mock_result.metadata = {"failed": False}
    mock_result.signals = [_make_signal(f"00000{i}", float(i * 10)) for i in range(1, signal_count + 1)]
    mock_result.regime_label = "bull"
    mock_result.regime_score = 0.6
    mock_strategy.run.return_value = mock_result
    mock_strategy.publish.return_value = None
    return mock_strategy


def test_nightly_scan_saves_snapshot_on_success(tmp_path):
    """非 dry-run 时，策略成功后 artifacts 应包含 snapshot=N。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import jobs.nightly_scan as ns
        import run_manifest as rm
        import snapshot_store as ss
        importlib.reload(rm)
        importlib.reload(ns)
        importlib.reload(ss)

        mock_strategy = _make_mock_strategy(signal_count=3)
        artifacts_captured = []

        original_finish = ns.finish_run

        def capture_finish(run_id, ok, **kwargs):
            artifacts_captured.extend(kwargs.get("artifacts") or [])
            return original_finish(run_id, ok, **kwargs)

        with patch("jobs.nightly_scan.start_run", return_value=42), \
             patch("jobs.nightly_scan.finish_run", side_effect=capture_finish), \
             patch("strategies.base.get_strategy", return_value=mock_strategy):
            ns._run_strategy("test", "test_job", "main", {}, dry_run=False)

    assert any(a.startswith("snapshot=") for a in artifacts_captured)
    snap_artifact = next(a for a in artifacts_captured if a.startswith("snapshot="))
    assert snap_artifact == "snapshot=3"


def test_nightly_scan_skips_snapshot_on_dry_run(tmp_path):
    """dry-run 时不应写入快照（artifacts 中不含 snapshot=）。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import jobs.nightly_scan as ns
        import run_manifest as rm
        importlib.reload(rm)
        importlib.reload(ns)

        mock_strategy = _make_mock_strategy(signal_count=3)
        artifacts_captured = []

        def capture_finish(run_id, ok, **kwargs):
            artifacts_captured.extend(kwargs.get("artifacts") or [])

        with patch("jobs.nightly_scan.start_run", return_value=42), \
             patch("jobs.nightly_scan.finish_run", side_effect=capture_finish), \
             patch("strategies.base.get_strategy", return_value=mock_strategy):
            ns._run_strategy("test", "test_job", "main", {}, dry_run=True)

    assert not any(a.startswith("snapshot=") for a in artifacts_captured)
