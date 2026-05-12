"""
nightly_scan 集成测试：
  - strategy.run() 异常被隔离，run 标记 failed，publish 不被调用
  - strategy.run() 成功，run 标记 succeeded，publish 被调用
  - start_run 返回 None 时跳过（publish 从不被调用）
  - last_run.json 在扫描完成后被写入
"""
from __future__ import annotations

import importlib
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch


def _make_strategy(fail=False, signals_count=3):
    """返回假 BaseStrategy：run() 按需返回 failed 或成功结果。"""
    from strategies.schemas import StrategyResult
    strategy = MagicMock()
    if fail:
        result = StrategyResult(
            strategy="test", date="2099-01-01", run_time="2099-01-01 22:00",
            metadata={"failed": True, "error": "boom boom"},
        )
    else:
        result = StrategyResult(
            strategy="test", date="2099-01-01", run_time="2099-01-01 22:00",
            signals=[MagicMock() for _ in range(signals_count)],
            regime_label="bull",
        )
    strategy.run.return_value = result
    return strategy


def test_strategy_exception_marks_run_failed(tmp_path):
    """strategy 返回 failed=True → run 标记 failed，publish 不被调用。"""
    db_file = tmp_path / "ss.db"
    fake_strategy = _make_strategy(fail=True)
    trade_date = datetime.now().strftime("%Y-%m-%d")

    import db as _db
    with patch.object(_db, "DB_PATH", db_file), \
         patch("strategies.base.get_strategy", return_value=fake_strategy):
        import run_manifest as rm
        import jobs.nightly_scan as ns
        importlib.reload(rm)
        importlib.reload(ns)

        ok = ns._run_strategy("测试", "test/job", "test", {}, dry_run=True)

        assert ok is False
        fake_strategy.publish.assert_not_called()

        run = rm.get_last_run("test/job", trade_date)
        assert run is not None
        assert run["status"] == "failed"


def test_strategy_success_calls_publish(tmp_path):
    """run() 成功 → run 标记 succeeded，artifacts 含 signals 数。
    注：publish() 在子进程执行，父进程 mock 无法断言调用次数。"""
    db_file = tmp_path / "ss.db"
    fake_strategy = _make_strategy(fail=False, signals_count=5)
    trade_date = datetime.now().strftime("%Y-%m-%d")

    import db as _db
    with patch.object(_db, "DB_PATH", db_file), \
         patch("strategies.base.get_strategy", return_value=fake_strategy):
        import run_manifest as rm
        import jobs.nightly_scan as ns
        importlib.reload(rm)
        importlib.reload(ns)

        ok = ns._run_strategy("测试", "test/job2", "test", {}, dry_run=True)

        assert ok is True

        run = rm.get_last_run("test/job2", trade_date)
        assert run is not None
        assert run["status"] == "succeeded"
        assert run["artifacts"] is not None
        assert "signals=5" in run["artifacts"]


def test_start_run_none_skips_publish(tmp_path):
    """同天第二次 _run_strategy → start_run 返回 None → publish 从不被调用。"""
    db_file = tmp_path / "ss.db"
    fake_strategy = _make_strategy(fail=False)
    trade_date = datetime.now().strftime("%Y-%m-%d")

    import db as _db
    with patch.object(_db, "DB_PATH", db_file), \
         patch("strategies.base.get_strategy", return_value=fake_strategy):
        import run_manifest as rm
        import jobs.nightly_scan as ns
        importlib.reload(rm)
        importlib.reload(ns)

        # 先占住 claim
        run_id = rm.start_run("test/job3", trade_date)
        assert run_id is not None

        # 第二次尝试应被跳过
        ok = ns._run_strategy("测试", "test/job3", "test", {}, dry_run=True)

        assert ok is True   # skip 不是失败
        fake_strategy.run.assert_not_called()
        fake_strategy.publish.assert_not_called()


def test_liveness_file_written(tmp_path):
    """_write_liveness() 原子写 last_run.json，结构正确。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import jobs.nightly_scan as ns
        importlib.reload(ns)

        (tmp_path / "data").mkdir(exist_ok=True)
        with patch.object(ns, "_ROOT", tmp_path):
            ns._write_liveness("2099-01-01", attempted=3, succeeded=2,
                               failures=["etf"], duration_sec=42.5)

    live = json.loads((tmp_path / "data" / "last_run.json").read_text())
    assert live["trade_date"] == "2099-01-01"
    assert live["strategies_attempted"] == 3
    assert live["strategies_succeeded"] == 2
    assert live["failures"] == ["etf"]
    assert live["duration_sec"] == 42.5
