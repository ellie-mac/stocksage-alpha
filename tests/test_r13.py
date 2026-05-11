"""
R13 测试：前置数据质量门控
  - 非交易日跳过
  - --force 绕过非交易日检查
  - 交易日历接口报错时保守放行（fail open）
  - 零信号时记录 warning
"""
from __future__ import annotations

import importlib
from unittest.mock import patch, MagicMock


# ── _pre_run_checks ───────────────────────────────────────────────────────────

def test_pre_check_skips_on_non_trading_day():
    """非交易日且未 --force 时应返回 False。"""
    import jobs.nightly_scan as ns
    importlib.reload(ns)
    with patch("trading_calendar.is_trading_day", return_value=False):
        result = ns._pre_run_checks(force=False)
    assert result is False


def test_pre_check_force_overrides_non_trading_day():
    """--force 时即使非交易日也应返回 True。"""
    import jobs.nightly_scan as ns
    importlib.reload(ns)
    with patch("trading_calendar.is_trading_day", return_value=False):
        result = ns._pre_run_checks(force=True)
    assert result is True


def test_pre_check_trading_day_passes():
    """交易日正常返回 True。"""
    import jobs.nightly_scan as ns
    importlib.reload(ns)
    with patch("trading_calendar.is_trading_day", return_value=True):
        result = ns._pre_run_checks(force=False)
    assert result is True


def test_pre_check_calendar_error_fails_open():
    """交易日历接口抛异常时，保守放行（返回 True），避免误杀夜跑。"""
    import jobs.nightly_scan as ns
    importlib.reload(ns)
    with patch("trading_calendar.is_trading_day", side_effect=Exception("network error")):
        result = ns._pre_run_checks(force=False)
    assert result is True


# ── 零信号 warning ────────────────────────────────────────────────────────────

def test_zero_signals_emits_warning(tmp_path, caplog):
    """策略返回 0 个信号时应有 strategy_zero_signals warning 日志。"""
    import db as _db
    import logging

    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import jobs.nightly_scan as ns
        import run_manifest as rm
        importlib.reload(rm)
        importlib.reload(ns)

        mock_strategy = MagicMock()
        mock_result = MagicMock()
        mock_result.metadata = {"failed": False}
        mock_result.signals = []          # 零信号
        mock_result.regime_label = "bull"
        mock_strategy.run.return_value = mock_result
        mock_strategy.publish.return_value = None

        with patch("jobs.nightly_scan.start_run", return_value=1), \
             patch("jobs.nightly_scan.finish_run"), \
             patch("strategies.base.get_strategy", return_value=mock_strategy), \
             caplog.at_level(logging.WARNING, logger="nightly_scan"):
            ns._run_strategy("test", "test_job", "main", {}, dry_run=True)

    assert any("zero_signals" in r.message for r in caplog.records)
