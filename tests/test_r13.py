"""
R13 测试：前置数据质量门控 + check_liveness 非交易日语义修复
  - 非交易日跳过
  - --force 绕过非交易日检查
  - 交易日历接口报错时保守放行（fail open）
  - 零信号时记录 warning
  - check_liveness: skipped 状态不告警
  - check_liveness: skipped 超期仍告警
  - nightly_scan 跳过时写入 status=skipped 的 liveness 文件
"""
from __future__ import annotations

import importlib
import json
from datetime import datetime, timedelta
from pathlib import Path
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


# ── check_liveness: skipped 状态 ─────────────────────────────────────────────

def _write_live(tmp_path: Path, completed_at: str, status: str = "ok",
                succeeded: int = 3, attempted: int = 3, failures=None) -> None:
    (tmp_path / "data").mkdir(exist_ok=True)
    (tmp_path / "data" / "last_run.json").write_text(json.dumps({
        "completed_at":          completed_at,
        "trade_date":            completed_at[:10],
        "status":                status,
        "duration_sec":          0.0,
        "strategies_attempted":  attempted,
        "strategies_succeeded":  succeeded,
        "failures":              failures or [],
    }), encoding="utf-8")


def test_liveness_skipped_recent_is_ok(tmp_path):
    """非交易日跳过且记录在 25h 内，check_liveness 应返回 True、不告警。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_live(tmp_path, now, status="skipped", succeeded=0, attempted=0)

    import jobs.check_liveness as cl
    importlib.reload(cl)
    with patch.object(cl, "_LIVE_FILE", tmp_path / "data" / "last_run.json"), \
         patch.object(cl, "push_wechat") as mock_push:
        result = cl.check(dry_run=True)

    assert result is True
    mock_push.assert_not_called()


def test_liveness_skipped_stale_alerts(tmp_path):
    """skipped 记录超过 25h（如连续多天未更新），应告警。"""
    old = (datetime.now() - timedelta(hours=30)).strftime("%Y-%m-%d %H:%M:%S")
    _write_live(tmp_path, old, status="skipped", succeeded=0, attempted=0)

    import jobs.check_liveness as cl
    importlib.reload(cl)
    with patch.object(cl, "_LIVE_FILE", tmp_path / "data" / "last_run.json"), \
         patch.object(cl, "push_wechat") as mock_push:
        result = cl.check(dry_run=True)

    assert result is False
    mock_push.assert_called_once()


def test_nightly_scan_writes_skipped_liveness(tmp_path):
    """_write_liveness(status='skipped') 应写出含 status 字段的 liveness 文件。"""
    import jobs.nightly_scan as ns
    importlib.reload(ns)

    live_file = tmp_path / "data" / "last_run.json"
    trade_date = datetime.now().strftime("%Y-%m-%d")

    with patch.object(ns, "_ROOT", tmp_path):
        ns._write_liveness(trade_date, 0, 0, [], 0.0, status="skipped")

    assert live_file.exists()
    data = json.loads(live_file.read_text(encoding="utf-8"))
    assert data["status"] == "skipped"
    assert data["strategies_attempted"] == 0
