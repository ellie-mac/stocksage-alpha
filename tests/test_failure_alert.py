"""
失败告警闭环测试：
  - 有失败记录时 send_failure_alert 被调用
  - 无失败记录时不调用
  - 返回计数等于传入记录数
"""
from __future__ import annotations

import importlib
from unittest.mock import patch


def test_send_failure_alert_called_when_failures_exist(tmp_path):
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "stocksage.db"):
        import run_manifest as rm
        from notify import notify_failure as nf
        importlib.reload(rm)
        importlib.reload(nf)

        run_id = rm.start_run("bad_job", "2099-05-01")
        rm.finish_run(run_id, success=False, error="boom")
        failed = rm.get_failed_runs(days=365)
        assert len(failed) >= 1

        with patch.object(nf, "push_wechat") as mock_push:
            count = nf.send_failure_alert(failed)

    assert count == len(failed)
    mock_push.assert_called_once()
    assert "bad_job" in mock_push.call_args[0][1]


def test_send_failure_alert_no_call_when_empty(tmp_path):
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "stocksage.db"):
        from notify import notify_failure as nf
        importlib.reload(nf)

        with patch.object(nf, "push_wechat") as mock_push:
            count = nf.send_failure_alert([])

    assert count == 0
    mock_push.assert_not_called()


def test_send_failure_alert_dry_run_passes_through(tmp_path):
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "stocksage.db"):
        import run_manifest as rm
        from notify import notify_failure as nf
        importlib.reload(rm)
        importlib.reload(nf)

        run_id = rm.start_run("dry_job", "2099-06-01")
        rm.finish_run(run_id, success=False, error="dry error")
        failed = rm.get_failed_runs(days=365)

        with patch.object(nf, "push_wechat") as mock_push:
            nf.send_failure_alert(failed, dry_run=True)

    mock_push.assert_called_once()
    _, kwargs = mock_push.call_args
    assert kwargs.get("dry_run") is True
