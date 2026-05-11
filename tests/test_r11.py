"""
R11 新增功能测试：
  - signals_store.append() 自动从上下文补填 run_id
  - 失败告警去重（同 run_id 只推一次）
  - check_liveness.py 各场景
"""
from __future__ import annotations

import importlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch


# ── helpers ───────────────────────────────────────────────────────────────────

def _reload_all(tmp_path):
    """在 tmp_path DB 下重新加载所有相关模块，返回 (rm, ss) tuple。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import run_manifest as rm
        import signals_store as ss
        importlib.reload(rm)
        importlib.reload(ss)
    return rm, ss


# ── signals_store run_id auto-fill ────────────────────────────────────────────

def test_append_auto_fills_run_id_from_context(tmp_path):
    """bind_run_id 后 append() 不传 run_id，应走 INSERT OR IGNORE 路径（幂等）。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import signals_store as ss
        import logger as lg
        importlib.reload(ss)

        lg.bind_run_id(42)
        entry = {
            "date": "2099-01-01", "run_time": "2099-01-01 22:00",
            "regime_score": 5.0, "source": "main",
            "buy_signals": [{"code": "000001"}], "sell_signals": [],
        }

        r1 = ss.append(entry)   # first write: inserted
        r2 = ss.append(entry)   # second write with same run_id: should be ignored

        lg.bind_run_id(None)

    assert r1 is True
    assert r2 is False   # INSERT OR IGNORE → deduped


def test_append_no_context_still_uses_upsert(tmp_path):
    """无 bind_run_id 时 append() 走 ON CONFLICT DO UPDATE 覆盖路径（向后兼容）。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import signals_store as ss
        import logger as lg
        importlib.reload(ss)

        lg.bind_run_id(None)  # no context
        entry = {
            "date": "2099-02-01", "run_time": "2099-02-01 22:00",
            "regime_score": 5.0, "source": "main",
            "buy_signals": [], "sell_signals": [],
        }

        r1 = ss.append(entry)
        r2 = ss.append(entry)  # second write: upsert (rowcount=1)

    assert r1 is True
    assert r2 is True   # upsert counts as "inserted"


# ── failure alert dedup ───────────────────────────────────────────────────────

def test_send_failure_alert_dedup_skips_already_sent(tmp_path):
    """同一 run_id 的失败记录第二次调用不触发 push_wechat。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import run_manifest as rm
        from notify import notify_failure as nf
        importlib.reload(rm)
        importlib.reload(nf)

        run_id = rm.start_run("dedup_job", "2099-03-01")
        rm.finish_run(run_id, success=False, error="boom")
        failed = rm.get_failed_runs(days=365)
        assert len(failed) == 1

        with patch.object(nf, "push_wechat") as mock_push:
            c1 = nf.send_failure_alert(failed)
            c2 = nf.send_failure_alert(failed)  # same run_id → dedup

    assert c1 == 1
    assert c2 == 0          # already sent
    assert mock_push.call_count == 1


def test_send_failure_alert_dry_run_bypasses_dedup(tmp_path):
    """dry_run=True 时跳过去重，总是推送（不写 alerts_sent）。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import run_manifest as rm
        from notify import notify_failure as nf
        importlib.reload(rm)
        importlib.reload(nf)

        run_id = rm.start_run("dry_job", "2099-04-01")
        rm.finish_run(run_id, success=False)
        failed = rm.get_failed_runs(days=365)

        with patch.object(nf, "push_wechat") as mock_push:
            c1 = nf.send_failure_alert(failed, dry_run=True)
            c2 = nf.send_failure_alert(failed, dry_run=True)

    assert c1 == 1
    assert c2 == 1    # dry_run: no dedup
    assert mock_push.call_count == 2


# ── check_liveness ────────────────────────────────────────────────────────────

def _write_live(tmp_path, completed_at: str, succeeded=3, attempted=3, failures=None):
    (tmp_path / "data").mkdir(exist_ok=True)
    (tmp_path / "data" / "last_run.json").write_text(json.dumps({
        "completed_at":          completed_at,
        "trade_date":            completed_at[:10],
        "duration_sec":          60.0,
        "strategies_attempted":  attempted,
        "strategies_succeeded":  succeeded,
        "failures":              failures or [],
    }), encoding="utf-8")


def test_liveness_ok(tmp_path):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_live(tmp_path, now)

    import jobs.check_liveness as cl
    importlib.reload(cl)
    with patch.object(cl, "_LIVE_FILE", tmp_path / "data" / "last_run.json"), \
         patch.object(cl, "push_wechat") as mock_push:
        result = cl.check(dry_run=True)

    assert result is True
    mock_push.assert_not_called()


def test_liveness_file_missing_alerts(tmp_path):
    import jobs.check_liveness as cl
    importlib.reload(cl)
    missing = tmp_path / "data" / "last_run.json"
    with patch.object(cl, "_LIVE_FILE", missing), \
         patch.object(cl, "push_wechat") as mock_push:
        result = cl.check(dry_run=True)

    assert result is False
    mock_push.assert_called_once()


def test_liveness_stale_alerts(tmp_path):
    old_time = (datetime.now() - timedelta(hours=30)).strftime("%Y-%m-%d %H:%M:%S")
    _write_live(tmp_path, old_time)

    import jobs.check_liveness as cl
    importlib.reload(cl)
    with patch.object(cl, "_LIVE_FILE", tmp_path / "data" / "last_run.json"), \
         patch.object(cl, "push_wechat") as mock_push:
        result = cl.check(dry_run=True)

    assert result is False
    mock_push.assert_called_once()
    assert "超时" in mock_push.call_args[0][0]


def test_liveness_failures_alerts(tmp_path):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_live(tmp_path, now, succeeded=2, attempted=3, failures=["etf"])

    import jobs.check_liveness as cl
    importlib.reload(cl)
    with patch.object(cl, "_LIVE_FILE", tmp_path / "data" / "last_run.json"), \
         patch.object(cl, "push_wechat") as mock_push:
        result = cl.check(dry_run=True)

    assert result is False
    mock_push.assert_called_once()
