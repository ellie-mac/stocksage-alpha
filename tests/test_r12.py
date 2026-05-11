"""
R12 测试：
  - DB 每日备份（创建、幂等、旧文件归档）
  - alerts_sent 写入失败不影响 send_failure_alert 返回值
  - publish 失败时 artifacts 记录 publish=failed
"""
from __future__ import annotations

import importlib
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock


# ── DB 备份 ───────────────────────────────────────────────────────────────────

def _make_db(path: Path) -> None:
    """在指定路径建一个最小 SQLite 文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
    conn.commit()
    conn.close()


def test_backup_creates_file(tmp_path):
    """_backup_db() 应在 backup_dir 下创建 stocksage-YYYYMMDD.db。"""
    import db as _db
    db_file = tmp_path / "ss.db"
    _make_db(db_file)

    with patch.object(_db, "DB_PATH", db_file):
        import jobs.nightly_scan as ns
        importlib.reload(ns)
        backup_dir = tmp_path / "backups"
        ns._backup_db(backup_dir=backup_dir)

    today = datetime.now().strftime("%Y%m%d")
    assert (backup_dir / f"stocksage-{today}.db").exists()


def test_backup_idempotent(tmp_path):
    """同一天调用两次不应报错，且只有一个备份文件。"""
    import db as _db
    db_file = tmp_path / "ss.db"
    _make_db(db_file)

    with patch.object(_db, "DB_PATH", db_file):
        import jobs.nightly_scan as ns
        importlib.reload(ns)
        backup_dir = tmp_path / "backups"
        ns._backup_db(backup_dir=backup_dir)
        ns._backup_db(backup_dir=backup_dir)  # second call: dest already exists, skip

    db_files = list(backup_dir.glob("stocksage-*.db"))
    assert len(db_files) == 1


def test_backup_archives_old_files(tmp_path):
    """超过14天的备份文件应被移至 archive/ 子目录。"""
    import db as _db
    db_file = tmp_path / "ss.db"
    _make_db(db_file)

    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    old_name = (datetime.now() - timedelta(days=15)).strftime("stocksage-%Y%m%d.db")
    old_file = backup_dir / old_name
    _make_db(old_file)

    with patch.object(_db, "DB_PATH", db_file):
        import jobs.nightly_scan as ns
        importlib.reload(ns)
        ns._backup_db(backup_dir=backup_dir)

    assert not old_file.exists(), "旧备份应已移出 backups/"
    assert (backup_dir / "archive" / old_name).exists(), "旧备份应在 archive/"


# ── mark_alerted 失败隔离 ─────────────────────────────────────────────────────

def test_mark_alerted_failure_doesnt_crash(tmp_path):
    """_mark_alerted 抛异常时 send_failure_alert 不应崩溃，仍返回正确计数。"""
    import db as _db
    with patch.object(_db, "DB_PATH", tmp_path / "ss.db"):
        import run_manifest as rm
        from notify import notify_failure as nf
        importlib.reload(rm)
        importlib.reload(nf)

        run_id = rm.start_run("crash_job", "2099-05-01")
        rm.finish_run(run_id, success=False, error="test")
        failed = rm.get_failed_runs(days=365)

        with patch.object(nf, "push_wechat"), \
             patch.object(nf, "_mark_alerted", side_effect=Exception("db full")):
            count = nf.send_failure_alert(failed)

    assert count == 1   # push 成功，计数正确；mark_alerted 失败被吞掉


# ── publish=failed artifact ───────────────────────────────────────────────────

def test_publish_failure_recorded_in_artifacts(tmp_path):
    """publish() 抛异常时，finish_run 的 artifacts 应含 'publish=failed'。"""
    import db as _db
    db_file = tmp_path / "ss.db"

    with patch.object(_db, "DB_PATH", db_file):
        import run_manifest as rm
        import jobs.nightly_scan as ns
        importlib.reload(rm)
        importlib.reload(ns)

        recorded: dict = {}

        def _fake_finish_run(run_id, ok, *, duration_sec, artifacts, error):
            recorded["artifacts"] = artifacts
            recorded["ok"] = ok
            rm.finish_run(run_id, ok, duration_sec=duration_sec,
                          artifacts=artifacts, error=error)

        mock_strategy = MagicMock()
        mock_result = MagicMock()
        mock_result.metadata = {"failed": False}
        mock_result.signals = []
        mock_result.regime_label = "bull"
        mock_strategy.run.return_value = mock_result
        mock_strategy.publish.side_effect = Exception("push timeout")

        with patch("jobs.nightly_scan.finish_run", side_effect=_fake_finish_run), \
             patch("jobs.nightly_scan.start_run", return_value=1), \
             patch("strategies.base.get_strategy", return_value=mock_strategy):
            ns._run_strategy("test", "test_job", "main", {}, dry_run=True)

    assert recorded.get("ok") is True        # publish 失败不影响 run 状态
    assert "publish=failed" in (recorded.get("artifacts") or [])
