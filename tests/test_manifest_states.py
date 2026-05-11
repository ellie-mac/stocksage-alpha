"""
run_manifest 状态机测试

覆盖：
  - started → succeeded
  - started → failed
  - 孤儿 started（> 2h）→ crashed（由下次 start_run 触发）
  - was_successful_for_trade_date 语义
  - get_failed_runs 包含 crashed
"""
from __future__ import annotations

import importlib
from unittest.mock import patch


def _fresh_run_manifest(tmp_path):
    import db as _db_mod
    with patch.object(_db_mod, "DB_PATH", tmp_path / "stocksage.db"):
        import run_manifest
        importlib.reload(run_manifest)
        return run_manifest


def test_started_to_succeeded(tmp_path):
    import db as _db_mod
    with patch.object(_db_mod, "DB_PATH", tmp_path / "stocksage.db"):
        import run_manifest
        importlib.reload(run_manifest)

        run_id = run_manifest.start_run("test_job", "2099-01-01")
        assert run_id is not None, "first start_run should succeed"
        run_manifest.finish_run(run_id, success=True, duration_sec=1.0)

        assert run_manifest.was_successful_for_trade_date("test_job", "2099-01-01")


def test_atomic_claim_blocks_duplicate(tmp_path):
    """同天同 job 第二次 start_run 应返回 None（claim 被占）"""
    import db as _db_mod
    with patch.object(_db_mod, "DB_PATH", tmp_path / "stocksage.db"):
        import run_manifest
        importlib.reload(run_manifest)

        run_id1 = run_manifest.start_run("job_x", "2099-02-01")
        assert run_id1 is not None

        run_id2 = run_manifest.start_run("job_x", "2099-02-01")
        assert run_id2 is None  # blocked by claim


def test_failed_run_allows_retry(tmp_path):
    """failed 状态不阻塞同天重试"""
    import db as _db_mod
    with patch.object(_db_mod, "DB_PATH", tmp_path / "stocksage.db"):
        import run_manifest
        importlib.reload(run_manifest)

        run_id1 = run_manifest.start_run("job_y", "2099-03-01")
        run_manifest.finish_run(run_id1, success=False)

        run_id2 = run_manifest.start_run("job_y", "2099-03-01")
        assert run_id2 is not None  # retry allowed after failure


def test_started_to_failed(tmp_path):
    import db as _db_mod
    with patch.object(_db_mod, "DB_PATH", tmp_path / "stocksage.db"):
        import run_manifest
        importlib.reload(run_manifest)

        run_id = run_manifest.start_run("test_job", "2099-01-01")
        run_manifest.finish_run(run_id, success=False, error="boom")

        assert not run_manifest.was_successful_for_trade_date("test_job", "2099-01-01")
        failed = run_manifest.get_failed_runs(days=365)
        assert any(r["id"] == run_id for r in failed)


def test_orphan_started_marked_crashed(tmp_path):
    """手动植入一条 started_at 超时的行，下次 start_run 应把它标成 crashed"""
    import db as _db_mod
    from db import _conn

    db_file = tmp_path / "stocksage.db"
    with patch.object(_db_mod, "DB_PATH", db_file):
        import run_manifest
        importlib.reload(run_manifest)

        # 直接插入一条假 started，时间设在 3 小时前
        with _conn() as conn:
            conn.execute("""
                INSERT INTO runs (job_name, trade_date, status, started_at)
                VALUES ('old_job', '2099-01-01', 'started',
                        datetime('now','localtime','-3 hours'))
            """)

        # 触发孤儿扫描
        run_manifest.start_run("new_job", "2099-01-01")

        failed = run_manifest.get_failed_runs(days=365)
        crashed = [r for r in failed if r["status"] == run_manifest.STATUS_CRASHED]
        assert len(crashed) >= 1
        assert crashed[0]["job_name"] == "old_job"


def test_get_failed_runs_includes_crashed(tmp_path):
    import db as _db_mod
    with patch.object(_db_mod, "DB_PATH", tmp_path / "stocksage.db"):
        import run_manifest
        importlib.reload(run_manifest)

        run_id = run_manifest.start_run("j", "2099-01-02")
        run_manifest.finish_run(run_id, success=False)

        runs = run_manifest.get_failed_runs(days=365)
        statuses = {r["status"] for r in runs}
        assert run_manifest.STATUS_FAILED in statuses
