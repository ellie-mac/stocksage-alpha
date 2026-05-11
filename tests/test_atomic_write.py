"""
原子写入测试

覆盖：
  - signals_store._json_append 写失败时旧文件仍可读（tmp 替换语义）
  - load() 在 DB 不存在时 fallback 到 JSON
  - signals_store 与 run_manifest 共用同一 DB 文件
"""
from __future__ import annotations

import importlib
import json
from pathlib import Path
from unittest.mock import patch


def test_json_fallback_when_db_missing(tmp_path):
    """DB 不存在时，load() 应从 signals_log.json 返回数据"""
    import db as _db_mod
    # 指向不存在的 DB 路径（但 JSON 存在）
    fake_db = tmp_path / "nonexistent.db"
    json_path = tmp_path / "data" / "signals_log.json"
    json_path.parent.mkdir(parents=True)
    entries = [{"date": "2099-01-01", "source": "test",
                "buy_signals": [], "sell_signals": [], "run_time": "2099-01-01 18:00"}]
    json_path.write_text(json.dumps(entries), encoding="utf-8")

    with patch.object(_db_mod, "DB_PATH", fake_db):
        import signals_store
        importlib.reload(signals_store)
        # 重定向 _JSON
        with patch.object(signals_store, "_JSON", json_path):
            rows = signals_store.load(n=100)
        assert len(rows) == 1
        assert rows[0]["date"] == "2099-01-01"


def test_signals_and_manifest_share_db(tmp_path):
    """signals_store 和 run_manifest 写入同一 stocksage.db"""
    import db as _db_mod
    db_file = tmp_path / "stocksage.db"

    with patch.object(_db_mod, "DB_PATH", db_file):
        import signals_store, run_manifest
        importlib.reload(signals_store)
        importlib.reload(run_manifest)

        run_id = run_manifest.start_run("job", "2099-01-01")
        signals_store.append(
            {"date": "2099-01-01", "source": "main",
             "run_time": "2099-01-01 18:00", "buy_signals": [], "sell_signals": []},
            run_id=run_id,
        )
        run_manifest.finish_run(run_id, success=True)

    # 两张表应在同一文件里
    import sqlite3
    conn = sqlite3.connect(str(db_file))
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    conn.close()
    assert "runs" in tables
    assert "signal_runs" in tables


def test_tmp_replace_leaves_old_file_intact(tmp_path):
    """模拟 _json_append 中途失败：旧 JSON 不应被损坏"""
    import db as _db_mod
    db_file = tmp_path / "stocksage.db"
    json_path = tmp_path / "signals_log.json"
    old_content = [{"date": "2099-01-01", "source": "old"}]
    json_path.write_text(json.dumps(old_content), encoding="utf-8")

    with patch.object(_db_mod, "DB_PATH", db_file):
        import signals_store
        importlib.reload(signals_store)

        # 模拟 tmp.replace() 失败（权限错误）
        original_replace = Path.replace
        call_count = [0]

        def mock_replace(self, target):
            call_count[0] += 1
            if call_count[0] == 1:
                raise OSError("simulated failure")
            return original_replace(self, target)

        with patch.object(Path, "replace", mock_replace):
            with patch.object(signals_store, "_JSON", json_path):
                try:
                    signals_store._json_append({"date": "2099-01-01"})
                except OSError:
                    pass

        # 旧 JSON 应仍可读
        data = json.loads(json_path.read_text(encoding="utf-8"))
        assert data == old_content
