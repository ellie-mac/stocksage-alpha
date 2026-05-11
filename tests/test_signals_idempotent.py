"""
signals_store 幂等性测试

覆盖：
  - 有 run_id 时同批次不重复写
  - 无 run_id 时覆盖当日同 source 记录
  - load() 返回正确数量
"""
from __future__ import annotations

import importlib
import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch


def _make_entry(date="2099-01-01", source="test", run_time="2099-01-01 18:00"):
    return {
        "date": date,
        "run_time": run_time,
        "source": source,
        "regime_score": 5.0,
        "buy_signals": [{"code": "000001", "name": "A", "score": 80}],
        "sell_signals": [],
    }


def _fresh_signals_store(tmp_path: Path):
    """返回指向临时 DB 的 signals_store 模块实例"""
    import db as _db_mod
    # 重定向 DB_PATH 到临时目录
    with patch.object(_db_mod, "DB_PATH", tmp_path / "stocksage.db"):
        import signals_store
        importlib.reload(signals_store)
        return signals_store


def test_same_run_id_no_duplicate(tmp_path):
    """同一 run_id 重复 append 只保留一条"""
    import db as _db_mod
    with patch.object(_db_mod, "DB_PATH", tmp_path / "stocksage.db"):
        import signals_store
        importlib.reload(signals_store)

        entry = _make_entry()
        r1 = signals_store.append(entry, run_id=1)
        r2 = signals_store.append(entry, run_id=1)  # duplicate

        assert r1 is True
        assert r2 is False  # ignored

        rows = signals_store.load(n=100, source="test")
        assert len(rows) == 1


def test_no_run_id_overwrites_same_day(tmp_path):
    """无 run_id 时，同 source+date 第二次写覆盖第一次"""
    import db as _db_mod
    with patch.object(_db_mod, "DB_PATH", tmp_path / "stocksage.db"):
        import signals_store
        importlib.reload(signals_store)

        e1 = _make_entry(run_time="2099-01-01 18:00")
        e2 = _make_entry(run_time="2099-01-01 20:00")
        e2["buy_signals"] = [{"code": "000002", "name": "B", "score": 90}]

        signals_store.append(e1, run_id=None)
        signals_store.append(e2, run_id=None)

        rows = signals_store.load(n=100, source="test")
        # 只有1条（被覆盖），且内容是第二次写的
        assert len(rows) == 1
        assert rows[0]["buy_signals"][0]["code"] == "000002"


def test_different_run_ids_both_kept(tmp_path):
    """不同 run_id 的同日同 source 记录各自保留"""
    import db as _db_mod
    with patch.object(_db_mod, "DB_PATH", tmp_path / "stocksage.db"):
        import signals_store
        importlib.reload(signals_store)

        entry = _make_entry()
        signals_store.append(entry, run_id=10)
        signals_store.append(entry, run_id=11)

        rows = signals_store.load(n=100, source="test")
        assert len(rows) == 2
