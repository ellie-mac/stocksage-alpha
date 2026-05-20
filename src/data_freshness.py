"""价格数据新鲜度检查 — 纯库，无 jobs/strategies 依赖。

供 scanner 在 EOD 启动时调用，确保 fetcher cache 有当日数据，否则触发
jobs/prefetch.py 重跑（通过 subprocess，不直接 import）。

原本 wait_for_fresh_prices 定义在 src/jobs/prefetch.py 里，但 5 处 scanner
（gc / sideways / chip/pipeline / escalator / monitor）都 `from jobs.prefetch
import wait_for_fresh_prices` —— strategies/ ↔ jobs/ 反向依赖，违反分层。
本文件把这个 helper 抽出作为公共纯库，jobs/prefetch.py 保留 wrapper 向后兼容。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

_UNIVERSE_PATH = _REPO_ROOT / "data" / "universe_main.json"
_PREFETCH_SCRIPT = _REPO_ROOT / "src" / "jobs" / "prefetch.py"


def _load_universe_sample(n: int = 10) -> list[str]:
    """Load first N codes from universe_main.json (sampling — only used to check freshness)."""
    if not _UNIVERSE_PATH.exists():
        return []
    try:
        codes = json.loads(_UNIVERSE_PATH.read_text(encoding="utf-8"))
        if isinstance(codes, list):
            return codes[:n]
        return list(codes.keys())[:n]
    except Exception:
        return []


def wait_for_fresh_prices() -> bool:
    """检查当日收盘价是否已缓存。若否，触发 prefetch.py 重跑直至 fresh。

    在 EOD scanner（gc/sideways/chip/main/monitor）启动时调用，作为"数据新鲜度门"。
    工作日 15:00 之后才生效（盘前 / 周末直接 return True 不阻塞）。

    Returns True if data is confirmed fresh for today's trading date.
    """
    import cache as _cache
    import pandas as _pd

    now = datetime.now()
    if now.weekday() >= 5 or now.hour < 15:
        return True

    expected = now.strftime("%Y%m%d")

    def _is_fresh() -> bool:
        codes = _load_universe_sample(10)
        for code in codes:
            raw = _cache.get_df(f"price_{code[-6:]}_550", 999_999_999)
            if raw is None or raw.empty:
                continue
            latest = raw.iloc[-1]["date"]
            if isinstance(latest, _pd.Timestamp):
                latest = latest.strftime("%Y%m%d")
            if str(latest).replace("-", "")[:8] >= expected:
                return True
        return False

    if _is_fresh():
        return True

    # 锁文件：另一个进程已在 prefetch 时等它，不重复跑
    lock_file = _REPO_ROOT / "src" / "cache" / ".price_prefetch.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)

    if lock_file.exists():
        try:
            lock_age = time.time() - lock_file.stat().st_mtime
        except Exception:
            lock_age = 0
        if lock_age < 1800:   # 30 min 内的锁视为有效
            print(f"[wait_prices] 检测到并发 prefetch 运行中（锁 {lock_age:.0f}s 前创建），等待...", flush=True)
            for _ in range(120):  # 等最多 10 min
                time.sleep(5)
                if _is_fresh():
                    print("[wait_prices] 数据已更新到今日 ✓", flush=True)
                    return True
                if not lock_file.exists():
                    break
            fresh = _is_fresh()
            if not fresh:
                print("[wait_prices] 等待超时，继续执行", flush=True)
            return fresh

    print(f"[wait_prices] 价格数据未到今日 ({expected})，触发 prefetch ...", flush=True)
    try:
        lock_file.write_text(str(os.getpid()))
        subprocess.run(
            [sys.executable, "-X", "utf8", str(_PREFETCH_SCRIPT), "--price", "--force"],
            cwd=str(_REPO_ROOT),
        )
    finally:
        try:
            lock_file.unlink()
        except Exception:
            pass

    fresh = _is_fresh()
    if fresh:
        print("[wait_prices] 价格数据已更新到今日 ✓", flush=True)
    else:
        print("[wait_prices] prefetch 完成但数据仍非今日（可能为非交易日或数据源延迟），继续执行", flush=True)
    return fresh
