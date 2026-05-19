#!/usr/bin/env python3
"""
Shared utilities for StockSage monitor scripts.

Centralises:
  - A-share trading calendar (holiday-aware)
  - Trading hours helpers
  - WeChat push (PushPlus preferred, Server酱 fallback)
  - ETF / T+0 identification
"""

from __future__ import annotations

import functools
import json
import os
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(__file__))

import cache as _cache

# Trading calendar — canonical implementation lives in trading_calendar.py.
# Re-exported here for backward compatibility with callers that do
# `from common import is_trading_day` etc.
from trading_calendar import (  # noqa: F401
    _MORNING_OPEN, _MORNING_CLOSE, _AFTERNOON_OPEN, _AFTERNOON_CLOSE,
    _load_trade_dates, get_trade_dates,
    is_trading_day, is_trading_hours, next_session_seconds,
)


def retry(attempts: int = 3, backoff: float = 3.0, exc_types: tuple = (Exception,)):
    """Decorator: retry a function up to `attempts` times with `backoff` seconds sleep."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for i in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exc_types as e:
                    if i == attempts:
                        raise
                    print(f"[retry] {fn.__name__} attempt {i}/{attempts} failed: {e}")
                    time.sleep(backoff)
        return wrapper
    return decorator



# ── WeChat push ────────────────────────────────────────────────────────────────

_pushplus_token: str = ""   # set once at startup via configure_pushplus()


def configure_pushplus(token: str) -> None:
    """Call once at startup with the PushPlus token from config.json."""
    global _pushplus_token
    _pushplus_token = token.strip() if token else ""


def _send_pushplus(title: str, desp: str, token: str, retries: int = 3) -> None:
    payload = json.dumps({
        "token":    token,
        "title":    title[:100],
        "content":  desp,
        "template": "markdown",
    }).encode("utf-8")
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                "https://www.pushplus.plus/send",
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                resp = json.loads(r.read().decode("utf-8"))
            if resp.get("code") == 200:
                print(f"[OK] 微信推送成功: {title}")
                return
            print(f"[WARN] PushPlus: code={resp.get('code')} msg={resp.get('msg')}（第{attempt}次）")
            if attempt < retries:
                time.sleep(3)
        except Exception as e:
            print(f"[WARN] PushPlus 推送失败（第{attempt}次）: {e}")
            if attempt < retries:
                time.sleep(3)


def send_wechat(title: str, desp: str, sendkey: str, dry_run: bool = False) -> None:
    if dry_run:
        print(f"[DRY-RUN] 微信推送: {title}")
        print(f"[DRY-RUN] 内容预览:\n{desp[:300]}{'...' if len(desp) > 300 else ''}")
        return
    if _pushplus_token:
        _send_pushplus(title, desp, _pushplus_token)
    elif sendkey:
        from serverchan_sdk import sc_send
        resp = sc_send(sendkey, title, desp)
        if resp.get("code") == 0:
            print(f"[OK] 微信推送成功: {title}")
        else:
            print(f"[WARN] 微信推送: code={resp.get('code')} msg={resp.get('message')}")
    else:
        print(f"[WARN] 未配置推送渠道（pushplus.token / serverchan.sendkey），跳过: {title}")


@functools.lru_cache(maxsize=1)
def load_alert_config() -> dict:
    """Load alert_config.json from repo root. Returns {} on failure.

    lru_cache(1) — alert_config 只在启动时读，进程内复用，避免 24+ callers 重复 IO。
    config 改动需要重启进程（已通过 watchdog / setup_scheduler 重新生成 bat 实现）。
    """
    cfg_path = Path(__file__).resolve().parent.parent / "alert_config.json"
    return read_json(cfg_path, default={})


def read_json(path, default=None):
    """从 JSON 文件读取。失败返回 default（默认 None，传 {} 或 [] 适应 dict/list 期望）。

    替代散落各处的 `json.loads(path.read_text(encoding="utf-8"))` 模板，统一错误处理。
    """
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


from contextlib import contextmanager as _contextmanager


@_contextmanager
def file_lock(path, timeout: float = 30.0, stale_after: float = 120.0):
    """简单的 lock-file 互斥（跨进程，跨平台）。

    用于保护对同一文件的 read-modify-write —— 例如 main/small_strategy.save_picks
    都读改写 latest_picks.json。

    实现：在 path.lock 路径创建标记文件；等待最多 timeout 秒；超 stale_after 的
    锁文件视为僵尸（前一个进程崩了），强制清理。

    用法：
        with file_lock(LATEST_PICKS_PATH):
            data = read_json(LATEST_PICKS_PATH, default={})
            data["xxx"] = "yyy"
            write_json(LATEST_PICKS_PATH, data)
    """
    lock_path = Path(str(path) + ".lock")
    start = time.time()
    while True:
        try:
            # O_EXCL: 文件已存在时失败 — 原子 claim
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            break  # 拿到锁
        except FileExistsError:
            # 检查 stale
            try:
                age = time.time() - lock_path.stat().st_mtime
                if age > stale_after:
                    lock_path.unlink(missing_ok=True)
                    continue
            except FileNotFoundError:
                continue
            if time.time() - start > timeout:
                raise TimeoutError(f"file_lock({path}) timeout after {timeout}s")
            time.sleep(0.1)
    try:
        yield
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass


def write_json(path, data, *, indent: int | None = 2, ensure_ascii: bool = False, atomic: bool = True) -> bool:
    """原子写入 JSON。atomic=True 时先写 .tmp 再 replace，避免半残文件。

    Returns True on success.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        text = json.dumps(data, ensure_ascii=ensure_ascii, indent=indent)
        if atomic:
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(text, encoding="utf-8")
            tmp.replace(p)
        else:
            p.write_text(text, encoding="utf-8")
        return True
    except Exception:
        return False


def push_wechat(title: str, body: str, dry_run: bool = False) -> None:
    """Load alert_config, configure pushplus, and send WeChat — one call."""
    cfg = load_alert_config()
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
    send_wechat(title, body, cfg.get("serverchan", {}).get("sendkey", ""), dry_run=dry_run)


def setup_push(config: dict) -> str:
    """Configure PushPlus from config and return the Server酱 sendkey."""
    configure_pushplus(config.get("pushplus", {}).get("token", ""))
    return config.get("serverchan", {}).get("sendkey", "")


def regime_emoji(score: float) -> str:
    """Map a 0–10 regime score to a bear/neutral/bull emoji."""
    if score <= 3:
        return "🐻"
    if score <= 6:
        return "🟡"
    return "🐂"


def is_limit_locked(pct_chg: float, threshold: float = 9.5) -> bool:
    """True if stock hit limit-up or limit-down (|pct_chg| >= threshold)."""
    return abs(pct_chg) >= threshold


# ── Spot market (cached) ──────────────────────────────────────────────────────

def get_spot_em(retries: int = 3):
    """
    Fetch full A-share spot market data (stock_zh_a_spot_em) with caching.

    TTL: 90s during trading hours, 4h after close (prices are final).
    Multiple scripts can share one API call per session.
    Returns a pandas DataFrame with all columns from stock_zh_a_spot_em,
    or an empty DataFrame on failure.
    """
    import pandas as pd
    now = datetime.now()
    hm  = now.hour * 60 + now.minute
    in_trading = (
        (9 * 60 + 25 <= hm <= 11 * 60 + 35) or
        (12 * 60 + 55 <= hm <= 15 * 60 + 5)
    )
    ttl = 90 if in_trading else 4 * 3600   # 90s live; 4h after close

    cached = _cache.get_df("spot_em", ttl)
    if cached is not None:
        return cached
    # Check if fetcher already pulled fresh spot data (avoids duplicate API call)
    spot_all = _cache.get("spot_all", ttl)
    if spot_all is not None:
        try:
            import pandas as _pd_inner
            return _pd_inner.DataFrame(spot_all)
        except Exception:
            pass
    import akshare as ak
    for attempt in range(1, retries + 1):
        try:
            df = ak.stock_zh_a_spot_em()
            _cache.set("spot_em", df)
            return df
        except Exception as e:
            print(f"[spot_em] 获取失败（第{attempt}次）: {e}")
            if attempt < retries:
                time.sleep(3)
    # EM 全部失败，fallback 到新浪（Singapore VM 可访问）
    print("[spot_em] EM 全部失败，尝试新浪 fallback...")
    try:
        df_sina = ak.stock_zh_a_spot()
        if df_sina is not None and not df_sina.empty:
            df_sina = df_sina.copy()
            if "代码" in df_sina.columns:
                df_sina["代码"] = (df_sina["代码"].astype(str)
                                   .str.replace(r"^(sh|sz)", "", regex=True)
                                   .str.zfill(6))
            # 不伪造总市值列 — NaN会让市值过滤把所有股票全排掉
            _cache.set("spot_em", df_sina)
            print(f"[spot_em] 新浪 fallback 成功: {len(df_sina)} 只股票")
            return df_sina
    except Exception as e:
        print(f"[spot_em] 新浪 fallback 失败: {e}")
    return pd.DataFrame()


# ── ETF / T+0 identification ───────────────────────────────────────────────────

def is_etf(code: str, name: str = "", is_t0_override: Optional[bool] = None) -> bool:
    """
    True for exchange-listed ETFs eligible for T+0 secondary-market trading.

    Priority:
      1. Explicit `is_t0` flag in the holding dict (set by user — most reliable).
      2. Unambiguous ETF code ranges (510xxx-518xxx, 588xxx, 159xxx).
      3. All other ranges → conservatively T+1 unless explicitly flagged.
    """
    if is_t0_override is not None:
        return is_t0_override
    c = str(code).zfill(6)
    return c.startswith("51") or c.startswith("159") or c.startswith("588")


def is_t1_locked(holding: dict) -> bool:
    """True if holding was bought today and is subject to T+1 restriction."""
    flag = holding.get("is_t0")
    if is_etf(holding.get("code", ""), holding.get("name", ""), flag):
        return False
    bought_date = holding.get("bought_date")
    if not bought_date:
        return False
    return bought_date == datetime.now().strftime("%Y-%m-%d")
