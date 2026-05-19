#!/usr/bin/env python3
"""
小盘策略夜间选股 — 从全市场小市值股中选明日候选

用法：
    python -X utf8 src/strategies/small_strategy.py
    python -X utf8 src/strategies/small_strategy.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from factors.config import REGIME_WEIGHTS_SMALLCAP, SMALLCAP_CONFIG
from factors import score_market_regime
import fetcher
import pandas as pd
from common import setup_push
from strategies._push import regime_header_line, wechat_send_with_log, DISCLAIMER
from strategies._scoring import score_universe
from report.utils import (
    regime_key as _regime_key,
    compact_factor_scores as _compact_factor_scores,
)

_ROOT             = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LATEST_PICKS_PATH = os.path.join(_ROOT, "data", "latest_picks.json")


# ── 核心扫描 ───────────────────────────────────────────────────────────────────

def scan(
    config: dict,
    thresholds: dict,
    held_codes: set | None = None,
    regime_score: float = 5.0,
) -> list[dict]:
    """扫描全市场小市值候选，返回 top_n picks。"""
    held_codes = held_codes or set()
    rk = _regime_key(regime_score)

    sc_cfg    = {**SMALLCAP_CONFIG, **config.get("smallcap", {})}
    max_cap   = sc_cfg["max_cap_yi"] * 1e8
    prefilt_n = sc_cfg["prefilter_n"]
    top_n     = sc_cfg["top_n"]
    buy_trig  = thresholds.get("buy_score_trigger", 60)

    spot_df = fetcher._get_spot_df()
    if spot_df is None or spot_df.empty:
        print("[small_strategy] spot_df unavailable")
        return []
    if not {"名称", "代码"}.issubset(spot_df.columns):
        print(f"[small_strategy] spot_df missing essential columns")
        return []
    if "总市值" not in spot_df.columns:
        from strategies._quality import inject_marketcap as _inject_mc
        spot_df = _inject_mc(spot_df)
    if "总市值" not in spot_df.columns:
        print("[small_strategy] 无总市值数据（EM不可用且缓存为空），跳过")
        return []

    try:
        suspended = fetcher.get_suspended_codes()
    except Exception:
        suspended = set()

    df = spot_df[~spot_df["名称"].str.contains("ST|退", na=False)].copy()
    df = df[~df["代码"].isin(suspended)]
    mktcap = pd.to_numeric(df["总市值"], errors="coerce")
    df = df[(mktcap > 0) & (mktcap <= max_cap)].copy()
    df = df[~df["名称"].str.match(r"^N ", na=False)]
    if df.empty:
        return []

    if "换手率" in df.columns:
        df["_tr"] = pd.to_numeric(df["换手率"], errors="coerce").fillna(0)
        df = df.nlargest(prefilt_n, "_tr")
    else:
        df = df.head(prefilt_n)

    candidates = df["代码"].tolist()
    print(f"[small_strategy] scanning {len(candidates)} candidates "
          f"(cap≤{sc_cfg['max_cap_yi']}亿, regime={rk})...")

    scored = score_universe(candidates, REGIME_WEIGHTS_SMALLCAP[rk], max_workers=8)

    results: list[dict] = []
    for s in scored:
        if s.get("error") or not s.get("buy_score"):
            continue
        if s["code"] in held_codes:
            continue
        s["_sc_signal"] = s["buy_score"] >= buy_trig
        results.append(s)
        if len(results) >= top_n:
            break
    return results


# ── 持久化 ────────────────────────────────────────────────────────────────────

def save_picks(candidates: list[dict], regime_signal: str, regime_score: float | None = None) -> None:
    """写 latest_picks.json["smallcap"]，保留当天已有的 results 字段。"""
    def _pick(b):
        return {"code": b["code"], "name": b.get("name", b["code"]),
                "score": b.get("buy_score", 0), "change_pct": b.get("change_pct"),
                "buy_score": b.get("buy_score"), "sell_score": b.get("sell_score"),
                "bullish": b.get("bullish", []), "bearish": b.get("bearish", []),
                "market_cap_b": b.get("market_cap_b")}

    from common import file_lock, read_json, write_json
    today = datetime.now().strftime("%Y-%m-%d")
    # 互斥 read-modify-write，防止与 main_strategy.save_picks 并发覆盖 results
    with file_lock(LATEST_PICKS_PATH):
        existing = read_json(LATEST_PICKS_PATH, default={}) or {}
        if existing.get("timestamp", "")[:10] == today:
            existing_results   = existing.get("results", [])
            existing_timestamp = existing.get("timestamp", datetime.now().isoformat())
            existing_source    = existing.get("source", regime_signal)
        else:
            existing_results   = []
            existing_timestamp = datetime.now().isoformat()
            existing_source    = regime_signal
        payload = {
            "timestamp":    existing_timestamp,
            "source":       existing_source,
            "results":      existing_results,
            "smallcap":     [_pick(b) for b in candidates],
            "regime":       existing_source,
            "regime_score": regime_score,
        }
        write_json(LATEST_PICKS_PATH, payload, atomic=True)


# ── 推送/副作用（与 scan 分离，供适配器复用）──────────────────────────────────

def _push_results(
    candidates: list[dict],
    regime_score: float,
    regime_signal: str,
    run_time: str,
    config: dict,
    dry_run: bool = False,
) -> None:
    """WeChat 推送。JSON 写文件由 save_picks() 独立调用。"""
    sendkey = setup_push(config)

    if not candidates:
        print("[small_strategy] 无候选，跳过推送")
        return

    rk = _regime_key(regime_score)
    alerts = [s for s in candidates if s.get("_sc_signal")]
    parts  = [f"📊 {len(alerts)} 信号"] if alerts else ["明日关注"]
    title  = f"[小盘] {' | '.join(parts)}"

    rows = [regime_header_line(run_time, regime_score, rk),
            "<br>**今日关注（小市值策略）**"]
    for s in candidates:
        cap_str  = f" {s['market_cap_b']:.0f}亿" if s.get("market_cap_b") else ""
        mark = " ✅" if s.get("_sc_signal") else ""
        rows.append(f"**{s['code']} {s['name']}** 买入分:{s['buy_score']:.0f}{cap_str}{mark}")
    desp = "<br>".join(rows) + DISCLAIMER

    wechat_send_with_log(title, desp, sendkey, "small_strategy", dry_run)


def push_from_json(config: dict, dry_run: bool = False) -> None:
    """从 latest_picks.json 读取今日小盘数据并推送微信（不重新扫描）。"""
    if not os.path.exists(LATEST_PICKS_PATH):
        raise FileNotFoundError("latest_picks.json 不存在")
    d = json.load(open(LATEST_PICKS_PATH, encoding="utf-8"))
    today = datetime.now().strftime("%Y-%m-%d")
    ts = d.get("timestamp", "")
    if ts[:10] != today:
        print(f"[small_strategy] latest_picks.json 非今日数据({ts[:10]})，跳过推送")
        return
    candidates = d.get("smallcap", [])
    _push_results(
        candidates=candidates,
        regime_score=d.get("regime_score") or 5.0,
        regime_signal=d.get("regime", "unknown"),
        run_time=ts[:16].replace("T", " "),
        config=config,
        dry_run=dry_run,
    )


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> list[dict]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config_path = os.path.join(_ROOT, "alert_config.json")
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)
    thresholds = config.get("thresholds", {})

    regime_score  = 5.0
    regime_signal = "unknown"
    try:
        mkt = score_market_regime(fetcher.get_market_regime_data())
        if mkt:
            regime_score  = mkt.get("score", 5.0)
            regime_signal = mkt.get("details", {}).get("signal", "unknown")
    except Exception as e:
        print(f"[small_strategy] regime fetch failed: {e}")
    print(f"[small_strategy] regime={regime_score}/10 {regime_signal}")

    candidates = scan(config, thresholds, regime_score=regime_score)
    print(f"[small_strategy] {len(candidates)} candidates")

    run_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    _push_results(candidates, regime_score, regime_signal, run_time, config, args.dry_run)
    return candidates


if __name__ == "__main__":
    main()
