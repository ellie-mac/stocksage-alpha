#!/usr/bin/env python3
"""
小盘策略夜间选股 — 从全市场小市值股中选明日候选

用法：
    python -X utf8 scripts/small_strategy.py
    python -X utf8 scripts/small_strategy.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import partial
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from factors import weights_from_config_dict
from factors.config import REGIME_WEIGHTS_SMALLCAP, SMALLCAP_CONFIG
from factors_extended import score_market_regime
import fetcher
import pandas as pd
from common import configure_pushplus, send_wechat
from report_utils import (
    regime_key as _regime_key,
    compact_factor_scores as _compact_factor_scores,
    score_one_buy as _score_one_buy,
)


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
    fw = weights_from_config_dict(REGIME_WEIGHTS_SMALLCAP[rk])
    _score = partial(_score_one_buy, weights=fw)

    sc_cfg    = {**SMALLCAP_CONFIG, **config.get("smallcap", {})}
    max_cap   = sc_cfg["max_cap_yi"] * 1e8
    prefilt_n = sc_cfg["prefilter_n"]
    top_n     = sc_cfg["top_n"]
    buy_trig  = thresholds.get("buy_score_trigger", 60)

    spot_df = fetcher._get_spot_df()
    if spot_df is None or spot_df.empty:
        print("[small_strategy] spot_df unavailable")
        return []
    required = {"名称", "总市值", "代码"}
    if not required.issubset(spot_df.columns):
        print(f"[small_strategy] spot_df missing columns: {required - set(spot_df.columns)}")
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

    scored: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_score, code): code for code in candidates}
        for fut in as_completed(futs):
            scored.append(fut.result())
    scored.sort(key=lambda x: -x.get("buy_score", 0))

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

def save_picks(candidates: list[dict], regime_signal: str) -> None:
    """写 latest_picks.json["smallcap"]，保留当天已有的 results 字段。"""
    def _pick(b):
        return {"code": b["code"], "name": b.get("name", b["code"]),
                "score": b.get("buy_score", 0), "change_pct": b.get("change_pct"),
                "buy_score": b.get("buy_score"), "sell_score": b.get("sell_score"),
                "bullish": b.get("bullish", []), "bearish": b.get("bearish", []),
                "market_cap_b": b.get("market_cap_b")}

    today = datetime.now().strftime("%Y-%m-%d")
    existing_results: list = []
    existing_timestamp = datetime.now().isoformat()
    existing_source = regime_signal
    if os.path.exists(LATEST_PICKS_PATH):
        try:
            existing = json.load(open(LATEST_PICKS_PATH, encoding="utf-8"))
            if existing.get("timestamp", "")[:10] == today:
                existing_results   = existing.get("results", [])
                existing_timestamp = existing.get("timestamp", existing_timestamp)
                existing_source    = existing.get("source", regime_signal)
        except Exception:
            pass

    payload = {
        "timestamp": existing_timestamp,
        "source":    existing_source,
        "results":   existing_results,
        "smallcap":  [_pick(b) for b in candidates],
        "regime":    existing_source,
    }
    tmp = LATEST_PICKS_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, LATEST_PICKS_PATH)
    except Exception:
        try:
            os.remove(tmp)
        except Exception:
            pass


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> list[dict]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config_path = os.path.join(_ROOT, "alert_config.json")
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)
    thresholds = config.get("thresholds", {})
    sendkey    = config.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(config.get("pushplus", {}).get("token", ""))

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

    if not args.dry_run:
        save_picks(candidates, regime_signal)

    if not candidates:
        print("[small_strategy] 无候选，跳过推送")
        return candidates

    rk = _regime_key(regime_score)
    _re_emoji = "🐻" if regime_score <= 3 else ("🟡" if regime_score <= 6 else "🐂")
    alerts = [s for s in candidates if s.get("_sc_signal")]
    parts  = [f"📊 {len(alerts)} 信号"] if alerts else ["明日关注"]
    title  = f"小盘策略 {' | '.join(parts)}"

    rows = [f"*{datetime.now():%Y-%m-%d %H:%M}*<br>市场 {_re_emoji} {regime_score:.0f}/10 {rk}",
            "<br>**今日关注（小市值策略）**"]
    for s in candidates:
        cap_str  = f" {s['market_cap_b']:.0f}亿" if s.get("market_cap_b") else ""
        mark = " ✅" if s.get("_sc_signal") else ""
        rows.append(f"**{s['code']} {s['name']}** 买入分:{s['buy_score']:.0f}{cap_str}{mark}")
    desp = "<br>".join(rows) + "<br><br>> 仅供参考，不构成投资建议"

    if not args.dry_run:
        try:
            send_wechat(title, desp, sendkey, dry_run=False)
            print("[small_strategy] 微信推送完成")
        except Exception as e:
            print(f"[small_strategy] 微信推送失败: {e}")
    else:
        print(f"[small_strategy] dry-run:\n{title}\n{desp}")

    return candidates


if __name__ == "__main__":
    main()
