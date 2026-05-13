#!/usr/bin/env python3
"""
主策略夜间选股 — 每日收盘后运行，从 universe 里选明日买入候选

用法：
    python -X utf8 src/strategies/main_strategy.py
    python -X utf8 src/strategies/main_strategy.py --dry-run
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
from factors.config import REGIME_WEIGHTS
from factors import score_market_regime
import fetcher
from common import send_wechat, setup_push, regime_emoji
from report.utils import (
    regime_key as _regime_key,
    compact_factor_scores as _compact_factor_scores,
    score_one_buy as _score_one_buy,
)
import signals_store as _signals_store

_ROOT             = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LATEST_PICKS_PATH = os.path.join(_ROOT, "data", "latest_picks.json")
SIGNALS_LOG_PATH  = os.path.join(_ROOT, "data", "signals_log.json")


# ── 核心扫描 ───────────────────────────────────────────────────────────────────

def scan(
    universe: list[str],
    thresholds: dict,
    regime_score: float = 5.0,
    held_codes: set | None = None,
) -> tuple[list[dict], list[dict]]:
    """评分 universe，返回 (buy_alerts, all_scored)。"""
    held_codes = held_codes or set()
    rk   = _regime_key(regime_score)
    fw   = weights_from_config_dict(REGIME_WEIGHTS[rk])
    _score = partial(_score_one_buy, weights=fw)

    buy_trig  = thresholds.get("buy_score_trigger", 70)
    sell_trig = thresholds.get("sell_score_trigger", 60)
    if regime_score <= 2:
        buy_trig = round(buy_trig * 1.25, 1)
        bear_sell_cap = 25
    elif regime_score <= 4:
        buy_trig = round(buy_trig * 1.15, 1)
        bear_sell_cap = None
    else:
        bear_sell_cap = None

    try:
        suspended = fetcher.get_suspended_codes()
        universe = [c for c in universe if c not in suspended]
    except Exception:
        pass

    scored: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_score, code): code for code in universe}
        for fut in as_completed(futs):
            scored.append(fut.result())
    scored.sort(key=lambda x: -x.get("buy_score", 0))

    top_n     = thresholds.get("buy_universe_top_n", 5)
    sell_guard = max(0, sell_trig - 10)
    buy_alerts: list[dict] = []
    for s in scored[:top_n * 3]:
        if s.get("error") or s["code"] in held_codes:
            continue
        if s["buy_score"] < buy_trig:
            break
        if s["sell_score"] >= sell_guard:
            continue
        if bear_sell_cap is not None and s["sell_score"] >= bear_sell_cap:
            continue
        if (s.get("change_pct") or 0) >= 9.5:
            continue
        buy_alerts.append(s)
        if len(buy_alerts) >= top_n:
            break

    return buy_alerts, scored


# ── 持久化 ────────────────────────────────────────────────────────────────────

def save_picks(
    buy_alerts: list[dict],
    regime_signal: str,
    scored: list[dict] | None = None,
    regime_score: float | None = None,
) -> None:
    """写 latest_picks.json["results"]，保留当天已有的 smallcap 字段。
    scored: top候选列表（含未触发阈值的股票），供 nightly_push 重推时使用。
    """
    def _pick(b):
        return {"code": b["code"], "name": b.get("name", b["code"]),
                "score": b.get("buy_score", 0), "change_pct": b.get("change_pct"),
                "buy_score": b.get("buy_score"), "sell_score": b.get("sell_score"),
                "bullish": b.get("bullish", []), "bearish": b.get("bearish", [])}

    today = datetime.now().strftime("%Y-%m-%d")
    existing_smallcap: list = []
    if os.path.exists(LATEST_PICKS_PATH):
        try:
            existing = json.load(open(LATEST_PICKS_PATH, encoding="utf-8"))
            if existing.get("timestamp", "")[:10] == today:
                existing_smallcap = existing.get("smallcap", [])
        except Exception:
            pass

    payload = {
        "timestamp":    datetime.now().isoformat(),
        "source":       regime_signal,
        "results":      [_pick(b) for b in buy_alerts],
        "smallcap":     existing_smallcap,
        "regime":       regime_signal,
        "regime_score": regime_score,
        "candidates":   [_pick(s) for s in (scored or [])[:15]
                         if not s.get("error") and s.get("buy_score", 0) > 0],
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


def append_signals_log(buy_alerts: list[dict], run_time: str,
                       regime_score: Optional[float], source: str = "main") -> None:
    if not buy_alerts:
        return
    def _common(s):
        return {k: s.get(k) for k in
                ("code", "name", "price", "change_pct", "buy_score", "sell_score",
                 "bullish", "bearish", "industry", "market_cap_b", "pe_ttm", "pb",
                 "turnover_rate", "volume_ratio", "volume_million", "factor_scores")}
    entry = {"date": datetime.now().strftime("%Y-%m-%d"), "run_time": run_time,
             "regime_score": regime_score, "source": source,
             "buy_signals": [_common(b) for b in buy_alerts], "sell_signals": []}
    _signals_store.append(entry)


# ── 推送/副作用（与 scan 分离，供适配器复用）──────────────────────────────────

def _push_results(
    buy_alerts: list[dict],
    scored: list[dict],
    regime_score: float,
    regime_signal: str,
    run_time: str,
    config: dict,
    dry_run: bool = False,
) -> None:
    """append_signals_log + WeChat 推送。JSON 写文件由 save_picks() 独立调用。"""
    sendkey = setup_push(config)

    if not dry_run:
        append_signals_log(buy_alerts, run_time, regime_score)

    top_candidates = [s for s in scored[:15]
                      if not s.get("error") and s.get("buy_score", 0) > 0][:10]
    if not buy_alerts and not top_candidates:
        print("[main_strategy] 无信号，跳过推送")
        return

    rk = _regime_key(regime_score)
    _re_emoji = regime_emoji(regime_score)
    strong = [b for b in buy_alerts if b["buy_score"] >= 80]
    add    = [b for b in buy_alerts if b["buy_score"] < 80]
    parts  = []
    if strong: parts.append(f"✅ {len(strong)} 强买")
    if add:    parts.append(f"💡 {len(add)} 加仓")
    if not parts: parts.append("明日关注")
    title = f"主策略 {' | '.join(parts)}"

    rows = [f"*{run_time}*<br>市场 {_re_emoji} {regime_score:.0f}/10 {rk}",
            "<br>**今日关注（低波动主策略）**"]
    for s in top_candidates:
        mark = " ✅" if s in buy_alerts else ""
        rows.append(f"**{s['code']} {s['name']}** 买入分:{s['buy_score']:.0f}{mark}")
    desp = "<br>".join(rows) + "<br><br>> 仅供参考，不构成投资建议"

    if not dry_run:
        try:
            send_wechat(title, desp, sendkey, dry_run=False)
            print("[main_strategy] 微信推送完成")
        except Exception as e:
            print(f"[main_strategy] 微信推送失败: {e}")
            raise
    else:
        print(f"[main_strategy] dry-run:\n{title}\n{desp}")


def push_from_json(config: dict, dry_run: bool = False) -> None:
    """从 latest_picks.json 读取今日数据并推送微信（不重新扫描）。"""
    if not os.path.exists(LATEST_PICKS_PATH):
        raise FileNotFoundError("latest_picks.json 不存在")
    d = json.load(open(LATEST_PICKS_PATH, encoding="utf-8"))
    today = datetime.now().strftime("%Y-%m-%d")
    ts = d.get("timestamp", "")
    if ts[:10] != today:
        print(f"[main_strategy] latest_picks.json 非今日数据({ts[:10]})，跳过推送")
        return
    _push_results(
        buy_alerts=d.get("results", []),
        scored=d.get("candidates", []),
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

    uni_file = os.path.join(_ROOT, "data", "universe_main.json")
    universe = (json.loads(open(uni_file, encoding="utf-8").read())
                if os.path.exists(uni_file)
                else config.get("screener_universe", []))
    print(f"[main_strategy] universe={len(universe)} stocks")

    regime_score  = 5.0
    regime_signal = "unknown"
    try:
        mkt = score_market_regime(fetcher.get_market_regime_data())
        if mkt:
            regime_score  = mkt.get("score", 5.0)
            regime_signal = mkt.get("details", {}).get("signal", "unknown")
    except Exception as e:
        print(f"[main_strategy] regime fetch failed: {e}")
    print(f"[main_strategy] regime={regime_score}/10 {regime_signal}")

    run_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    buy_alerts, scored = scan(universe, thresholds, regime_score)
    print(f"[main_strategy] {len(buy_alerts)} buy alerts, {len(scored)} scored")

    _push_results(buy_alerts, scored, regime_score, regime_signal, run_time, config, args.dry_run)
    return buy_alerts


if __name__ == "__main__":
    main()
