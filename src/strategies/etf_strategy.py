#!/usr/bin/env python3
"""
ETF 策略 — 对 etf_watchlist 打分，推送买卖信号

用法：
    python -X utf8 src/strategies/etf_strategy.py
    python -X utf8 src/strategies/etf_strategy.py --dry-run
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
from factors.config import FACTOR_WEIGHTS_ETF
from factors import score_market_regime
import fetcher
from common import configure_pushplus, send_wechat
from report.utils import score_one_buy as _score_one

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def scan(
    etf_list: list[dict],
    thresholds: dict,
    regime_score: float = 5.0,
) -> tuple[list[dict], list[dict], list[dict]]:
    """对 etf_list 打分，返回 (buy_alerts, sell_alerts, all_scores)。"""
    if not etf_list:
        return [], [], []

    fw = weights_from_config_dict(FACTOR_WEIGHTS_ETF)
    _score = partial(_score_one, weights=fw)

    sell_trig = thresholds.get("sell_score_trigger", 60)
    stall     = thresholds.get("stall_sell_score", 40)
    stop_loss = thresholds.get("stop_loss_pct", -8.0)
    buy_trig  = thresholds.get("buy_score_trigger", 65)
    if regime_score <= 2:
        buy_trig = round(buy_trig * 1.25, 1)
    elif regime_score <= 4:
        buy_trig = round(buy_trig * 1.15, 1)

    all_scores: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(len(etf_list), 4)) as ex:
        futs = {ex.submit(_score, e["code"] if isinstance(e, dict) else e): e
                for e in etf_list}
        for fut in as_completed(futs):
            entry = futs[fut]
            s = fut.result()
            if isinstance(entry, dict):
                s["shares"]     = entry.get("shares", 0)
                s["cost_price"] = entry.get("cost_price", 0)
                cost  = s["cost_price"] or 0
                price = s.get("price") or 0
                s["pnl_pct"] = round((price - cost) / cost * 100, 2) if cost > 0 else 0.0
            all_scores.append(s)
    all_scores.sort(key=lambda x: -x.get("buy_score", 0))

    buy_alerts:  list[dict] = []
    sell_alerts: list[dict] = []
    for s in all_scores:
        if s.get("error"):
            continue
        # 卖出
        if (s.get("shares", 0) or 0) > 0:
            reasons: list[str] = []
            if s["sell_score"] >= sell_trig:
                reasons.append(f"综合卖出评分 {s['sell_score']:.0f}/100 ≥ {sell_trig}")
            elif stall <= s["sell_score"] < sell_trig:
                reasons.append(f"逢高减仓参考: 卖出信号 **{s['sell_score']:.0f}**"
                               f"（阈值 {stall}–{sell_trig}）")
            if s.get("pnl_pct", 0) <= stop_loss:
                reasons.append(f"止损触发: 浮亏 {s['pnl_pct']:+.1f}%")
            if reasons:
                sell_alerts.append({**s, "reasons": reasons})
        # 买入
        if (s["buy_score"] >= buy_trig and
                s["sell_score"] < sell_trig * 0.7):
            buy_alerts.append(s)

    return buy_alerts, sell_alerts, all_scores


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config_path = os.path.join(_ROOT, "alert_config.json")
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)
    thresholds = config.get("thresholds", {})
    sendkey    = config.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(config.get("pushplus", {}).get("token", ""))

    etf_list = config.get("etf_watchlist", [])
    if not etf_list:
        print("[etf_strategy] etf_watchlist 为空，退出")
        return
    print(f"[etf_strategy] scanning {len(etf_list)} ETFs")

    regime_score  = 5.0
    regime_signal = "unknown"
    try:
        mkt = score_market_regime(fetcher.get_market_regime_data())
        if mkt:
            regime_score  = mkt.get("score", 5.0)
            regime_signal = mkt.get("details", {}).get("signal", "unknown")
    except Exception as e:
        print(f"[etf_strategy] regime fetch failed: {e}")

    buys, sells, all_scores = scan(etf_list, thresholds, regime_score)
    print(f"[etf_strategy] buy={len(buys)} sell={len(sells)}")

    # 无论有无信号，始终保存评分结果供 reporter 午间/收盘快报使用
    _out_path = os.path.join(_ROOT, "data", "etf_scan_latest.json")
    try:
        _out_data = {
            "date":      datetime.now().strftime("%Y%m%d"),
            "timestamp": datetime.now().isoformat(),
            "scores": [
                {"code": s["code"], "name": s.get("name", s["code"]),
                 "buy_score": round(s.get("buy_score") or 0, 1),
                 "sell_score": round(s.get("sell_score") or 0, 1),
                 "price": s.get("price"), "pnl_pct": s.get("pnl_pct", 0)}
                for s in all_scores if not s.get("error")
            ],
        }
        _tmp = _out_path + ".tmp"
        with open(_tmp, "w", encoding="utf-8") as f:
            json.dump(_out_data, f, ensure_ascii=False, indent=2)
        os.replace(_tmp, _out_path)
        print(f"[etf_strategy] 已保存 {len(_out_data['scores'])} 只评分 → etf_scan_latest.json")
    except Exception as e:
        print(f"[etf_strategy] 保存评分失败: {e}")

    if not buys and not sells:
        print("[etf_strategy] 无信号，跳过推送")
        return

    _re_emoji = "🐻" if regime_score <= 3 else ("🟡" if regime_score <= 6 else "🐂")
    strong_s = [a for a in sells if a["sell_score"] >= thresholds.get("sell_score_trigger", 60)]
    stall_s  = [a for a in sells if a not in strong_s]
    strong_b = [a for a in buys  if a["buy_score"] >= 80]
    add_b    = [a for a in buys  if a not in strong_b]
    def _en(lst): return "、".join((a.get("name") or a["code"]) for a in lst[:3])
    parts = []
    if strong_s: parts.append(f"🔴 {len(strong_s)} 强卖（{_en(strong_s)}）")
    if stall_s:  parts.append(f"⚠️ {len(stall_s)} 减仓（{_en(stall_s)}）")
    if strong_b: parts.append(f"✅ {len(strong_b)} 强买（{_en(strong_b)}）")
    if add_b:    parts.append(f"💡 {len(add_b)} 加仓（{_en(add_b)}）")
    title = f"ETF {' | '.join(parts)}"

    rows = [f"*{datetime.now():%Y-%m-%d %H:%M}*<br>市场 {_re_emoji} {regime_score:.0f}/10 {regime_signal}"]
    for a in sells:
        rows.append(f"**{a['name']} ({a['code']})**<br>"
                    f"卖出分 **{a['sell_score']:.0f}** | 浮盈 **{a.get('pnl_pct', 0):+.1f}%**")
        for r in a["reasons"]:
            rows.append(f"- {r}")
    for a in buys:
        p = a.get("price") or 0
        rows.append(f"**{a['name']} ({a['code']})**<br>"
                    f"买入分 **{a['buy_score']:.0f}** | 现价 **{p}**")
    rows.append("<br>> T+0 / 仅供参考")
    desp = "<br>".join(rows)

    if not args.dry_run:
        try:
            send_wechat(title, desp, sendkey, dry_run=False)
            print("[etf_strategy] 微信推送完成")
        except Exception as e:
            print(f"[etf_strategy] 微信推送失败: {e}")
    else:
        print(f"[etf_strategy] dry-run:\n{title}\n{desp}")


if __name__ == "__main__":
    main()
