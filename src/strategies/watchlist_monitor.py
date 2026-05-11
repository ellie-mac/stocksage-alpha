#!/usr/bin/env python3
"""
自选池实时监控 — 盘中常驻，强买即时推送

- 合并手动池 + 动态池每 POLL_INTERVAL 秒打一次分
- buy_score >= STRONG_BUY_THRESHOLD 立即推送
- 同一股票 COOLDOWN_MIN 分钟内不重复推送
- 强买信号追加到 data/wl_strong_buy_log.json 供胜率统计

运行方式（定时任务 9:15 启动）：
    python -X utf8 src/strategies/watchlist_monitor.py
    python -X utf8 src/strategies/watchlist_monitor.py --dry-run   # 不推送，只打印
    python -X utf8 src/strategies/watchlist_monitor.py --force     # 忽略交易时间限制
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

from factors import weights_from_config_dict
from factors.config import REGIME_WEIGHTS
from factors import score_market_regime
import fetcher
from common import configure_pushplus, send_wechat
from report.utils import regime_key as _regime_key, score_one_buy as _score_one
from research import _FACTOR_ZH_REPORT

_ROOT            = str(ROOT)
DYNAMIC_WL_PATH  = ROOT / "data" / "watchlist_dynamic.json"
SIGNAL_LOG_PATH  = ROOT / "data" / "wl_strong_buy_log.json"

POLL_INTERVAL        = 180    # 秒，3 分钟
STRONG_BUY_THRESHOLD = 90     # buy_score 阈值
COOLDOWN_MIN         = 30     # 同一只股票冷却分钟数
MARKET_OPEN          = (9, 30)
MARKET_CLOSE         = (15, 5)


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _load_config() -> dict:
    cfg_path = ROOT / "alert_config.json"
    with open(cfg_path, encoding="utf-8") as f:
        return json.load(f)


def _load_watchlist(cfg: dict) -> list[str]:
    """返回手动池 + 动态池合并去重后的代码列表。"""
    raw = cfg.get("watchlist", [])
    codes: list[str] = []
    for item in raw:
        c = item["code"] if isinstance(item, dict) else item
        codes.append(c[-6:] if len(c) > 6 else c)

    if DYNAMIC_WL_PATH.exists():
        try:
            dynamic = json.loads(DYNAMIC_WL_PATH.read_text(encoding="utf-8"))
            seen = set(codes)
            for e in dynamic:
                if isinstance(e, dict) and e.get("code") and e["code"] not in seen:
                    codes.append(e["code"])
                    seen.add(e["code"])
        except Exception:
            pass

    return codes


def _dynamic_source(code: str) -> str:
    """从动态池元数据查找该股票的来源。"""
    if not DYNAMIC_WL_PATH.exists():
        return "manual"
    try:
        for e in json.loads(DYNAMIC_WL_PATH.read_text(encoding="utf-8")):
            if isinstance(e, dict) and e.get("code") == code:
                return e.get("source", "dynamic")
    except Exception:
        pass
    return "manual"


def _append_signal(record: dict) -> None:
    existing: list[dict] = []
    if SIGNAL_LOG_PATH.exists():
        try:
            existing = json.loads(SIGNAL_LOG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    existing.append(record)
    tmp = str(SIGNAL_LOG_PATH) + ".tmp"
    Path(tmp).write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, str(SIGNAL_LOG_PATH))


def _is_trading() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    hm = (now.hour, now.minute)
    return MARKET_OPEN <= hm <= MARKET_CLOSE


def _seconds_to_open() -> int:
    now = datetime.now()
    if now.weekday() >= 5:
        return 99999
    target = now.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
    diff = (target - now).total_seconds()
    return max(0, int(diff))


# ── 单次扫描 ──────────────────────────────────────────────────────────────────

def _scan_once(
    codes: list[str],
    thresholds: dict,
    regime_score: float,
) -> list[dict]:
    """对 codes 打分，返回 buy_score >= STRONG_BUY_THRESHOLD 的结果。"""
    from functools import partial
    from concurrent.futures import ThreadPoolExecutor, as_completed

    rk  = _regime_key(regime_score)
    fw  = weights_from_config_dict(REGIME_WEIGHTS[rk])
    _sw = partial(_score_one, weights=fw)

    sell_guard = max(0, thresholds.get("sell_score_trigger", 60) - 10)

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_sw, c): c for c in codes}
        for fut in as_completed(futs):
            s = fut.result()
            if not s.get("error"):
                results.append(s)

    alerts = []
    for s in results:
        if (s["buy_score"] >= STRONG_BUY_THRESHOLD and
                s["sell_score"] < sell_guard and
                (s.get("change_pct") or 0) < 9.5):
            alerts.append(s)
    alerts.sort(key=lambda x: -x["buy_score"])
    return alerts


# ── 推送 ─────────────────────────────────────────────────────────────────────

def _push(
    alert: dict,
    regime_score: float,
    regime_signal: str,
    sendkey: str,
    dry_run: bool,
) -> None:
    name  = alert.get("name") or alert["code"]
    code  = alert["code"]
    score = alert["buy_score"]
    price = alert.get("price") or 0
    src   = _dynamic_source(code)
    src_label = {"main_scan": "主策略", "gc_scan": "金叉", "hot_scan": "热榜", "manual": "手动"}.get(src, src)

    bull_tags = []
    for b in (alert.get("bullish") or []):
        if isinstance(b, dict) and b.get("factor"):
            zh = _FACTOR_ZH_REPORT.get(b["factor"], b["factor"])
            fs = b.get("score")
            bull_tags.append(f"{zh} {fs:.1f}" if fs is not None else zh)

    bear_tags = []
    for b in (alert.get("bearish") or []):
        if isinstance(b, dict) and b.get("factor"):
            zh = _FACTOR_ZH_REPORT.get(b["factor"], b["factor"])
            fs = b.get("sell_score")
            bear_tags.append(f"{zh} {fs:.1f}" if fs is not None else zh)

    _re_emoji = "🐻" if regime_score <= 3 else ("🟡" if regime_score <= 6 else "🐂")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    title = f"🔔 强买 {name}({code}) {score:.0f}分 [{src_label}]"
    rows = [
        f"*{now_str}*<br>市场 {_re_emoji} {regime_score:.0f}/10",
        f"**{name}({code})**<br>买入分 **{score:.0f}** | 现价 **{price}**",
    ]
    if bull_tags:
        rows.append("+ " + " / ".join(f"`{t}`" for t in bull_tags))
    if bear_tags:
        rows.append("- " + " / ".join(f"`{t}`" for t in bear_tags))
    rows.append("<br>> 仅供参考")
    desp = "<br>".join(rows)

    if dry_run:
        print(f"[monitor] dry-run: {title}")
        print(desp)
        return

    try:
        send_wechat(title, desp, sendkey, dry_run=False)
        print(f"[monitor] 推送: {name}({code}) {score:.0f}分", flush=True)
    except Exception as e:
        print(f"[monitor] 微信推送失败: {e}", flush=True)

    # 飞书补发
    try:
        from notify.notify import push_feishu_content
        text = f"🔔 强买 {name}({code})\n买入分 {score:.0f} | 现价 {price}\n来源: {src_label}\n因子: {' / '.join(factors) if factors else '-'}"
        push_feishu_content(text)
    except Exception:
        pass


# ── 主循环 ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force",   action="store_true", help="忽略交易时间限制")
    args = parser.parse_args()

    cfg        = _load_config()
    thresholds = cfg.get("thresholds", {})
    sendkey    = cfg.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))

    # 等待开盘
    if not args.force:
        wait = _seconds_to_open()
        if wait > 0:
            print(f"[monitor] 等待开盘，{wait//60}分{wait%60}秒后开始扫描 ({MARKET_OPEN[0]:02d}:{MARKET_OPEN[1]:02d})")
            time.sleep(wait)

    print(f"[monitor] 启动  阈值={STRONG_BUY_THRESHOLD}  冷却={COOLDOWN_MIN}min  间隔={POLL_INTERVAL}s")

    cooldown: dict[str, datetime] = {}  # code -> 上次推送时间

    while True:
        now = datetime.now()

        if not args.force and not _is_trading():
            print(f"[monitor] {now:%H:%M} 已过收盘，退出")
            break

        codes = _load_watchlist(cfg)
        if not codes:
            print("[monitor] 自选池为空，等待下次")
            time.sleep(POLL_INTERVAL)
            continue

        # 市场状态
        regime_score  = 5.0
        regime_signal = "unknown"
        try:
            mkt = score_market_regime(fetcher.get_market_regime_data())
            if mkt:
                regime_score  = mkt.get("score", 5.0)
                regime_signal = mkt.get("details", {}).get("signal", "unknown")
        except Exception:
            pass

        alerts = _scan_once(codes, thresholds, regime_score)
        print(f"[monitor] {now:%H:%M}  池={len(codes)}只  强买={len(alerts)}只  市场{regime_score:.0f}/10", flush=True)

        today_str = date.today().strftime("%Y%m%d")
        for alert in alerts:
            code = alert["code"]
            last = cooldown.get(code)
            if last and (now - last).total_seconds() < COOLDOWN_MIN * 60:
                remaining = COOLDOWN_MIN - int((now - last).total_seconds() / 60)
                print(f"[monitor] {code} 冷却中，{remaining}min后可再推", flush=True)
                continue

            # 推送
            _push(alert, regime_score, regime_signal, sendkey, args.dry_run)
            cooldown[code] = now

            # 记录信号
            factors = [_FACTOR_ZH_REPORT.get(b["factor"], b["factor"])
                       for b in (alert.get("bullish") or [])
                       if isinstance(b, dict) and b.get("factor")]
            record = {
                "date":       today_str,
                "time":       now.strftime("%H:%M"),
                "code":       code,
                "name":       alert.get("name") or code,
                "price":      alert.get("price"),
                "buy_score":  round(alert["buy_score"], 1),
                "sell_score": round(alert.get("sell_score", 0), 1),
                "factors":    factors,
                "source":     _dynamic_source(code),
            }
            if not args.dry_run:
                try:
                    _append_signal(record)
                except Exception as e:
                    print(f"[monitor] 记录信号失败: {e}", flush=True)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
