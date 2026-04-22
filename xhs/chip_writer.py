#!/usr/bin/env python3
"""
筹码策略小红书三段式推送
  morning : 早报 — 全部筹码选股按档位列出
  midday  : 午间快报 — 涨跌幅 Top5、胜率、收益率、下午重点
  evening : 收盘总结 — 最终胜率、收益率、Top5、总结

用法：
  python -X utf8 xhs/chip_writer.py morning
  python -X utf8 xhs/chip_writer.py midday
  python -X utf8 xhs/chip_writer.py evening [--dry-run]

每个命令内置最大延迟检查，超出窗口自动跳过（防 Task Scheduler 补跑）。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, time as dtime, timedelta
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
XHS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "scripts"))

SCAN_PATH = ROOT / "data" / "chip_scan_latest.json"
CAD_PATH  = ROOT / "data" / "chip_cad_latest.json"

# 超出计划时间多少分钟后跳过（防止补跑）
MAX_DELAY = {"morning": 60, "midday": 40, "evening": 90}
SCHED_TIME = {"morning": dtime(9, 25), "midday": dtime(11, 35), "evening": dtime(15, 30)}


# ── helpers ──────────────────────────────────────────────────────────────────

def _push(title: str, body: str) -> None:
    try:
        cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        from common import send_wechat, configure_pushplus
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
        send_wechat(title, body, sendkey)
        print("[notify] 推送成功")
    except Exception as e:
        print(f"[notify] 推送失败: {e}")


def _in_window(slot: str) -> bool:
    sched = SCHED_TIME.get(slot)
    if not sched:
        return True
    now_dt   = datetime.now()
    sched_dt = datetime.combine(now_dt.date(), sched)
    delay    = (now_dt - sched_dt).total_seconds() / 60
    if delay > MAX_DELAY.get(slot, 60):
        print(f"[{slot}] 超出执行窗口（延迟 {delay:.0f}min），跳过")
        return False
    return True


def _load_scan() -> dict:
    """优先读 chip_cad_latest.json（bekh过滤），fallback 到全量 chip_scan_latest.json。"""
    if CAD_PATH.exists():
        raw = json.loads(CAD_PATH.read_text(encoding="utf-8"))
        all_picks = []
        for tier_key, picks in raw.get("tiers", {}).items():
            for p in picks:
                p2 = dict(p)
                p2["tier"] = tier_key
                all_picks.append(p2)
        if all_picks:
            return {
                "date":      raw.get("date", datetime.now().strftime("%Y%m%d")),
                "all_picks": all_picks,
                "tiers":     raw.get("tiers", {}),
                "filter":    raw.get("mods", ""),
            }
    if not SCAN_PATH.exists():
        print(f"[chip_writer] 找不到 {SCAN_PATH} 和 {CAD_PATH}")
        return {}
    return json.loads(SCAN_PATH.read_text(encoding="utf-8"))


def _fetch_prices(codes: list[str]) -> dict[str, dict]:
    """通过共享缓存拉取实时行情，返回 {code: {price, change_pct}}。"""
    if not codes:
        return {}
    import pandas as pd
    from common import get_spot_em
    df = get_spot_em()
    if df.empty:
        return {}
    df["_code"] = df["代码"].astype(str).str.zfill(6)
    df = df[df["_code"].isin(codes)].copy()
    df["_price"] = pd.to_numeric(df["最新价"], errors="coerce")
    df["_pct"]   = pd.to_numeric(df["涨跌幅"], errors="coerce")
    df = df.dropna(subset=["_price", "_pct"])
    return dict(zip(df["_code"], [{"price": p, "change_pct": c}
                                   for p, c in zip(df["_price"], df["_pct"])]))


def _fetch_with_retry(codes: list[str], picks: list[dict], slot: str,
                      retry_interval: int = 600, max_retries: int = 4) -> dict:
    """行情获取失败时，在窗口期内每隔 retry_interval 秒重试，直到拿到数据或超时。"""
    import time as _time
    for attempt in range(max_retries):
        prices = _fetch_prices(codes)
        s = _calc_stats(picks, prices)
        if s["results"]:
            return s
        if attempt < max_retries - 1 and _in_window(slot):
            print(f"[{slot}] 行情未就绪，{retry_interval//60}分钟后重试（第{attempt+1}次）")
            _time.sleep(retry_interval)
        else:
            break
    return _calc_stats(picks, {})


def _calc_stats(picks: list[dict], prices: dict[str, dict]) -> dict:
    results = []
    for p in picks:
        code = p["code"]
        pr   = prices.get(code)
        if not pr:
            continue
        results.append({
            "code":       code,
            "name":       p.get("name", code),
            "industry":   p.get("industry", ""),
            "change_pct": pr["change_pct"],
            "tier":       p.get("tier", ""),
        })
    if not results:
        return {"results": [], "n_total": 0, "n_win": 0,
                "win_rate": 0.0, "avg_ret": 0.0, "top5": [],
                "watch_up": [], "watch_dn": []}
    import math
    nan_stocks = [r for r in results if math.isnan(r["change_pct"])]
    valid    = [r["change_pct"] for r in results if not math.isnan(r["change_pct"])]
    n_win    = sum(1 for v in valid if v > 0)
    win_rate = n_win / len(valid) * 100 if valid else 0.0
    avg_ret  = sum(valid) / len(valid) if valid else 0.0
    by_chg   = sorted([r for r in results if not math.isnan(r["change_pct"])],
                      key=lambda r: r["change_pct"], reverse=True)
    return {
        "results":    results,
        "n_total":    len(results),
        "n_win":      n_win,
        "win_rate":   win_rate,
        "avg_ret":    avg_ret,
        "top5":       by_chg[:5],
        "watch_up":   [r for r in by_chg if 0 < r["change_pct"] <= 3.0],
        "watch_dn":   [r for r in by_chg if r["change_pct"] < 0][:3],
        "nan_stocks": nan_stocks,
    }


def _fmt_date(date: str) -> str:
    return f"{date[4:6]}/{date[6:]}" if len(date) == 8 else date


# ── 三段式命令 ────────────────────────────────────────────────────────────────

def cmd_morning(dry_run: bool = False, force: bool = False) -> None:
    """早报：全部筹码选股按档位列出"""
    if not _in_window("morning"):
        return
    data = _load_scan()
    if not data:
        return

    date      = data.get("date", datetime.now().strftime("%Y%m%d"))
    all_tiers = data.get("tiers", {})
    total     = sum(len(v) for v in all_tiers.values())
    flt       = data.get("filter", "")

    lines = [f"📊 筹码早报 {_fmt_date(date)}"]
    lines.append(f"今日共选出 **{total}只**（{flt}）\n")

    for tier_key, tier_range in [("T1","≥95%"),("T2","90-95%"),
                                  ("T3","85-90%"),("T4","75-85%"),("T5","65-75%")]:
        picks = all_tiers.get(tier_key, [])
        if not picks:
            continue
        lines.append(f"**【{tier_key} {tier_range}】{len(picks)}只**")
        for p in picks:
            close_s = f"¥{float(p['close']):.2f}" if p.get("close") and not __import__("math").isnan(float(p["close"])) else ""
            lines.append(f"{p['code']} {p['name']} {p['industry']} {close_s}  ")
        lines.append("")

    lines.append("⚠️ 仅供参考，不构成投资建议")
    lines.append("#量化记录 #筹码分布 #数据实验 #记录帖")

    title = f"筹码早报 {_fmt_date(date)}（{total}只）"
    body  = "\n".join(lines)
    print(f"\n{title}\n{body}")
    if not dry_run:
        _push(title, body)


def cmd_midday(dry_run: bool = False, force: bool = False) -> None:
    """午间快报：涨跌幅 Top5、胜率、收益率、下午重点"""
    if not _in_window("midday"):
        return
    data = _load_scan()
    if not data:
        return

    date  = data.get("date", datetime.now().strftime("%Y%m%d"))
    picks = data.get("all_picks", [])
    if not picks:
        print("[midday] 无选股")
        return

    codes  = [p["code"] for p in picks]
    s      = _fetch_with_retry(codes, picks, slot="midday")
    if not s["results"]:
        print("[midday] 行情获取失败，发送无数据版本")
        title = f"筹码午报 {_fmt_date(date)}（行情暂不可用）"
        body  = (f"📈 午间快报 {_fmt_date(date)} 11:30\n"
                 f"筹码选股 {len(picks)} 只（行情数据暂时不可用，请自行查看）\n"
                 f"\n⚠️ 仅供参考，不构成投资建议\n#量化记录 #筹码分布 #数据实验")
        if not dry_run:
            _push(title, body)
        return

    lines = [f"📈 午间快报 {_fmt_date(date)} 11:30"]
    lines.append(f"筹码选股 {s['n_total']} 只")
    lines.append(f"**胜率 {s['win_rate']:.0f}%**（{s['n_win']}/{s['n_total']}只盈利）  "
                 f"**综合涨幅 {s['avg_ret']:+.2f}%**\n")

    lines.append("**涨幅前五：**")
    for i, r in enumerate(s["top5"], 1):
        lines.append(f"{i}. {r['code']} {r['name']} **{r['change_pct']:+.2f}%**  ")

    watch_up = s["watch_up"]
    watch_dn = s["watch_dn"]
    if watch_up or watch_dn:
        lines.append("\n**下午可关注：**")
        for r in watch_up[:5]:
            lines.append(f"• {r['code']} {r['name']} {r['change_pct']:+.2f}% 温和上涨可跟踪  ")
        for r in watch_dn[:2]:
            lines.append(f"• {r['code']} {r['name']} {r['change_pct']:+.2f}% 回调中，筹码仍健康  ")

    lines.append("\n⚠️ 仅供参考，不构成投资建议")
    lines.append("#量化记录 #筹码分布 #数据实验")

    title = f"筹码午报 {_fmt_date(date)}（胜率{s['win_rate']:.0f}% 均{s['avg_ret']:+.2f}%）"
    body  = "\n".join(lines)
    print(f"\n{title}\n{body}")
    if not dry_run:
        _push(title, body)


def cmd_evening(dry_run: bool = False, force: bool = False) -> None:
    """收盘总结：最终胜率、收益率、Top5、总结"""
    if not force and not _in_window("evening"):
        return
    data = _load_scan()
    if not data:
        return

    date  = data.get("date", datetime.now().strftime("%Y%m%d"))
    picks = data.get("all_picks", [])
    if not picks:
        print("[evening] 无选股")
        return

    codes  = [p["code"] for p in picks]
    s      = _fetch_with_retry(codes, picks, slot="evening")
    if not s["results"]:
        print("[evening] 行情获取失败，发送无数据版本")
        title = f"筹码收盘 {_fmt_date(date)}（行情暂不可用）"
        body  = (f"📊 收盘总结 {_fmt_date(date)}\n"
                 f"筹码选股 {len(picks)} 只（行情数据暂时不可用，请自行查看）\n"
                 f"\n⚠️ 仅供参考，不构成投资建议\n#量化记录 #筹码分布 #数据实验 #记录帖")
        if not dry_run:
            _push(title, body)
        return

    if s["win_rate"] >= 70 and s["avg_ret"] >= 0.5:
        summary = f"今天整体不错，胜率{s['win_rate']:.0f}%，均涨{s['avg_ret']:+.2f}%，筹码策略跑出来了。"
    elif s["win_rate"] >= 50:
        summary = f"今天中规中矩，胜率{s['win_rate']:.0f}%，均涨{s['avg_ret']:+.2f}%。"
    else:
        summary = f"今天偏弱，胜率{s['win_rate']:.0f}%，均涨{s['avg_ret']:+.2f}%，市场整体承压。"

    lines = [f"📊 收盘总结 {_fmt_date(date)}"]

    lines.append("**收益前五：**")
    for i, r in enumerate(s["top5"], 1):
        lines.append(f"{i}. {r['code']} {r['name']} **{r['change_pct']:+.2f}%**  ")

    lines.append(f"\n筹码选股 {s['n_total']} 只（覆盖 {len(s['results'])} 只）")
    lines.append(f"**最终胜率 {s['win_rate']:.0f}%**（{s['n_win']}/{s['n_total']}只盈利）")
    lines.append(f"**综合收益率 {s['avg_ret']:+.2f}%**\n")

    lines.append(summary)

    if s["nan_stocks"]:
        codes = " ".join(f"{r['code']}{r['name']}" for r in s["nan_stocks"])
        lines.append(f"\n⚠️ 行情缺失（停牌/数据异常）：{codes}")

    lines.append("\n⚠️ 仅供参考，不构成投资建议")
    lines.append("#量化记录 #筹码分布 #数据实验 #记录帖")

    title = f"筹码收盘 {_fmt_date(date)}（胜率{s['win_rate']:.0f}% 均{s['avg_ret']:+.2f}%）"
    body  = "\n".join(lines)
    print(f"\n{title}\n{body}")
    if not dry_run:
        _push(title, body)


# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("slot", choices=["morning", "midday", "evening"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="跳过时间窗口检查")
    args = parser.parse_args()
    fn = {"morning": cmd_morning, "midday": cmd_midday, "evening": cmd_evening}[args.slot]
    fn(args.dry_run, args.force)


if __name__ == "__main__":
    main()
