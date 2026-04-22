#!/usr/bin/env python3
"""
主策略每日胜率记录器
每日 17:15 运行，读取前一日主策略扫盘结果（latest_picks.json），
统计今日涨幅表现，推送微信，追加写入 data/main_daily_perf.json。

用法：
    python -X utf8 scripts/main_perf_log.py [--dry-run] [--force]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

ROOT      = Path(__file__).resolve().parent.parent
PERF_PATH = ROOT / "data" / "main_daily_perf.json"
PICKS_PATH = ROOT / "data" / "latest_picks.json"
SIG_PATH  = ROOT / "data" / "signals_log.json"


def _load_prev_picks(today: str) -> list[dict]:
    """读取最近一次主策略扫盘中 buy_score 最高的一批股票。
    优先 latest_picks.json（只要不是今天18:30之后写的），
    fallback signals_log.json。
    """
    # latest_picks.json
    if PICKS_PATH.exists():
        raw = json.loads(PICKS_PATH.read_text(encoding="utf-8"))
        ts  = raw.get("timestamp", "")
        pick_date = ts[:10].replace("-", "") if ts else ""
        results = raw.get("results", [])
        if results and pick_date < today:
            return results

    # signals_log.json fallback
    if SIG_PATH.exists():
        entries = json.loads(SIG_PATH.read_text(encoding="utf-8"))
        for entry in reversed(entries):
            if entry.get("date", "") < today:
                buys = entry.get("buy_signals", [])
                if buys:
                    return buys
    return []


def _fetch_prices(codes: list[str]) -> dict[str, float]:
    import sys, pandas as pd
    sys.path.insert(0, str(ROOT / "scripts"))
    from common import get_spot_em
    df = get_spot_em()
    if df.empty:
        return {}
    df["_code"] = df["代码"].astype(str).str.zfill(6)
    df = df[df["_code"].isin(codes)].copy()
    df["_pct"]  = pd.to_numeric(df["涨跌幅"], errors="coerce")
    df = df.dropna(subset=["_pct"])
    return dict(zip(df["_code"], df["_pct"]))


def _score_tier(score: float) -> str:
    if score >= 85:  return "S1"
    if score >= 75:  return "S2"
    return "S3"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force",   action="store_true", help="跳过时间/去重检查")
    args = parser.parse_args()

    now = datetime.now()
    if not args.force:
        hm = now.hour * 60 + now.minute
        if hm < 15 * 60 + 10:
            print(f"[main_perf] 当前 {now:%H:%M}，需 15:10 后运行，跳过")
            return

    today = now.strftime("%Y%m%d")

    picks = _load_prev_picks(today)
    if not picks:
        print("[main_perf] 无前日选股数据，退出")
        return

    print(f"[main_perf] 前日选股 {len(picks)} 只，获取今日行情 ...")

    codes  = [str(p.get("code", p.get("ts_code", ""))).split(".")[0] for p in picks]
    prices = _fetch_prices(codes)
    print(f"[main_perf] 获取到 {len(prices)} 只行情")

    # Compute stats
    results = []
    for p in picks:
        code  = str(p.get("code", p.get("ts_code", ""))).split(".")[0]
        score = float(p.get("buy_score", p.get("score", 0)))
        pct   = prices.get(code)
        if pct is None:
            continue
        results.append({
            "code":  code,
            "name":  p.get("name", code),
            "score": score,
            "tier":  _score_tier(score),
            "pct":   pct,
        })

    if not results:
        print("[main_perf] 行情获取失败，退出")
        return

    n_total  = len(results)
    n_win    = sum(1 for r in results if r["pct"] > 0)
    win_rate = round(n_win / n_total * 100, 1)
    avg_ret  = round(sum(r["pct"] for r in results) / n_total, 2)
    by_pct   = sorted(results, key=lambda r: r["pct"], reverse=True)
    top5     = by_pct[:5]

    # Per-tier stats
    tier_stats: dict[str, dict] = {}
    for tier in ["S1", "S2", "S3"]:
        t_picks = [r for r in results if r["tier"] == tier]
        if not t_picks:
            continue
        tw = sum(1 for r in t_picks if r["pct"] > 0)
        tier_stats[tier] = {
            "n": len(t_picks),
            "win_rate": round(tw / len(t_picks) * 100, 1),
            "avg_ret":  round(sum(r["pct"] for r in t_picks) / len(t_picks), 2),
        }

    # Build push
    date_fmt = f"{today[4:6]}/{today[6:]}"
    emoji    = "🟢" if win_rate >= 60 else ("🟡" if win_rate >= 40 else "🔴")

    lines = [f"📈 主策略收盘 {date_fmt}"]
    lines.append(f"选股 **{n_total}只**  {emoji} 胜率 **{win_rate}%**  均涨 **{avg_ret:+.2f}%**\n")

    if tier_stats:
        lines.append("**按评分分层：**")
        tier_labels = {"S1": "≥85分", "S2": "75-85分", "S3": "<75分"}
        for tier, s in tier_stats.items():
            e = "🟢" if s["win_rate"] >= 60 else ("🟡" if s["win_rate"] >= 40 else "🔴")
            lines.append(f"{e} {tier}({tier_labels[tier]}) {s['n']}只  胜率{s['win_rate']}%  均{s['avg_ret']:+.2f}%")
        lines.append("")

    lines.append("**涨幅前五：**")
    for i, r in enumerate(top5, 1):
        lines.append(f"{i}. {r['code']} {r['name']} **{r['pct']:+.2f}%** (评分{r['score']:.0f})")

    dn3 = [r for r in by_pct if r["pct"] < 0][-3:]
    if dn3:
        lines.append("\n**跌幅前三：**")
        for r in reversed(dn3):
            lines.append(f"• {r['code']} {r['name']} {r['pct']:+.2f}%")

    lines.append("\n⚠️ 仅供参考，不构成投资建议")
    push_body = "\n".join(lines)
    print(f"\n{push_body}\n")

    if args.dry_run:
        print("[main_perf] dry-run，不写入不推送")
        return

    # Dedup and save
    record = {
        "date":      today,
        "logged":    now.isoformat(timespec="seconds"),
        "n":         n_total,
        "win_rate":  win_rate,
        "avg_ret":   avg_ret,
        "tiers":     tier_stats,
        "top5":      top5,
    }
    existing: list[dict] = []
    if PERF_PATH.exists():
        existing = json.loads(PERF_PATH.read_text(encoding="utf-8"))
    if any(r["date"] == today for r in existing):
        if args.force:
            existing = [r for r in existing if r["date"] != today]
        else:
            print(f"[main_perf] {today} 已记录，跳过（--force 覆盖）")
            return
    existing.append(record)
    PERF_PATH.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[main_perf] 已写入（共 {len(existing)} 条）")

    # WeChat push
    try:
        import sys
        sys.path.insert(0, str(ROOT / "scripts"))
        from common import send_wechat, configure_pushplus
        cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
        title = f"主策略收盘 {date_fmt} | 胜率{win_rate}% 均{avg_ret:+.2f}%"
        send_wechat(title, push_body, sendkey)
        print("[main_perf] 微信推送成功")
    except Exception as e:
        print(f"[main_perf] 微信推送失败: {e}")


if __name__ == "__main__":
    main()
