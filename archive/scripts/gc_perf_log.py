#!/usr/bin/env python3
"""
金叉共振每日胜率记录
每日 15:15 运行，读取当日 golden_cross_latest.json，统计 G0-G2 今日涨幅表现。

用法：
    python -X utf8 scripts/gc_perf_log.py [--dry-run] [--force]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

ROOT      = Path(__file__).resolve().parent.parent
GC_PATH   = ROOT / "data" / "golden_cross_latest.json"
PERF_PATH = ROOT / "data" / "gc_daily_perf.json"

TRACKED_TIERS = ["G0", "G1", "G2"]


def _fetch_prices(codes: list[str]) -> dict[str, float]:
    import sys
    import pandas as pd
    sys.path.insert(0, str(Path(__file__).parent))
    from common import get_spot_em
    df = get_spot_em()
    if df.empty:
        return {}
    df["_code"] = df["代码"].astype(str).str.zfill(6)
    df = df[df["_code"].isin(codes)].copy()
    df["_pct"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
    df = df.dropna(subset=["_pct"])
    return dict(zip(df["_code"], df["_pct"]))


def _tier_stats(picks: list[dict], prices: dict[str, float]) -> dict:
    rets = [(p, prices[p["code"]]) for p in picks if p["code"] in prices]
    if not rets:
        return {"n": 0, "win_rate": None, "avg_ret": None, "top3": []}
    vals = [r for _, r in rets]
    n_win    = sum(1 for r in vals if r > 0)
    win_rate = round(n_win / len(vals) * 100, 1)
    avg_ret  = round(sum(vals) / len(vals), 2)
    top3 = sorted(
        [{"code": p["code"], "name": p.get("name", ""), "pct": prices[p["code"]]}
         for p in picks if p["code"] in prices],
        key=lambda x: x["pct"], reverse=True,
    )[:3]
    return {"n": len(vals), "win_rate": win_rate, "avg_ret": avg_ret, "top3": top3}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force",   action="store_true", help="跳过时间/去重检查")
    args = parser.parse_args()

    now = datetime.now()
    if not args.force:
        hm = now.hour * 60 + now.minute
        if hm < 15 * 60 + 10:
            print(f"[gc_perf] 当前 {now:%H:%M}，需 15:10 后运行，跳过")
            return

    today = now.strftime("%Y%m%d")

    if not GC_PATH.exists():
        print("[gc_perf] golden_cross_latest.json 不存在，退出")
        return

    gc = json.loads(GC_PATH.read_text(encoding="utf-8"))
    gc_date = gc.get("date", "")
    if gc_date != today and not args.force:
        print(f"[gc_perf] gc 数据日期 {gc_date} 不是今天 {today}，跳过（--force 覆盖）")
        return

    tiers = gc.get("tiers", {})
    all_picks: list[dict] = []
    picks_by_tier: dict[str, list[dict]] = {}
    for t in TRACKED_TIERS:
        p = tiers.get(t, [])
        picks_by_tier[t] = p
        all_picks.extend(p)

    if not all_picks:
        print("[gc_perf] G0-G2 无选股，退出")
        return

    all_codes = [p["code"] for p in all_picks]
    print(f"[gc_perf] 获取 {len(all_codes)} 只股票行情（G0-G2）...")
    prices = _fetch_prices(all_codes)
    print(f"[gc_perf] 获取到 {len(prices)} 只")

    tiers_stats: dict[str, dict] = {}
    total_rets: list[float] = []
    for t in TRACKED_TIERS:
        s = _tier_stats(picks_by_tier[t], prices)
        tiers_stats[t] = s
        if s["avg_ret"] is not None:
            total_rets.extend(prices[p["code"]] for p in picks_by_tier[t] if p["code"] in prices)

    total_n        = len(total_rets)
    total_win_rate = round(sum(1 for r in total_rets if r > 0) / total_n * 100, 1) if total_rets else None
    total_avg_ret  = round(sum(total_rets) / total_n, 2) if total_rets else None

    record = {
        "date":           today,
        "logged":         now.isoformat(timespec="seconds"),
        "gc_date":        gc_date,
        "total_n":        total_n,
        "total_win_rate": total_win_rate,
        "total_avg_ret":  total_avg_ret,
        "tiers":          tiers_stats,
    }

    date_fmt = f"{today[4:6]}/{today[6:]}"
    wr_s = f"{total_win_rate}%" if total_win_rate is not None else "-"
    ar_s = f"{total_avg_ret:+.2f}%" if total_avg_ret is not None else "-"
    print(f"[gc_perf] G0-G2 共{total_n}只  胜率{wr_s}  均涨{ar_s}")

    lines = [f"📡 金叉共振胜率 {date_fmt}"]
    lines.append(f"**G0-G2 共{total_n}只  胜率 {wr_s}  均涨 {ar_s}**\n")
    for t in TRACKED_TIERS:
        s = tiers_stats[t]
        if s["win_rate"] is None:
            continue
        emoji = "🟢" if s["win_rate"] >= 50 else "🔴"
        lines.append(f"{emoji} {t}（{s['n']}只）  胜率 {s['win_rate']}%  均 {s['avg_ret']:+.2f}%")
        top = "  ".join(f"{x['name']}{x['pct']:+.1f}%" for x in s["top3"])
        if top:
            lines.append(f"  ↑ {top}")
    lines.append("\n⚠️ 仅供参考，不构成投资建议")
    push_body = "\n".join(lines)
    print(f"\n{push_body}\n")

    if args.dry_run:
        print("[gc_perf] dry-run，不写入")
        return

    existing: list[dict] = []
    if PERF_PATH.exists():
        existing = json.loads(PERF_PATH.read_text(encoding="utf-8"))
    if any(r["date"] == today for r in existing):
        if args.force:
            existing = [r for r in existing if r["date"] != today]
        else:
            print(f"[gc_perf] {today} 已记录，跳过（--force 覆盖）")
            return

    existing.append(record)
    PERF_PATH.write_text(
        json.dumps(existing, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[gc_perf] 已写入（共 {len(existing)} 条）")

    try:
        import sys
        sys.path.insert(0, str(ROOT / "scripts"))
        from common import send_wechat, configure_pushplus
        cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
        title = f"金叉胜率 G0-G2 {date_fmt} | 胜率{wr_s} 均{ar_s}"
        send_wechat(title, push_body, sendkey)
        print("[gc_perf] 微信推送成功")
    except Exception as e:
        print(f"[gc_perf] 微信推送失败: {e}")


if __name__ == "__main__":
    main()
