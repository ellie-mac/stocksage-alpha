#!/usr/bin/env python3
"""
每周胜率周报 — 周五收盘后汇总本周每日胜率
运行时间：周五 16:30（daily_PerfLog 跑完之后）

汇总 main_daily_perf.json / chip_daily_perf.json / gc_daily_perf.json
中当周（周一至当天）的记录，推送周报微信。

用法：
    python -X utf8 scripts/weekly_perf_report.py [--dry-run] [--force]
    --force  不限制周几，强制生成当周数据
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

MAIN_PERF_PATH = DATA_DIR / "main_daily_perf.json"
CHIP_PERF_PATH = DATA_DIR / "chip_daily_perf.json"
GC_PERF_PATH   = DATA_DIR / "gc_daily_perf.json"
HOT_PERF_PATH  = DATA_DIR / "hot_daily_perf.json"


def _load(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def _week_dates(today: datetime) -> tuple[str, str]:
    """周六零点运行时，返回上周一和上周五的 YYYYMMDD 字符串。"""
    fri = today - timedelta(days=today.weekday() - 4)   # 上周五
    mon = fri - timedelta(days=4)                        # 上周一
    return mon.strftime("%Y%m%d"), fri.strftime("%Y%m%d")


def _filter_week(records: list[dict], mon: str, today: str) -> list[dict]:
    return [r for r in records if mon <= r.get("date", "") <= today]


def _wr_str(win_rate) -> str:
    return f"{win_rate}%" if win_rate is not None else "-"


def _ar_str(avg_ret) -> str:
    return f"{avg_ret:+.2f}%" if avg_ret is not None else "-"


def _emoji(win_rate) -> str:
    if win_rate is None: return "⚪"
    if win_rate >= 60:   return "🟢"
    if win_rate >= 40:   return "🟡"
    return "🔴"


def _week_avg(records: list[dict], win_key: str, ret_key: str) -> tuple[float | None, float | None]:
    wins = [r[win_key] for r in records if r.get(win_key) is not None]
    rets = [r[ret_key] for r in records if r.get(ret_key) is not None]
    return (round(sum(wins) / len(wins), 1) if wins else None,
            round(sum(rets) / len(rets), 2) if rets else None)


def _date_fmt(d: str) -> str:
    return f"{d[4:6]}/{d[6:]}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force",   action="store_true",
                        help="不限制周几，强制生成（测试用）")
    args = parser.parse_args()

    now = datetime.now()
    if not args.force and now.weekday() != 5:  # 5 = 周六
        print(f"[weekly_perf] 今天是{['一','二','三','四','五','六','日'][now.weekday()]}，只在周六运行，跳过")
        return

    mon_str, today_str = _week_dates(now)
    week_label = f"{_date_fmt(mon_str)}～{_date_fmt(today_str)}"
    print(f"[weekly_perf] 统计范围：{week_label}", flush=True)

    main_recs = _filter_week(_load(MAIN_PERF_PATH), mon_str, today_str)
    chip_recs = _filter_week(_load(CHIP_PERF_PATH), mon_str, today_str)
    gc_recs   = _filter_week(_load(GC_PERF_PATH),   mon_str, today_str)
    hot_recs  = _filter_week(_load(HOT_PERF_PATH),  mon_str, today_str)

    print(f"[weekly_perf] 主策略 {len(main_recs)}天 / 筹码 {len(chip_recs)}天 / 金叉 {len(gc_recs)}天 / 热榜 {len(hot_recs)}天")

    if not main_recs and not chip_recs and not gc_recs and not hot_recs:
        print("[weekly_perf] 本周无胜率数据，跳过")
        return

    sections: list[str] = [f"📅 **周胜率周报 {week_label}**"]

    # 主策略
    if main_recs:
        avg_wr, avg_ret = _week_avg(main_recs, "win_rate", "avg_ret")
        rows = [f"{_emoji(avg_wr)} **主策略** 周均胜率{_wr_str(avg_wr)}  均涨{_ar_str(avg_ret)}"]
        for r in sorted(main_recs, key=lambda x: x["date"]):
            e = _emoji(r.get("win_rate"))
            rows.append(f"  {e} {_date_fmt(r['date'])} {r['n']}只  胜率{_wr_str(r.get('win_rate'))}  均{_ar_str(r.get('avg_ret'))}")
        sections.append("  \n".join(rows))

    # 筹码策略
    if chip_recs:
        avg_wr, avg_ret = _week_avg(chip_recs, "total_win_rate", "total_avg_ret")
        rows = [f"{_emoji(avg_wr)} **筹码策略** 周均胜率{_wr_str(avg_wr)}  均涨{_ar_str(avg_ret)}"]
        for r in sorted(chip_recs, key=lambda x: x["date"]):
            e = _emoji(r.get("total_win_rate"))
            rows.append(f"  {e} {_date_fmt(r['date'])} {r['total_n']}只  胜率{_wr_str(r.get('total_win_rate'))}  均{_ar_str(r.get('total_avg_ret'))}")
        sections.append("  \n".join(rows))

    # 金叉共振
    if gc_recs:
        avg_wr, avg_ret = _week_avg(gc_recs, "total_win_rate", "total_avg_ret")
        rows = [f"{_emoji(avg_wr)} **金叉共振** 周均胜率{_wr_str(avg_wr)}  均涨{_ar_str(avg_ret)}"]
        for r in sorted(gc_recs, key=lambda x: x["date"]):
            e = _emoji(r.get("total_win_rate"))
            rows.append(f"  {e} {_date_fmt(r['date'])} {r['total_n']}只  胜率{_wr_str(r.get('total_win_rate'))}  均{_ar_str(r.get('total_avg_ret'))}")
        sections.append("  \n".join(rows))

    # 热榜策略
    if hot_recs:
        avg_wr, avg_ret = _week_avg(hot_recs, "win_rate", "avg_ret")
        rows = [f"{_emoji(avg_wr)} **热榜策略** 周均胜率{_wr_str(avg_wr)}  均涨{_ar_str(avg_ret)}"]
        for r in sorted(hot_recs, key=lambda x: x["date"]):
            e = _emoji(r.get("win_rate"))
            rows.append(f"  {e} {_date_fmt(r['date'])} {r['n']}只  胜率{_wr_str(r.get('win_rate'))}  均{_ar_str(r.get('avg_ret'))}")
        sections.append("  \n".join(rows))

    sections.append("⚠️ 仅供参考，不构成投资建议")
    body = "\n\n".join(sections)
    print(f"\n{body}\n")

    if args.dry_run:
        print("[weekly_perf] dry-run，不推送")
        return

    try:
        import sys
        sys.path.insert(0, str(ROOT / "scripts"))
        from common import send_wechat, configure_pushplus
        cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
        title = f"周胜率周报 {week_label}"
        send_wechat(title, body, sendkey)
        print("[weekly_perf] 微信推送成功")
    except Exception as e:
        print(f"[weekly_perf] 微信推送失败: {e}")


if __name__ == "__main__":
    main()
