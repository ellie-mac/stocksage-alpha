#!/usr/bin/env python3
"""
cad — 数据驱动筹码全档扫描，支持多组 mods 一次加载数据分别推送。

用法：
    python -X utf8 scripts/chip_cad.py                      # 默认跑 bekh + bekhm
    python -X utf8 scripts/chip_cad.py --mods bekh bekhm    # 同上，显式指定
    python -X utf8 scripts/chip_cad.py --mods bekh          # 只跑 cad
    python -X utf8 scripts/chip_cad.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from chip_strategy import (
    _get_pro, fetch_chip_data, screen, load_names,
    fetch_6m_high, add_indicators, _latest_trade_date,
)
from common import send_wechat, configure_pushplus

TIER_ORDER = [
    ("T1", 95.0, None),
    ("T2", 90.0, 95.0),
    ("T3", 85.0, 90.0),
    ("T4", 75.0, 85.0),
    ("T5", 65.0, 75.0),
]


def _run_one(df_all, mods: str, trade_date: str,
             sendkey: str, dry_run: bool) -> None:
    """Run a single mods variant against pre-loaded df_all and push."""
    mods = mods.lower()
    boll_near   = "b" in mods
    cheap       = "e" in mods
    no_kcb      = "k" in mods
    high_filter = "h" in mods
    macd_conv   = "m" in mods
    macd_zero   = "z" in mods

    max_price    = 50.0 if cheap else None
    max_6m_ratio = 0.9  if high_filter else None

    date_fmt  = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    mod_label = " ".join(filter(None, [
        "BOLL"     if boll_near   else "",
        "≤50元"   if cheap       else "",
        "排科创"   if no_kcb     else "",
        "排高位"   if high_filter else "",
        "MACD绿柱" if macd_conv   else "",
        "MACD近零" if macd_zero   else "",
    ]))

    sections: list[str] = []
    total = 0
    saves: dict[str, list[dict]] = {}
    pro = _get_pro()

    for tier_name, min_win, max_win in TIER_ORDER:
        win_range = f"{min_win:.0f}-{max_win:.0f}%" if max_win else f"≥{min_win:.0f}%"

        result = screen(df_all, min_win, max_win=max_win, max_today_pct=5.0,
                        max_6m_ratio=None, six_month_high=None,
                        max_price=max_price, exclude_kcb=no_kcb)

        if max_6m_ratio is not None and not result.empty:
            six_m  = fetch_6m_high(result["ts_code"].tolist(), trade_date, pro)
            result = screen(df_all, min_win, max_win=max_win, max_today_pct=5.0,
                            max_6m_ratio=max_6m_ratio, six_month_high=six_m,
                            max_price=max_price, exclude_kcb=no_kcb)

        if (boll_near or macd_conv or macd_zero) and not result.empty:
            result = add_indicators(result)
            result = screen(result, min_win, max_win=max_win, max_today_pct=None,
                            max_6m_ratio=None, six_month_high=None,
                            max_price=None, exclude_kcb=False,
                            boll_near_mid=boll_near, macd_converging=macd_conv,
                            macd_near_zero=macd_zero)

        n = len(result)
        total += n
        print(f"[cad/{mods}] {tier_name} ({win_range}): {n} 只", flush=True)

        picks_list: list[dict] = []
        header = f"\n### {tier_name}（获利盘 {win_range}）— {n} 只"
        if n == 0:
            saves[tier_name] = []
            sections.append(header + "\n无符合条件股票")
            continue

        rows = ["| 名称 | 行业 | 收盘 | 获利盘% |",
                "|------|------|-----:|--------:|"]
        for _, row in result.iterrows():
            code    = row.get("code", "")
            name    = str(row.get("name", "") or "").strip()[:8] or str(code)
            ind     = str(row.get("industry", "") or "")[:6]
            close   = row.get("close", float("nan"))
            win     = row.get("winner_rate", float("nan"))
            close_s = f"{close:.2f}" if not math.isnan(close) else "-"
            win_s   = f"{win:.1f}%" if not math.isnan(win) else "-"
            rows.append(f"| {name} | {ind} | {close_s} | {win_s} |")
            picks_list.append({"code": str(code), "name": name})

        saves[tier_name] = picks_list
        sections.append(header + "\n" + "\n".join(rows))

    prefix  = "chip_cadm" if "m" in mods else "chip_cad"
    payload = json.dumps({"date": trade_date, "mods": mods, "tiers": saves},
                         ensure_ascii=False, indent=2)
    dated   = ROOT / "data" / f"{prefix}_{trade_date}.json"
    latest  = ROOT / "data" / f"{prefix}_latest.json"
    dated.write_text(payload, encoding="utf-8")
    latest.write_text(payload, encoding="utf-8")
    print(f"[cad/{mods}] 已保存 {dated.name}（共{total}只）")

    body  = "\n".join(sections) + "\n\n> ⚠️ 仅供参考，不构成投资建议"
    title = f"筹码驱动 {date_fmt} ({mod_label}) 共{total}只"
    print(f"\n{title}\n")
    send_wechat(title, body, sendkey, dry_run=dry_run)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mods", nargs="+", default=["bekh", "bekhm"],
                        help="一个或多个修饰符组合，数据只加载一次")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    sendkey = cfg.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))

    pro        = _get_pro()
    query_date = _latest_trade_date()
    df_all     = fetch_chip_data(query_date, pro)

    if df_all.empty:
        print("[cad] 无数据，退出")
        return

    trade_date = str(df_all["trade_date"].iloc[0]) if "trade_date" in df_all.columns else query_date

    names = load_names()
    if names:
        df_all["name"]     = df_all["ts_code"].map(lambda c: names.get(c, {}).get("name", ""))
        df_all["industry"] = df_all["ts_code"].map(lambda c: names.get(c, {}).get("industry", ""))
    else:
        df_all["name"] = df_all["industry"] = ""

    print(f"[cad] trade_date={trade_date}  mods={args.mods}", flush=True)

    for mods_str in args.mods:
        _run_one(df_all, mods_str, trade_date, sendkey, args.dry_run)


if __name__ == "__main__":
    main()
