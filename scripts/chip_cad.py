#!/usr/bin/env python3
"""
cad — 数据驱动筹码全档扫描，按回测最优顺序 T4→T5→T3→T2→T1 推送一条微信。

用法：
    python -X utf8 scripts/chip_cad.py [--mods bekhm] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from chip_strategy import (
    _get_pro, fetch_chip_data, screen, load_names,
    fetch_6m_high, add_indicators,
)
from common import send_wechat, configure_pushplus

# T4→T5→T3→T2→T1 by backtest win rate
TIER_ORDER = [
    ("T4", 75.0, 85.0),
    ("T1", 95.0, None),
    ("T2", 90.0, 95.0),
    ("T3", 85.0, 90.0),
    ("T5", 65.0, 75.0),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mods",     default="bekhm", help="修饰符: b=BOLL e=≤50 k=排科创 h=排高位 m=MACD绿柱 z=MACD近零")
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()

    mods = args.mods.lower()
    boll_near   = "b" in mods
    cheap       = "e" in mods
    no_kcb      = "k" in mods
    high_filter = "h" in mods
    macd_conv   = "m" in mods
    macd_zero   = "z" in mods

    max_price    = 50.0 if cheap else None
    max_6m_ratio = 0.9  if high_filter else None

    cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    sendkey = cfg.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))

    pro          = _get_pro()
    trade_date   = datetime.now().strftime("%Y%m%d")
    df_all       = fetch_chip_data(trade_date, pro)

    if df_all.empty:
        print("[cad] 无数据，退出")
        return

    names = load_names()
    if names:
        df_all["name"]     = df_all["ts_code"].map(lambda c: names.get(c, {}).get("name", ""))
        df_all["industry"] = df_all["ts_code"].map(lambda c: names.get(c, {}).get("industry", ""))
    else:
        df_all["name"] = df_all["industry"] = ""

    date_fmt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    mod_label = " ".join(filter(None, [
        "BOLL" if boll_near else "",
        "≤50元" if cheap else "",
        "排科创" if no_kcb else "",
        "排高位" if high_filter else "",
        "MACD绿柱" if macd_conv else "",
        "MACD近零" if macd_zero else "",
    ]))

    sections: list[str] = [f"## 筹码数据驱动 {date_fmt}  ({mod_label})"]
    total = 0

    for tier_name, min_win, max_win in TIER_ORDER:
        win_range = f"{min_win:.0f}-{max_win:.0f}%" if max_win else f"≥{min_win:.0f}%"

        result = screen(df_all, min_win, max_win=max_win, max_today_pct=5.0,
                        max_6m_ratio=None, six_month_high=None,
                        max_price=max_price, exclude_kcb=no_kcb)

        if max_6m_ratio is not None and not result.empty:
            six_m = fetch_6m_high(result["ts_code"].tolist(), trade_date, pro)
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
        print(f"[cad] {tier_name} ({win_range}): {n} 只", flush=True)

        header = f"\n### {tier_name}（获利盘 {win_range}）— {n} 只"
        if n == 0:
            sections.append(header + "\n无符合条件股票")
            continue

        rows = ["| 代码 | 名称 | 行业 | 收盘 | 涨跌% | 获利盘% |",
                "|------|------|------|-----:|------:|--------:|"]
        for _, row in result.iterrows():
            code    = row.get("code", "")
            name    = str(row.get("name", "") or "")[:8]
            ind     = str(row.get("industry", "") or "")[:6]
            close   = row.get("close", float("nan"))
            pct_chg = row.get("pct_chg", float("nan"))
            win     = row.get("winner_rate", float("nan"))
            import math
            close_s = f"{close:.2f}" if not math.isnan(close) else "-"
            pct_s   = f"{pct_chg:+.2f}%" if not math.isnan(pct_chg) else "-"
            win_s   = f"{win:.1f}%" if not math.isnan(win) else "-"
            rows.append(f"| {code} | {name} | {ind} | {close_s} | {pct_s} | {win_s} |")

        sections.append(header + "\n" + "\n".join(rows))

    body  = "\n".join(sections) + f"\n\n> 共 **{total}** 只  |  ⚠️ 仅供参考"
    title = f"筹码数据驱动 {date_fmt} | 共{total}只"
    print(f"\n{title}\n")
    send_wechat(title, body, sendkey, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
