#!/usr/bin/env python3
"""
每日筹码全档扫描：T1-T5 全部档位，结果合并推微信。
用法：python -X utf8 scripts/daily_chip_scan.py [--date YYYYMMDD] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from chip_strategy import fetch_chip_data, screen, load_names, _get_pro, _latest_trade_date
from common import send_wechat, configure_pushplus

TIERS = [
    {"label": "T1 ≥95%",    "min_win": 95,  "max_win": None},
    {"label": "T2 90-95%",  "min_win": 90,  "max_win": 95},
    {"label": "T3 85-90%",  "min_win": 85,  "max_win": 90},
    {"label": "T4 75-85%",  "min_win": 75,  "max_win": 85},
    {"label": "T5 65-75%",  "min_win": 65,  "max_win": 75},
]


def _push(title: str, body: str) -> None:
    try:
        cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
        send_wechat(title, body, sendkey)
        print(f"[notify] 微信推送成功")
    except Exception as e:
        print(f"[notify] 推送失败: {e}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",    type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    trade_date = args.date or _latest_trade_date()
    print(f"[scan] trade_date={trade_date}", flush=True)

    pro   = _get_pro()
    names = load_names()

    df = fetch_chip_data(trade_date, pro)
    if df.empty:
        print("[scan] 无数据，退出")
        _push(f"筹码扫描 {trade_date}", "无数据，可能是非交易日或 API 限流")
        return

    if names:
        df["name"]     = df["ts_code"].map(lambda c: names.get(c, {}).get("name", ""))
        df["industry"] = df["ts_code"].map(lambda c: names.get(c, {}).get("industry", ""))
    else:
        df["name"] = df["industry"] = ""

    sections: list[str] = []
    for tier in TIERS:
        result = screen(
            df,
            min_win       = tier["min_win"],
            max_win       = tier["max_win"],
            max_today_pct = 5.0,
            max_price     = None,
            exclude_kcb   = False,
        )
        picks = len(result)
        header = f"【{tier['label']}】{picks}只"
        if result.empty:
            sections.append(f"{header}\n（无）")
            continue

        rows = []
        for _, r in result.iterrows():
            code  = r["ts_code"].split(".")[0]          # always from ts_code, preserves leading zeros
            name  = r.get("name", code)
            ind   = r.get("industry", "")
            close = r.get("close", 0)
            rows.append(f"{code} {name} {ind} ¥{close:.2f}  ")
        sections.append(header + "  \n" + "\n".join(rows))

    title = f"筹码全档 {trade_date}"
    body  = "\n\n".join(sections)
    print(f"\n{title}\n{body}")

    if not args.dry_run:
        _push(title, body)


if __name__ == "__main__":
    main()
