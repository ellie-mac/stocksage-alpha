#!/usr/bin/env python3
"""
每日资金流向批量预取 — 通过 tushare moneyflow_ths 一次拉全市场当日数据。

用法：
    python -X utf8 scripts/tools/prefetch_fund_flow.py           # 拉今日
    python -X utf8 scripts/tools/prefetch_fund_flow.py --days 2  # 拉今日+昨日
    python -X utf8 scripts/tools/prefetch_fund_flow.py --date 20260507

tushare moneyflow_ths 限速 2次/小时，每次拉全市场一天（约5000只），
拆分后写入各股票 fundflow 缓存，供 get_fund_flow() 直接命中。
推荐定时：每日 15:15（收盘后）运行。
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent.parent
SCRIPTS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS))

import fetcher as _fetcher


def _latest_trade_date() -> str:
    """返回今天或最近的交易日（YYYYMMDD）。"""
    today = date.today().strftime("%Y%m%d")
    try:
        raw = _fetcher.get_trade_calendar()
        all_dates = sorted(d.replace("-", "") for d in raw)
        past = [d for d in all_dates if d <= today]
        return past[-1] if past else today
    except Exception:
        return today


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="", help="指定交易日 YYYYMMDD（默认今日）")
    parser.add_argument("--days", type=int, default=1, help="向前拉取天数（默认1，最多2）")
    args = parser.parse_args()

    if args.date:
        base_date = args.date
    else:
        base_date = _latest_trade_date()

    # 收集待拉日期（从 base_date 往前 days 个交易日）
    try:
        raw = _fetcher.get_trade_calendar()
        all_dates = sorted(d.replace("-", "") for d in raw)
        before = [d for d in all_dates if d <= base_date]
        target_dates = before[-args.days:] if len(before) >= args.days else before
    except Exception:
        target_dates = [base_date]

    print(f"[prefetch_fundflow] 待拉日期: {target_dates}", flush=True)

    total_cached = 0
    for td in target_dates:
        n = _fetcher.prefetch_fund_flow_by_date(td)
        total_cached += n

    print(f"[prefetch_fundflow] 完成，共写入缓存 {total_cached} 条（{len(target_dates)} 个交易日）")


if __name__ == "__main__":
    main()
