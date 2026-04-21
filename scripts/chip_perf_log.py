#!/usr/bin/env python3
"""
筹码每日胜率记录器
每日收盘后（15:10+）运行，统计当日 chip_scan_latest.json 各档表现，
追加写入 data/chip_daily_perf.json。

用法：
    python -X utf8 scripts/chip_perf_log.py [--dry-run] [--force]
"""
from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime, date as _date
from pathlib import Path

ROOT       = Path(__file__).resolve().parent.parent
SCAN_PATH  = ROOT / "data" / "chip_scan_latest.json"
PERF_PATH  = ROOT / "data" / "chip_daily_perf.json"

TIERS = ["T1", "T2", "T3", "T4", "T5"]


def _fetch_prices(codes: list[str], retries: int = 3) -> dict[str, float]:
    """返回 {code: pct_chg}，失败自动重试。"""
    import akshare as ak
    for attempt in range(1, retries + 1):
        try:
            df = ak.stock_zh_a_spot_em()
            df = df[df["代码"].isin(codes)].copy()
            result: dict[str, float] = {}
            for _, row in df.iterrows():
                code = str(row["代码"]).zfill(6)
                try:
                    pct = float(row["涨跌幅"])
                    if not math.isnan(pct):
                        result[code] = pct
                except Exception:
                    pass
            return result
        except Exception as e:
            print(f"[perf] 行情获取失败（第{attempt}次）: {e}")
            if attempt < retries:
                time.sleep(5)
    return {}


def _tier_stats(picks: list[dict], prices: dict[str, float]) -> dict:
    rets = [prices[p["code"]] for p in picks if p["code"] in prices]
    if not rets:
        return {"n": 0, "win_rate": None, "avg_ret": None, "top3": []}
    n_win    = sum(1 for r in rets if r > 0)
    win_rate = round(n_win / len(rets) * 100, 1)
    avg_ret  = round(sum(rets) / len(rets), 2)
    top3 = sorted(
        [{"code": p["code"], "name": p.get("name",""), "pct": prices[p["code"]]}
         for p in picks if p["code"] in prices],
        key=lambda x: x["pct"], reverse=True
    )[:3]
    return {"n": len(rets), "win_rate": win_rate, "avg_ret": avg_ret, "top3": top3}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force",   action="store_true", help="跳过时间窗口检查")
    args = parser.parse_args()

    # 时间窗口检查：15:10 后才有完整收盘数据
    now = datetime.now()
    if not args.force:
        hm = now.hour * 60 + now.minute
        if hm < 15 * 60 + 10:
            print(f"[perf] 当前 {now:%H:%M}，需 15:10 后运行，跳过")
            return

    if not SCAN_PATH.exists():
        print(f"[perf] 找不到 {SCAN_PATH}，请先运行筹码扫描")
        return

    scan = json.loads(SCAN_PATH.read_text(encoding="utf-8"))
    scan_date = scan.get("date", now.strftime("%Y%m%d"))

    # 防重复：同一日期只记录一次
    existing: list[dict] = []
    if PERF_PATH.exists():
        existing = json.loads(PERF_PATH.read_text(encoding="utf-8"))
    if any(r["date"] == scan_date for r in existing):
        print(f"[perf] {scan_date} 已记录，跳过")
        return

    tiers_data = scan.get("tiers", {})
    all_codes  = [p["code"] for tier in TIERS for p in tiers_data.get(tier, [])]
    if not all_codes:
        print("[perf] 无选股数据")
        return

    print(f"[perf] 获取 {len(all_codes)} 只股票行情 ...")
    prices = _fetch_prices(all_codes)
    print(f"[perf] 获取到 {len(prices)} 只")

    record: dict = {
        "date":   scan_date,
        "logged": now.isoformat(timespec="seconds"),
        "filter": scan.get("filter", ""),
        "tiers":  {},
    }

    total_rets: list[float] = []
    for tier in TIERS:
        picks = tiers_data.get(tier, [])
        stats = _tier_stats(picks, prices)
        record["tiers"][tier] = stats
        if stats["avg_ret"] is not None:
            total_rets.extend(
                prices[p["code"]] for p in picks if p["code"] in prices
            )
        n    = stats["n"]
        wr   = f"{stats['win_rate']}%" if stats["win_rate"] is not None else "-"
        ar   = f"{stats['avg_ret']:+.2f}%" if stats["avg_ret"] is not None else "-"
        print(f"  {tier}: {n}只  胜率{wr}  均涨{ar}")

    if total_rets:
        record["total_win_rate"] = round(sum(1 for r in total_rets if r > 0) / len(total_rets) * 100, 1)
        record["total_avg_ret"]  = round(sum(total_rets) / len(total_rets), 2)
        print(f"  全档: {len(total_rets)}只  胜率{record['total_win_rate']}%  均涨{record['total_avg_ret']:+.2f}%")

    if args.dry_run:
        print("[perf] dry-run，不写入文件")
        return

    existing.append(record)
    PERF_PATH.write_text(
        json.dumps(existing, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"[perf] 已写入 {PERF_PATH.name}（共 {len(existing)} 条记录）")


if __name__ == "__main__":
    main()
