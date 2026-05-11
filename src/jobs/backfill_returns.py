"""
backfill_returns.py — 每日收盘后把 forward return 回填到 snapshots 表。

逻辑：
  1. 找出 T-5 交易日的快照（ret_5d IS NULL，price NOT NULL）
  2. 找出 T-20 交易日的快照（ret_20d IS NULL，price NOT NULL）
  3. 批量拉这些股票今日收盘价
  4. 计算 (close_today - snapshot_price) / snapshot_price → ret
  5. UPDATE snapshots

由 nightly_scan.py 在主流程末尾调用；也可独立执行（每日 17:30+）。
"""
from __future__ import annotations

import os
import sys
import traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import _conn
from logger import get_logger

log = get_logger("backfill_returns")


def _nth_trading_day_before(n: int) -> str | None:
    """返回今天往前数第 n 个交易日（'YYYY-MM-DD'）。"""
    try:
        from trading_calendar import nth_trading_day_before
        return nth_trading_day_before(n)
    except Exception:
        return None


def _fetch_today_close(codes: set[str]) -> dict[str, float]:
    """获取今日收盘价，返回 {code: close}。拉不到的不入字典。"""
    from fetcher import get_price_history
    today = datetime.now().strftime("%Y-%m-%d")
    result: dict[str, float] = {}
    for code in codes:
        try:
            df = get_price_history(code, days=3)
            if df is None or df.empty:
                continue
            last = df.iloc[-1]
            last_date = str(last.get("date", ""))[:10]
            if last_date == today:
                close = float(last["close"])
                if close > 0:
                    result[code] = close
        except Exception:
            pass
    return result


def run_backfill(dry_run: bool = False) -> dict:
    """回填 ret_5d / ret_20d；返回执行摘要。"""
    target_5d  = _nth_trading_day_before(5)
    target_20d = _nth_trading_day_before(20)

    summary = {
        "target_5d": target_5d,
        "target_20d": target_20d,
        "updated_5d": 0,
        "updated_20d": 0,
        "dry_run": dry_run,
    }

    # 收集需要回填的 (date, col, rows)
    tasks: list[tuple[str, str, list]] = []
    with _conn() as conn:
        if target_5d:
            rows = conn.execute(
                "SELECT code, price FROM snapshots "
                "WHERE date=? AND ret_5d IS NULL AND price IS NOT NULL",
                (target_5d,),
            ).fetchall()
            if rows:
                tasks.append((target_5d, "ret_5d", [dict(r) for r in rows]))

        if target_20d:
            rows = conn.execute(
                "SELECT code, price FROM snapshots "
                "WHERE date=? AND ret_20d IS NULL AND price IS NOT NULL",
                (target_20d,),
            ).fetchall()
            if rows:
                tasks.append((target_20d, "ret_20d", [dict(r) for r in rows]))

    if not tasks:
        log.info("backfill_nothing_to_do", extra={"dry_run": dry_run})
        return summary

    all_codes = {r["code"] for _, _, rows in tasks for r in rows}
    log.info("backfill_fetching_prices", extra={"codes": len(all_codes), "dry_run": dry_run})

    if dry_run:
        summary["codes_needed"] = len(all_codes)
        return summary

    today_prices = _fetch_today_close(all_codes)
    log.info("backfill_prices_fetched", extra={"found": len(today_prices), "total": len(all_codes)})

    with _conn() as conn:
        for date_val, ret_col, rows in tasks:
            updated = 0
            for r in rows:
                today_close = today_prices.get(r["code"])
                if today_close is None:
                    continue
                snapshot_price = r["price"]
                ret = (today_close - snapshot_price) / snapshot_price
                conn.execute(
                    f"UPDATE snapshots SET {ret_col}=? WHERE date=? AND code=?",
                    (round(ret, 6), date_val, r["code"]),
                )
                updated += 1
            summary[f"updated_{ret_col[4:]}"] = updated  # ret_5d → updated_5d
            log.info("backfill_updated", extra={
                "col": ret_col, "date": date_val, "updated": updated, "total": len(rows),
            })

    return summary


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = run_backfill(dry_run=args.dry_run)
    print(result)
