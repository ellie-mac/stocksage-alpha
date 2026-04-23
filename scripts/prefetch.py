#!/usr/bin/env python3
"""
scripts/prefetch.py — 定时数据预热

  --price    收盘后预热全市场价格历史缓存（15:45 触发，~1-1.5h）
  --market   收盘后预热市场数据：CSI300、市值PE、申万PE、行业map、停牌表、交易日历（15:35 触发，<1min）
  --concept  早盘前预热概念板块反查 map（08:30 触发，~30s）

用法:
  python -X utf8 scripts/prefetch.py --price
  python -X utf8 scripts/prefetch.py --market
  python -X utf8 scripts/prefetch.py --concept
  python -X utf8 scripts/prefetch.py --price --market  # 组合
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

UNIVERSE_PATH = ROOT / "data" / "universe_main.json"


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_universe() -> list[str]:
    if not UNIVERSE_PATH.exists():
        print(f"[prefetch] 找不到 {UNIVERSE_PATH}，跳过", flush=True)
        return []
    return json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))


def _now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ── price history prefetch ────────────────────────────────────────────────────

def prefetch_price(force: bool = False) -> None:
    """预热全市场价格历史缓存。跳过已有有效缓存的股票，只补缺失的。"""
    import fetcher
    import cache as _cache

    codes = _load_universe()
    if not codes:
        return

    now_h = datetime.now().hour
    if not force and now_h < 15:
        print(f"[prefetch/price] 当前 {_now_str()}，需 15:00 后运行（今日数据尚未就绪），跳过", flush=True)
        return

    print(f"[prefetch/price] 开始预热 {len(codes)} 只价格历史  {_now_str()}", flush=True)
    ttl   = _cache.smart_price_ttl()
    total = len(codes)
    done  = 0
    skipped = 0
    failed  = 0
    t0 = time.time()

    def _warm_one(code: str) -> str:
        cached = _cache.get_df(f"price_{code}_550", ttl)
        if cached is not None:
            return "skip"
        df = fetcher.get_price_history(code, days=365)
        if df is None or df.empty:
            return "fail"
        return "ok"

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(_warm_one, c): c for c in codes}
        for fut in as_completed(futures):
            result = fut.result()
            done += 1
            if result == "skip":
                skipped += 1
            elif result == "fail":
                failed += 1
            if done % 200 == 0 or done == total:
                elapsed = time.time() - t0
                rate    = done / elapsed if elapsed > 0 else 0
                eta     = (total - done) / rate if rate > 0 else 0
                print(
                    f"[prefetch/price] {done}/{total}  "
                    f"新增:{done-skipped-failed}  跳过:{skipped}  失败:{failed}  "
                    f"eta:{eta/60:.0f}min  {_now_str()}",
                    flush=True,
                )

    elapsed = time.time() - t0
    print(
        f"[prefetch/price] 完成 {_now_str()}  "
        f"总耗时:{elapsed/60:.1f}min  新增:{done-skipped-failed}  跳过:{skipped}  失败:{failed}",
        flush=True,
    )


# ── market data prefetch ──────────────────────────────────────────────────────

def prefetch_market() -> None:
    """预热市场整体数据：CSI300走势、市值PE、申万PE、行业map、停牌表、交易日历。"""
    import fetcher

    tasks = [
        ("CSI300 走势",   fetcher.get_market_regime_data),
        ("市场估值 PE",   fetcher.get_market_valuation),
        ("申万一级 PE",   fetcher.get_sw_industry_pe),
        ("申万行业 map",  fetcher.get_sw_industry_map),
        ("交易日历",      fetcher.get_trade_calendar),
        ("今日停牌表",    fetcher.get_suspension_list),
    ]

    print(f"[prefetch/market] 开始预热市场数据  {_now_str()}", flush=True)
    for name, fn in tasks:
        try:
            result = fn()
            ok = result is not None and (
                (hasattr(result, "__len__") and len(result) > 0) or result is True
            )
            mark = "✓" if ok else "✗"
            print(f"  {mark}  {name}", flush=True)
        except Exception as e:
            print(f"  ✗  {name}: {e}", flush=True)
    print(f"[prefetch/market] 完成  {_now_str()}", flush=True)


# ── concept map prefetch ──────────────────────────────────────────────────────

def prefetch_concept() -> None:
    """预热概念板块反查 map（~30s 冷启动，缓存 6h）。"""
    import fetcher

    print(f"[prefetch/concept] 开始预热概念 map  {_now_str()}", flush=True)
    try:
        m = fetcher._build_concept_reverse_map()
        print(f"[prefetch/concept] 完成，共 {len(m)} 只股票有概念标签  {_now_str()}", flush=True)
    except Exception as e:
        print(f"[prefetch/concept] 失败: {e}", flush=True)


# ── entry ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="数据预热工具")
    parser.add_argument("--price",   action="store_true", help="预热全市场价格历史")
    parser.add_argument("--market",  action="store_true", help="预热市场整体数据")
    parser.add_argument("--concept", action="store_true", help="预热概念 map")
    parser.add_argument("--force",   action="store_true", help="跳过时间窗口检查")
    args = parser.parse_args()

    if not any([args.price, args.market, args.concept]):
        parser.print_help()
        sys.exit(0)

    if args.market:
        prefetch_market()
    if args.concept:
        prefetch_concept()
    if args.price:
        prefetch_price(force=args.force)


if __name__ == "__main__":
    main()
