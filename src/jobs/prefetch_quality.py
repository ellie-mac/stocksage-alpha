#!/usr/bin/env python3
"""
全市场质量指标预热 — price_Prefetch 17:00 后跑，落盘 data/quality_metrics_latest.json。

落盘字段 {code6: {amt_5d_yi, vol_ratio, is_limit_today, is_yi_zi, close}}。

下游策略（gc / sideways / hot / marketcap / chip / evening_strategy）直接读这个
缓存避免重复 compute_metrics —— price cache 命中后单只 ~5ms × 5000 票 = 25s，
但每个 scanner 都做一遍就是 5×25s = 125s 重复 CPU。预热到位后压到 0。

执行策略（fast path + fallback）：
  1. 优先用 Tushare pro.daily(trade_date=...) 批量按日期拉全市场（N 次 API call
     vs 逐票 5000×6 路回退 30000 次），需要 5000+ 积分。
  2. batch 失败或覆盖率 <70% 时 fallback 到 fetcher.get_price_history 逐票方式。

用法：
    python -X utf8 src/jobs/prefetch_quality.py [--workers N] [--no-batch]
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as _date, datetime, timedelta as _td
from pathlib import Path

from tqdm import tqdm

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

OUT_LATEST = ROOT / "data" / "quality_metrics_latest.json"
_MIN_COVERAGE = 0.70   # 至少 70% universe 算出 metrics 才覆盖 latest（否则保留旧文件）


def _try_batch_via_tushare(universe: list[str], days: int) -> dict[str, dict]:
    """Fast path: Tushare pro.daily(trade_date=...) 批量按日期拉全市场。

    Returns {code6: metrics_dict} on success, {} on any failure (caller falls back).
    """
    try:
        import fetcher as _f
        import pandas as pd
        from strategies._quality import compute_metrics

        pro = _f._get_tushare_pro()
        if pro is None:
            print("[prefetch_quality/batch] tushare 未配置，跳过 batch", flush=True)
            return {}

        # 收集最近 days 个交易日（多预留 buffer 给周末/节假日）
        trade_dates: list[str] = []
        d = _date.today()
        max_calendar = int(days * 1.5) + 5
        for _ in range(max_calendar):
            if d.weekday() < 5:
                trade_dates.append(d.strftime("%Y%m%d"))
            d -= _td(days=1)
        trade_dates = list(reversed(trade_dates))  # 最早到最新

        codes_set = {c[-6:] for c in universe}
        per_code: dict[str, list[tuple]] = {}

        fetched = 0
        for td in tqdm(trade_dates, desc="batch tushare", total=len(trade_dates)):
            try:
                df = pro.daily(
                    trade_date=td,
                    fields="ts_code,trade_date,open,high,low,close,vol,pct_chg",
                )
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    code6 = str(row["ts_code"]).split(".")[0]
                    if code6 not in codes_set:
                        continue
                    per_code.setdefault(code6, []).append((
                        str(row["trade_date"]),
                        float(row["close"]),
                        float(row["vol"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["pct_chg"]) if row.get("pct_chg") is not None else 0.0,
                    ))
                fetched += 1
                if fetched >= days:
                    # 已经够 days 个有效交易日
                    break
            except Exception as e:
                print(f"[prefetch_quality/batch] {td} failed: {e}", flush=True)
                continue

        if not per_code:
            return {}

        # 转 DataFrame 调 compute_metrics
        metrics_out: dict[str, dict] = {}
        for code6, rows in per_code.items():
            if len(rows) < 5:
                continue
            df_pc = pd.DataFrame(
                rows, columns=["date", "close", "volume", "high", "low", "change_pct"]
            ).sort_values("date").reset_index(drop=True)
            m = compute_metrics(df_pc, code6)
            if not m:
                continue
            m["close"] = round(float(df_pc["close"].iloc[-1]), 2)
            metrics_out[code6] = m

        cov = len(metrics_out) / max(len(universe), 1)
        print(f"[prefetch_quality/batch] {len(metrics_out)} / {len(universe)} "
              f"(coverage {cov*100:.1f}%) via {fetched} trade_date API calls",
              flush=True)
        return metrics_out
    except Exception as e:
        print(f"[prefetch_quality/batch] exception, falling back: {e}", flush=True)
        return {}


def _per_code_enrich(universe: list[str], days: int, workers: int) -> dict[str, dict]:
    """Fallback: 逐票 fetcher.get_price_history → compute_metrics（命中 fetcher cache 时快）。"""
    import fetcher as _f
    from strategies._quality import compute_metrics

    def _calc(code: str) -> tuple[str, dict | None]:
        code6 = code[-6:]
        try:
            df = _f.get_price_history(code6, days=days)
            if df is None or len(df) < 5:
                return code6, None
            m = compute_metrics(df, code6)
            if not m:
                return code6, None
            m["close"] = round(float(df["close"].iloc[-1]), 2)
            return code6, m
        except Exception:
            return code6, None

    metrics_out: dict[str, dict] = {}
    fail = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_calc, c): c for c in universe}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="per-code"):
            code6, m = fut.result()
            if m:
                metrics_out[code6] = m
            else:
                fail += 1
    print(f"[prefetch_quality/per-code] {len(metrics_out)} OK / {fail} fail",
          flush=True)
    return metrics_out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=10,
                        help="per-code fallback 并发线程数")
    parser.add_argument("--days", type=int, default=65,
                        help="拉历史天数（用于算 60 日均量；默认 65 = 60+5 buffer）")
    parser.add_argument("--no-batch", action="store_true",
                        help="跳过 Tushare batch 路径，直接走 per-code")
    args = parser.parse_args()

    from strategies._quality import load_universe

    universe = load_universe(drop_bj=True, drop_st=True)
    print(f"[prefetch_quality] universe {len(universe)} 只（已剔北证+ST）", flush=True)

    # Fast path: Tushare batch
    metrics_out: dict[str, dict] = {}
    if not args.no_batch:
        metrics_out = _try_batch_via_tushare(universe, args.days)
        cov = len(metrics_out) / max(len(universe), 1)
        if 0 < cov < _MIN_COVERAGE:
            print(f"[prefetch_quality] batch coverage {cov*100:.1f}% < "
                  f"{_MIN_COVERAGE*100:.0f}%，fallback per-code", flush=True)
            metrics_out = {}

    # Fallback: 逐票
    if not metrics_out:
        metrics_out = _per_code_enrich(universe, args.days, args.workers)

    # Coverage 守门：低于阈值不覆盖 latest 保护已有 cache
    coverage = len(metrics_out) / max(len(universe), 1)
    if coverage < _MIN_COVERAGE:
        print(f"[prefetch_quality] 覆盖率 {len(metrics_out)}/{len(universe)} = "
              f"{coverage*100:.1f}% < {_MIN_COVERAGE*100:.0f}%，跳过写入 "
              f"(保留旧 latest)", flush=True)
        return

    output = {
        "date": datetime.now().strftime("%Y%m%d"),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "coverage": round(coverage, 3),
        "metrics": metrics_out,
    }
    tmp = OUT_LATEST.with_suffix(".tmp")
    tmp.write_text(json.dumps(output, ensure_ascii=False), encoding="utf-8")
    tmp.replace(OUT_LATEST)
    print(f"[prefetch_quality] {len(metrics_out)} 只 (覆盖率 {coverage*100:.1f}%) "
          f"→ quality_metrics_latest.json", flush=True)


if __name__ == "__main__":
    main()
