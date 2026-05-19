#!/usr/bin/env python3
"""
全市场质量指标预热 — price_Prefetch 17:00 后跑，落盘 data/quality_metrics_latest.json。

落盘字段 {code6: {amt_5d_yi, vol_ratio, is_limit_today, is_yi_zi, close}}。

下游策略（gc / sideways / hot / marketcap / chip / evening_strategy）直接读这个
缓存避免重复 compute_metrics —— price cache 命中后单只 ~5ms × 5000 票 = 25s，
但每个 scanner 都做一遍就是 5×25s = 125s 重复 CPU。预热到位后压到 0。

用法：
    python -X utf8 src/jobs/prefetch_quality.py [--workers N]
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

OUT_LATEST = ROOT / "data" / "quality_metrics_latest.json"
_MIN_COVERAGE = 0.70   # 至少 70% universe 算出 metrics 才覆盖 latest（否则保留旧文件）


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=10,
                        help="并发线程数（fetcher 缓存命中时基本无 IO 等待）")
    parser.add_argument("--days", type=int, default=65,
                        help="拉历史天数（用于算 60 日均量；默认 65 = 60+5 buffer）")
    args = parser.parse_args()

    from strategies._quality import compute_metrics, load_universe
    import fetcher as _f

    universe = load_universe(drop_bj=True, drop_st=True)
    print(f"[prefetch_quality] universe {len(universe)} 只（已剔北证+ST）", flush=True)

    def _calc(code: str) -> tuple[str, dict | None]:
        code6 = code[-6:]
        try:
            df = _f.get_price_history(code6, days=args.days)
            if df is None or len(df) < 5:
                return code6, None
            m = compute_metrics(df, code6)
            if not m:
                return code6, None
            # 把当日 close 一起存进缓存，scanner 读取时无需再 fetch
            m["close"] = round(float(df["close"].iloc[-1]), 2)
            return code6, m
        except Exception:
            return code6, None

    metrics_out: dict[str, dict] = {}
    fail = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(_calc, c): c for c in universe}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="quality"):
            code6, m = fut.result()
            if m:
                metrics_out[code6] = m
            else:
                fail += 1

    # Coverage 守门：如果 price cache 未热（fetcher 大量 miss），新计算的 metrics
    # 覆盖率会很低 — 此时不覆盖已有 latest 文件，保护 good cache
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
    print(f"[prefetch_quality] {len(metrics_out)} 只 OK / {fail} 只 fail "
          f"(覆盖率 {coverage*100:.1f}%) → quality_metrics_latest.json", flush=True)


if __name__ == "__main__":
    main()
