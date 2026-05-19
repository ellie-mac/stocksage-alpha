#!/usr/bin/env python3
"""
横盘策略扫描 — 滑动窗口振幅判定

档位（窗口长度 × 严格/宽松）：
  HX0 30天严格 · HX1 20天严格 · HX2 10天严格 · HX3 5天严格
  HS0 30天宽松 · HS1 20天宽松 · HS2 10天宽松 · HS3 5天宽松

严格 (HX)：窗口内 max/mid ≤ +5% 且 min/mid ≥ -5%（mid = (max+min)/2，全程稳定）
宽松 (HS)：窗口首尾两点 |chg_pct| ≤ 5%（仅首尾偶合，可能有中段大波动）

归属规则：取最强档（窗口越长越强；同窗口严格 > 宽松）。一只股只归一档。

用法：
    python -X utf8 src/strategies/sideways_scan.py            # 不推送
    python -X utf8 src/strategies/sideways_scan.py --dry-run  # 打印不落盘
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
from tqdm import tqdm

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

UNIVERSE_PATH = ROOT / "data" / "universe_main.json"
OUT_LATEST    = ROOT / "data" / "sideways_latest.json"

# 档位优先级：窗口越长越强，同窗口严格优先
_TIER_ORDER = ["HX0", "HS0", "HX1", "HS1", "HX2", "HS2", "HX3", "HS3"]
_TIER_SPEC: dict[str, tuple[int, str]] = {
    "HX0": (30, "strict"), "HS0": (30, "loose"),
    "HX1": (20, "strict"), "HS1": (20, "loose"),
    "HX2": (10, "strict"), "HS2": (10, "loose"),
    "HX3": (5,  "strict"), "HS3": (5,  "loose"),
}
_PCT = 0.05      # ±5%
_MIN_BARS = 35


def _load_universe() -> list[str]:
    raw = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    return raw if isinstance(raw, list) else list(raw.keys())


def _build_name_maps() -> tuple[dict[str, str], dict[str, str]]:
    names_file = ROOT / "data" / "stock_names.json"
    names: dict[str, str] = {}
    inds:  dict[str, str] = {}
    try:
        raw = json.loads(names_file.read_text(encoding="utf-8"))
        for ts_code, info in raw.items():
            code6 = ts_code.split(".")[0]
            names[code6] = info.get("name", code6) if isinstance(info, dict) else str(info)
            inds[code6]  = info.get("industry", "")  if isinstance(info, dict) else ""
        print(f"[sideways] 名称缓存 {len(names)} 条", flush=True)
    except Exception as e:
        print(f"[sideways] 名称加载失败: {e}", flush=True)
    return names, inds


def _classify(closes: np.ndarray) -> Optional[dict]:
    """Return metrics for the strongest tier this series qualifies for, else None."""
    if len(closes) < 5:
        return None
    for tier in _TIER_ORDER:
        n, mode = _TIER_SPEC[tier]
        if len(closes) < n:
            continue
        window = closes[-n:]
        hi = float(np.max(window))
        lo = float(np.min(window))
        if lo <= 0:
            continue
        mid = (hi + lo) / 2.0
        if mid <= 0:
            continue
        range_pct = (hi - lo) / mid * 100
        if mode == "strict":
            ok = (hi / mid - 1) <= _PCT and (1 - lo / mid) <= _PCT
        else:
            first = float(window[0])
            last  = float(window[-1])
            ok = first > 0 and abs(last / first - 1) <= _PCT
        if ok:
            return {"tier": tier, "window": n, "mode": mode,
                    "range_pct": round(range_pct, 2),
                    "hi": round(hi, 2), "lo": round(lo, 2)}
    return None


def run_scan(dry_run: bool = False) -> dict:
    import fetcher as _fetcher
    try:
        from jobs.prefetch import wait_for_fresh_prices
        wait_for_fresh_prices()
    except Exception:
        pass

    universe = _load_universe()
    name_map, ind_map = _build_name_maps()
    date = datetime.now().strftime("%Y%m%d")

    def _fetch_and_classify(code: str) -> Optional[dict]:
        try:
            df = _fetcher.get_price_history(code, days=_MIN_BARS + 5)
            if df is None or len(df) < 5:
                return None
            code6 = code[-6:]
            name = name_map.get(code6, code6)
            if "ST" in name.upper():
                return None
            closes = df["close"].values
            close = float(closes[-1])
            if not (3.0 <= close <= 500.0):
                return None
            if len(df) >= 2:
                prev = float(closes[-2])
                if prev > 0 and abs(close - prev) / prev * 100 >= 9.5:
                    return None
            if "high" in df.columns and "low" in df.columns and \
                    float(df["high"].iloc[-1]) == float(df["low"].iloc[-1]):
                return None
            metrics = _classify(closes)
            if not metrics:
                return None
            avg_vol_5d = float(df["volume"].tail(5).mean()) if "volume" in df.columns else 0.0
            avg_amt_5d_yi = avg_vol_5d * close * 100 / 1e8   # 手→股 ×100，元→亿 /1e8
            return {
                "code":          code6,
                "name":          name,
                "industry":      ind_map.get(code6, ""),
                "close":         round(close, 2),
                "tier":          metrics["tier"],
                "window":        metrics["window"],
                "mode":          metrics["mode"],
                "range_pct":     metrics["range_pct"],
                "hi":            metrics["hi"],
                "lo":            metrics["lo"],
                "avg_amt_5d_yi": round(avg_amt_5d_yi, 2),
            }
        except Exception:
            return None

    print(f"[sideways] 扫描 {len(universe)} 只股票...", flush=True)
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_and_classify, c): c for c in universe}
        for fut in tqdm(as_completed(futs), total=len(futs)):
            res = fut.result()
            if res:
                results.append(res)

    results.sort(key=lambda x: (_TIER_ORDER.index(x["tier"]), x["range_pct"], x["code"]))

    tiers: dict[str, list] = {t: [] for t in _TIER_ORDER}
    for r in results:
        tiers[r["tier"]].append(r)

    counts = " ".join(f"{t}={len(tiers[t])}" for t in _TIER_ORDER)
    print(f"[sideways] 共 {len(results)} 只：{counts}", flush=True)

    output = {"date": date, "tiers": tiers, "all_picks": results}

    if not dry_run:
        OUT_LATEST.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        dated = ROOT / "data" / f"sideways_{date}.json"
        dated.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[sideways] 已保存 → sideways_latest.json")

    try:
        import event_log as _elog
        _rows = [{"date": date, "strategy": "sideways", "code": r["code"],
                  "signal_type": "sideways_scan",
                  "price": r.get("close"),
                  "score": -r.get("range_pct", 100.0),
                  "details": {"name": r.get("name"), "tier": r.get("tier"),
                              "window": r.get("window"), "mode": r.get("mode"),
                              "range_pct": r.get("range_pct"),
                              "avg_amt_5d_yi": r.get("avg_amt_5d_yi"),
                              "industry": r.get("industry", "")}}
                 for r in results]
        if _rows:
            _elog.log_events(_rows)
    except Exception:
        pass

    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="打印不落盘")
    args = parser.parse_args()
    run_scan(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
