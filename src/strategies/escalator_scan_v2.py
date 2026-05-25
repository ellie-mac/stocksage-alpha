#!/usr/bin/env python3
"""
扶梯策略扫描 V2 — 抓"活跃慢牛"形态，并尽量规避末端冲刺。
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

OUT_LATEST = ROOT / "data" / "escalator_v2_latest.json"

_TIER_ORDER = ["E1", "E2"]
_TIER_SPEC: dict[str, dict] = {
    # "E0": {"window": 30, "slope_lo": 10.0, "slope_hi": 30.0, "r2_min": 0.75},
    "E1": {"window": 20, "slope_lo": 5.0, "slope_hi": 25.0, "r2_min": 0.80},
    "E2": {"window": 10, "slope_lo": 3.0, "slope_hi": 15.0, "r2_min": 0.85},
}
_TIER_LABEL = {
    # "E0": "30 天慢牛 (10-30% / R²≥0.75)",
    "E1": "20 天慢牛 (5-25% / R²≥0.80)",
    "E2": "10 天慢牛 (3-15% / R²≥0.85)",
}
_TIER_CAP = {"E1": 9999, "E2": 9999}
_TIER_OUTPUT_CAP = 30

_MIN_BARS = 25
_AMP_MIN = 1.5
_AMP_MAX = 5.0
_DRAWDOWN_FLOOR = -5.0
_MAX_EXT_MA20 = 1.12
_MAX_EXT_MA10 = 1.08
_MAX_5D_RUNUP = 10.0

_TECH_KEYWORDS = (
    "半导体", "集成电路", "芯片",
    "软件", "计算机", "互联网", "信息",
    "通信",
    "元器件", "电子", "光电",
    "网络", "数据", "云", "操作系统",
    "智能", "人工智", "IT",
)


def _is_tech(industry: str) -> bool:
    if not industry:
        return False
    return any(kw in industry for kw in _TECH_KEYWORDS)


def _classify(closes: np.ndarray, highs: np.ndarray, lows: np.ndarray) -> Optional[dict]:
    if len(closes) < 20:
        return None

    amp_all = float(np.mean((highs - lows) / closes) * 100) if len(closes) > 0 else 0.0
    if amp_all < _AMP_MIN or amp_all > _AMP_MAX:
        return None

    if len(closes) >= 2:
        rets = np.diff(closes) / closes[:-1] * 100
        max_dd = float(np.min(rets))
    else:
        max_dd = 0.0
    if max_dd < _DRAWDOWN_FLOOR:
        return None

    c = float(closes[-1])
    ma10 = float(np.mean(closes[-10:]))
    ma20 = float(np.mean(closes[-20:]))
    if ma10 <= 0 or ma20 <= 0:
        return None
    if c / ma10 > _MAX_EXT_MA10 or c / ma20 > _MAX_EXT_MA20:
        return None

    if len(closes) >= 6:
        runup_5d = float((closes[-1] / closes[-6] - 1.0) * 100)
        if runup_5d > _MAX_5D_RUNUP:
            return None

    for tier in _TIER_ORDER:
        spec = _TIER_SPEC[tier]
        n = spec["window"]
        if len(closes) < n:
            continue

        win_c = closes[-n:]
        win_h = highs[-n:]
        win_l = lows[-n:]

        mean_c = float(np.mean(win_c))
        if mean_c <= 0:
            continue

        t = np.arange(n, dtype=float)
        slope, intercept = np.polyfit(t, win_c, 1)
        slope_pct = float(slope * (n - 1) / mean_c * 100)
        if not (spec["slope_lo"] <= slope_pct <= spec["slope_hi"]):
            continue

        fit = slope * t + intercept
        ss_res = float(np.sum((win_c - fit) ** 2))
        ss_tot = float(np.sum((win_c - mean_c) ** 2))
        if ss_tot <= 0:
            continue
        r2 = 1.0 - ss_res / ss_tot
        if r2 < spec["r2_min"]:
            continue

        amp_win = float(np.mean((win_h - win_l) / win_c) * 100)
        return {
            "tier": tier,
            "window": n,
            "slope_pct": round(slope_pct, 2),
            "r2": round(float(r2), 4),
            "daily_amp": round(amp_win, 2),
            "max_dd": round(max_dd, 2),
        }
    return None


def _ma_stack_ok(closes: np.ndarray) -> bool:
    if len(closes) < 20:
        return False
    c = float(closes[-1])
    ma5 = float(np.mean(closes[-5:]))
    ma10 = float(np.mean(closes[-10:]))
    ma20 = float(np.mean(closes[-20:]))
    return c > ma5 > ma10 > ma20


def _push_results(data: dict) -> None:
    from common import push_wechat

    date = data.get("date", "?")
    tiers = data.get("tiers", {})
    total = sum(len(v) for v in tiers.values())
    date_s = f"{date[4:6]}/{date[6:]}" if len(date) == 8 else date
    title = f"[扶梯V2] 📈 {date_s}  {total}只"

    if total == 0:
        push_wechat(title, "今日无慢牛信号<br>")
        print("[escalator_v2] 微信推送完成（无信号）", flush=True)
        return

    sections: list[str] = []
    for tier in _TIER_ORDER:
        picks = tiers.get(tier, [])
        if not picks:
            continue
        shown = picks[:_TIER_CAP.get(tier, 9999)]
        lines = []
        for p in shown:
            close = p.get("close", 0) or 0
            sp = p.get("slope_pct") or 0
            r2 = p.get("r2") or 0
            amp = p.get("daily_amp") or 0
            amt = p.get("amt_5d_yi") or 0
            ind = p.get("industry", "")
            lines.append(
                f"**{p['code']} {p['name']}** ({ind}) ¥{close:.2f}  斜率{sp:+.1f}% / R²{r2:.2f} / 日振幅{amp:.1f}% / 额{amt:.1f}亿<br>"
            )
        section = f"**【{tier} {_TIER_LABEL[tier]}】{len(picks)}只**<br>" + "".join(lines)
        sections.append(section)

    legend = (
        "扶梯V2：E0 30d / E1 20d / E2 10d（窗口越长越强）<br>"
        "判定：线性回归涨幅区间 + R²达标 + 日振幅1.5%-5.0% + 无大跌(≥-5%) + MA多头<br>"
        "新增：MA10/MA20乖离上限 + 最近5日涨幅不过热 + 质量过滤 + 行业黑名单<br><br>"
    )
    body = legend + "".join(sections)
    push_wechat(title, body)
    print("[escalator_v2] 微信推送完成", flush=True)


def run_scan(push: bool = False, dry_run: bool = False, tech_only: bool = False) -> dict:
    import fetcher as _fetcher
    try:
        from jobs.prefetch import wait_for_fresh_prices
        wait_for_fresh_prices()
    except Exception:
        pass

    from strategies._quality import (
        load_name_industry_map, load_universe,
        compute_metrics, passes_quality, is_blacklisted, load_quality_cache,
    )

    name_map, ind_map = load_name_industry_map()
    print(f"[escalator_v2] 名称缓存 {len(name_map)} 条", flush=True)
    universe = load_universe()
    date = datetime.now().strftime("%Y%m%d")

    if tech_only:
        universe = [c for c in universe if _is_tech(ind_map.get(c[-6:], ""))]

    qcache = load_quality_cache()

    def worker(code: str) -> Optional[dict]:
        code6 = code[-6:]
        name = name_map.get(code6, code6)
        ind = ind_map.get(code6, "")
        if is_blacklisted(ind):
            return None
        try:
            df = _fetcher.get_price_history(code, days=_MIN_BARS)
        except Exception:
            return None
        if df is None or len(df) < 20:
            return None
        try:
            closes = df["close"].astype(float).to_numpy()
            highs = df["high"].astype(float).to_numpy()
            lows = df["low"].astype(float).to_numpy()
        except Exception:
            return None
        if not _ma_stack_ok(closes):
            return None
        cls = _classify(closes, highs, lows)
        if not cls:
            return None
        metrics = qcache.get(code6) or compute_metrics(df)
        if not passes_quality(metrics):
            return None
        return {
            "code": code6,
            "name": name,
            "industry": ind,
            "close": float(closes[-1]),
            "amt_5d_yi": metrics.get("amt_5d_yi", 0.0),
            "vol_ratio": metrics.get("vol_ratio", 0.0),
            **cls,
        }

    picks_by_tier = {tier: [] for tier in _TIER_ORDER}
    workers = 12 if len(universe) < 1500 else 16
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(worker, code): code for code in universe}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="escalator_v2"):
            p = fut.result()
            if not p:
                continue
            picks_by_tier[p["tier"]].append(p)

    for tier in _TIER_ORDER:
        picks_by_tier[tier].sort(key=lambda x: (x.get("r2", 0), x.get("slope_pct", 0), x.get("amt_5d_yi", 0)), reverse=True)
        picks_by_tier[tier] = picks_by_tier[tier][:_TIER_OUTPUT_CAP]

    data = {
        "date": date,
        "strategy": "escalator_v2",
        "tiers": picks_by_tier,
    }
    if not dry_run:
        OUT_LATEST.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    if push:
        _push_results(data)
    return data


def main() -> None:
    ap = argparse.ArgumentParser(description="Escalator V2 / 慢牛扫描")
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--tech-only", action="store_true")
    args = ap.parse_args()
    run_scan(push=args.push, dry_run=args.dry_run, tech_only=args.tech_only)


if __name__ == "__main__":
    main()
