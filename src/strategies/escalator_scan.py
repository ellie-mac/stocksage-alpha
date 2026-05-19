#!/usr/bin/env python3
"""
扶梯策略扫描 — 抓"活跃慢牛"形态

形态：每天都活跃（日内有起伏），整体稳步上行（贴近一条上升直线）。
与横盘的区别：横盘要求 range 窄（无方向），扶梯要求**有上行斜率**。
与金叉的区别：金叉是"刚拐点"，扶梯是"已在涨且稳"的中段。
与热榜的区别：热榜是热度榜，扶梯是技术形态（斜率+拟合度）。

档位（窗口长度，越长越强）：
  E0  30 天   累计 10-30%   R² ≥ 0.75
  E1  20 天   累计  5-25%   R² ≥ 0.70
  E2  10 天   累计  3-15%   R² ≥ 0.65

共同硬条件（所有档）：
  · 日均振幅 (high-lo)/close ≥ 2%        ← 活跃
  · 窗口内最大单日跌幅 ≥ -5%              ← 没塌过
  · close > MA5 > MA10 > MA20            ← 多头排列
  · 5 日均成交额 ≥ 0.5 亿 + 量比 ≥ 0.5    ← 不是死水
  · 行业不在黑名单

归属：取最强档（窗口越长越强）；一只票一档。

用法：
    python -X utf8 src/strategies/escalator_scan.py             # 全市场
    python -X utf8 src/strategies/escalator_scan.py --push      # + 推微信
    python -X utf8 src/strategies/escalator_scan.py --tech-only # 仅 TMT
    python -X utf8 src/strategies/escalator_scan.py --dry-run   # 打印不落盘
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

OUT_LATEST = ROOT / "data" / "escalator_latest.json"

# 档位：窗口越长越强（同 sideways 风格）
_TIER_ORDER = ["E0", "E1", "E2"]
_TIER_SPEC: dict[str, dict] = {
    "E0": {"window": 30, "slope_lo": 10.0, "slope_hi": 30.0, "r2_min": 0.75},
    "E1": {"window": 20, "slope_lo":  5.0, "slope_hi": 25.0, "r2_min": 0.70},
    "E2": {"window": 10, "slope_lo":  3.0, "slope_hi": 15.0, "r2_min": 0.65},
}
_TIER_LABEL = {
    "E0": "30 天慢牛 (10-30% / R²≥0.75)",
    "E1": "20 天慢牛 (5-25% / R²≥0.70)",
    "E2": "10 天慢牛 (3-15% / R²≥0.65)",
}
_TIER_CAP = {"E0": 10, "E1": 10, "E2": 8}      # 推送展示上限
_TIER_OUTPUT_CAP = 30                          # 写盘每档上限（按 R² 降序）

_MIN_BARS    = 35     # 拉 35 天满足最长 30d 窗口 + buffer
_AMP_MIN     = 2.0    # 日均振幅下限 (%)
_DRAWDOWN_FLOOR = -5.0  # 最大单日跌幅下限 (%)


# 科技 (TMT) 行业关键词 — 与 sideways_scan / evening_strategy 保持同步
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
    """对一只股的最近 N 天 K 线，判定最强档（E0 > E1 > E2），不入选返回 None。

    所有档共同硬条件：日均振幅 ≥ 2%、最大单日跌幅 ≥ -5%。
    各档独有：window/slope/R² 边界。
    """
    if len(closes) < 5:
        return None

    # 共同硬条件 1: 日均振幅
    amp_all = float(np.mean((highs - lows) / closes) * 100) if len(closes) > 0 else 0.0

    # 共同硬条件 2: 单日跌幅
    if len(closes) >= 2:
        rets = np.diff(closes) / closes[:-1] * 100
        max_dd = float(np.min(rets))
    else:
        max_dd = 0.0

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

        # 线性回归: close = slope * t + intercept
        t = np.arange(n, dtype=float)
        slope, intercept = np.polyfit(t, win_c, 1)
        slope_pct = float(slope * (n - 1) / mean_c * 100)  # 期间累计涨幅近似
        if not (spec["slope_lo"] <= slope_pct <= spec["slope_hi"]):
            continue

        # R²
        fit = slope * t + intercept
        ss_res = float(np.sum((win_c - fit) ** 2))
        ss_tot = float(np.sum((win_c - mean_c) ** 2))
        if ss_tot <= 0:
            continue
        r2 = 1.0 - ss_res / ss_tot
        if r2 < spec["r2_min"]:
            continue

        # 日均振幅（窗口内）
        amp_win = float(np.mean((win_h - win_l) / win_c) * 100)
        if amp_win < _AMP_MIN:
            continue

        # 最大单日跌幅（窗口内）
        if n >= 2:
            rets_win = np.diff(win_c) / win_c[:-1] * 100
            dd_win = float(np.min(rets_win))
        else:
            dd_win = 0.0
        if dd_win < _DRAWDOWN_FLOOR:
            continue

        return {
            "tier": tier,
            "window": n,
            "slope_pct": round(slope_pct, 2),
            "r2": round(r2, 3),
            "daily_amp": round(amp_win, 2),
            "max_drawdown": round(dd_win, 2),
            "hi": round(float(np.max(win_c)), 2),
            "lo": round(float(np.min(win_c)), 2),
        }
    return None


def _ma_bullish(closes: np.ndarray) -> bool:
    """close > MA5 > MA10 > MA20 多头排列。"""
    if len(closes) < 20:
        return False
    c = float(closes[-1])
    ma5  = float(np.mean(closes[-5:]))
    ma10 = float(np.mean(closes[-10:]))
    ma20 = float(np.mean(closes[-20:]))
    return c > ma5 > ma10 > ma20


def _push_results(data: dict) -> None:
    from common import push_wechat

    date  = data.get("date", "?")
    tiers = data.get("tiers", {})
    total = sum(len(v) for v in tiers.values())
    date_s = f"{date[4:6]}/{date[6:]}" if len(date) == 8 else date
    title = f"[扶梯] 📈 {date_s}  {total}只"

    if total == 0:
        push_wechat(title, "今日无慢牛信号")
        print("[escalator] 微信推送完成（无信号）", flush=True)
        return

    sections: list[str] = []
    for tier in _TIER_ORDER:
        picks = tiers.get(tier, [])
        if not picks:
            continue
        cap = _TIER_CAP.get(tier, 5)
        shown = picks[:cap]
        omitted = len(picks) - len(shown)
        lines = []
        for p in shown:
            close = p.get("close", 0) or 0
            sp = p.get("slope_pct") or 0
            r2 = p.get("r2") or 0
            amp = p.get("daily_amp") or 0
            amt = p.get("amt_5d_yi") or 0
            ind = p.get("industry", "")
            lines.append(
                f"**{p['code']} {p['name']}** ({ind}) ¥{close:.2f}  "
                f"斜率{sp:+.1f}% / R²{r2:.2f} / 日振幅{amp:.1f}% / 额{amt:.1f}亿"
            )
        section = f"**【{tier} {_TIER_LABEL[tier]}】{len(picks)}只**  \n" + "\n".join(lines)
        if omitted > 0:
            section += f"\n_...还有{omitted}只_"
        sections.append(section)

    legend = (
        "```\n"
        "档位：E0 30d / E1 20d / E2 10d（窗口越长越强）\n"
        "判定：线性回归累计涨幅在区间 + R² ≥ 阈值 + 日振幅≥2% + 无大跌(≥-5%) + MA多头\n"
        "已过滤：死水股(5日均额≥0.5亿+量比≥0.5) + 行业黑名单\n"
        "```"
    )
    body = legend + "\n\n" + "\n\n".join(sections)
    push_wechat(title, body)
    print("[escalator] 微信推送完成", flush=True)


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
    print(f"[escalator] 名称缓存 {len(name_map)} 条", flush=True)
    universe = load_universe()
    date = datetime.now().strftime("%Y%m%d")

    if tech_only:
        before = len(universe)
        universe = [c for c in universe if _is_tech(ind_map.get(c[-6:], ""))]
        print(f"[escalator] 科技行业过滤: {before} → {len(universe)} 只", flush=True)

    quality_cache = load_quality_cache()
    if quality_cache:
        print(f"[escalator] 命中 quality cache（{len(quality_cache)} 只）", flush=True)

    def _fetch_and_classify(code: str) -> Optional[dict]:
        try:
            code6 = code[-6:]
            if is_blacklisted(ind_map.get(code6, "")):
                return None
            cached_m = quality_cache.get(code6)
            if cached_m is not None and not passes_quality(cached_m):
                return None
            df = _fetcher.get_price_history(code, days=_MIN_BARS + 5)
            if df is None or len(df) < 20:
                return None
            name = name_map.get(code6, code6)
            if "ST" in name.upper() or "退" in name:
                return None

            closes = df["close"].astype(float).values
            highs  = df["high"].astype(float).values if "high" in df.columns else closes
            lows   = df["low"].astype(float).values if "low" in df.columns else closes

            close_now = float(closes[-1])
            if not (3.0 <= close_now <= 500.0):
                return None

            # MA 多头预筛 — 不符合直接淘汰，省 _classify
            if not _ma_bullish(closes):
                return None

            metrics = cached_m if cached_m is not None else compute_metrics(df, code6)
            if not passes_quality(metrics):
                return None

            res = _classify(closes, highs, lows)
            if not res:
                return None

            return {
                "code":         code6,
                "name":         name,
                "industry":     ind_map.get(code6, ""),
                "close":        round(close_now, 2),
                "tier":         res["tier"],
                "window":       res["window"],
                "slope_pct":    res["slope_pct"],
                "r2":           res["r2"],
                "daily_amp":    res["daily_amp"],
                "max_drawdown": res["max_drawdown"],
                "hi":           res["hi"],
                "lo":           res["lo"],
                "amt_5d_yi":    metrics["amt_5d_yi"],
                "vol_ratio":    metrics["vol_ratio"],
            }
        except Exception:
            return None

    print(f"[escalator] 扫描 {len(universe)} 只股票...", flush=True)
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_and_classify, c): c for c in universe}
        for fut in tqdm(as_completed(futs), total=len(futs)):
            r = fut.result()
            if r:
                results.append(r)

    # 按 tier 优先级 + R² 降序排序
    results.sort(key=lambda x: (_TIER_ORDER.index(x["tier"]), -x["r2"], x["code"]))

    tiers_full: dict[str, list] = {t: [] for t in _TIER_ORDER}
    for r in results:
        tiers_full[r["tier"]].append(r)

    # 每 tier 按 R² 降序 cap _TIER_OUTPUT_CAP 只（贴线最直的最优先）
    tiers: dict[str, list] = {}
    for t in _TIER_ORDER:
        capped = sorted(tiers_full[t], key=lambda x: -(x.get("r2") or 0))[:_TIER_OUTPUT_CAP]
        tiers[t] = capped
    capped_results = [r for t in _TIER_ORDER for r in tiers[t]]

    raw_counts = " ".join(f"{t}={len(tiers_full[t])}" for t in _TIER_ORDER)
    cap_counts = " ".join(f"{t}={len(tiers[t])}" for t in _TIER_ORDER)
    print(f"[escalator] 原始 {len(results)} 只: {raw_counts}", flush=True)
    print(f"[escalator] cap 后 {len(capped_results)} 只 (R² top {_TIER_OUTPUT_CAP}/tier): {cap_counts}", flush=True)

    output = {"date": date, "tiers": tiers, "all_picks": capped_results}

    if not dry_run:
        OUT_LATEST.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        dated = ROOT / "data" / f"escalator_{date}.json"
        dated.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[escalator] 已保存 → escalator_latest.json", flush=True)

    if push and not dry_run:
        try:
            _push_results(output)
        except Exception as e:
            print(f"[escalator] 微信推送失败: {e}", flush=True)

    try:
        import event_log as _elog
        rows = [{"date": date, "strategy": "escalator", "code": r["code"],
                 "signal_type": "escalator_scan",
                 "price": r.get("close"),
                 "score": r.get("r2"),
                 "details": {"name": r.get("name"), "tier": r.get("tier"),
                             "window": r.get("window"),
                             "slope_pct": r.get("slope_pct"),
                             "r2": r.get("r2"),
                             "daily_amp": r.get("daily_amp"),
                             "max_drawdown": r.get("max_drawdown"),
                             "amt_5d_yi": r.get("amt_5d_yi"),
                             "vol_ratio": r.get("vol_ratio"),
                             "industry": r.get("industry", "")}}
                for r in capped_results]
        if rows:
            _elog.log_events(rows)
    except Exception:
        pass

    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--push",      action="store_true", help="推送微信")
    parser.add_argument("--dry-run",   action="store_true", help="打印不落盘")
    parser.add_argument("--tech-only", action="store_true",
                        help="仅扫描科技 TMT")
    args = parser.parse_args()
    run_scan(push=args.push, dry_run=args.dry_run, tech_only=args.tech_only)


if __name__ == "__main__":
    main()
