#!/usr/bin/env python3
"""
扶梯策略扫描 — 抓"活跃慢牛"形态

形态：每天都活跃（日内有起伏），整体稳步上行（贴近一条上升直线）。
与横盘的区别：横盘要求 range 窄（无方向），扶梯要求**有上行斜率**。
与金叉的区别：金叉是"刚拐点"，扶梯是"已在涨且稳"的中段。
与热榜的区别：热榜是热度榜，扶梯是技术形态（斜率+拟合度）。
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

_TIER_ORDER = ["E0", "E1"]
_TIER_SPEC: dict[str, dict] = {
    # E0 R²_min 从 0.80 → 0.90 (回填 20 天数据：R² 0.90+ 桶 T+5 胜率 81%/+8.1%，
    # 远高于 0.80-0.90 桶的 60%/+2.4%。收紧 R² 把 E0 picks 从 ~295 砍到 ~57，
    # 但质量大幅提升)。
    "E0": {"window": 20, "slope_lo": 5.0, "slope_hi": 25.0, "r2_min": 0.90},
    # E1 slope_lo 3.0 → 5.0 跟 E0 对齐，过滤温吞涨。E1 单独胜率 54%/+1.4%，
    # 主要作为"共振检测器"价值（E0+E1 共振 = 75% win/+5.2%）。
    "E1": {"window": 10, "slope_lo": 5.0, "slope_hi": 15.0, "r2_min": 0.85},
    # E2 (5d/2-6%/R²≥0.92) 已删除——回填 20 天数据：T+5 win 43%/+0.3% 实质
    # 预测力为负；5 点拟合 R² 噪声大，独立使用得不偿失；共振贡献占比也很小。
}
_TIER_LABEL = {
    "E0": "20 天慢牛 (5-25% / R²≥0.90)",
    "E1": "10 天慢牛 (5-15% / R²≥0.85)",
}

_MIN_BARS = 25
_AMP_MIN = 2.0
_DRAWDOWN_FLOOR = -5.0
_MAX_5D_RUNUP = 8.0

_TECH_KEYWORDS = (
    "半导体", "集成电路", "芯片",
    "软件", "计算机", "互联网", "信息",
    "通信", "元器件", "电子", "光电",
    "网络", "数据", "云", "操作系统",
    "智能", "人工智", "IT",
)


def _is_tech(industry: str) -> bool:
    if not industry:
        return False
    return any(kw in industry for kw in _TECH_KEYWORDS)


def _classify(closes: np.ndarray, highs: np.ndarray, lows: np.ndarray) -> Optional[dict]:
    """检查所有 tier criteria，返回 primary tier（最长 window 优先）+ 全部 matched_tiers。

    primary 用于推送展示（每只票一档），matched_tiers 用于 perf_log 做 criterion-level
    分析（一只票若同时命中 E0/E1/E2，每个 criterion 都得分该票的 forward return）。
    """
    if len(closes) < 5:
        return None

    if len(closes) >= 2:
        rets = np.diff(closes) / closes[:-1] * 100
        max_dd = float(np.min(rets))
    else:
        max_dd = 0.0

    primary: Optional[dict] = None
    matched: list[str] = []

    for tier in _TIER_ORDER:   # 顺序：E0 (20d) → E1 (10d) → E2 (5d)，长窗口优先
        spec = _TIER_SPEC[tier]
        n = spec["window"]
        if len(closes) < n:
            continue

        c = closes[-n:]
        h = highs[-n:]
        l = lows[-n:]

        amp = float(np.mean((h - l) / c) * 100)
        if amp < _AMP_MIN:
            continue
        if max_dd < _DRAWDOWN_FLOOR:
            continue

        x = np.arange(n, dtype=float)
        y = c.astype(float)
        A = np.vstack([x, np.ones(n)]).T
        a, b = np.linalg.lstsq(A, y, rcond=None)[0]
        yhat = a * x + b
        ss_res = float(np.sum((y - yhat) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
        if yhat[0] <= 0:
            continue
        slope_pct = (yhat[-1] / yhat[0] - 1.0) * 100

        if not (spec["slope_lo"] <= slope_pct <= spec["slope_hi"]):
            continue
        if r2 < spec["r2_min"]:
            continue

        matched.append(tier)
        if primary is None:
            primary = {
                "tier": tier,
                "window": n,
                "slope_pct": round(float(slope_pct), 2),
                "r2": round(float(r2), 3),
                "daily_amp": round(float(amp), 2),
                "max_dd": round(float(max_dd), 2),
            }

    if primary is None:
        return None
    primary["matched_tiers"] = matched
    return primary


def _ma_bullish(closes: np.ndarray) -> bool:
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
        lines = []
        for p in picks:
            close = p.get("close", 0) or 0
            sp = p.get("slope_pct") or 0
            r2 = p.get("r2") or 0
            amp = p.get("daily_amp") or 0
            amt = p.get("amt_5d_yi") or 0
            ind = p.get("industry", "")
            matched = p.get("matched_tiers") or []
            badge = f"  [共振 {'·'.join(matched)}]" if len(matched) > 1 else ""
            lines.append(f"{p['code']} {p['name']} ({ind}){badge}<br>收盘 ¥{close:.2f}｜斜率 {sp:+.1f}%｜R² {r2:.2f}｜日振幅 {amp:.1f}%｜5日额 {amt:.1f}亿")
        section = f"【{tier} {_TIER_LABEL[tier]}】共{len(picks)}只<br>" + "<br>".join(lines)
        sections.append(section)

    legend = (
        "扶梯策略（v3）<br>"
        "档位：E0 20d (R²≥0.90) / E1 10d (R²≥0.85)<br>"
        "判定：线性回归斜率 5-25%（E0）/5-15%（E1）+ R² 阈值 + 日振幅≥2% + 无大跌(≥-5%) + MA多头<br>"
        "排序：共振（E0+E1）优先 → 斜率 → R² → 5日额；共振票打 [共振 E0·E1] 标签<br>"
        "新增防冲顶：5日涨幅≤8%<br>"
        "已过滤：死水股(5日均额≥0.5亿+量比≥0.5) + 行业黑名单"
    )
    body = legend + "<br>" + "<br>".join(sections)
    push_wechat(title, body)
    print("[escalator] 微信推送完成", flush=True)


def run_scan(push: bool = False, dry_run: bool = False, tech_only: bool = False,
             as_of_date: str = "") -> dict:
    """as_of_date: 'YYYYMMDD' 用于回填 — fetch 大窗口后 slice 到 ≤ as_of_date 模拟当日扫描。"""
    import fetcher as _fetcher
    try:
        from strategies._quality import load_name_industry_map, load_universe, compute_metrics, passes_quality, is_blacklisted
    except Exception:
        from _quality import load_name_industry_map, load_universe, compute_metrics, passes_quality, is_blacklisted

    name_map, ind_map = load_name_industry_map()
    universe = sorted(load_universe())
    rows: list[dict] = []

    def worker(code: str):
        code6 = code[-6:]
        try:
            # 回填模式拉 90 天给 slice 留 buffer；live 模式只拉 35 天足够
            fetch_days = 90 if as_of_date else max(_MIN_BARS, 35)
            df = _fetcher.get_price_history(code, days=fetch_days)
            if df is None or df.empty:
                return None
            if as_of_date:
                # df["date"] 是 datetime64，要转 Timestamp 再比较；之前 .astype(str)
                # 拿到的是 "2026-05-14" 带连字符，跟 "20260515" 做字符串比较结果错乱
                import pandas as _pd
                cutoff_ts = _pd.to_datetime(as_of_date, format="%Y%m%d")
                df = df[df["date"] <= cutoff_ts]
            if len(df) < _MIN_BARS:
                return None
            # 回填时只用最近 35 行（匹配 live 模式 fetch 窗口）
            if as_of_date:
                df = df.iloc[-max(_MIN_BARS, 35):]
            closes = df["close"].astype(float).to_numpy()
            highs = df["high"].astype(float).to_numpy()
            lows = df["low"].astype(float).to_numpy()
            vols = df.get("volume")
            amts = df.get("amount")
            if not _ma_bullish(closes):
                return None
            if len(closes) < 6:
                return None
            runup_5d = (float(closes[-1]) / float(closes[-6]) - 1.0) * 100
            if runup_5d > _MAX_5D_RUNUP:
                return None
            cls = _classify(closes, highs, lows)
            if not cls:
                return None
            hist_df = df.iloc[-35:].copy()
            metrics = compute_metrics(hist_df)
            if not passes_quality(metrics):
                return None
            industry = ind_map.get(code6, "")
            if is_blacklisted(industry):
                return None
            if tech_only and not _is_tech(industry):
                return None
            close = float(closes[-1])
            amt_5d_yi = float(metrics.get("amt_5d_yi", 0.0) or 0.0)
            return {
                "code": code6,
                "name": name_map.get(code6, code6),
                "industry": industry,
                "close": round(close, 2),
                "runup_5d": round(runup_5d, 2),
                "amt_5d_yi": round(amt_5d_yi, 2),
                **cls,
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(worker, code): code for code in universe}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="escalator"):
            row = fut.result()
            if row:
                rows.append(row)

    tiers_full = {t: [] for t in _TIER_ORDER}
    for r in rows:
        tiers_full[r["tier"]].append(r)
    for t in _TIER_ORDER:
        # 多档共振优先 → 斜率 → R² → 5日额；同档内高确定性票浮顶
        tiers_full[t].sort(key=lambda x: (
            -len(x.get("matched_tiers") or []),
            -x["slope_pct"],
            -x["r2"],
            -x["amt_5d_yi"],
        ))

    tiers = {t: list(tiers_full[t]) for t in _TIER_ORDER}
    date_str = as_of_date or datetime.now().strftime("%Y%m%d")
    output = {
        "date": date_str,
        "strategy": "escalator",
        "version": "强化版v1+5日涨幅8%过滤",
        "tiers": tiers,
        "raw_tiers": tiers_full,
        "counts": {t: len(v) for t, v in tiers.items()},
        "raw_counts": {t: len(v) for t, v in tiers_full.items()},
    }
    if not dry_run:
        # 回填模式只写 dated 文件，避免覆盖当日 latest；live 模式两个都写
        dated_path = ROOT / "data" / f"escalator_{date_str}.json"
        dated_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        if not as_of_date:
            OUT_LATEST.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    raw_counts = " ".join(f"{t}={len(tiers_full[t])}" for t in _TIER_ORDER)
    cap_counts = " ".join(f"{t}={len(tiers[t])}" for t in _TIER_ORDER)
    print(f"[escalator] raw counts: {raw_counts}")
    print(f"[escalator] output counts: {cap_counts}")
    if push and not dry_run:
        try:
            _push_results(output)
        except Exception as e:
            print(f"[WARN] escalator push failed: {e}", flush=True)
    return output




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="扶梯策略扫描")
    parser.add_argument("--push", action="store_true", help="推送微信")
    parser.add_argument("--dry-run", action="store_true", help="仅打印，不落盘")
    parser.add_argument("--tech-only", action="store_true", help="仅保留 TMT 行业")
    parser.add_argument("--date", type=str, default="", help="回填模式 YYYYMMDD（按该日 as-of 扫描，结果存 escalator_<date>.json，不覆盖 latest）")
    parser.add_argument("--backfill", type=int, default=0, help="回填最近 N 个交易日，跑完退出（不推送）")
    args = parser.parse_args()

    if args.backfill > 0:
        from datetime import date as _d, timedelta as _td
        dates = []
        cur = _d.today()
        while len(dates) < args.backfill:
            cur -= _td(days=1)
            if cur.weekday() < 5:
                dates.append(cur.strftime("%Y%m%d"))
        for ds in dates:
            print(f"\n=== backfill {ds} ===")
            out = run_scan(push=False, dry_run=False, tech_only=args.tech_only, as_of_date=ds)
            print(f"[escalator] {ds} counts: {out.get('counts')}")
    else:
        run_scan(push=args.push, dry_run=args.dry_run, tech_only=args.tech_only,
                 as_of_date=args.date)
