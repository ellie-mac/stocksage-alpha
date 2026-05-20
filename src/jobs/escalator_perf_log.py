#!/usr/bin/env python3
"""扶梯策略胜率分析 — 按 tier × R² 分桶看 T+1/T+5/T+10 forward return。

读 data/escalator_YYYYMMDD.json 历史快照，对每只 pick 用 fetcher 拿后续 N 天
收盘价计算 forward return（相对 pick 日收盘价）。聚合按 (tier, r² bucket)
分组算 win_rate / avg_ret / N，看看是否 r² 越高胜率越高。

输出：
  - 文字（wechat）：紧凑表格摘要
  - 折线图（飞书）：r² bucket → win_rate 趋势（2 子图 E1/E2）
  - 历史落盘：data/escalator_perf_history.json（每次跑追加一行）

用法：
  python -X utf8 src/jobs/escalator_perf_log.py [--push] [--dry-run]
  python -X utf8 src/jobs/escalator_perf_log.py --dry-run     # 仅打印
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

DATA = ROOT / "data"
HIST_PATH = DATA / "escalator_perf_history.json"
CHART_PATH = DATA / "escalator_perf_chart.png"

# R² buckets — 边界与 escalator_scan _TIER_SPEC 对齐（E1≥0.80, E2≥0.85）。
R2_BUCKETS = [(0.80, 0.85), (0.85, 0.90), (0.90, 0.95), (0.95, 1.01)]
HOLD_PERIODS = [1, 5, 10]
TIERS = ["E1", "E2", "E3"]
MIN_SAMPLE_DAYS_WARN = 10   # 样本天数低于此值打 ⚠️


def _r2_bucket(r2: float) -> Optional[str]:
    for lo, hi in R2_BUCKETS:
        if lo <= r2 < hi:
            return f"{lo:.2f}-{hi:.2f}" if hi <= 1.0 else f"{lo:.2f}-1.00"
    return None


def _load_picks() -> list[dict]:
    """加载所有 data/escalator_YYYYMMDD.json，扁平为 pick 记录。
    forward return 不够的 pick 在 _fetch_forward_returns 里自然返回 None，聚合时跳过。
    """
    today = datetime.now().strftime("%Y%m%d")
    out: list[dict] = []
    for path in sorted(DATA.glob("escalator_????????.json")):
        ds = path.stem.split("_")[-1]
        if len(ds) != 8 or not ds.isdigit():
            continue
        if ds >= today:
            continue   # 今天的 pick 无 forward data
        try:
            d = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for tier, picks in d.get("tiers", {}).items():
            for p in picks:
                if not p.get("code") or p.get("r2") is None:
                    continue
                out.append({
                    "date": ds,
                    "code": p["code"],
                    "name": p.get("name", ""),
                    "tier": tier,
                    "r2": float(p["r2"]),
                    "slope_pct": float(p.get("slope_pct") or 0),
                    "close": float(p.get("close") or 0),
                })
    return out


def _fetch_forward_returns(code: str, pick_date: str, hold_periods: list[int],
                           entry_close: float) -> dict[int, Optional[float]]:
    """从 fetcher 拉 pick_date 之后的收盘价，算 T+n 相对 entry_close 的收益率%。
    entry_close: 用 pick 日收盘（escalator 20:15 跑，picks 反映当日 close）。
    """
    import fetcher as _f
    try:
        df = _f.get_price_history(code, days=60)
    except Exception:
        return {n: None for n in hold_periods}
    if df is None or df.empty or "date" not in df.columns:
        return {n: None for n in hold_periods}
    # df["date"] 是 datetime64，转 Timestamp 比较；用 .astype(str) 会拿
    # "2026-05-14" 跟 "20260514" 字符串比错乱
    import pandas as _pd
    pick_ts = _pd.to_datetime(pick_date, format="%Y%m%d")
    df = df[df["date"] > pick_ts].sort_values("date")
    out: dict[int, Optional[float]] = {}
    for n in hold_periods:
        if len(df) < n:
            out[n] = None
        else:
            forward_close = float(df["close"].iloc[n - 1])
            if entry_close > 0:
                out[n] = round((forward_close / entry_close - 1) * 100, 2)
            else:
                out[n] = None
    return out


def _aggregate(picks_with_ret: list[dict]) -> dict:
    """聚合：{tier: {bucket: {hold_n: {win_rate, avg_ret, n, picks_ret}}}}"""
    agg: dict = {t: {} for t in TIERS}
    for p in picks_with_ret:
        tier = p["tier"]
        bucket = _r2_bucket(p["r2"])
        if not bucket or tier not in TIERS:
            continue
        if bucket not in agg[tier]:
            agg[tier][bucket] = {n: [] for n in HOLD_PERIODS}
        for n in HOLD_PERIODS:
            r = p.get(f"ret_t{n}")
            if r is not None:
                agg[tier][bucket][n].append(r)

    # 计算 stats
    summary: dict = {t: {} for t in TIERS}
    for t in TIERS:
        for bucket, periods in agg[t].items():
            summary[t][bucket] = {}
            for n, rets in periods.items():
                if rets:
                    win = sum(1 for r in rets if r > 0)
                    summary[t][bucket][n] = {
                        "n": len(rets),
                        "win_rate": round(win / len(rets) * 100, 1),
                        "avg_ret": round(mean(rets), 2),
                    }
                else:
                    summary[t][bucket][n] = {"n": 0, "win_rate": None, "avg_ret": None}
    return summary


def _tier_overall(picks_with_ret: list[dict], tier: str) -> dict:
    """tier 整体 stats 不分桶。"""
    out = {}
    for n in HOLD_PERIODS:
        rets = [p[f"ret_t{n}"] for p in picks_with_ret
                if p["tier"] == tier and p.get(f"ret_t{n}") is not None]
        if rets:
            out[n] = {
                "n": len(rets),
                "win_rate": round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1),
                "avg_ret": round(mean(rets), 2),
            }
        else:
            out[n] = {"n": 0, "win_rate": None, "avg_ret": None}
    return out


def _format_body(summary: dict, overall: dict, n_days: int, n_picks: int) -> str:
    lines = [f"[扶梯·胜率] {datetime.now():%Y-%m-%d %H:%M}<br>",
             f"样本：{n_days} 天历史 / {n_picks} 个 pick × hold period<br><br>"]

    # 总览（不分 r² 桶）
    lines.append("**📊 总览**<br>")
    lines.append("```")
    lines.append(f"{'tier':<5}{'T+1':>14}{'T+5':>14}{'T+10':>14}")
    for tier in TIERS:
        row = [f"{tier:<5}"]
        for n in HOLD_PERIODS:
            s = overall[tier].get(n, {})
            wr = s.get("win_rate")
            ret = s.get("avg_ret")
            nn = s.get("n", 0)
            if wr is None:
                row.append(f"{'-':>14}")
            else:
                row.append(f"{wr:>4.0f}% {ret:+.1f}% n={nn:<3}".rjust(14))
        lines.append("".join(row))
    lines.append("```<br>")

    # 按 r² 分桶
    for tier in TIERS:
        if not summary.get(tier):
            continue
        lines.append(f"**📊 {tier} 按 R² 分桶**<br>")
        lines.append("```")
        lines.append(f"{'R² range':<14}{'T+1':>14}{'T+5':>14}{'T+10':>14}")
        for lo, hi in R2_BUCKETS:
            label = f"{lo:.2f}-{hi:.2f}" if hi <= 1.0 else f"{lo:.2f}-1.00"
            if label not in summary[tier]:
                continue
            row = [f"{label:<14}"]
            for n in HOLD_PERIODS:
                s = summary[tier][label].get(n, {})
                wr = s.get("win_rate")
                ret = s.get("avg_ret")
                nn = s.get("n", 0)
                if wr is None or nn == 0:
                    row.append(f"{'-':>14}")
                else:
                    row.append(f"{wr:>4.0f}% {ret:+.1f}% n={nn:<3}".rjust(14))
            lines.append("".join(row))
        lines.append("```<br>")

    if n_days < MIN_SAMPLE_DAYS_WARN:
        lines.append(f"⚠️ 样本量 {n_days} 天不足 {MIN_SAMPLE_DAYS_WARN} 天，建议累积观察后再调阈值<br>")
    return "".join(lines)


def _render_chart(summary: dict, overall: dict) -> Optional[Path]:
    """2x1 panel: E1/E2 各画 r² bucket → win_rate 折线（3 条线对应 T+1/T+5/T+10）。"""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        print(f"[escalator_perf] matplotlib 不可用: {e}", flush=True)
        return None

    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    fig, axes = plt.subplots(1, len(TIERS), figsize=(5 * len(TIERS), 5), squeeze=False)
    axes = axes[0]
    bucket_labels = [f"{lo:.2f}-{hi:.2f}" if hi <= 1.0 else f"{lo:.2f}-1.00"
                     for lo, hi in R2_BUCKETS]

    colors = {1: "#2ca02c", 5: "#d62728", 10: "#9467bd"}  # A股配色：5/10日 = 红/紫
    for ax, tier in zip(axes, TIERS):
        for n in HOLD_PERIODS:
            xs, ys, ns = [], [], []
            for lbl in bucket_labels:
                s = summary.get(tier, {}).get(lbl, {}).get(n, {})
                if s and s.get("win_rate") is not None and s.get("n", 0) >= 3:
                    xs.append(lbl)
                    ys.append(s["win_rate"])
                    ns.append(s["n"])
            if xs:
                ax.plot(xs, ys, label=f"T+{n}", color=colors[n],
                        linewidth=2, marker="o", markersize=6)
                # 在每个点上标 N
                for xi, yi, ni in zip(xs, ys, ns):
                    ax.annotate(f"n={ni}", xy=(xi, yi), xytext=(5, 5),
                                textcoords="offset points", fontsize=8, color="#444")

        overall_t = overall.get(tier, {})
        t5 = overall_t.get(5, {})
        title = f"{tier} 整体 T+5 胜率 {t5.get('win_rate','-')}% (n={t5.get('n',0)})"
        ax.set_title(title, fontsize=11, fontweight="bold")
        ax.set_xlabel("R² 桶")
        ax.set_ylabel("胜率 (%)")
        ax.axhline(50, color="gray", linestyle="--", alpha=0.5, linewidth=1)
        ax.set_ylim(0, 100)
        ax.grid(True, alpha=0.3)
        ax.legend(loc="best", fontsize=9)

    fig.suptitle(f"扶梯策略胜率 vs R² 分桶 — {datetime.now():%Y-%m-%d}",
                 fontsize=13, fontweight="bold")
    plt.tight_layout(rect=(0, 0, 1, 0.95))

    CHART_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(CHART_PATH, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"[escalator_perf] 图已生成 → {CHART_PATH.name}", flush=True)
    return CHART_PATH


def _save_history(summary: dict, overall: dict, n_days: int, n_picks: int) -> None:
    """累积写 data/escalator_perf_history.json — 每次跑追加一条 snapshot。"""
    record = {
        "logged":  datetime.now().isoformat(timespec="seconds"),
        "n_days":  n_days,
        "n_picks": n_picks,
        "overall": overall,
        "buckets": summary,
    }
    existing: list = []
    if HIST_PATH.exists():
        try:
            existing = json.loads(HIST_PATH.read_text(encoding="utf-8"))
        except Exception:
            existing = []
    if not isinstance(existing, list):
        existing = []
    existing.append(record)
    existing = existing[-90:]  # keep last 90 snapshots
    tmp = HIST_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(HIST_PATH)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--push", action="store_true", help="推 wechat 文字 + 飞书图")
    parser.add_argument("--dry-run", action="store_true", help="不写盘不推送")
    args = parser.parse_args()

    # 1. 拿历史 picks（forward data 不足的在 fetch 时返回 None，聚合时跳过）
    picks = _load_picks()
    if not picks:
        print("[escalator_perf] 无历史 escalator_YYYYMMDD.json，先跑 --backfill")
        return 1

    unique_dates = sorted({p["date"] for p in picks})
    n_days = len(unique_dates)
    print(f"[escalator_perf] 加载 {len(picks)} 只 pick，覆盖 {n_days} 个交易日")

    # 2. 算 forward return（每个 pick 拉一次价格历史）
    from concurrent.futures import ThreadPoolExecutor, as_completed
    enriched: list[dict] = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_forward_returns, p["code"], p["date"],
                          HOLD_PERIODS, p["close"]): p for p in picks}
        for fut in as_completed(futs):
            p = futs[fut]
            rets = fut.result()
            enriched.append({**p, **{f"ret_t{n}": rets.get(n) for n in HOLD_PERIODS}})

    # 3. 聚合
    summary = _aggregate(enriched)
    overall = {t: _tier_overall(enriched, t) for t in TIERS}

    body = _format_body(summary, overall, n_days, len(enriched))
    print(f"\n{body.replace('<br>', chr(10))}\n")

    if args.dry_run:
        print("[escalator_perf] dry-run 完成")
        return 0

    _save_history(summary, overall, n_days, len(enriched))

    if args.push:
        from common import push_wechat
        title = f"[扶梯·胜率] {datetime.now():%m-%d} | N={n_days}天"
        push_wechat(title, body)
        print("[escalator_perf] 微信推送完成", flush=True)

        chart = _render_chart(summary, overall)
        if chart:
            try:
                from notify.notify import push_feishu_image
                push_feishu_image(chart)
            except Exception as e:
                print(f"[escalator_perf] 飞书图推送失败: {e}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
