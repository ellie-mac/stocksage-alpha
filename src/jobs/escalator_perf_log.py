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
TIERS = ["E0", "E1"]   # E0=20d / E1=10d (E2 删除，回填验证后预测力为负)
MIN_SAMPLE_DAYS_WARN = 10   # 样本天数低于此值打 ⚠️


def _apply_cooldown(picks: list[dict], cooldown_days: int) -> list[dict]:
    """同一只 code 在 cooldown_days 个交易日内多次入选只保留首次。

    避免持续触发的票被反复计数（forward window 重叠 + 样本量虚高）。
    用 picks 里出现过的 unique 日期排序当作交易日 calendar；cooldown 用
    "在该 calendar 上的 index 差" 度量。
    """
    if not picks:
        return []
    all_dates = sorted({p["date"] for p in picks})
    idx_of = {d: i for i, d in enumerate(all_dates)}

    last_idx: dict[str, int] = {}
    kept: list[dict] = []
    # 按日期升序处理；同日内顺序不影响（同 code 同日只一条）
    for p in sorted(picks, key=lambda x: x["date"]):
        i = idx_of[p["date"]]
        prev = last_idx.get(p["code"])
        if prev is None or (i - prev) >= cooldown_days:
            kept.append(p)
            last_idx[p["code"]] = i
    return kept


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
                    "matched_tiers": p.get("matched_tiers") or [tier],
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
    """聚合：{tier: {bucket: {hold_n: stats}}}。按 (tier, bucket) 分组后再
    应用 cooldown=n 去重，保证 forward windows 不重叠。"""
    # 1) 按 (tier, bucket) 分桶
    by_group: dict = {}
    for p in picks_with_ret:
        tier = p["tier"]
        bucket = _r2_bucket(p["r2"])
        if not bucket or tier not in TIERS:
            continue
        key = (tier, bucket)
        by_group.setdefault(key, []).append(p)

    # 2) 每个 (tier, bucket) 各自应用 cooldown 算 stats
    agg: dict = {t: {} for t in TIERS}
    for (tier, bucket), picks_in_group in by_group.items():
        if bucket not in agg[tier]:
            agg[tier][bucket] = {n: [] for n in HOLD_PERIODS}
        for n in HOLD_PERIODS:
            relevant = [p for p in picks_in_group if p.get(f"ret_t{n}") is not None]
            deduped = _apply_cooldown(relevant, cooldown_days=n)
            agg[tier][bucket][n] = [p[f"ret_t{n}"] for p in deduped]

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


def _tier_overall(picks_with_ret: list[dict], tier: str, by: str = "primary") -> dict:
    """tier 整体 stats 不分桶；每个 hold period 应用对应天数冷却去重。

    by='primary': 按 primary tier 算（first-match-wins，每只票归一档；residual 视图）
    by='criterion': 按 matched_tiers 多重计数（每只票若同时命中多 tier 给每个 tier 都 +1；criterion 评估视图）
    """
    out = {}
    for n in HOLD_PERIODS:
        # 1) 筛 tier-membership
        relevant = []
        for p in picks_with_ret:
            if by == "primary":
                hits = [p["tier"]]
            else:
                hits = p.get("matched_tiers") or [p["tier"]]
            if tier in hits and p.get(f"ret_t{n}") is not None:
                relevant.append(p)
        # 2) 应用 cooldown=n (对齐 hold period — forward windows 不重叠)
        deduped = _apply_cooldown(relevant, cooldown_days=n)
        rets = [p[f"ret_t{n}"] for p in deduped]
        if rets:
            out[n] = {
                "n": len(rets),
                "win_rate": round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1),
                "avg_ret": round(mean(rets), 2),
            }
        else:
            out[n] = {"n": 0, "win_rate": None, "avg_ret": None}
    return out


def _match_count_overall(picks_with_ret: list[dict]) -> dict:
    """按 matched_tiers 数量分组算 win_rate（冷却去重），验证"多档共振 → 高胜率"。
    现在 TIERS 只有 E0/E1，match count 范围 1-2。
    """
    out: dict[int, dict] = {}
    max_cnt = len(TIERS)
    for cnt in range(1, max_cnt + 1):
        out[cnt] = {}
        for n in HOLD_PERIODS:
            relevant = []
            for p in picks_with_ret:
                m = [t for t in (p.get("matched_tiers") or [p["tier"]]) if t in TIERS]
                if len(m) == cnt and p.get(f"ret_t{n}") is not None:
                    relevant.append(p)
            deduped = _apply_cooldown(relevant, cooldown_days=n)
            rets = [p[f"ret_t{n}"] for p in deduped]
            if rets:
                out[cnt][n] = {
                    "n": len(rets),
                    "win_rate": round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1),
                    "avg_ret": round(mean(rets), 2),
                }
            else:
                out[cnt][n] = {"n": 0, "win_rate": None, "avg_ret": None}
    return out


def _overlap_stats(picks_with_ret: list[dict]) -> dict:
    """各 tier 之间 matched_tiers 重合率。
    返回 {tier: {"total": N, "also_in_E0": x%, "also_in_E1": y%, "only": z%}}
    """
    out: dict[str, dict] = {}
    for t in TIERS:
        total = sum(1 for p in picks_with_ret
                    if t in (p.get("matched_tiers") or [p["tier"]]))
        row = {"total": total}
        if total > 0:
            for other in TIERS:
                if other == t:
                    continue
                also = sum(1 for p in picks_with_ret
                           if t in (p.get("matched_tiers") or [p["tier"]])
                           and other in (p.get("matched_tiers") or [p["tier"]]))
                row[f"also_{other}"] = round(also / total * 100, 1)
            only = sum(1 for p in picks_with_ret
                       if (p.get("matched_tiers") or [p["tier"]]) == [t])
            row["only"] = round(only / total * 100, 1)
        out[t] = row
    return out


def _format_body(summary: dict, overall_primary: dict, overall_criterion: dict,
                 overlap: dict, match_count: dict,
                 n_days: int, n_picks: int) -> str:
    lines = [f"[扶梯·胜率] {datetime.now():%Y-%m-%d %H:%M}<br>",
             f"样本：{n_days} 天历史 / {n_picks} 个 pick × hold period<br><br>"]

    def _tier_row(tier: str, src: dict) -> str:
        row = [f"{tier:<5}"]
        for n in HOLD_PERIODS:
            s = src.get(tier, {}).get(n, {})
            wr = s.get("win_rate"); ret = s.get("avg_ret"); nn = s.get("n", 0)
            if wr is None:
                row.append(f"{'-':>14}")
            else:
                # T+5 加粗（最关键指标）
                if n == 5:
                    row.append(f"**{wr:>4.0f}% {ret:+.1f}% n={nn}**".rjust(14))
                else:
                    row.append(f"{wr:>4.0f}% {ret:+.1f}% n={nn:<3}".rjust(14))
        return "".join(row)

    # 视图 A：按 primary tier（每只票归一档，长窗口优先）
    lines.append("**📊 视图 A：按 primary tier（first-match，每票归一档）**<br>")
    lines.append("```")
    lines.append(f"{'tier':<5}{'T+1':>14}{'T+5':>14}{'T+10':>14}")
    for tier in TIERS:
        lines.append(_tier_row(tier, overall_primary))
    lines.append("```<br>")

    # 视图 B：按 criterion 多重计数（每只票若同时命中多档给每个 +1）
    lines.append("**📊 视图 B：按 criterion 多重计数（评估每个判定独立预测力）**<br>")
    lines.append("```")
    lines.append(f"{'tier':<5}{'T+1':>14}{'T+5':>14}{'T+10':>14}")
    for tier in TIERS:
        lines.append(_tier_row(tier, overall_criterion))
    lines.append("```<br>")

    # 视图 C：按共振档数分组（验证"多档共振=高胜率"假设）
    max_cnt = len(TIERS)
    lines.append(f"**📊 视图 C：按共振档数（命中 1-{max_cnt} 档 vs 胜率）**<br>")
    lines.append("```")
    lines.append(f"{'matches':<9}{'T+1':>14}{'T+5':>14}{'T+10':>14}")
    for cnt in range(max_cnt, 0, -1):
        s_row = match_count.get(cnt, {})
        row_label = f"{cnt} 档共振" if cnt > 1 else "1 档单中"
        row = [f"{row_label:<9}"]
        for n in HOLD_PERIODS:
            s = s_row.get(n, {})
            wr = s.get("win_rate"); ret = s.get("avg_ret"); nn = s.get("n", 0)
            if wr is None or nn == 0:
                row.append(f"{'-':>14}")
            else:
                if n == 5:
                    row.append(f"**{wr:>4.0f}% {ret:+.1f}% n={nn}**".rjust(14))
                else:
                    row.append(f"{wr:>4.0f}% {ret:+.1f}% n={nn:<3}".rjust(14))
        lines.append("".join(row))
    lines.append("```<br>")

    # 重合率
    lines.append("**📊 重合率（matched_tiers 共现）**<br>")
    lines.append("```")
    header = f"{'tier':<5}{'total':>8}"
    for other in TIERS:
        header += f"{'also_'+other:>10}"
    header += f"{'only_this':>11}"
    lines.append(header)
    for tier in TIERS:
        ov = overlap.get(tier, {})
        row = f"{tier:<5}{ov.get('total', 0):>8}"
        for other in TIERS:
            if other == tier:
                row += f"{'-':>10}"
            else:
                v = ov.get(f"also_{other}")
                row += f"{v:>9.1f}%" if v is not None else f"{'-':>10}"
        only = ov.get("only")
        row += f"{only:>10.1f}%" if only is not None else f"{'-':>11}"
        lines.append(row)
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

    # 3. 聚合（R² 分桶仍按 primary tier；overall 出两个视图）
    summary = _aggregate(enriched)
    overall_primary   = {t: _tier_overall(enriched, t, by="primary")   for t in TIERS}
    overall_criterion = {t: _tier_overall(enriched, t, by="criterion") for t in TIERS}
    overlap = _overlap_stats(enriched)
    match_count = _match_count_overall(enriched)

    body = _format_body(summary, overall_primary, overall_criterion, overlap,
                       match_count, n_days, len(enriched))
    print(f"\n{body.replace('<br>', chr(10))}\n")

    if args.dry_run:
        print("[escalator_perf] dry-run 完成")
        return 0

    _save_history(summary, overall_criterion, n_days, len(enriched))   # 持久化用 criterion 视图（独立预测力更有分析价值）

    if args.push:
        # 提取关键摘要数字给文字解读用
        e0_p = overall_primary.get("E0", {}).get(5, {})
        e1_p = overall_primary.get("E1", {}).get(5, {})
        m2 = match_count.get(2, {}).get(5, {})
        m1 = match_count.get(1, {}).get(5, {})

        explanation = (
            f"[扶梯·胜率] {datetime.now():%Y-%m-%d %H:%M} | N={n_days}天\n"
            "==========\n"
            "📖 这张图怎么读：\n"
            "• 2 panel：E0 (20天慢牛) / E1 (10天中期)\n"
            "• X 轴 = R² 分桶（拟合度区间，越右拟合越线性 = 趋势越干净）\n"
            "• Y 轴 = 胜率%（虚线 50% = coin flip 基线）\n"
            "• 每个 panel 三条线 = T+1 / T+5 / T+10 三种持有周期，T+5 是扶梯的 sweet spot\n"
            "• 点旁的 n=N 是该桶样本量，N 越小数字越不可信（<10 仅供参考）\n"
            "==========\n"
            "🔑 今日要点：\n"
            f"• E0 整体 T+5 胜率 {e0_p.get('win_rate','-')}%（n={e0_p.get('n',0)}），avg {e0_p.get('avg_ret','-')}%\n"
            f"• E1 整体 T+5 胜率 {e1_p.get('win_rate','-')}%（n={e1_p.get('n',0)}），avg {e1_p.get('avg_ret','-')}%\n"
            f"• 2 档共振 T+5 胜率 {m2.get('win_rate','-')}%（n={m2.get('n',0)}）vs 1 档 {m1.get('win_rate','-')}%（n={m1.get('n',0)}） — 共振 boost {('显著' if (m2.get('win_rate') or 0) > (m1.get('win_rate') or 0) + 15 else '边际')}\n"
            "==========\n"
            "📊 看图时盯什么：\n"
            "1. E0 panel 右半（R²≥0.90）的 T+5 线在不在 70%+ — 决定 R²_min 阈值是否再收紧\n"
            "2. E1 panel 整体能不能爬过 50% 虚线 — 决定 E1 是否值得保留作信号\n"
            "3. 同一桶里 T+1/T+5/T+10 的差异 — 持有越久胜率应该越高（趋势策略本意）"
        )

        try:
            from notify.notify import push_feishu_content, push_feishu_image
            push_feishu_content(explanation)
            print("[escalator_perf] 飞书文字解读推送成功", flush=True)

            chart = _render_chart(summary, overall_criterion)
            if chart:
                push_feishu_image(chart)
                print("[escalator_perf] 飞书图推送成功", flush=True)
        except Exception as e:
            print(f"[escalator_perf] 飞书推送失败: {e}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
