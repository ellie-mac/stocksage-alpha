#!/usr/bin/env python3
"""多策略胜率横向对比 — 一图看清谁稳谁飘。

读 7 个 *_daily_perf.json history files，对每个策略提取每日 open_win_rate
（次开盘到当日收盘胜率，daily_perf_log 写出来的口径），绘成多线时间序列。
扶梯单独追踪 T+5，本图不含；本图是 T+1 维度横向比较。

输出双通道（参考 cffex / escalator_perf）：
  - wechat 文字：每个策略累计 win_rate + 样本量 + 排名
  - 飞书图：所有策略叠加一张折线图 + 50% 基线

用法：
    python -X utf8 src/jobs/strategy_compare.py [--push] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

DATA = ROOT / "data"
CHART = DATA / "strategy_compare_chart.png"

# 各策略 perf 文件 + 抽 win_rate 的取值路径
#   "flat" = 文件 root 直接有 win_rate/open_win_rate
#   "total" = 嵌在 root.total 下（chip/gc）
#   "h1" = 嵌在 root.H1 下（hot 的 H1 是全量 picks）
SOURCES = [
    {"tag": "主",  "label": "主策略",  "file": "main_daily_perf.json",       "shape": "flat",  "color": "#1f77b4"},
    {"tag": "小",  "label": "小盘",    "file": "sc_daily_perf.json",         "shape": "flat",  "color": "#ff7f0e"},
    {"tag": "筹",  "label": "筹码",    "file": "chip_daily_perf.json",       "shape": "total", "color": "#2ca02c"},
    {"tag": "叉",  "label": "金叉",    "file": "gc_daily_perf.json",         "shape": "total", "color": "#d62728"},
    {"tag": "热",  "label": "热榜",    "file": "hot_daily_perf.json",        "shape": "h1",    "color": "#9467bd"},
    {"tag": "ETF", "label": "ETF",     "file": "etf_daily_perf.json",        "shape": "flat",  "color": "#8c564b"},
    {"tag": "监",  "label": "监控强买","file": "wl_monitor_perf.json",       "shape": "flat",  "color": "#e377c2"},
]


def _extract_record(rec: dict, shape: str) -> Optional[dict]:
    """从 daily_perf 单天记录里抽 {date, win_rate, n}。win_rate 优先用 open_win_rate。"""
    date = str(rec.get("date") or "")
    if not date:
        return None
    if shape == "flat":
        src = rec
    elif shape == "total":
        src = rec.get("total") or {}
    elif shape == "h1":
        src = rec.get("H1") or {}
    else:
        return None
    wr = src.get("open_win_rate") if src.get("open_win_rate") is not None else src.get("win_rate")
    n = src.get("n", 0) or 0
    if wr is None or n <= 0:
        return None
    return {"date": date, "win_rate": float(wr), "n": int(n)}


def _load_series(path: Path, shape: str) -> list[dict]:
    if not path.exists():
        return []
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(d, list):
        return []
    series = []
    for rec in d:
        if not isinstance(rec, dict):
            continue
        ex = _extract_record(rec, shape)
        if ex:
            series.append(ex)
    series.sort(key=lambda x: x["date"])
    return series


def _rolling_avg(values: list[float], window: int = 5) -> list[Optional[float]]:
    out: list[Optional[float]] = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        chunk = values[start:i + 1]
        out.append(round(mean(chunk), 1) if chunk else None)
    return out


def _build_text(series_map: dict[str, list[dict]]) -> str:
    """文字摘要：每个策略累计 win_rate / 样本量 / 排名。"""
    lines = [f"[策略对比·T+1] {datetime.now():%Y-%m-%d %H:%M}<br>"]

    rows = []
    for src in SOURCES:
        series = series_map.get(src["tag"], [])
        if not series:
            rows.append({"tag": src["tag"], "label": src["label"], "n_days": 0,
                         "total_picks": 0, "weighted_wr": None,
                         "recent_wr": None, "recent_days": 0})
            continue
        total_picks = sum(s["n"] for s in series)
        # 按 N 加权的累计 win_rate
        weighted = sum(s["win_rate"] * s["n"] for s in series) / total_picks if total_picks else None
        recent = series[-5:]  # 最近 5 个日跑
        recent_n = sum(s["n"] for s in recent)
        recent_wr = (sum(s["win_rate"] * s["n"] for s in recent) / recent_n) if recent_n else None
        rows.append({
            "tag": src["tag"], "label": src["label"],
            "n_days": len(series), "total_picks": total_picks,
            "weighted_wr": weighted, "recent_wr": recent_wr,
            "recent_days": len(recent),
        })

    # 按累计 weighted_wr 排序展示（None 放最后）
    rows_sorted = sorted(rows, key=lambda r: (r["weighted_wr"] is None, -(r["weighted_wr"] or 0)))

    lines.append("**📊 累计胜率排名（N 加权）**<br>")
    lines.append("```")
    lines.append(f"{'排名':<4}{'策略':<8}{'累计':>8}{'近5日':>9}{'天数':>6}{'样本':>7}")
    for i, r in enumerate(rows_sorted, 1):
        wr = f"{r['weighted_wr']:.1f}%" if r['weighted_wr'] is not None else "  -  "
        rwr = f"{r['recent_wr']:.1f}%" if r['recent_wr'] is not None else "  -  "
        lines.append(f"{i:<4}{r['label']:<8}{wr:>8}{rwr:>9}{r['n_days']:>6}{r['total_picks']:>7}")
    lines.append("```<br>")
    lines.append("📖 累计 = 全部历史按样本量加权 / 近5日 = 最近 5 个跑日加权 / 天数 = 有效记录数<br>")
    lines.append("⚠️ 维度=T+1 次开盘到当日收盘 open_win_rate（daily_perf_log 口径），扶梯单独 T+5 追踪不在此图<br>")
    return "".join(lines)


def _render_chart(series_map: dict[str, list[dict]]) -> Optional[Path]:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.dates import DateFormatter
    except Exception as e:
        print(f"[strategy_compare] matplotlib 不可用: {e}", flush=True)
        return None

    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    fig, ax = plt.subplots(figsize=(12, 6.5))
    has_data = False
    for src in SOURCES:
        series = series_map.get(src["tag"], [])
        if not series:
            continue
        has_data = True
        dates = [datetime.strptime(s["date"], "%Y%m%d") for s in series]
        wrs = [s["win_rate"] for s in series]
        smoothed = _rolling_avg(wrs, window=5)
        # 实线：rolling 5-day；细虚线：原始（看噪声）
        ax.plot(dates, smoothed, label=src["label"], color=src["color"], linewidth=2.2, marker="o", markersize=4)
        ax.plot(dates, wrs, color=src["color"], linewidth=0.7, alpha=0.35, linestyle=":")

    if not has_data:
        print("[strategy_compare] 没有可绘图的数据", flush=True)
        plt.close(fig)
        return None

    ax.axhline(50, color="gray", linestyle="--", alpha=0.5, linewidth=1, label="50% 基线")
    ax.set_ylim(0, 100)
    ax.set_ylabel("T+1 open 胜率 (%)")
    ax.set_xlabel("日期")
    ax.set_title(f"多策略胜率对比（T+1 open，5 日 rolling）— {datetime.now():%Y-%m-%d}",
                 fontsize=13, fontweight="bold")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=9, ncol=4)
    ax.xaxis.set_major_formatter(DateFormatter("%m-%d"))
    for tick in ax.get_xticklabels():
        tick.set_rotation(45)
        tick.set_ha("right")

    fig.text(0.5, 0.02, "实线=5 日滚动均值 / 点线=每日原始 / 扶梯单独追踪 T+5 不含本图",
             ha="center", fontsize=8, color="#666")
    plt.tight_layout(rect=(0, 0.04, 1, 1))

    CHART.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(CHART, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"[strategy_compare] 图已生成 → {CHART.name}", flush=True)
    return CHART


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--push", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    series_map: dict[str, list[dict]] = {}
    for src in SOURCES:
        series_map[src["tag"]] = _load_series(DATA / src["file"], src["shape"])
        print(f"[strategy_compare] {src['label']:<8} 加载 {len(series_map[src['tag']])} 天有效数据")

    body = _build_text(series_map)
    print(f"\n{body.replace('<br>', chr(10))}\n")

    if args.dry_run:
        if not args.push:
            print("[strategy_compare] dry-run 完成")
            return 0

    if args.push and not args.dry_run:
        # 给文字解读用：抽出 top-3 策略 + 整体观察
        rows_sorted = []
        for src in SOURCES:
            s = series_map.get(src["tag"], [])
            if not s:
                continue
            tot = sum(x["n"] for x in s)
            wr = (sum(x["win_rate"] * x["n"] for x in s) / tot) if tot else None
            recent5 = s[-5:]
            rec_tot = sum(x["n"] for x in recent5)
            rec_wr = (sum(x["win_rate"] * x["n"] for x in recent5) / rec_tot) if rec_tot else None
            rows_sorted.append((src["label"], wr, rec_wr, tot, len(s)))
        rows_sorted.sort(key=lambda r: (r[1] is None, -(r[1] or 0)))
        top3 = rows_sorted[:3]
        top3_lines = "\n".join(
            f"  • {name} 累计 {wr:.1f}% (近5日 {rwr:.1f}% / n={n})"
            for name, wr, rwr, n, _ in top3 if wr is not None
        )

        explanation = (
            f"[策略对比·T+1] {datetime.now():%Y-%m-%d %H:%M}\n"
            "==========\n"
            "🔑 累计胜率 Top 3：\n"
            f"{top3_lines or '  数据不足'}\n"
            "⚠️ 维度=T+1 open 胜率；扶梯单独追踪 T+5 不在本图"
        )

        try:
            from notify.notify import push_feishu_content, push_feishu_image
            push_feishu_content(explanation)
            print("[strategy_compare] 飞书文字解读推送成功", flush=True)
            chart = _render_chart(series_map)
            if chart:
                push_feishu_image(chart)
                print("[strategy_compare] 飞书图推送成功", flush=True)
        except Exception as e:
            print(f"[strategy_compare] 飞书推送失败: {e}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
