#!/usr/bin/env python3
"""审核精选回测 — 追踪 resonance_audit_picks.json 中我(太子)推送的精选票实际表现。

每天 16:20 跑（收盘后），自动：
1. 读取 copilot/data/resonance_audit_picks.json
2. 为每条 pick 填充 T+1 open/low（entry）、T0 close、T1 close、T5 close、T10 close
3. 计算胜率和平均收益:
   - open入场: entry = T+1 open（严格无前瞻偏差）
   - limit入场: 如果 pick 设置了 recommend_price（默认=审核日收盘价×0.98）且 T+1 low <= recommend_price,
     则 entry = recommend_price（模拟挂单买入）；否则视为未成交
4. 按 regime / 共振路数 分桶统计
5. 输出汇总到 stdout（供晚间精选参考）

用法：
    python -X utf8 src/jobs/audit_picks_backtest.py [--no-fill] [--summary]
    --no-fill   不填充价格
    --summary   输出胜率汇总（默认开启）
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent.parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

PICKS_PATH = ROOT / "copilot" / "data" / "resonance_audit_picks.json"


def _load_picks() -> dict:
    if not PICKS_PATH.exists():
        return {"records": []}
    return json.loads(PICKS_PATH.read_text(encoding="utf-8"))


def _save_picks(data: dict) -> None:
    PICKS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _code_to_prefix(code: str) -> str:
    """6位代码 → sh/sz前缀"""
    c = str(code).zfill(6)
    if c.startswith(("6", "9")):
        return f"sh{c}"
    return f"sz{c}"


def _fetch_prices(code: str, pick_date: str) -> dict:
    """获取 pick 相关的价格：T+1 open/low, T0 close, T1 close, T5 close, T10 close"""
    import fetcher as _f
    import pandas as _pd

    try:
        df = _f.get_price_history(code, days=60)
    except Exception:
        return {}

    if df is None or df.empty or "date" not in df.columns:
        return {}

    pick_ts = _pd.to_datetime(pick_date, format="%Y%m%d")
    fwd = df[df["date"] > pick_ts].sort_values("date")

    if fwd.empty:
        return {}

    result = {}
    # T+1 open = entry price
    if "open" in fwd.columns and len(fwd) >= 1:
        try:
            v = float(fwd["open"].iloc[0])
            if v > 0:
                result["entry_open"] = round(v, 3)
        except Exception:
            pass

    # T+1 low = intraday low (for recommend_price validation)
    if "low" in fwd.columns and len(fwd) >= 1:
        try:
            v = float(fwd["low"].iloc[0])
            if v > 0:
                result["T0_low"] = round(v, 3)
        except Exception:
            pass

    # T0 close = T+1 close (same day as entry)
    if len(fwd) >= 1:
        try:
            result["T0_close"] = round(float(fwd["close"].iloc[0]), 3)
        except Exception:
            pass

    # T1 to T10 close (每天都记录)
    for i in range(1, 11):
        if len(fwd) >= i + 1:
            try:
                result[f"T{i}_close"] = round(float(fwd["close"].iloc[i]), 3)
            except Exception:
                pass

    return result


def fill_prices(data: dict) -> int:
    """为所有缺少价格的 pick 填充数据，返回更新数量。"""
    tasks = []  # (record_idx, pick_idx, code, date)
    for ri, rec in enumerate(data.get("records", [])):
        date = rec.get("audit_date") or rec.get("date", "")
        date = date.replace("-", "")
        if not date:
            continue
        for pi, pick in enumerate(rec.get("picks", [])):
            # 强制更新所有picks以补充T2-T9数据
            tasks.append((ri, pi, pick["code"], date))

    if not tasks:
        print("[audit_backtest] 所有 pick 价格已填充，无需更新")
        return 0

    print(f"[audit_backtest] 拉取 {len(tasks)} 个 pick 的价格...", flush=True)
    results = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_fetch_prices, code, date): (ri, pi)
                for ri, pi, code, date in tasks}
        for fut in as_completed(futs):
            ri, pi = futs[fut]
            try:
                prices = fut.result()
                if prices:
                    results[(ri, pi)] = prices
            except Exception:
                pass

    updated = 0
    for (ri, pi), prices in results.items():
        pick = data["records"][ri]["picks"][pi]
        pick.update(prices)
        updated += 1

    print(f"[audit_backtest] 已填充 {updated}/{len(tasks)} 个 pick")
    return updated


def _calc_stats(picks: list, entry_key: str, exit_key: str) -> tuple:
    """计算胜率和均收益。返回 (win_rate, avg_return, n) 或 None。"""
    valid = [(p[entry_key], p[exit_key]) for p in picks
             if p.get(entry_key) and p.get(exit_key) and p[entry_key] > 0]
    if not valid:
        return None
    returns = [(exit_p / entry_p - 1) * 100 for entry_p, exit_p in valid]
    win_rate = sum(1 for r in returns if r > 0) / len(returns) * 100
    avg_ret = sum(returns) / len(returns)
    return (win_rate, avg_ret, len(valid))


def _count_resonance(signal: str) -> int:
    """从 signal 字符串中提取共振路数。"""
    if not signal:
        return 0
    # 计算 '+' 分隔的段数, 或逗号分隔
    separators = signal.count("+") + signal.count(",") + signal.count("·")
    return separators + 1 if separators > 0 else 1


def summary(data: dict) -> str:
    """生成胜率汇总报告，支持 recommend_price 和多维分桶。"""
    all_picks = []
    for rec in data.get("records", []):
        regime = rec.get("regime")
        for pick in rec.get("picks", []):
            if pick.get("entry_open") and pick.get("T0_close"):
                pick["_regime"] = regime
                pick["_resonance_n"] = _count_resonance(pick.get("signal", ""))
                all_picks.append(pick)

    if not all_picks:
        return "暂无足够数据（需至少1条有 entry_open + T0_close 的 pick）"

    lines = [f"📋 审核精选回测（共 {len(all_picks)} 条有效 pick）", ""]

    # === 1. Open入场统计 ===
    lines.append("━━━ Open入场 (T+1开盘价买入) ━━━")
    horizons = [
        ("T+1 (open→close)", "entry_open", "T0_close"),
        ("T+2 (open→T1close)", "entry_open", "T1_close"),
        ("T+5", "entry_open", "T5_close"),
        ("T+10", "entry_open", "T10_close"),
    ]
    for label, entry_key, exit_key in horizons:
        stats = _calc_stats(all_picks, entry_key, exit_key)
        if not stats:
            lines.append(f"  {label}: 数据不足")
            continue
        wr, ar, n = stats
        emoji = "🟢" if wr >= 60 else ("🟡" if wr >= 50 else "🔴")
        lines.append(f"  {emoji} {label}: 胜率{wr:.1f}% 均收益{ar:+.2f}% (n={n})")

    # === 2. Limit入场统计 (recommend_price) ===
    limit_picks = [p for p in all_picks if p.get("recommend_price") and p.get("T0_low")]
    if limit_picks:
        lines.append("")
        lines.append("━━━ Limit入场 (recommend_price挂单) ━━━")
        filled = [p for p in limit_picks if p["T0_low"] <= p["recommend_price"]]
        not_filled = [p for p in limit_picks if p["T0_low"] > p["recommend_price"]]
        fill_rate = len(filled) / len(limit_picks) * 100 if limit_picks else 0
        lines.append(f"  成交率: {fill_rate:.0f}% ({len(filled)}/{len(limit_picks)})")

        if filled:
            # 用 recommend_price 作为 entry 计算收益
            valid = [(p["recommend_price"], p["T0_close"]) for p in filled
                     if p["recommend_price"] > 0]
            if valid:
                returns = [(exit_p / entry_p - 1) * 100 for entry_p, exit_p in valid]
                wr = sum(1 for r in returns if r > 0) / len(returns) * 100
                ar = sum(returns) / len(returns)
                emoji = "🟢" if wr >= 60 else ("🟡" if wr >= 50 else "🔴")
                lines.append(f"  {emoji} T+1 (limit→close): 胜率{wr:.1f}% 均{ar:+.2f}% (n={len(valid)})")

            # T5/T10
            for label, exit_key in [("T+5", "T5_close"), ("T+10", "T10_close")]:
                valid = [(p["recommend_price"], p[exit_key]) for p in filled
                         if p.get(exit_key) and p["recommend_price"] > 0]
                if valid:
                    returns = [(exit_p / entry_p - 1) * 100 for entry_p, exit_p in valid]
                    wr = sum(1 for r in returns if r > 0) / len(returns) * 100
                    ar = sum(returns) / len(returns)
                    emoji = "🟢" if wr >= 60 else ("🟡" if wr >= 50 else "🔴")
                    lines.append(f"  {emoji} {label} (limit→close): 胜率{wr:.1f}% 均{ar:+.2f}% (n={len(valid)})")

    # === 3. 按 regime 分桶 ===
    by_regime = {}
    for p in all_picks:
        r = p.get("_regime", "?")
        by_regime.setdefault(r, []).append(p)

    if len(by_regime) >= 1:
        lines.append("")
        lines.append("━━━ 按 regime 分桶 (T+1 open→close) ━━━")
        for regime, picks in sorted(by_regime.items(), key=lambda x: x[0] or 0):
            stats = _calc_stats(picks, "entry_open", "T0_close")
            if not stats:
                continue
            wr, ar, n = stats
            emoji = "🟢" if wr >= 60 else ("🟡" if wr >= 50 else "🔴")
            lines.append(f"  {emoji} regime={regime}: 胜率{wr:.1f}% 均{ar:+.2f}% (n={n})")

    # === 4. 按共振路数分桶 ===
    by_resonance = {}
    for p in all_picks:
        n = p.get("_resonance_n", 0)
        by_resonance.setdefault(n, []).append(p)

    if by_resonance:
        lines.append("")
        lines.append("━━━ 按共振路数分桶 (T+1 open→close) ━━━")
        for n_routes, picks in sorted(by_resonance.items()):
            stats = _calc_stats(picks, "entry_open", "T0_close")
            if not stats:
                continue
            wr, ar, n = stats
            emoji = "🟢" if wr >= 60 else ("🟡" if wr >= 50 else "🔴")
            lines.append(f"  {emoji} {n_routes}路共振: 胜率{wr:.1f}% 均{ar:+.2f}% (n={n})")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="审核精选回测")
    parser.add_argument("--no-fill", action="store_true", help="不填充价格")
    parser.add_argument("--summary", action="store_true", help="输出汇总（默认开启）")
    args = parser.parse_args()

    data = _load_picks()

    if not args.no_fill:
        updated = fill_prices(data)
        if updated > 0:
            _save_picks(data)

    print(summary(data))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
