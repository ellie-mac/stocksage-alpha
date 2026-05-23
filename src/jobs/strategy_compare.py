#!/usr/bin/env python3
"""精选追踪 — 复盘 evening_strategy 推送过的「今日精选」picks 实际 T+N 表现。

每天 16:10 跑，读 data/evening_picks_log_<date>.json 历史，针对每只 pick：
  - T+1 hold: pick 落盘的次日，看 next_close vs entry_price 收益
  - T+5 hold: 5 个交易日后
  - T+10 hold: 10 个交易日后
  - T+20 hold: 20 个交易日后

按 hold horizon 分组算实际胜率/均收益，跟落盘时的「期望」对比，让用户知道：
  - 规则在实战是不是真有 alpha
  - 不同 horizon / regime 下表现差异

推送：飞书文字汇总 + 累计追踪表（参考 cffex/escalator_perf）。

用法：
    python -X utf8 src/jobs/strategy_compare.py [--push] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

DATA = ROOT / "data"
PERF_LOG = DATA / "evening_perf_track.json"   # 累计追踪记录

HOLD_HORIZONS = {"T+1": 1, "T+3": 3, "T+5": 5, "T+10": 10, "T+20": 20}


def _parse_expected(expected_str: str) -> tuple[Optional[float], Optional[float]]:
    """从 '90% / +2.62%' 或 '100% / +17.95% (n小)' 抽 (win_pct, avg_pct)"""
    if not expected_str:
        return None, None
    m_win = re.search(r"(\d+(?:\.\d+)?)%", expected_str)
    win = float(m_win.group(1)) if m_win else None
    m_avg = re.search(r"([+\-]?\d+(?:\.\d+)?)%\s*(?:\(|$|/)", expected_str.replace(f"{win}%", "", 1) if win else expected_str)
    avg = float(m_avg.group(1)) if m_avg else None
    return win, avg


def _load_picks_logs() -> list[dict]:
    """读所有 evening_picks_log_*.json，按日期升序返回。"""
    out = []
    for p in sorted(DATA.glob("evening_picks_log_*.json")):
        try:
            out.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception as e:
            print(f"[evening_perf_track] 读取 {p.name} 失败: {e}")
    return out


def _fetch_forward_returns(code: str, pick_date: str,
                            entry_price: Optional[float]) -> dict[int, Optional[float]]:
    """T+1/3/5/10/20 forward return %。pick_date YYYYMMDD。entry_price 缺失时用 pick_date 当日 close 兜底。"""
    import fetcher as _f
    import pandas as _pd
    try:
        df = _f.get_price_history(code, days=60)
    except Exception:
        return {n: None for n in HOLD_HORIZONS.values()}
    if df is None or df.empty or "date" not in df.columns:
        return {n: None for n in HOLD_HORIZONS.values()}
    pick_ts = _pd.to_datetime(pick_date, format="%Y%m%d")

    if entry_price is None or entry_price <= 0:
        entry_df = df[df["date"] <= pick_ts]
        if entry_df.empty:
            return {n: None for n in HOLD_HORIZONS.values()}
        try:
            entry_price = float(entry_df["close"].iloc[-1])
        except Exception:
            return {n: None for n in HOLD_HORIZONS.values()}
        if entry_price <= 0:
            return {n: None for n in HOLD_HORIZONS.values()}

    fwd = df[df["date"] > pick_ts].sort_values("date")
    out: dict[int, Optional[float]] = {}
    for n in HOLD_HORIZONS.values():
        if len(fwd) < n:
            out[n] = None
        else:
            out[n] = round((float(fwd["close"].iloc[n - 1]) / entry_price - 1) * 100, 2)
    return out


def _attach_returns(logs: list[dict]) -> list[dict]:
    """给每条历史 log 的每个 pick 标 forward returns（按 hold 限定 horizon，避免无意义计算）"""
    import fetcher as _f
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # 把所有 (code, date, entry) 平铺出来，并发拉 forward
    flat: list[tuple[int, int, str, str, float | None]] = []  # (log_idx, pick_idx, code, date, price)
    for li, log in enumerate(logs):
        date = log["date"]
        for pi, pick in enumerate(log.get("picks", [])):
            flat.append((li, pi, pick["code"], date, pick.get("price")))

    print(f"[evening_perf_track] 拉 {len(flat)} 个 pick 的 forward returns...", flush=True)
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(_fetch_forward_returns, code, date, price): (li, pi)
                for (li, pi, code, date, price) in flat}
        for fut in as_completed(futs):
            li, pi = futs[fut]
            rets = fut.result()
            for n in HOLD_HORIZONS.values():
                logs[li]["picks"][pi][f"ret_t{n}"] = rets.get(n)
    return logs


def _summarize(logs: list[dict]) -> dict:
    """按 hold horizon 聚合: 总样本数、实际 win/avg、期望 win/avg（加权）"""
    by_hold: dict[str, list[dict]] = defaultdict(list)
    for log in logs:
        for pick in log.get("picks", []):
            hold = pick.get("hold", "")
            if hold in HOLD_HORIZONS:
                pick["_log_date"] = log["date"]
                pick["_regime_score"] = log.get("regime_score")
                by_hold[hold].append(pick)

    summary: dict[str, dict] = {}
    for hold, picks in by_hold.items():
        n_days = HOLD_HORIZONS[hold]
        # 只算 forward 数据齐的 picks
        valid = [p for p in picks if p.get(f"ret_t{n_days}") is not None]
        if not valid:
            summary[hold] = {"n": 0, "n_total": len(picks)}
            continue
        rets = [p[f"ret_t{n_days}"] for p in valid]
        win = sum(1 for r in rets if r > 0) / len(rets) * 100
        avg = mean(rets)
        # 期望值（加权平均）
        exp_wins = []
        exp_avgs = []
        for p in valid:
            ew, ea = _parse_expected(p.get("expected", ""))
            if ew is not None:
                exp_wins.append(ew)
            if ea is not None:
                exp_avgs.append(ea)
        summary[hold] = {
            "n": len(valid),
            "n_total": len(picks),
            "actual_win": round(win, 1),
            "actual_avg": round(avg, 2),
            "expected_win": round(mean(exp_wins), 1) if exp_wins else None,
            "expected_avg": round(mean(exp_avgs), 2) if exp_avgs else None,
            "picks": valid,
        }
    return summary


def _build_message(summary: dict) -> tuple[str, str]:
    title = f"[精选追踪] {datetime.now():%Y-%m-%d %H:%M}"
    parts = [f"*{datetime.now():%Y-%m-%d %H:%M}*"]
    parts.append("")
    parts.append("📊 **evening_strategy 「今日精选」实战追踪**")

    any_data = False
    for hold in ["T+1", "T+3", "T+5", "T+10", "T+20"]:
        s = summary.get(hold)
        if not s or s.get("n", 0) == 0:
            continue
        any_data = True
        actual_w = s["actual_win"]
        actual_a = s["actual_avg"]
        exp_w = s.get("expected_win")
        exp_a = s.get("expected_avg")
        emoji = "✅" if (actual_w >= (exp_w or 50) * 0.85) else "⚠️" if actual_w >= 50 else "❌"
        parts.append("")
        parts.append(f"**{hold} hold** {emoji} (n={s['n']}/{s['n_total']} 已到期)")
        line = f"  实际 {actual_w}% / {actual_a:+.2f}%"
        if exp_w is not None:
            line += f"   vs 期望 {exp_w}% / {exp_a:+.2f}%" if exp_a is not None else f"   vs 期望 {exp_w}%"
        parts.append(line)

        # 最近几只样本
        recent = sorted(s["picks"], key=lambda p: -int(p["_log_date"]))[:5]
        for p in recent:
            ret = p[f"ret_t{HOLD_HORIZONS[hold]}"]
            mark = "✅" if ret > 0 else "❌"
            parts.append(f"    {p['_log_date']} {p['code']} {p.get('name','')[:8]} {ret:+.2f}% {mark}")

    if not any_data:
        parts.append("")
        parts.append("还没有到期的 picks（evening_strategy 落盘 log 不够久）")
        parts.append("⚠️ T+1 需等次日，T+5 等 5 个交易日，T+10 等两周")
    body = "<br>".join(parts)
    return title, body


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    logs = _load_picks_logs()
    print(f"[evening_perf_track] 读到 {len(logs)} 个 evening_picks_log 文件")
    if not logs:
        print("[evening_perf_track] 无历史 picks log，退出")
        return 0

    logs = _attach_returns(logs)
    summary = _summarize(logs)
    print(f"[evening_perf_track] hold 分组样本数: " +
          " ".join(f"{h}={s.get('n', 0)}/{s.get('n_total', 0)}" for h, s in summary.items()))

    title, body = _build_message(summary)
    print(f"\n{title}\n{body.replace('<br>', chr(10))}")

    # 落盘 perf history
    PERF_LOG.write_text(json.dumps({
        "updated": datetime.now().isoformat(),
        "summary": {h: {k: v for k, v in s.items() if k != "picks"} for h, s in summary.items()},
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.dry_run:
        print("\n[evening_perf_track] dry-run，跳过推送")
        return 0

    if args.push:
        try:
            from notify.notify import push_feishu_content
            push_feishu_content(title + "\n\n" + body.replace("<br>", "\n"))
            print("[evening_perf_track] 飞书推送成功")
        except Exception as e:
            print(f"[evening_perf_track] 飞书推送失败: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
