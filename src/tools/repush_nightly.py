#!/usr/bin/env python3
"""
repush_nightly.py — 从 signal_runs + snapshots 重推昨晚 nightly_scan 结果。

  主策略：signal_runs 有完整 buy_signals → 精准重推
  小盘策略：仅 snapshots → 用 score 作 buy_score，_sc_signal 无法恢复（全标 False）

用法:
  python -X utf8 src/tools/repush_nightly.py --date 2026-05-11
  python -X utf8 src/tools/repush_nightly.py --date 2026-05-11 --dry-run
  python -X utf8 src/tools/repush_nightly.py --date 2026-05-11 --strategy main
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SRC  = ROOT / "src"
sys.path.insert(0, str(SRC))


def _repush_main(date: str, dry_run: bool) -> None:
    from db import _conn
    from strategies.main_strategy import _push_results

    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM signal_runs WHERE date=? AND source='main' ORDER BY id DESC LIMIT 1",
            (date,),
        ).fetchone()

    if not row:
        print(f"[repush] signal_runs 无 {date}/main 数据，跳过主策略")
        return

    row = dict(row)
    buy_alerts   = json.loads(row["buy_signals"] or "[]")
    regime_score = float(row["regime_score"] or 5.0)
    run_time     = row["run_time"] or date

    with _conn() as conn:
        snap_rows = conn.execute(
            "SELECT * FROM snapshots WHERE date=? AND source='main_strategy' ORDER BY rank LIMIT 15",
            (date,),
        ).fetchall()

    scored = [dict(r) for r in snap_rows]
    for s in scored:
        s.setdefault("buy_score", s.get("score", 0))
        s.setdefault("name", s.get("code", ""))

    config       = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    regime_label = (dict(snap_rows[0])["regime_label"] if snap_rows else None) or "unknown"

    print(f"[repush] 主策略  buy={len(buy_alerts)}  cands={len(scored)}"
          f"  regime={regime_score:.1f}/{regime_label}  dry_run={dry_run}")
    _push_results(buy_alerts, scored, regime_score, regime_label, run_time, config, dry_run=dry_run)


def _repush_small(date: str, dry_run: bool) -> None:
    from db import _conn
    from strategies.small_strategy import _push_results as _small_push

    with _conn() as conn:
        snap_rows = conn.execute(
            "SELECT * FROM snapshots WHERE date=? AND source='small_strategy' ORDER BY rank LIMIT 15",
            (date,),
        ).fetchall()

    if not snap_rows:
        print(f"[repush] snapshots 无 {date}/small_strategy 数据，跳过小盘策略")
        return

    first        = dict(snap_rows[0])
    regime_score = float(first.get("regime_score") or 5.0)
    regime_label = first.get("regime_label") or "unknown"

    candidates = []
    for r in snap_rows:
        d = dict(r)
        candidates.append({
            "code":      d["code"],
            "name":      d.get("name", d["code"]),
            "buy_score": d.get("score", 0),
            "_sc_signal": False,
            "market_cap_b": None,
        })

    config   = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    run_time = date

    print(f"[repush] 小盘策略  cands={len(candidates)}"
          f"  regime={regime_score:.1f}/{regime_label}  dry_run={dry_run}"
          f"  (注：_sc_signal 无法从快照恢复，均标 False)")
    _small_push(candidates, regime_score, regime_label, run_time, config, dry_run=dry_run)


def main() -> None:
    parser = argparse.ArgumentParser(description="重推指定日期 nightly_scan 结果")
    parser.add_argument("--date",     required=True, help="日期 YYYY-MM-DD")
    parser.add_argument("--dry-run",  action="store_true", help="只打印，不推送")
    parser.add_argument("--strategy", choices=["main", "small", "all"], default="all")
    args = parser.parse_args()

    if args.strategy in ("main", "all"):
        _repush_main(args.date, args.dry_run)
    if args.strategy in ("small", "all"):
        _repush_small(args.date, args.dry_run)


if __name__ == "__main__":
    main()
