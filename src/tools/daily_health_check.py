#!/usr/bin/env python3
"""今日任务健康检查：DB runs + 关键输出文件新鲜度。"""
import sys, json
from datetime import datetime, date
from pathlib import Path

ROOT = Path("C:/Users/jiapeichen/repos/stocksage-alpha")
sys.path.insert(0, str(ROOT / "src"))
from db import _conn

today = date.today().strftime("%Y-%m-%d")
print(f"=== 今日 DB runs ({today}) ===")

with _conn() as c:
    rows = c.execute(
        "SELECT id, job_name, status, started_at, finished_at, artifacts, error "
        "FROM runs WHERE trade_date=? ORDER BY id",
        (today,),
    ).fetchall()

if not rows:
    print("  (无记录)")
else:
    for r in rows:
        d = dict(r)
        mark = "✓" if d["status"] == "succeeded" else ("✗" if d["status"] == "failed" else "…")
        started = (d["started_at"] or "")[-8:][:5]  # HH:MM
        finished = (d["finished_at"] or "")[-8:][:5]
        art = d.get("artifacts") or ""
        err_preview = (d.get("error") or "")[:120].replace("\n", " ")
        print(f"  {mark} [{started}-{finished}] {d['job_name']}")
        if art:
            print(f"      artifacts: {art}")
        if d["status"] == "failed" and err_preview:
            print(f"      error: {err_preview}")

print()
print("=== 关键输出文件新鲜度 ===")

def file_age_str(path: Path) -> str:
    if not path.exists():
        return "❌ 不存在"
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    age_h = (datetime.now() - mtime).total_seconds() / 3600
    ts = mtime.strftime("%m-%d %H:%M")
    flag = "✓" if age_h < 24 else "⚠"
    return f"{flag} {ts} ({age_h:.0f}h ago)"

files = [
    ("latest_picks.json",       ROOT / "data" / "latest_picks.json"),
    ("etf_scan_latest.json",    ROOT / "data" / "etf_scan_latest.json"),
    ("signals_log.json",        ROOT / "data" / "signals_log.json"),
    ("last_run.json",           ROOT / "data" / "last_run.json"),
    ("universe_main.json",      ROOT / "data" / "universe_main.json"),
    ("config.json",             ROOT / "config.json"),
]
for label, p in files:
    print(f"  {label:30s}  {file_age_str(p)}")

print()
print("=== 今日快照行数 ===")
with _conn() as c:
    snap_rows = c.execute(
        "SELECT source, count(*) cnt FROM snapshots WHERE date=? GROUP BY source ORDER BY source",
        (today,),
    ).fetchall()
    if snap_rows:
        for r in snap_rows:
            print(f"  {r['source']:20s}: {r['cnt']} rows")
    else:
        print("  (无快照)")

print()
print("=== signal_runs 最近5条 ===")
with _conn() as c:
    sig_rows = c.execute(
        "SELECT date, source, run_time, length(buy_signals) bs_len FROM signal_runs ORDER BY id DESC LIMIT 5"
    ).fetchall()
    if sig_rows:
        for r in sig_rows:
            print(f"  {dict(r)}")
    else:
        print("  (空)")
