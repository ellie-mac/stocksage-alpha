#!/usr/bin/env python3
"""
hot_rank_logger.py — 定时抓取东方财富热榜并落盘，解决 look-ahead 偏差

使用：
    python -X utf8 scripts/tools/hot_rank_logger.py          # 抓一次并写盘
    python -X utf8 scripts/tools/hot_rank_logger.py --dry-run  # 只打印不写盘

定时任务示例（Windows Task Scheduler 或 cron）：
    每个交易日 10:00 / 11:00 / 13:30 / 14:30 各执行一次。
    Windows: schtasks /create /tn "HotRankLogger" /tr "python -X utf8 <path>" /sc DAILY /st 10:00

落盘格式：
    data/hot_rank_log/YYYYMMDD_HHMM.json   — 每次快照独立文件
    data/hot_rank_log/latest.json           — 最近一次快照（便于 hot_scan 读取）

每条记录包含 fetch_time（ISO 8601）、总数量，以及按排名升序的股票列表（code/name/rank）。
回测时使用 fetch_time 字段对齐信号可见窗口，避免未来信息泄露。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT    = Path(__file__).resolve().parent.parent.parent
_SCRIPTS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_SCRIPTS))

LOG_DIR     = _ROOT / "data" / "hot_rank_log"
LOG_LATEST  = LOG_DIR / "latest.json"


def _fetch_raw() -> list[dict]:
    """调用 akshare 抓取当前热榜，返回 [{code, name, rank}, ...]，按排名升序。"""
    import akshare as ak
    df = ak.stock_hot_rank_em()
    if df is None or df.empty:
        return []
    df.columns = [c.strip() for c in df.columns]
    code_col = next((c for c in df.columns if "代码" in c or c.lower() == "code"), None)
    name_col = next((c for c in df.columns if "名称" in c or c.lower() == "name"), None)
    rank_col = next((c for c in df.columns if "排名" in c or c.lower() == "rank"), None)
    if not code_col or not rank_col:
        raise ValueError(f"找不到必要列，实际列名: {df.columns.tolist()}")
    records = []
    for _, row in df.iterrows():
        code = str(row[code_col]).zfill(6)
        name = str(row[name_col]) if name_col else ""
        rank = int(row[rank_col])
        records.append({"code": code, "name": name, "rank": rank})
    records.sort(key=lambda r: r["rank"])
    return records


def capture(dry_run: bool = False) -> dict:
    """
    抓取热榜 → 构建 snapshot → 写盘（除非 dry_run）。
    返回 snapshot dict，供调用方直接使用。
    """
    fetch_time = datetime.now()
    fetch_time_str = fetch_time.strftime("%Y-%m-%dT%H:%M:%S")
    date_str  = fetch_time.strftime("%Y%m%d")
    hhmm_str  = fetch_time.strftime("%H%M")

    print(f"[hot_rank_logger] 抓取中... ({fetch_time_str})", flush=True)
    try:
        records = _fetch_raw()
    except Exception as e:
        print(f"[hot_rank_logger] 抓取失败: {e}", flush=True)
        return {}

    snapshot = {
        "fetch_time": fetch_time_str,
        "date":       date_str,
        "hhmm":       hhmm_str,
        "total":      len(records),
        "stocks":     records,
    }

    print(f"[hot_rank_logger] 抓到 {len(records)} 只股票", flush=True)

    if dry_run:
        print("[hot_rank_logger] dry-run，跳过写盘", flush=True)
        return snapshot

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_path = LOG_DIR / f"{date_str}_{hhmm_str}.json"
    _write_json(snapshot_path, snapshot)
    _write_json(LOG_LATEST, snapshot)
    print(f"[hot_rank_logger] 已写入 → {snapshot_path.name}", flush=True)
    return snapshot


def load_snapshot(date: str, hhmm: str) -> dict | None:
    """
    读取指定日期 + 时间点的快照，用于回测对齐。
    date: 'YYYYMMDD', hhmm: 'HHMM'（如 '1000'/'1330'）
    """
    path = LOG_DIR / f"{date}_{hhmm}.json"
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def list_snapshots(date: str = "") -> list[Path]:
    """返回指定日期（或全部）的快照文件列表，按时间升序。"""
    if not LOG_DIR.exists():
        return []
    pattern = f"{date}*.json" if date else "????????_????.json"
    return sorted(p for p in LOG_DIR.glob(pattern) if p.name != "latest.json")


def get_rank(code: str, snapshot: dict) -> int | None:
    """从 snapshot 中查询指定股票排名，不存在返回 None。"""
    code = str(code).zfill(6)
    for r in snapshot.get("stocks", []):
        if r["code"] == code:
            return r["rank"]
    return None


def _write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)  # atomic rename


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="抓取热榜快照并落盘")
    parser.add_argument("--dry-run", action="store_true", help="只抓取不写盘")
    parser.add_argument("--list",    action="store_true", help="列出已有快照")
    parser.add_argument("--date",    default="",          help="--list 时过滤日期 YYYYMMDD")
    args = parser.parse_args()

    if args.list:
        snaps = list_snapshots(args.date)
        if not snaps:
            print("暂无快照" + (f"（{args.date}）" if args.date else ""))
        else:
            for p in snaps:
                snap = json.loads(p.read_text(encoding="utf-8"))
                print(f"  {p.name}  total={snap.get('total', '?')}")
    else:
        result = capture(dry_run=args.dry_run)
        if not result:
            sys.exit(1)
