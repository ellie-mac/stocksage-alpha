"""
src/report/records.py — XHS 日记录管理

负责管理小红书连续记录实验的日记文件、元数据和发帖历史。
所有 I/O 函数都在这里，reporter.py 只做计算和格式化。
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

XHS_DIR   = Path(__file__).parent
REPO_ROOT = XHS_DIR.parent

RECORDS_DIR = XHS_DIR / "records"
POSTS_DIR   = RECORDS_DIR / "posts"
META_FILE   = RECORDS_DIR / "meta.json"

MILESTONE_DAYS = {7, 14, 21, 30, 60, 90}


def ensure_dirs() -> None:
    RECORDS_DIR.mkdir(exist_ok=True)
    POSTS_DIR.mkdir(exist_ok=True)


def load_meta() -> dict:
    if META_FILE.exists():
        return json.loads(META_FILE.read_text(encoding="utf-8"))
    return {
        "start_date":       str(date.today()),
        "day_count":        0,
        "last_record_date": None,
        "last_query":       "综合",
    }


def save_meta(meta: dict) -> None:
    META_FILE.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def today_record_file() -> Path:
    return RECORDS_DIR / f"{date.today()}.json"


def load_today() -> dict:
    f = today_record_file()
    return json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}


def load_yesterday() -> dict:
    f = RECORDS_DIR / f"{date.today() - timedelta(days=1)}.json"
    return json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}


def save_today(record: dict) -> None:
    today_record_file().write_text(
        json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def get_day_number() -> int:
    """Increment and return today's day number. Idempotent within a single calendar day."""
    meta  = load_meta()
    today = str(date.today())
    if meta.get("last_record_date") != today:
        meta["day_count"] = meta.get("day_count", 0) + 1
        meta["last_record_date"] = today
        save_meta(meta)
    return meta["day_count"]


def load_recent_history(n: int = 90) -> list[dict]:
    records = []
    for p in sorted(RECORDS_DIR.glob("????-??-??.json"), reverse=True)[:n]:
        try:
            records.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            pass
    return list(reversed(records))


def save_post_file(content: str, slot: str, style: int | str) -> Path:
    fname = POSTS_DIR / f"{date.today()}_{slot}_s{style}.txt"
    fname.write_text(content, encoding="utf-8-sig")
    return fname
