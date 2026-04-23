#!/usr/bin/env python3
"""
scripts/integrity_check.py — 每小时数据完整性检查

设计：
  - 由 Windows 定时任务每小时触发（08:00 起，每小时一次到 23:00）
  - 维护当日验证状态 data/.integrity_state.json
  - daily=True 的项目：当日首次通过检查后，后续运行自动跳过（不重复检查）
  - 快速可修复项（universe / market_data / concept_map）：自动触发修复
  - 耗时任务（chip_scan / price_cache / batch_financials）：只发通知，不重复触发
  - 通知去重：同一项目当日只通知一次

用法:
    python -X utf8 scripts/integrity_check.py
    python -X utf8 scripts/integrity_check.py --force    # 忽略已验证状态，强制重检所有项
    python -X utf8 scripts/integrity_check.py --dry-run  # 只检查，不修复，不发通知
"""
from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path

ROOT        = Path(__file__).resolve().parent.parent
SCRIPTS     = Path(__file__).resolve().parent
DATA        = ROOT / "data"
CACHE       = SCRIPTS / "cache"
TOOLS_CACHE = SCRIPTS / "tools" / ".cache"
STATE_FILE  = DATA / ".integrity_state.json"
LOG_DIR     = SCRIPTS / "logs"

sys.path.insert(0, str(SCRIPTS))
PYTHON = sys.executable


# ── helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _today_str() -> str:
    return date.today().strftime("%Y-%m-%d")


def _is_weekend() -> bool:
    return datetime.now().weekday() >= 5


def _max_stale_h(daily_h: int, weekend_h: int) -> int:
    """Return appropriate staleness threshold given current day/time.

    Weekends and Monday-before-close are treated the same: data from the
    previous Friday is expected to be up to ~87h old.
    """
    now = datetime.now()
    wd  = now.weekday()   # 0=Mon … 6=Sun
    if wd in (5, 6):                    # Saturday / Sunday
        return weekend_h
    if wd == 0 and now.hour < 16:       # Monday before ~market-close; Friday data still valid
        return weekend_h
    return daily_h


def _mtime_age_h(path: Path) -> float:
    try:
        return (time.time() - path.stat().st_mtime) / 3600
    except OSError:
        return float("inf")


def _load_state() -> dict:
    today = _today_str()
    try:
        s = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if s.get("date") == today:
            return s
    except Exception:
        pass
    return {"date": today, "verified": {}, "notified": {}}


def _save_state(state: dict) -> None:
    try:
        DATA.mkdir(exist_ok=True)
        STATE_FILE.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        print(f"[integrity] 状态保存失败: {e}", flush=True)


def _run_fix(cmd: list[str], log_name: str, timeout: int = 300) -> bool:
    LOG_DIR.mkdir(exist_ok=True)
    log_path = LOG_DIR / f"integrity_fix_{log_name}.log"
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(ROOT),
            timeout=timeout,
        )
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n=== {datetime.now().isoformat()} ===\n{result.stdout}")
            if result.stderr:
                f.write(result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"  [fix] 执行失败 {log_name}: {e}", flush=True)
        return False


def _notify(title: str, body: str) -> None:
    try:
        cfg = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        from common import configure_pushplus, send_wechat
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        send_wechat(title, body, sendkey)
        print(f"  [notify] ✓ 已推送", flush=True)
    except Exception as e:
        print(f"  [notify] 推送失败: {e}", flush=True)


# ── individual checks ─────────────────────────────────────────────────────────

def check_universe() -> tuple[bool, str]:
    p = DATA / "universe_main.json"
    if not p.exists():
        return False, "文件不存在"
    age_h = _mtime_age_h(p)
    try:
        codes = json.loads(p.read_text(encoding="utf-8"))
        cnt = len(codes)
    except Exception:
        return False, "JSON 损坏"
    if cnt < 4000:
        return False, f"股票数过少: {cnt}"
    if age_h > 48:
        return False, f"文件过旧: {age_h:.0f}h（main_Night 应每日 22:30 重新生成）"
    return True, f"{cnt} 只，{age_h:.0f}h 前更新"


def fix_universe() -> bool:
    print("  [fix] 重新生成 universe_main.json ...", flush=True)
    return _run_fix(
        [PYTHON, "-X", "utf8", str(SCRIPTS / "tools" / "generate_full_universe.py")],
        "universe",
    )


def check_stock_names() -> tuple[bool, str]:
    p = DATA / "stock_names.json"
    if not p.exists():
        return False, "文件不存在"
    age_h = _mtime_age_h(p)
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        cnt = len(d)
    except Exception:
        return False, "JSON 损坏"
    if cnt < 1000:
        return False, f"条目过少: {cnt}"
    if age_h > 8 * 24:
        return False, f"文件过旧: {age_h:.0f}h（chip_Night 会每周自动刷新）"
    return True, f"{cnt} 条，{age_h:.0f}h 前更新"


def check_market_data() -> tuple[bool, str]:
    market_dir = CACHE / "market"
    issues = []
    for prefix in ("market_regime", "market_valuation"):
        files = list(market_dir.glob(f"{prefix}*.json")) if market_dir.exists() else []
        if not files:
            issues.append(f"{prefix} 缺失")
            continue
        newest = max(files, key=lambda p: p.stat().st_mtime)
        age_h = _mtime_age_h(newest)
        max_age = _max_stale_h(daily_h=25, weekend_h=72)
        if age_h > max_age:
            issues.append(f"{prefix} 过旧 {age_h:.0f}h")
    if issues:
        return False, "；".join(issues)
    return True, "market_regime + market_valuation 已就绪"


def fix_market_data() -> bool:
    print("  [fix] 预热市场数据 ...", flush=True)
    return _run_fix(
        [PYTHON, "-X", "utf8", str(SCRIPTS / "prefetch.py"), "--market"],
        "market_data",
    )


def check_concept_map() -> tuple[bool, str]:
    concept_dir = CACHE / "concept"
    files = list(concept_dir.glob("concept_reverse*.json")) if concept_dir.exists() else []
    if not files:
        return False, "concept_reverse 缓存不存在"
    newest = max(files, key=lambda p: p.stat().st_mtime)
    age_h = _mtime_age_h(newest)
    # Concept reverse map TTL is 6h; warn if stale beyond 7h (1h grace)
    if age_h > 7:
        return False, f"缓存过旧: {age_h:.1f}h（TTL=6h）"
    return True, f"{age_h:.1f}h 前更新"


def fix_concept_map() -> bool:
    print("  [fix] 预热概念 map ...", flush=True)
    return _run_fix(
        [PYTHON, "-X", "utf8", str(SCRIPTS / "prefetch.py"), "--concept"],
        "concept_map",
    )


def check_chip_scan() -> tuple[bool, str]:
    p = DATA / "chip_scan_latest.json"
    if not p.exists():
        return False, "文件不存在"
    age_h = _mtime_age_h(p)
    # chip_Night runs at 18:00 daily; chip_Premarket is fallback at 07:00
    max_age = _max_stale_h(daily_h=36, weekend_h=72)
    if age_h > max_age:
        return False, f"文件过旧: {age_h:.0f}h（chip_Night 应每日 18:00 更新）"
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        scan_date = d.get("date", "?")
        return True, f"date={scan_date}，{age_h:.0f}h 前更新"
    except Exception:
        return False, "JSON 损坏"


def check_price_cache() -> tuple[bool, str]:
    universe_path = DATA / "universe_main.json"
    if not universe_path.exists():
        return False, "universe_main.json 不存在，无法抽样"
    try:
        codes = json.loads(universe_path.read_text(encoding="utf-8"))
    except Exception:
        return False, "universe_main.json 读取失败"
    if not codes:
        return False, "universe 为空"

    price_dir = CACHE / "price"
    sample = random.sample(codes, min(30, len(codes)))
    fresh = 0
    max_age = _max_stale_h(daily_h=25, weekend_h=72)
    for code in sample:
        p = price_dir / f"price_{code}_550.json"
        if p.exists():
            try:
                entry = json.loads(p.read_text(encoding="utf-8"))
                if time.time() - entry.get("ts", 0) < max_age * 3600:
                    fresh += 1
            except Exception:
                pass

    ratio = fresh / len(sample)
    msg = f"覆盖率 {ratio*100:.0f}% ({fresh}/{len(sample)} 抽样)"
    return ratio >= 0.5, msg


def check_batch_financials() -> tuple[bool, str]:
    p = TOOLS_CACHE / "batch_financials.csv"
    if not p.exists():
        return False, "batch_financials.csv 不存在"
    age_h = _mtime_age_h(p)
    if age_h > 15 * 24:
        return False, f"文件过旧: {age_h/24:.1f} 天（TTL=14 天，main_Night 每日 22:30 更新）"
    size = p.stat().st_size
    if size < 1024:
        return False, f"文件异常小: {size} bytes"
    return True, f"{size // 1024} KB，{age_h / 24:.1f} 天前更新"


def check_latest_picks() -> tuple[bool, str]:
    p = DATA / "latest_picks.json"
    if not p.exists():
        return False, "文件不存在"
    age_h = _mtime_age_h(p)
    max_age = _max_stale_h(daily_h=36, weekend_h=72)
    if age_h > max_age:
        return False, f"文件过旧: {age_h:.0f}h（main_Scan 应每日 18:30 更新）"
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        results = d.get("results", [])
        ts = d.get("timestamp", "?")
        ts_short = ts[:16] if isinstance(ts, str) else str(ts)
        return True, f"{len(results)} 只选股，{ts_short}，{age_h:.0f}h 前更新"
    except Exception:
        return False, "JSON 损坏"


# ── check registry ────────────────────────────────────────────────────────────

# Each item: id, desc, check fn, fix fn (or None), daily-skip flag
CHECKS: list[dict] = [
    {
        "id":    "universe",
        "desc":  "股票池 universe_main.json",
        "check": check_universe,
        "fix":   fix_universe,   # auto-fix: ~30s, no quota
        "daily": True,
    },
    {
        "id":    "stock_names",
        "desc":  "股票名称 stock_names.json",
        "check": check_stock_names,
        "fix":   None,           # requires Tushare quota; chip_Night handles weekly refresh
        "daily": True,
    },
    {
        "id":    "market_data",
        "desc":  "市场数据（CSI300/PE/行业）",
        "check": check_market_data,
        "fix":   fix_market_data,  # auto-fix: ~1min, free
        "daily": True,
    },
    {
        "id":    "concept_map",
        "desc":  "概念板块反查 map",
        "check": check_concept_map,
        "fix":   fix_concept_map,  # auto-fix: ~30s, free; TTL=6h so may fail mid-day
        "daily": False,            # don't skip: TTL=6h means it expires within the day
    },
    {
        "id":    "chip_scan",
        "desc":  "当日筹码扫描 chip_scan_latest.json",
        "check": check_chip_scan,
        "fix":   None,           # heavy ~1.5h; chip_Night handles it
        "daily": True,
    },
    {
        "id":    "price_cache",
        "desc":  "价格历史缓存（全市场抽样）",
        "check": check_price_cache,
        "fix":   None,           # heavy ~1.5h; price_Prefetch handles it
        "daily": True,
    },
    {
        "id":    "batch_financials",
        "desc":  "批量财务数据 batch_financials.csv",
        "check": check_batch_financials,
        "fix":   None,           # heavy, runs overnight via main_Night
        "daily": True,
    },
    {
        "id":    "latest_picks",
        "desc":  "最新选股 latest_picks.json",
        "check": check_latest_picks,
        "fix":   None,           # depends on main_Scan (18:30); no auto-fix
        "daily": True,
    },
]


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="数据完整性检查")
    parser.add_argument("--force",   action="store_true", help="忽略已验证状态，强制重检所有项")
    parser.add_argument("--dry-run", action="store_true", help="只检查，不修复，不发通知")
    args = parser.parse_args()

    state    = _load_state()
    verified = state.setdefault("verified", {})
    notified = state.setdefault("notified", {})

    print(
        f"[integrity] {_today_str()} {_now()}"
        f"{' (dry-run)' if args.dry_run else ''}"
        f"{' (weekend)' if _is_weekend() else ''}",
        flush=True,
    )

    n_ok = n_skip = n_fail = 0

    for item in CHECKS:
        item_id   = item["id"]
        item_desc = item["desc"]

        # Skip if already verified today (and daily-skip is enabled)
        if item["daily"] and not args.force and verified.get(item_id):
            print(f"  ⊙ {item_desc}  [已验证于 {verified[item_id]}，跳过]", flush=True)
            n_skip += 1
            continue

        ok, msg = item["check"]()

        if ok:
            verified[item_id] = _now()
            print(f"  ✓ {item_desc}  {msg}", flush=True)
            n_ok += 1
            continue

        # Check failed — try auto-fix first
        print(f"  ✗ {item_desc}  {msg}", flush=True)
        fixed_ok = False

        if item.get("fix") and not args.dry_run:
            if item["fix"]():
                ok2, msg2 = item["check"]()
                if ok2:
                    verified[item_id] = _now()
                    print(f"  ✓ {item_desc}  修复成功: {msg2}", flush=True)
                    n_ok += 1
                    fixed_ok = True
                else:
                    print(f"  ✗ {item_desc}  修复后仍失败: {msg2}", flush=True)

        if not fixed_ok:
            n_fail += 1
            # Notify at most once per item per day
            if not notified.get(item_id) and not args.dry_run:
                _notify(
                    f"⚠️ 数据完整性检查失败: {item_desc}",
                    f"**{item_desc}**\n原因: {msg}\n\n"
                    f"时间: {_today_str()} {_now()}\n"
                    f"请检查相关定时任务日志，必要时手动重跑。",
                )
                notified[item_id] = _now()
            elif notified.get(item_id):
                print(f"  [notify] 今日已通知过，跳过重复推送", flush=True)

    print(
        f"\n[integrity] 完成  ✓{n_ok} 通过  ⊙{n_skip} 跳过  ✗{n_fail} 失败  {_now()}",
        flush=True,
    )

    _save_state(state)


if __name__ == "__main__":
    main()
