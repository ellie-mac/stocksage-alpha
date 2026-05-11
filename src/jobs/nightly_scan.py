#!/usr/bin/env python3
"""
夜间扫描编排器 — 依次运行主策略、小盘策略、ETF策略

用法：
    python -X utf8 src/jobs/nightly_scan.py
    python -X utf8 src/jobs/nightly_scan.py --dry-run
    python -X utf8 src/jobs/nightly_scan.py --only main
    python -X utf8 src/jobs/nightly_scan.py --only small
    python -X utf8 src/jobs/nightly_scan.py --only etf
    python -X utf8 src/jobs/nightly_scan.py --force   # 非交易日强制运行

Windows 任务计划示例（每日 22:00）：
    pythonw -X utf8 C:/path/to/src/jobs/nightly_scan.py
"""
from __future__ import annotations

import argparse
import json
import sys
import os
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))           # src/jobs/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # src/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "strategies"))

from run_manifest import start_run, finish_run, get_failed_runs  # noqa: E402
from logger import get_logger, bind_run_id                       # noqa: E402

_ROOT = Path(__file__).resolve().parent.parent.parent  # repo root
log = get_logger("nightly_scan")


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _load_config() -> dict:
    cfg = _ROOT / "alert_config.json"
    return json.loads(cfg.read_text(encoding="utf-8")) if cfg.exists() else {}


def _run_strategy(
    label: str,
    job_name: str,
    strategy_name: str,
    config: dict,
    dry_run: bool,
) -> bool:
    """运行单个策略。返回 True 表示成功。"""
    trade_date = datetime.now().strftime("%Y-%m-%d")
    print(f"\n[nightly_scan {_ts()}] === {label} ===")
    run_id = start_run(job_name, trade_date)
    if run_id is None:
        print(f"[nightly_scan {_ts()}] {label} 今日已运行或进行中，跳过")
        log.info("strategy_skipped", extra={"strategy": job_name, "trade_date": trade_date})
        return True  # skip is not a failure

    bind_run_id(run_id)
    log.info("strategy_started", extra={"strategy": job_name, "trade_date": trade_date, "dry_run": dry_run})

    t0 = time.monotonic()
    ok = False
    err_msg = None
    artifacts: list[str] | None = None

    try:
        from strategies.base import get_strategy
        strategy = get_strategy(strategy_name)
        result = strategy.run(config, dry_run=dry_run)  # never raises

        ok = not result.metadata.get("failed", False)
        if not ok:
            err_msg = result.metadata.get("error", "strategy reported failed")
            log.error("strategy_failed", extra={"strategy": job_name, "error": (err_msg or "")[:300]})
        else:
            signal_count = len(result.signals)
            if signal_count == 0:
                log.warning("strategy_zero_signals", extra={"strategy": job_name, "trade_date": trade_date})
            artifacts = [
                f"signals={signal_count}",
                f"regime={result.regime_label or '?'}",
            ]
            # snapshot save: best-effort, skipped in dry-run
            if not dry_run:
                try:
                    from snapshot_store import save_snapshot
                    snap_count = save_snapshot(
                        date=trade_date,
                        source=strategy_name,
                        signals=result.signals,
                        run_id=run_id,
                        regime_score=result.regime_score,
                        regime_label=result.regime_label,
                    )
                    artifacts.append(f"snapshot={snap_count}")
                    log.info("snapshot_saved", extra={"strategy": job_name, "rows": snap_count})
                except Exception:
                    snap_err = traceback.format_exc()
                    log.error("snapshot_failed", extra={"strategy": job_name, "error": snap_err[:200]})
                    artifacts.append("snapshot=failed")
            # publish() is best-effort: push failure doesn't mark the run as failed
            try:
                strategy.publish(result, config, dry_run=dry_run)
            except Exception:
                pub_err = traceback.format_exc()
                print(f"[nightly_scan] {label} 推送失败:\n{pub_err}")
                log.error("publish_failed", extra={"strategy": job_name, "error": pub_err[:300]})
                artifacts.append("publish=failed")

    except Exception:
        err_msg = traceback.format_exc()
        print(f"[nightly_scan] {label} 异常:\n{err_msg}")
        log.error("strategy_error", extra={"strategy": job_name, "error": err_msg[:300]})

    duration = round(time.monotonic() - t0, 1)
    finish_run(run_id, ok, duration_sec=duration, artifacts=artifacts, error=err_msg)
    log.info("strategy_finished", extra={
        "strategy": job_name, "success": ok, "duration_sec": duration,
    })
    bind_run_id(None)
    return ok


def _alert_failures(trade_date: str, dry_run: bool) -> None:
    """扫描完成后推送今日失败任务通知。"""
    try:
        from notify.notify_failure import send_failure_alert
        today_failed = [
            r for r in get_failed_runs(days=1)
            if r.get("trade_date") == trade_date
        ]
        if today_failed:
            sent = send_failure_alert(today_failed, dry_run=dry_run)
            log.info("failure_alert_sent", extra={"count": sent, "dry_run": dry_run})
        else:
            log.info("failure_alert_skipped", extra={"reason": "no failures today"})
    except Exception:
        log.error("failure_alert_error", extra={"error": traceback.format_exc()[:300]})


def _write_liveness(
    trade_date: str,
    attempted: int,
    succeeded: int,
    failures: list[str],
    duration_sec: float,
    status: str = "ok",
) -> None:
    """原子写 data/last_run.json —— 用于外部监控判断夜跑是否正常完成。
    status: 'ok' | 'skipped'（非交易日跳过）
    """
    try:
        out = _ROOT / "data" / "last_run.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "completed_at":         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "trade_date":           trade_date,
            "status":               status,
            "duration_sec":         round(duration_sec, 1),
            "strategies_attempted": attempted,
            "strategies_succeeded": succeeded,
            "failures":             failures,
        }
        tmp = out.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(out)
        log.info("liveness_written", extra={"path": str(out), "status": status})
    except Exception:
        log.error("liveness_write_failed", extra={"error": traceback.format_exc()[:200]})


def _backup_db(backup_dir: Path | None = None) -> None:
    """每日备份 stocksage.db（VACUUM INTO），保留最近14天；旧文件移至 archive/ 子目录。"""
    try:
        import shutil
        from db import DB_PATH, _conn

        bdir = backup_dir if backup_dir is not None else _ROOT / "data" / "backups"
        bdir.mkdir(parents=True, exist_ok=True)

        dest = bdir / f"stocksage-{datetime.now():%Y%m%d}.db"
        if not dest.exists():
            with _conn() as conn:
                conn.execute(f"VACUUM INTO '{dest}'")
            log.info("db_backed_up", extra={"path": str(dest), "source": str(DB_PATH)})

        cutoff = datetime.now() - timedelta(days=14)
        archive = bdir / "archive"
        for f in sorted(bdir.glob("stocksage-*.db")):
            try:
                file_date = datetime.strptime(f.stem, "stocksage-%Y%m%d")
                if file_date < cutoff:
                    archive.mkdir(exist_ok=True)
                    shutil.move(str(f), str(archive / f.name))
                    log.info("db_backup_archived", extra={"file": f.name})
            except ValueError:
                pass
    except Exception:
        log.error("db_backup_failed", extra={"error": traceback.format_exc()[:300]})


def _run_backfill(dry_run: bool) -> None:
    """回填 forward return；best-effort，失败不影响主流程。"""
    try:
        from backfill_returns import run_backfill
        summary = run_backfill(dry_run=dry_run)
        log.info("backfill_done", extra={
            "updated_5d": summary.get("updated_5d", 0),
            "updated_20d": summary.get("updated_20d", 0),
            "dry_run": dry_run,
        })
    except Exception:
        log.error("backfill_error", extra={"error": traceback.format_exc()[:300]})


def _pre_run_checks(force: bool = False) -> bool:
    """前置质量门控：交易日检查。
    返回 True 表示可以继续，False 表示应跳过。
    calendar 获取失败时保守地放行（fail open），避免因日历接口抖动误杀夜跑。
    """
    try:
        from trading_calendar import is_trading_day
        if not is_trading_day():
            if force:
                print(f"[nightly_scan {_ts()}] 非交易日，--force 强制继续")
                log.info("pre_check_non_trading_day_forced")
            else:
                print(f"[nightly_scan {_ts()}] 今日非交易日，跳过 (--force 可强制运行)")
                log.info("nightly_scan_skipped", extra={"reason": "non_trading_day"})
                return False
    except Exception:
        log.warning("pre_check_calendar_error", extra={"error": traceback.format_exc()[:200]})
    return True


def run_main(config: dict, dry_run: bool) -> bool:
    return _run_strategy("主策略", "nightly_scan/main_strategy", "main", config, dry_run)


def run_small(config: dict, dry_run: bool) -> bool:
    return _run_strategy("小盘策略", "nightly_scan/small_strategy", "small", config, dry_run)


def run_etf(config: dict, dry_run: bool) -> bool:
    return _run_strategy("ETF策略", "nightly_scan/etf_strategy", "etf", config, dry_run)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="非交易日也强制运行")
    parser.add_argument(
        "--only", choices=["main", "small", "etf"],
        help="只运行指定策略"
    )
    args = parser.parse_args()

    trade_date = datetime.now().strftime("%Y-%m-%d")

    if not _pre_run_checks(force=args.force):
        _write_liveness(trade_date, 0, 0, [], 0.0, status="skipped")
        return
    t_total = time.monotonic()

    print(f"[nightly_scan {_ts()}] 开始夜间扫描 dry_run={args.dry_run}")
    log.info("nightly_scan_started", extra={"dry_run": args.dry_run})

    config = _load_config()

    results: dict[str, bool] = {}
    if args.only == "main":
        results["main"] = run_main(config, args.dry_run)
    elif args.only == "small":
        results["small"] = run_small(config, args.dry_run)
    elif args.only == "etf":
        results["etf"] = run_etf(config, args.dry_run)
    else:
        results["main"]  = run_main(config, args.dry_run)
        results["small"] = run_small(config, args.dry_run)
        results["etf"]   = run_etf(config, args.dry_run)

    attempted  = len(results)
    succeeded  = sum(1 for v in results.values() if v)
    failures   = [k for k, v in results.items() if not v]

    _alert_failures(trade_date, args.dry_run)
    _write_liveness(trade_date, attempted, succeeded, failures,
                    time.monotonic() - t_total)
    _backup_db()
    _run_backfill(args.dry_run)

    print(f"\n[nightly_scan {_ts()}] 全部完成 ({succeeded}/{attempted} 成功)")
    log.info("nightly_scan_finished", extra={"succeeded": succeeded, "attempted": attempted})


if __name__ == "__main__":
    main()
