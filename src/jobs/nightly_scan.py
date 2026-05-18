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
import multiprocessing
import subprocess
import sys
import os
import socket
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

_STRATEGY_TIMEOUT_SEC = 7200   # 每个策略最长 120 分钟（5084 票 × ~6s/票 ÷ 16 workers ≈ 30min，留 4× buffer）
_SOCKET_TIMEOUT_SEC   = 120    # 单次 akshare HTTP 调用最长 2 分钟
_BACKFILL_TIMEOUT_SEC = 300    # backfill 最长 5 分钟


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _strategy_worker(
    strategy_name: str,
    config: dict,
    dry_run: bool,
    run_id: int,
    trade_date: str,
    result_q: "multiprocessing.Queue",
    no_push: bool = False,
) -> None:
    """Runs in a child process. Returns result via Queue so the parent can kill()
    the entire process tree on timeout — unlike threads, which cannot be forcibly
    stopped and would keep the nightly_scan process alive for hours."""
    _src_dir = Path(__file__).resolve().parent.parent
    for _p in [str(_src_dir), str(_src_dir / "jobs"), str(_src_dir / "strategies")]:
        if _p not in sys.path:
            sys.path.insert(0, _p)

    import traceback as _tb
    ok = False
    err_msg = None
    artifacts = None

    try:
        from logger import bind_run_id as _bind
        _bind(run_id)

        from strategies.base import get_strategy
        strategy = get_strategy(strategy_name)
        result = strategy.run(config, dry_run=dry_run)

        ok = not result.metadata.get("failed", False)
        if not ok:
            err_msg = result.metadata.get("error", "strategy reported failed")
        else:
            signal_count = len(result.signals)
            artifacts = [
                f"signals={signal_count}",
                f"regime={result.regime_label or '?'}",
            ]
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
                except Exception:
                    artifacts.append("snapshot=failed")
            try:
                strategy.save(result, dry_run=dry_run)
            except Exception:
                artifacts.append("save_json=failed")
            if not no_push:
                try:
                    strategy.publish(result, config, dry_run=dry_run)
                except Exception:
                    artifacts.append("publish=failed")
                    ok = False  # push 失败 → run_manifest 记 failed → 允许重跑
                    try:
                        sys.stderr.buffer.write(f"[strategy_worker] publish failed:\n{_tb.format_exc()}\n".encode("utf-8", errors="replace"))
                        sys.stderr.buffer.flush()
                    except Exception:
                        pass
    except Exception:
        err_msg = _tb.format_exc()
        try:
            sys.stderr.buffer.write(f"[strategy_worker] exception:\n{err_msg}\n".encode("utf-8", errors="replace"))
            sys.stderr.buffer.flush()
        except Exception:
            pass

    result_q.put({"ok": ok, "err_msg": err_msg, "artifacts": artifacts})
    try:
        result_q.close()
        result_q.join_thread()  # flush queue buffer to pipe before force-exit
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    os._exit(0)  # bypass Python thread-join cleanup; avoids hang when strategy used ThreadPoolExecutor


def _load_config() -> dict:
    cfg = _ROOT / "alert_config.json"
    return json.loads(cfg.read_text(encoding="utf-8")) if cfg.exists() else {}


def _run_strategy(
    label: str,
    job_name: str,
    strategy_name: str,
    config: dict,
    dry_run: bool,
    no_push: bool = False,
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
        result_q: multiprocessing.Queue = multiprocessing.Queue()
        proc = multiprocessing.Process(
            target=_strategy_worker,
            args=(strategy_name, config, dry_run, run_id, trade_date, result_q, no_push),
            daemon=False,
        )
        proc.start()
        proc.join(timeout=_STRATEGY_TIMEOUT_SEC)

        if proc.is_alive():
            proc.terminate()
            proc.join(10)
            if proc.is_alive():
                proc.kill()
                proc.join(5)
            raise TimeoutError(
                f"{label} 运行超时 ({_STRATEGY_TIMEOUT_SEC // 60} 分钟)"
            )

        if proc.exitcode != 0:
            err_msg = f"{label} subprocess 退出码 {proc.exitcode}"
            log.error("strategy_error", extra={"strategy": job_name, "error": err_msg})
        else:
            try:
                res = result_q.get_nowait()
                ok = res.get("ok", False)
                err_msg = res.get("err_msg")
                artifacts = res.get("artifacts")
                if ok and not artifacts:
                    artifacts = []
                if ok and any(a == "signals=0" for a in (artifacts or [])):
                    log.warning("strategy_zero_signals", extra={"strategy": job_name, "trade_date": trade_date})
                if not ok:
                    log.error("strategy_failed", extra={"strategy": job_name, "error": (err_msg or "")[:300]})
            except Exception:
                err_msg = "subprocess 未返回结果"
                log.error("strategy_error", extra={"strategy": job_name, "error": err_msg})

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


def _cleanup_data_json(retain_days: int = 30) -> None:
    """将 data/ 下超过 retain_days 天的日期型 JSON 文件移至 archive/ 子目录。
    匹配模式: *_YYYYMMDD.json，*_latest.json 不受影响。best-effort，失败不中断主流程。
    """
    try:
        import re, shutil
        cutoff = datetime.now() - timedelta(days=retain_days)
        data_dir = _ROOT / "data"
        archive = data_dir / "archive"
        _date_pattern = re.compile(r"_(\d{8})\.json$")
        moved = 0
        for f in data_dir.glob("*_????????.json"):
            m = _date_pattern.search(f.name)
            if not m:
                continue
            try:
                file_date = datetime.strptime(m.group(1), "%Y%m%d")
            except ValueError:
                continue
            if file_date < cutoff:
                archive.mkdir(exist_ok=True)
                shutil.move(str(f), str(archive / f.name))
                moved += 1
        if moved:
            log.info("data_json_archived", extra={"count": moved, "retain_days": retain_days})
    except Exception:
        log.error("data_json_cleanup_failed", extra={"error": traceback.format_exc()[:300]})


def _run_backfill(dry_run: bool) -> None:
    """回填 forward return；best-effort，失败不影响主流程。
    用子进程运行，_BACKFILL_TIMEOUT_SEC 到期后强制终止，避免 baostock 锁竞争导致无限阻塞。
    """
    try:
        script = str(_ROOT / "src" / "jobs" / "backfill_returns.py")
        cmd = [sys.executable, "-X", "utf8", script]
        if dry_run:
            cmd.append("--dry-run")
        result = subprocess.run(
            cmd,
            timeout=_BACKFILL_TIMEOUT_SEC,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode == 0:
            log.info("backfill_done", extra={"output": result.stdout.strip()[:200], "dry_run": dry_run})
        else:
            log.error("backfill_error", extra={"returncode": result.returncode,
                                                "stderr": result.stderr.strip()[:300]})
    except subprocess.TimeoutExpired:
        log.error("backfill_timeout", extra={"timeout_sec": _BACKFILL_TIMEOUT_SEC})
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


def run_main(config: dict, dry_run: bool, job_prefix: str = "nightly_scan", no_push: bool = False) -> bool:
    return _run_strategy("主策略", f"{job_prefix}/main_strategy", "main", config, dry_run, no_push)


def run_small(config: dict, dry_run: bool, job_prefix: str = "nightly_scan", no_push: bool = False) -> bool:
    return _run_strategy("小盘策略", f"{job_prefix}/small_strategy", "small", config, dry_run, no_push)


def run_etf(config: dict, dry_run: bool, job_prefix: str = "nightly_scan", no_push: bool = False) -> bool:
    return _run_strategy("ETF策略", f"{job_prefix}/etf_strategy", "etf", config, dry_run, no_push)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force",    action="store_true", help="非交易日也强制运行")
    parser.add_argument("--no-push",  action="store_true", help="只扫描存 JSON，跳过微信推送")
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

    socket.setdefaulttimeout(_SOCKET_TIMEOUT_SEC)
    print(f"[nightly_scan {_ts()}] 开始夜间扫描 dry_run={args.dry_run}")
    log.info("nightly_scan_started", extra={"dry_run": args.dry_run})

    config = _load_config()

    no_push = args.no_push
    results: dict[str, bool] = {}
    if args.only == "main":
        results["main"] = run_main(config, args.dry_run, job_prefix="main_scan", no_push=no_push)
    elif args.only == "small":
        results["small"] = run_small(config, args.dry_run, job_prefix="small_scan", no_push=no_push)
    elif args.only == "etf":
        results["etf"] = run_etf(config, args.dry_run, job_prefix="etf_scan", no_push=no_push)
    else:
        results["main"]  = run_main(config, args.dry_run, no_push=no_push)
        results["small"] = run_small(config, args.dry_run, no_push=no_push)
        results["etf"]   = run_etf(config, args.dry_run, no_push=no_push)

    attempted  = len(results)
    succeeded  = sum(1 for v in results.values() if v)
    failures   = [k for k, v in results.items() if not v]

    _alert_failures(trade_date, args.dry_run)
    _write_liveness(trade_date, attempted, succeeded, failures,
                    time.monotonic() - t_total)
    _backup_db()
    _cleanup_data_json()
    _run_backfill(args.dry_run)

    print(f"\n[nightly_scan {_ts()}] 全部完成 ({succeeded}/{attempted} 成功)")
    log.info("nightly_scan_finished", extra={"succeeded": succeeded, "attempted": attempted})
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
