#!/usr/bin/env python3
"""
scripts/run_cad_pipeline.py — 自愈筹码流水线

流程：
  1. 检查今日 AK 筹码缓存是否完整（>= MIN_ROWS 条）
  2. 不完整 → 运行 chip_Night（daily_chip_scan.py --ak --no-push）
  3. 运行 chip_CadScan（chip_cad.py --mods bekh bekhm）
  4. 任一步失败 → 发详细微信通知（步骤/退出码/日志尾部/修复命令）

chip_Night（18:00）作为独立预热任务保留，大多数情况 20:30 直接命中缓存。
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
LOGS    = SCRIPTS / "logs"
sys.path.insert(0, str(SCRIPTS))

PYTHON     = sys.executable
DAILY_SCAN = SCRIPTS / "daily_chip_scan.py"
CHIP_CAD   = SCRIPTS / "chip_cad.py"

MIN_ROWS = 4000   # AK 缓存低于此行数视为不完整（完整约 5000+）


# ── helpers ──────────────────────────────────────────────────────────────────

def _send_failure(step: str, rc: int, log_path: Path, fix_cmd: str) -> None:
    """发送详细失败通知：步骤名、退出码、日志尾 30 行、修复命令。"""
    try:
        from common import configure_pushplus, send_wechat
        cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))

        log_tail = ""
        if log_path.exists():
            lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            tail  = [l for l in lines[-50:] if l.strip()][-30:]
            log_tail = "\n".join(tail)

        title = f"⚠️ 筹码流水线失败: {step}"
        body  = (
            f"**步骤**: {step}\n"
            f"**退出码**: {rc}\n"
            f"**时间**: {datetime.now():%Y-%m-%d %H:%M:%S}\n\n"
            f"**日志末尾**（{log_path.name}）：\n"
            f"```\n{log_tail or '(空)'}\n```\n\n"
            f"**手动修复命令**：\n"
            f"```\n{fix_cmd}\n```"
        )
        send_wechat(title, body, sendkey)
        print(f"[pipeline] 失败通知已发送: {title}", flush=True)
    except Exception as e:
        print(f"[pipeline] 通知发送失败: {e}", flush=True)


def _ak_cache_rows(trade_date: str) -> int:
    """返回今日 AK 筹码缓存行数，0 表示不存在或为空。"""
    try:
        import cache as _cache
        from chip_strategy import _chip_cache_key, _CHIP_TTL
        raw = _cache.get(_chip_cache_key(trade_date, "ak"), _CHIP_TTL)
        if raw is None:
            return 0
        if isinstance(raw, dict) and raw.get("__type") == "dataframe":
            import io, pandas as pd
            df = pd.read_json(io.StringIO(raw["records"]), orient="records")
        else:
            import pandas as pd
            df = pd.DataFrame(raw)
        return len(df)
    except Exception as e:
        print(f"[pipeline] 检查 AK 缓存失败: {e}", flush=True)
        return 0


def _run(cmd: list[str], log_path: Path) -> int:
    """运行子进程，stdout/stderr 追加写入 log_path，返回退出码。"""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8", errors="replace") as f:
        f.write(f"\n{'='*60}\n[pipeline] 启动 {datetime.now():%Y-%m-%d %H:%M:%S}\n")
        f.flush()
        result = subprocess.run(cmd, stdout=f, stderr=f, text=True, encoding="utf-8",
                                errors="replace")
    return result.returncode


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    LOGS.mkdir(parents=True, exist_ok=True)

    from chip_strategy import _latest_trade_date
    trade_date = _latest_trade_date()
    print(f"[pipeline] 开始  trade_date={trade_date}  {datetime.now():%H:%M:%S}", flush=True)

    # ── Step 1: 检查 AK 缓存完整性 ─────────────────────────────────────────
    rows = _ak_cache_rows(trade_date)
    print(f"[pipeline] AK 缓存: {rows} 条（需 >= {MIN_ROWS}）", flush=True)

    if rows < MIN_ROWS:
        if rows > 0:
            print(f"[pipeline] 缓存不完整（{rows} < {MIN_ROWS}），重建", flush=True)
        else:
            print(f"[pipeline] 无今日缓存，启动 chip_Night", flush=True)

        # ── Step 2: chip_Night ──────────────────────────────────────────────
        night_log = LOGS / "chip_scan_night.log"
        night_cmd = [PYTHON, "-X", "utf8", str(DAILY_SCAN), "--ak", "--no-push"]
        print(f"[pipeline] chip_Night 开始 ...", flush=True)
        rc = _run(night_cmd, night_log)

        if rc != 0:
            print(f"[pipeline] chip_Night 失败 (rc={rc})", flush=True)
            _send_failure(
                step    = "chip_Night",
                rc      = rc,
                log_path= night_log,
                fix_cmd = (
                    f"cd {ROOT}\n"
                    f"python -X utf8 scripts/daily_chip_scan.py --ak --no-push\n"
                    f"python -X utf8 scripts/run_cad_pipeline.py"
                ),
            )
            sys.exit(1)

        # 再次确认行数
        rows_after = _ak_cache_rows(trade_date)
        print(f"[pipeline] chip_Night 完成，AK 缓存 {rows_after} 条", flush=True)
        if rows_after < MIN_ROWS:
            print(f"[pipeline] 重建后缓存仍不足 ({rows_after})，继续用已有数据", flush=True)
    else:
        print(f"[pipeline] AK 缓存完整，跳过 chip_Night", flush=True)

    # ── Step 3: chip_CadScan ───────────────────────────────────────────────
    cad_log = LOGS / "chip_cad.log"
    cad_cmd = [PYTHON, "-X", "utf8", str(CHIP_CAD), "--mods", "bekh", "bekhm"]
    print(f"[pipeline] chip_CadScan 开始 ...", flush=True)
    rc = _run(cad_cmd, cad_log)

    if rc != 0:
        print(f"[pipeline] chip_CadScan 失败 (rc={rc})", flush=True)
        _send_failure(
            step    = "chip_CadScan",
            rc      = rc,
            log_path= cad_log,
            fix_cmd = (
                f"cd {ROOT}\n"
                f"python -X utf8 scripts/chip_cad.py --mods bekh bekhm"
            ),
        )
        sys.exit(1)

    print(f"[pipeline] 全部完成 {datetime.now():%H:%M:%S}", flush=True)


if __name__ == "__main__":
    main()
