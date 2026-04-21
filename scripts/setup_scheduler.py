#!/usr/bin/env python3
"""
scripts/setup_scheduler.py — 一次性运行，注册 Windows 定时任务

用法（从仓库根目录，管理员权限运行）:
    python scripts/setup_scheduler.py           # 注册所有定时任务
    python scripts/setup_scheduler.py --remove  # 删除任务
    python scripts/setup_scheduler.py --status  # 查看任务状态
"""

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SCRIPTS   = REPO_ROOT / "scripts"
XHS_DIR   = REPO_ROOT / "xhs"
PYTHON    = sys.executable
LOGS_DIR  = SCRIPTS / "logs"

# ── Script paths ─────────────────────────────────────────────────────────────
CHIP_WRITER   = XHS_DIR   / "chip_writer.py"
DAILY_SCAN    = SCRIPTS   / "daily_chip_scan.py"
PERF_LOG      = SCRIPTS   / "chip_perf_log.py"
MONITOR       = SCRIPTS   / "monitor.py"
BATCH_FIN     = SCRIPTS   / "tools" / "batch_financials.py"

# ── Old tasks (remove only) ───────────────────────────────────────────────────
OLD_TASKS = [
    "StockSage_Morning",
    "StockSage_Midday",
    "StockSage_Evening",
    "StockSage_Night",
]

# ── Scheduled tasks ───────────────────────────────────────────────────────────
# (name, time HH:MM, slot_key)
TASKS = [
    # (name, time, slot, description, wechat_push)
    # ── Chip scan pipeline ──────────────────────────────────────────────────
    ("StockSage_MainNight",     "22:30", "main_night",     "预热财务缓存（batch_financials），不推送", False),
    ("StockSage_ChipNight",     "23:00", "chip_night",     "夜间预取筹码缓存，不推送",          False),
    ("StockSage_ChipPremarket", "07:00", "chip_premarket", "盘前兜底缓存（夜间未跑时补救），不推送", False),
    ("StockSage_ChipMorning",   "09:25", "chip_morning",   "盘前筹码分析推送 📱",               True),
    ("StockSage_ChipMidday",    "11:35", "chip_midday",    "午间筹码分析推送 📱",               True),
    ("StockSage_ChipEvening",   "15:10", "chip_evening",   "收盘筹码分析推送 📱",               True),
    # ── EOD analytics ───────────────────────────────────────────────────────
    ("StockSage_ChipPerfLog",   "17:15", "perf_log",       "cad/cadm 每日胜率统计推送 📱",      True),
    ("StockSage_MainMorning",   "07:10", "monitor_scan",   "主策略盘前兜底（17:30未跑时补救），不推送", False),
    ("StockSage_MonitorScan",   "17:30", "monitor_scan",   "主策略扫盘，更新 latest_picks.json", False),
]


def _bat(slot: str) -> tuple[Path, str]:
    """Return (bat_path, bat_content) for a given slot key."""
    log = LOGS_DIR

    if slot == "main_night":
        path = XHS_DIR / "run_main_night.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{BATCH_FIN}" >> "{log}\\batch_financials.log" 2>&1'
    elif slot == "chip_night":
        path = XHS_DIR / "run_chip_night.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{DAILY_SCAN}" --ak --no-push >> "{log}\\chip_scan_night.log" 2>&1'
    elif slot == "chip_premarket":
        path = XHS_DIR / "run_chip_premarket.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{DAILY_SCAN}" --ak --no-push >> "{log}\\chip_scan_premarket.log" 2>&1'
    elif slot in ("chip_morning", "chip_midday", "chip_evening"):
        phase = slot.split("_")[1]
        path  = XHS_DIR / f"run_chip_{phase}.bat"
        cmd   = f'"{PYTHON}" -X utf8 "{CHIP_WRITER}" {phase} >> "{log}\\chip_writer_{phase}.log" 2>&1'
    elif slot == "perf_log":
        path = XHS_DIR / "run_chip_perf_log.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{PERF_LOG}" >> "{log}\\chip_perf_log.log" 2>&1'
    elif slot == "monitor_scan":
        path = XHS_DIR / "run_monitor_scan.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{MONITOR}" >> "{log}\\monitor_scan.log" 2>&1'
    else:
        raise ValueError(f"Unknown slot: {slot}")

    content = (
        f'@echo off\n'
        f'cd /d "{REPO_ROOT}"\n'
        f'mkdir "{LOGS_DIR}" 2>nul\n'
        f'{cmd}\n'
    )
    return path, content


def register():
    print(f"Python : {PYTHON}")
    print(f"Repo   : {REPO_ROOT}")
    print()

    # Remove old tasks silently
    for name in OLD_TASKS:
        subprocess.run(f'schtasks /delete /tn "{name}" /f',
                       shell=True, capture_output=True, text=True)

    for name, time_str, slot, desc, push in TASKS:
        bat_path, bat_content = _bat(slot)
        bat_path.write_text(bat_content, encoding="utf-8")
        cmd = (f'schtasks /create /tn "{name}" /tr "{bat_path}" /sc daily /st {time_str}'
               f' /f /rl HIGHEST')
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        mark = "✅" if result.returncode == 0 else "❌"
        err  = f"  失败: {result.stderr.strip()}" if result.returncode != 0 else ""
        print(f"{mark}  {time_str}  {name:<35}  {desc}{err}")

    print()
    print("完成。各脚本内置超时检查，超出窗口自动跳过。")


def remove():
    all_names = [n for n, _, _, _, _ in TASKS] + OLD_TASKS
    for name in all_names:
        result = subprocess.run(
            f'schtasks /delete /tn "{name}" /f',
            shell=True, capture_output=True, text=True,
        )
        if result.returncode == 0:
            print(f"✅  {name} 已删除")
        else:
            print(f"❌  {name}: {result.stderr.strip()}")


def status():
    for name, time_str, _, desc, push in TASKS:
        result = subprocess.run(
            f'schtasks /query /tn "{name}" /fo LIST',
            shell=True, capture_output=True, text=True,
        )
        push_tag = "📱" if push else "  "
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if any(kw in line for kw in ("状态", "Status", "下次运行", "Next Run")):
                    print(f"  {push_tag} {time_str}  {name:<35}  {desc}  [{line.strip()}]")
        else:
            print(f"  {push_tag} {time_str}  {name:<35}  {desc}  [未注册]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="注册/删除 StockSage 定时任务")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--remove", action="store_true", help="删除所有定时任务")
    group.add_argument("--status", action="store_true", help="查看任务状态")
    args = parser.parse_args()

    if args.remove:
        remove()
    elif args.status:
        status()
    else:
        register()
