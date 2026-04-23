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
MAIN_PERF_LOG = SCRIPTS   / "main_perf_log.py"
MONITOR       = SCRIPTS   / "monitor.py"
BATCH_FIN     = SCRIPTS   / "tools" / "batch_financials.py"
GEN_UNIVERSE  = SCRIPTS   / "tools" / "generate_full_universe.py"
CHIP_CAD      = SCRIPTS   / "chip_cad.py"
NOTIFY_FAIL   = SCRIPTS   / "notify_failure.py"

# ── Old tasks (remove only) ───────────────────────────────────────────────────
OLD_TASKS = [
    "StockSage_Morning",
    "StockSage_Midday",
    "StockSage_Evening",
    "StockSage_Night",
    "StockSage_MainNight",
    "StockSage_ChipNight",
    "StockSage_ChipPremarket",
    "StockSage_ChipMorning",
    "StockSage_ChipMidday",
    "StockSage_ChipEvening",
    "StockSage_ChipPerfLog",
    "StockSage_CadScan",
    "StockSage_MainMorning",
    "StockSage_MonitorScan",
    "xhs_MainNight", "xhs_ChipNight", "xhs_ChipPremarket",
    "xhs_ChipMorning", "xhs_ChipMidday", "xhs_ChipEvening",
    "xhs_ChipPerfLog", "xhs_CadScan", "xhs_MainMorning", "xhs_MonitorScan",
    "ss_MainNight", "ss_ChipNight", "ss_ChipPremarket",
    "ss_ChipMorning", "ss_ChipMidday", "ss_ChipEvening",
    "ss_ChipPerfLog", "ss_CadScan", "ss_CadmScan", "ss_MainMorning", "ss_MonitorScan",
    "chip_CadmScan",
]

# ── Scheduled tasks ───────────────────────────────────────────────────────────
TASKS = [
    # (name, time, slot, description, wechat_push)  — 按时间顺序排列
    # ── 盘前 ────────────────────────────────────────────────────────────────
    ("chip_Premarket",  "07:00", "chip_premarket", "筹码盘前兜底（chip_Night未跑时），不推送",        False),
    ("main_Morning",    "07:10", "monitor_scan",   "主策略盘前兜底（main_Scan未跑时），不推送",       False),
    ("xhs_Morning",     "09:25", "chip_morning",   "小红书盘前筹码分析推送 📱",                      True),
    # ── 盘中 ────────────────────────────────────────────────────────────────
    ("xhs_Midday",      "11:35", "chip_midday",    "小红书午间筹码分析推送 📱",                      True),
    # ── 收盘 ────────────────────────────────────────────────────────────────
    ("xhs_Evening",     "15:30", "chip_evening",   "小红书收盘筹码分析推送 📱",                      True),
    # ── 收盘后分析 ──────────────────────────────────────────────────────────
    ("chip_PerfLog",    "17:15", "perf_log",       "读昨日cad/cadm票，测今日胜率 📱",                True),
    ("main_PerfLog",    "17:20", "main_perf_log",  "读昨日主策略票，测今日胜率 📱",                  True),
    ("chip_Night",      "18:00", "chip_night",     "收盘后预取筹码缓存（AK重算~1.5h），不推送",      False),
    ("main_Scan",       "18:30", "monitor_scan",   "主策略扫盘，更新 latest_picks.json，推送 📱",    True),
    ("chip_CadScan",    "20:30", "cad_scan",       "筹码全档扫描 bekh+bekhm，用今日完整数据推送 📱", True),
    # ── 次日盘前准备 ────────────────────────────────────────────────────────
    ("main_Night",      "22:30", "main_night",     "预热财务缓存（batch_financials），不推送",        False),
    # ── 保活唤醒（填补 22:30-07:00 的睡眠空档，让 Discord bot 保持在线）────
    ("bot_Keepalive",   "03:00", "keepalive",      "凌晨唤醒机器，让 Discord bot 重连",              False),
]


def _bat(slot: str) -> tuple[Path, str]:
    """Return (bat_path, bat_content) for a given slot key."""
    log = LOGS_DIR

    if slot == "keepalive":
        path = XHS_DIR / "run_keepalive.bat"
        cmd  = f'echo keepalive {"{"}%DATE% %TIME%{"}"} >> "{log}\\keepalive.log" 2>&1'
    elif slot == "cad_scan":
        path = XHS_DIR / "run_cad_scan.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{CHIP_CAD}" --mods bekh bekhm >> "{log}\\chip_cad.log" 2>&1'
    elif slot == "main_night":
        path = XHS_DIR / "run_main_night.bat"
        notify = (f'"{PYTHON}" -X utf8 "{NOTIFY_FAIL}" "main_Night"'
                  f' >> "{log}\\notify_failure.log" 2>&1')
        cmd  = (f'"{PYTHON}" -X utf8 "{GEN_UNIVERSE}" >> "{log}\\universe_main.log" 2>&1\n'
                f'if errorlevel 1 ( {notify} & exit /b 1 )\n'
                f'"{PYTHON}" -X utf8 "{BATCH_FIN}" >> "{log}\\batch_financials.log" 2>&1')
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
    elif slot == "main_perf_log":
        path = XHS_DIR / "run_main_perf_log.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{MAIN_PERF_LOG}" >> "{log}\\main_perf_log.log" 2>&1'
    elif slot == "monitor_scan":
        path = XHS_DIR / "run_monitor_scan.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{MONITOR}" --always-send >> "{log}\\monitor_scan.log" 2>&1'
    else:
        raise ValueError(f"Unknown slot: {slot}")

    # Each slot gets a human-readable name for failure notifications
    slot_names = {
        "cad_scan":     "chip_CadScan",
        "main_night":   "main_Night",
        "chip_night":   "chip_Night",
        "chip_premarket": "chip_Premarket",
        "chip_morning": "xhs_Morning",
        "chip_midday":  "xhs_Midday",
        "chip_evening": "xhs_Evening",
        "perf_log":     "chip_PerfLog",
        "main_perf_log": "main_PerfLog",
        "monitor_scan": "main_Scan",
        "keepalive":    "bot_Keepalive",
    }
    task_name = slot_names.get(slot, slot)
    notify_cmd = (f'"{PYTHON}" -X utf8 "{NOTIFY_FAIL}" "{task_name}"'
                  f' >> "{log}\\notify_failure.log" 2>&1')

    content = (
        f'@echo off\n'
        f'cd /d "{REPO_ROOT}"\n'
        f'mkdir "{LOGS_DIR}" 2>nul\n'
        f'{cmd}\n'
        f'if errorlevel 1 (\n'
        f'    {notify_cmd}\n'
        f')\n'
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
        ps = (
            f"$a = New-ScheduledTaskAction -Execute '\"{bat_path}\"';"
            f"$t = New-ScheduledTaskTrigger -Daily -At '{time_str}';"
            f"$s = New-ScheduledTaskSettingsSet -WakeToRun -ExecutionTimeLimit (New-TimeSpan -Hours 2);"
            f"$p = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest;"
            f"Register-ScheduledTask -TaskName '{name}' -Action $a -Trigger $t -Settings $s -Principal $p -Force | Out-Null"
        )
        result = subprocess.run(
            ["powershell", "-NonInteractive", "-Command", ps],
            capture_output=True, text=True,
        )
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
