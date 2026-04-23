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
TASKS_DIR = REPO_ROOT / "tasks"
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
CHIP_CAD          = SCRIPTS   / "chip_cad.py"
CAD_PIPELINE      = SCRIPTS   / "run_cad_pipeline.py"
PREFETCH          = SCRIPTS   / "prefetch.py"
INTEGRITY_CHECK   = SCRIPTS   / "integrity_check.py"
NOTIFY_FAIL       = SCRIPTS   / "notify_failure.py"
NOTIFY_DISCORD    = SCRIPTS   / "notify_discord.py"

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
    # old prefetch task names (in case of re-registration)
    "price_Warm",
    # bot keepalive tasks (replaced by in-process keepalive thread)
    "bot_Keepalive0", "bot_Keepalive1", "bot_Keepalive2",
]

# ── Scheduled tasks ───────────────────────────────────────────────────────────
TASKS = [
    # (name, time, slot, description, wechat_push)  — 按时间顺序排列
    # ── 盘前 ────────────────────────────────────────────────────────────────
    ("chip_Premarket",  "07:00", "chip_premarket",  "筹码盘前兜底（chip_Night未跑时），不推送",        False),
    ("main_Morning",    "07:10", "monitor_scan",    "主策略盘前兜底（main_Scan未跑时），不推送",       False),
    ("integrity_Check", "08:00", "integrity_check", "每小时数据完整性检查（首次通过后当日跳过）",      False),
    ("concept_Warm",    "08:30", "concept_warm",    "预热概念板块反查 map（~30s），不推送",            False),
    ("xhs_Morning",     "09:25", "chip_morning",    "小红书盘前筹码分析推送 📱",                      True),
    # ── 盘中 ────────────────────────────────────────────────────────────────
    ("xhs_Midday",      "11:35", "chip_midday",     "小红书午间筹码分析推送 📱",                      True),
    # ── 收盘 ────────────────────────────────────────────────────────────────
    ("xhs_Evening",     "15:30", "chip_evening",    "小红书收盘筹码分析推送 📱",                      True),
    # ── 收盘后数据预热 ───────────────────────────────────────────────────────
    ("market_Warm",     "15:35", "market_warm",     "预热市场数据：CSI300/PE/申万/停牌表，不推送",    False),
    ("price_Prefetch",  "15:45", "price_prefetch",  "预热全市场价格历史缓存（~1-1.5h），不推送",      False),
    # ── 收盘后分析 ──────────────────────────────────────────────────────────
    ("chip_PerfLog",    "17:15", "perf_log",        "三者共有/cah独有/cad独有 T1-T4 胜率对比 📱",     True),
    ("main_PerfLog",    "17:20", "main_perf_log",   "主策略昨日选股今日胜率对比 📱",                  True),
    ("chip_Night",      "18:00", "chip_night",      "收盘后预取筹码缓存（AK重算~1.5h），不推送",      False),
    ("main_Scan",       "18:30", "monitor_scan",    "主策略扫盘，更新 latest_picks.json，推送 📱",    True),
    ("chip_CadScan",    "20:30", "cad_scan",        "筹码扫描 cah/cadm/cad，三者共有T1-T4推送 📱",    True),
    # ── 次日盘前准备 ────────────────────────────────────────────────────────
    ("main_Night",      "22:30", "main_night",      "预热财务缓存（batch_financials），不推送",        False),
]


def _bat(slot: str, task_name_override: str = "", desc: str = "") -> tuple[Path, str]:
    """Return (bat_path, bat_content) for a given slot key."""
    log = LOGS_DIR

    slot_names = {
        "cad_scan":        "chip_CadPipeline",
        "main_night":      "main_Night",
        "chip_night":      "chip_Night",
        "chip_premarket":  "chip_Premarket",
        "chip_morning":    "xhs_Morning",
        "chip_midday":     "xhs_Midday",
        "chip_evening":    "xhs_Evening",
        "perf_log":        "chip_PerfLog",
        "main_perf_log":   "main_PerfLog",
        "monitor_scan":    "main_Scan",
        "market_warm":     "market_Warm",
        "price_prefetch":  "price_Prefetch",
        "concept_warm":    "concept_Warm",
        "keepalive":       "bot_Keepalive",
        "integrity_check": "integrity_Check",
    }
    task_name = task_name_override or slot_names.get(slot, slot)
    notify_cmd      = (f'"{PYTHON}" -X utf8 "{NOTIFY_FAIL}" "{task_name}"'
                       f' >> "{log}\\notify_failure.log" 2>&1')
    discord_ok_cmd  = (f'"{PYTHON}" -X utf8 "{NOTIFY_DISCORD}" "{task_name}" "{desc}"'
                       f' >> "{log}\\notify_discord.log" 2>&1')
    discord_fail_cmd = (f'"{PYTHON}" -X utf8 "{NOTIFY_DISCORD}" "{task_name}" "{desc}" "failed"'
                        f' >> "{log}\\notify_discord.log" 2>&1')

    if slot == "keepalive":
        path = TASKS_DIR / "run_keepalive.bat"
        cmd  = f'echo keepalive {"{"}%DATE% %TIME%{"}"} >> "{log}\\keepalive.log" 2>&1'
    elif slot == "cad_scan":
        path = TASKS_DIR / "run_cad_scan.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{CAD_PIPELINE}" >> "{log}\\cad_pipeline.log" 2>&1'
    elif slot == "main_night":
        path = TASKS_DIR / "run_main_night.bat"
        content = (
            f'@echo off\n'
            f'cd /d "{REPO_ROOT}"\n'
            f'mkdir "{LOGS_DIR}" 2>nul\n'
            f'"{PYTHON}" -X utf8 "{GEN_UNIVERSE}" >> "{log}\\universe_main.log" 2>&1\n'
            f'if errorlevel 1 (\n'
            f'    {notify_cmd}\n'
            f'    {discord_fail_cmd}\n'
            f'    exit /b 1\n'
            f')\n'
            f'"{PYTHON}" -X utf8 "{BATCH_FIN}" >> "{log}\\batch_financials.log" 2>&1\n'
            f'if errorlevel 1 (\n'
            f'    {notify_cmd}\n'
            f'    {discord_fail_cmd}\n'
            f') else (\n'
            f'    {discord_ok_cmd}\n'
            f')\n'
        )
        return path, content
    elif slot == "chip_night":
        path = TASKS_DIR / "run_chip_night.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{DAILY_SCAN}" --ak --no-push >> "{log}\\chip_scan_night.log" 2>&1'
    elif slot == "chip_premarket":
        path = TASKS_DIR / "run_chip_premarket.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{DAILY_SCAN}" --ak --no-push >> "{log}\\chip_scan_premarket.log" 2>&1'
    elif slot in ("chip_morning", "chip_midday", "chip_evening"):
        phase = slot.split("_")[1]
        path  = TASKS_DIR / f"run_chip_{phase}.bat"
        cmd   = f'"{PYTHON}" -X utf8 "{CHIP_WRITER}" {phase} >> "{log}\\chip_writer_{phase}.log" 2>&1'
    elif slot == "perf_log":
        path = TASKS_DIR / "run_chip_perf_log.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{PERF_LOG}" --force >> "{log}\\chip_perf_log.log" 2>&1'
    elif slot == "main_perf_log":
        path = TASKS_DIR / "run_main_perf_log.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{MAIN_PERF_LOG}" --force >> "{log}\\main_perf_log.log" 2>&1'
    elif slot == "monitor_scan":
        path = TASKS_DIR / "run_monitor_scan.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{MONITOR}" --always-send >> "{log}\\monitor_scan.log" 2>&1'
    elif slot == "market_warm":
        path = TASKS_DIR / "run_market_warm.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{PREFETCH}" --market >> "{log}\\prefetch_market.log" 2>&1'
    elif slot == "price_prefetch":
        path = TASKS_DIR / "run_price_prefetch.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{PREFETCH}" --price --force >> "{log}\\prefetch_price.log" 2>&1'
    elif slot == "concept_warm":
        path = TASKS_DIR / "run_concept_warm.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{PREFETCH}" --concept >> "{log}\\prefetch_concept.log" 2>&1'
    elif slot == "integrity_check":
        path = TASKS_DIR / "run_integrity_check.bat"
        cmd  = f'"{PYTHON}" -X utf8 "{INTEGRITY_CHECK}" >> "{log}\\integrity_check.log" 2>&1'
    else:
        raise ValueError(f"Unknown slot: {slot}")

    content = (
        f'@echo off\n'
        f'cd /d "{REPO_ROOT}"\n'
        f'mkdir "{LOGS_DIR}" 2>nul\n'
        f'{cmd}\n'
        f'if errorlevel 1 (\n'
        f'    {notify_cmd}\n'
        f'    {discord_fail_cmd}\n'
        f') else (\n'
        f'    {discord_ok_cmd}\n'
        f')\n'
    )
    return path, content


# Tasks that repeat on a sub-daily schedule: slot → (interval ISO8601, duration ISO8601)
# integrity_check repeats every 1h for 15h (08:00 → 09:00 → ... → 23:00)
_REPEAT_TRIGGERS: dict[str, tuple[str, str]] = {
    "integrity_check": ("PT1H", "PT15H"),
}


def register():
    print(f"Python : {PYTHON}")
    print(f"Repo   : {REPO_ROOT}")
    print()

    TASKS_DIR.mkdir(parents=True, exist_ok=True)

    # Remove old tasks silently
    for name in OLD_TASKS:
        subprocess.run(f'schtasks /delete /tn "{name}" /f',
                       shell=True, capture_output=True, text=True)

    for name, time_str, slot, desc, push in TASKS:
        bat_path, bat_content = _bat(slot, task_name_override=name, desc=desc)
        bat_path.write_text(bat_content, encoding="utf-8")

        repeat = _REPEAT_TRIGGERS.get(slot)
        if repeat:
            interval, duration = repeat
            # Hourly-repeat task: shorter execution limit (1h), repeat trigger
            ps = (
                f"$a = New-ScheduledTaskAction -Execute '\"{bat_path}\"';"
                f"$t = New-ScheduledTaskTrigger -Daily -At '{time_str}';"
                f"$t.Repetition.Interval = '{interval}';"
                f"$t.Repetition.Duration = '{duration}';"
                f"$s = New-ScheduledTaskSettingsSet -WakeToRun -ExecutionTimeLimit (New-TimeSpan -Hours 1);"
                f"$p = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest;"
                f"Register-ScheduledTask -TaskName '{name}' -Action $a -Trigger $t -Settings $s -Principal $p -Force | Out-Null"
            )
        else:
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


def write_bats():
    """Regenerate all bat files without touching Windows Task Scheduler."""
    TASKS_DIR.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    for name, time_str, slot, desc, push in TASKS:
        bat_path, bat_content = _bat(slot, task_name_override=name, desc=desc)
        bat_path.write_text(bat_content, encoding="utf-8")
        key = bat_path.name
        if key not in seen:
            seen.add(key)
            print(f"✅  {name:<35}  → {key}")
        else:
            print(f"↩  {name:<35}  → {key} (覆写)")
    print("\n完成。Windows定时任务无需重新注册（bat文件路径未变）。")


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
    group.add_argument("--remove",     action="store_true", help="删除所有定时任务")
    group.add_argument("--status",     action="store_true", help="查看任务状态")
    group.add_argument("--write-bats", action="store_true", help="仅更新bat文件，不重新注册定时任务")
    args = parser.parse_args()

    if args.remove:
        remove()
    elif args.status:
        status()
    elif args.write_bats:
        write_bats()
    else:
        register()
