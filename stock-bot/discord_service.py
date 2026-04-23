#!/usr/bin/env python3
"""
stock-bot/discord_service.py — Windows service wrapper for discord_bot.py

以管理员权限运行以下命令（从仓库根目录）:
    python stock-bot/discord_service.py --startup auto install   # 安装服务（开机自启）
    python stock-bot/discord_service.py start                   # 启动
    python stock-bot/discord_service.py stop      # 停止
    python stock-bot/discord_service.py remove    # 卸载
    python stock-bot/discord_service.py status    # 查看状态

或用 sc 命令:
    sc start DiscordBotSvc
    sc stop  DiscordBotSvc
"""

import subprocess
import sys
import time
from pathlib import Path

import servicemanager
import win32event
import win32service
import win32serviceutil

ROOT   = Path(__file__).resolve().parent.parent
BOT    = Path(__file__).resolve().parent / "discord_bot.py"
# In a Windows service, sys.executable is pythonservice.exe — resolve to python.exe
PYTHON = str(Path(sys.executable).with_name("python.exe"))
LOG    = ROOT / "scripts" / "logs" / "discord_service.log"

_RESTART_DELAY = 10  # seconds before restarting after a crash


class DiscordBotService(win32serviceutil.ServiceFramework):
    _svc_name_         = "DiscordBotSvc"
    _svc_display_name_ = "StockSage Discord Bot"
    _svc_description_  = "StockSage Alpha Discord bot — auto-restarts on crash"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self._stop_event = win32event.CreateEvent(None, 0, 0, None)
        self._proc = None

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self._stop_event)
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, ""),
        )
        self._run()

    # ── helpers ────────────────────────────────────────────────────────────────

    def _log(self, msg: str):
        LOG.parent.mkdir(exist_ok=True)
        from datetime import datetime
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")

    def _run(self):
        while True:
            self._log("Starting discord_bot.py …")
            log_fh = open(LOG, "a", encoding="utf-8")
            self._proc = subprocess.Popen(
                [PYTHON, "-X", "utf8", str(BOT)],
                cwd=str(ROOT),
                stdout=log_fh,
                stderr=subprocess.STDOUT,
            )
            self._log(f"PID {self._proc.pid}")

            # Poll every 5 s; break out on stop-request or process death
            while True:
                rc = win32event.WaitForSingleObject(self._stop_event, 5000)
                if rc == win32event.WAIT_OBJECT_0:
                    self._log("Stop requested — terminating bot …")
                    if self._proc.poll() is None:
                        self._proc.terminate()
                    return
                if self._proc.poll() is not None:
                    self._log(
                        f"Bot exited (code {self._proc.returncode}), "
                        f"restarting in {_RESTART_DELAY}s …"
                    )
                    time.sleep(_RESTART_DELAY)
                    break  # outer while: restart


def _cmd_status():
    import win32con
    svc = DiscordBotService._svc_name_
    try:
        hscm = win32service.OpenSCManager(None, None, win32con.GENERIC_READ)
        hsvc = win32service.OpenService(hscm, svc, win32con.GENERIC_READ)
        info = win32service.QueryServiceStatus(hsvc)
        win32service.CloseServiceHandle(hsvc)
        win32service.CloseServiceHandle(hscm)
        state_map = {
            win32service.SERVICE_STOPPED:          "STOPPED",
            win32service.SERVICE_START_PENDING:    "START_PENDING",
            win32service.SERVICE_STOP_PENDING:     "STOP_PENDING",
            win32service.SERVICE_RUNNING:          "RUNNING",
            win32service.SERVICE_CONTINUE_PENDING: "CONTINUE_PENDING",
            win32service.SERVICE_PAUSE_PENDING:    "PAUSE_PENDING",
            win32service.SERVICE_PAUSED:           "PAUSED",
        }
        state = state_map.get(info[1], f"UNKNOWN({info[1]})")
        print(f"{svc}: {state}")
    except win32service.error as e:
        print(f"{svc}: NOT INSTALLED ({e.strerror})")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(DiscordBotService)
        servicemanager.StartServiceCtrlDispatcher()
    elif sys.argv[1] == "status":
        _cmd_status()
    else:
        win32serviceutil.HandleCommandLine(DiscordBotService)
