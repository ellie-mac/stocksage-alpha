"""
StockSage Alpha — Feishu Bot
============================
飞书版，功能与 discord_bot.py 完全对应。

配置 (stock-bot/feishu_config.json):
    feishu.app_id          飞书应用 App ID
    feishu.app_secret      飞书应用 App Secret
    feishu.allowed_open_ids  只接受这些用户 open_id（留空=全部）
    feishu.notify_chat_id  定时任务通知发送到哪个 chat_id（空=不推）

启动:
    python -X utf8 stock-bot/feishu_bot.py

依赖:
    pip install lark-oapi
"""
from __future__ import annotations

import ctypes
import io
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
    ReplyMessageRequest,
    ReplyMessageRequestBody,
)

# UTF-8 stdout/stderr on Windows
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).resolve().parent.parent
BOT_DIR = Path(__file__).resolve().parent
SCRIPTS = ROOT / "scripts"
LOGS    = SCRIPTS / "logs"
LOGS.mkdir(exist_ok=True)

_executor = ThreadPoolExecutor(max_workers=4)

# ── Config ────────────────────────────────────────────────────────────────────
_CFG_CACHE: dict | None = None

def _cfg() -> dict:
    global _CFG_CACHE
    if _CFG_CACHE is None:
        _CFG_CACHE = json.loads((BOT_DIR / "feishu_config.json").read_text(encoding="utf-8"))
    return _CFG_CACHE

def _fs_cfg() -> dict:
    return _cfg().get("feishu", {})

def _allowed_open_ids() -> set[str]:
    ids = _fs_cfg().get("allowed_open_ids", [])
    return set(ids) if ids else set()

# ── Feishu API client (global, set in main) ───────────────────────────────────
_client: lark.Client | None = None

_MSG_LIMIT = 4000  # Feishu text message limit

def _chunks(text: str, size: int = _MSG_LIMIT) -> list[str]:
    return [text[i:i+size] for i in range(0, len(text), size)]

def reply_text(message_id: str, text: str) -> None:
    if not text:
        text = "(无输出)"
    for chunk in _chunks(text):
        req = (
            ReplyMessageRequest.builder()
            .message_id(message_id)
            .request_body(
                ReplyMessageRequestBody.builder()
                .msg_type("text")
                .content(json.dumps({"text": chunk}))
                .build()
            )
            .build()
        )
        _client.im.v1.message.reply(req)

def send_to_chat(chat_id: str, text: str) -> None:
    if not text or not chat_id:
        return
    for chunk in _chunks(text):
        req = (
            CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("text")
                .content(json.dumps({"text": chunk}))
                .build()
            )
            .build()
        )
        _client.im.v1.message.create(req)

# ── Text constants ────────────────────────────────────────────────────────────
_HELP = """\
筹码策略
  ca 全档 | cah 全档+排高位 | cabekh 全档+BOLL+≤50+排科创+排高位
  修饰符：b BOLL  e 股价≤50  k 排科创  h 排高位  | ch 筹码详情

  z 状态 | t 定时任务 | sch 快捷命令 | fh 因子/回测 | 其他走AI对话"""

_FACTOR_HELP = """\
因子 & 分析
  ic 因子IC摘要 | ich 因子列表
  icf 因子名 因子说明 | fx600519 单股分析

回测
  bs 进度 | br 结果摘要
  bt 主板 | bts 小盘 | bte ETF
  数字=期数（bt默认16，bte默认12），s=小盘，如 bts24、bte6"""

_SC_LIST = """\
快捷命令 sc N
  sc1 启动 monitor | sc2 重启 monitor
  sc3 终止回测 | sc4 因子IC回测
  sc5 预热财务缓存 | sc6 重建股票池
  sc7 扫盘推送 | sc8 monitor日志"""

_CHIP_LIST = """\
筹码命令
  cad  数据驱动全档（T4→T1→T2→T3→T5，bekh）⭐
  cadm 同上 + MACD绿柱（bekhm）⭐
  ca  全档T1-T5 | cah 全档排高位 | cabekh 全档+全修饰

  c1 T1≥95%  c2 T2 90-95%  c3 T3 85-90%  c4 T4 75-85%  c5 T5 65-75%

修饰符（可叠加）
  b BOLL  e ≤50元  k 排科创  h 排高位  m MACD绿柱  z MACD近零
  示例：c1bmz  c2mz  c4kh  cad  cadm"""

# ── Chip tier config ──────────────────────────────────────────────────────────
_CHIP_TIERS = {
    "1": (95, None),
    "2": (90, 95),
    "3": (85, 90),
    "4": (75, 85),
    "5": (65, 75),
}

# ── Python process helpers ────────────────────────────────────────────────────
def _get_python_procs() -> list[tuple[str, str]]:
    ps_cmd = (
        "$procs = Get-WmiObject Win32_Process -Filter 'name=\"python.exe\"';"
        " foreach ($p in $procs) { Write-Output ($p.ProcessId.ToString() + '|||' + $p.CommandLine) }"
    )
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_cmd],
            capture_output=True, timeout=15, encoding="utf-8", errors="replace",
        )
        result = []
        for line in r.stdout.splitlines():
            line = line.strip()
            if "|||" not in line:
                continue
            pid, _, cmd = line.partition("|||")
            if pid.strip().isdigit():
                result.append((pid.strip(), cmd.strip()))
        return result
    except Exception:
        return []

def _find_monitor_pid() -> str | None:
    for pid, cmd in _get_python_procs():
        if "monitor" in cmd.lower() and "--loop" in cmd.lower():
            return pid
    return None

def _find_backtest_pid() -> str | None:
    for pid, cmd in _get_python_procs():
        if "backtest" in cmd.lower():
            return pid
    return None

# ── Command implementations (mirrored from discord_bot.py) ───────────────────

def _describe_cmdline(cmd: str) -> str:
    rules = [
        ("monitor.py",         "--loop",              "Monitor 循环"),
        ("monitor.py",         "--sell-only",          "Monitor 持仓检查"),
        ("monitor.py",         "--test-now",           "Monitor 全市场扫描"),
        ("monitor.py",         "",                     "Monitor（单次）"),
        ("feishu_bot.py",      "",                     "Feishu Bot"),
        ("discord_bot.py",     "",                     "Discord Bot"),
        ("factor_analysis.py", "--universe.*smallcap", "IC回测 小盘"),
        ("factor_analysis.py", "--universe.*etf",      "IC回测 ETF"),
        ("factor_analysis.py", "",                     "IC回测 主策略"),
        ("backtest.py",        "--smallcap",           "回测 小盘"),
        ("backtest.py",        "",                     "回测 主策略"),
        ("etf_backtest.py",    "",                     "回测 ETF"),
        ("batch_financials.py","",                     "财务数据预热"),
        ("build_universe.py",  "",                     "重建股票池"),
        ("chip_strategy.py",   "",                     "筹码策略扫描"),
        ("daily_chip_scan.py", "",                     "筹码全档扫描"),
        ("chip_cad.py",        "",                     "筹码 CAD 扫描"),
        ("run_cad_pipeline.py","",                     "筹码流水线"),
        ("prefetch.py",        "--price",              "价格缓存预热"),
        ("prefetch.py",        "--market",             "市场数据预热"),
        ("prefetch.py",        "--concept",            "概念 map 预热"),
        ("integrity_check.py", "",                     "完整性检查"),
        ("research.py",        "",                     "单股分析"),
    ]
    for script, arg_pattern, label in rules:
        if script in cmd:
            if not arg_pattern or re.search(arg_pattern, cmd):
                return label
    parts = cmd.split()
    for i, p in enumerate(parts[1:], 1):
        if not p.startswith("-"):
            return p.replace("\\", "/").split("/")[-1]
    return "python"


def _h_status() -> str:
    lines = [f"系统状态 @ {datetime.now():%Y-%m-%d %H:%M:%S}\n"]
    proc_list = _get_python_procs()
    root_str = str(ROOT).replace("\\", "/").lower()
    _SS = {
        "monitor.py", "feishu_bot.py", "discord_bot.py",
        "factor_analysis.py", "backtest.py", "etf_backtest.py",
        "batch_financials.py", "build_universe.py", "chip_strategy.py",
        "daily_chip_scan.py", "chip_cad.py", "run_cad_pipeline.py",
        "prefetch.py", "research.py", "integrity_check.py",
    }
    ss_procs, other_procs = [], []
    for pid, cmd in proc_list:
        c = cmd.replace("\\", "/").lower()
        if root_str in c or any(s in c for s in _SS):
            ss_procs.append((pid, cmd))
        else:
            other_procs.append((pid, cmd))

    if ss_procs:
        lines.append("StockSage 进程:")
        for pid, cmd in ss_procs:
            lines.append(f"  ✅ {_describe_cmdline(cmd)}  PID {pid}")
    else:
        lines.append("❌ 无 StockSage 进程运行")

    lines.append("")
    lines.append(_h_picks())
    return "\n".join(lines)


def _h_scan() -> str:
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"), "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return "已触发扫盘，结果稍后发送到微信 📱"


def _h_holdings() -> str:
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--sell-only", "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return "已触发持仓推送，结果稍后发送到微信 📱"


def _h_picks() -> str:
    picks_path = ROOT / "data" / "latest_picks.json"
    if not picks_path.exists():
        return "latest_picks.json 不存在，今日可能尚未选股。"
    data  = json.loads(picks_path.read_text(encoding="utf-8"))
    items = data.get("results", [])
    ts    = data.get("timestamp") or data.get("date", "")
    date_ = ts[:10] if ts else "?"
    lines = [f"今日推荐 ({date_})\n"]
    for i, s in enumerate(items[:10], 1):
        name  = s.get("name") or s.get("code", "?")
        score = s.get("composite", s.get("score", 0))
        lines.append(f"{i}. {name}  (得分 {score:.3f})")
    return "\n".join(lines)


def _h_logs(n: int = 20) -> str:
    log_path = LOGS / "monitor_loop.log"
    if not log_path.exists():
        return "monitor_loop.log 不存在。"
    tail = log_path.read_bytes()[-8000:].decode("utf-8", errors="replace")
    last = [l for l in tail.splitlines() if l.strip()][-n:]
    body = "\n".join(last) or "(空)"
    if len(body) > 3500:
        body = "..." + body[-3500:]
    return f"日志 -{n}\n```\n{body}\n```"


_TASK_DESC = {
    "chip_Premarket":     "盘前数据兜底",
    "main_Morning":       "主策略盘前兜底",
    "xhs_Morning":        "量化早报 📱",
    "xhs_Midday":         "午间快报 📱",
    "xhs_Evening":        "收盘总结 📱",
    "daily_PerfLog":      "三合一收盘胜率 📱",
    "market_Warm":        "市场数据预热",
    "price_Prefetch":     "价格缓存预热",
    "chip_Night":         "夜间筹码扫描",
    "main_Scan":          "主策略扫盘 📱",
    "gc_Scan":            "金叉策略扫描 📱",
    "chip_CadScan":       "筹码三模型扫描 📱",
    "main_Night":         "夜间数据预热",
    "StockSageFeishuBot": "飞书机器人",
}

def _h_tasks() -> str:
    names_list = "','".join(_TASK_DESC.keys())
    ps = (
        f"$today = (Get-Date).Date;"
        f"$names = @('{names_list}');"
        "Get-ScheduledTask | Where-Object { $names -contains $_.TaskName } | ForEach-Object {"
        "  $info = $_ | Get-ScheduledTaskInfo -ErrorAction SilentlyContinue;"
        "  $lr = $info.LastRunTime;"
        "  $done = $lr -and $lr -ge $today -and $lr -le (Get-Date);"
        "  $nr = $info.NextRunTime;"
        "  $st = if ($done) { 'OK' } else { '--' };"
        "  $lrS = if ($lr -and $lr.Year -gt 1) { $lr.ToString('HH:mm') } else { '--' };"
        "  $nrS = if ($nr -and $nr.Year -gt 1) { $nr.ToString('MM/dd HH:mm') } else { '--' };"
        "  Write-Output \"$($_.TaskName)|||$st|||$lrS|||$nrS\""
        "}"
    )
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True, timeout=20, encoding="utf-8", errors="replace",
        )
        rows = []
        for line in (r.stdout or "").splitlines():
            parts = line.strip().split("|||")
            if len(parts) == 4:
                name, status, last_run, next_run = parts
                rows.append((status, name, _TASK_DESC.get(name, ""), last_run, next_run))
        if not rows:
            return f"❌ 无任务数据"
        rows.sort(key=lambda x: x[4])
        today_mdd = date.today().strftime("%m/%d")
        lines = []
        for status, name, desc, last_run, next_run in rows:
            if status == "OK":
                tick = "✅"
            elif next_run != "--" and not next_run.startswith(today_mdd):
                tick = "❌"
            else:
                tick = "⬜"
            last_s = f"  上次{last_run}" if last_run != "--" else ""
            next_s = f"  下次{next_run}" if next_run != "--" else ""
            lines.append(f"{tick} {name} — {desc}{last_s}{next_s}")
        return "\n".join(lines)
    except Exception as e:
        return f"❌ 查询失败: {e}"


def _h_backtest_status() -> str:
    pid = _find_backtest_pid()
    logs = sorted(LOGS.glob("backtest_*.log"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not pid and logs:
        log = logs[0]
        out_json = ROOT / "data" / (log.stem + ".json")
        if time.time() - log.stat().st_mtime < 1800 and not out_json.exists():
            pid = "unknown"
    if not pid:
        results = sorted((ROOT / "data").glob("backtest_*.json"),
                         key=lambda f: f.stat().st_mtime, reverse=True)
        if results:
            age_min = int((time.time() - results[0].stat().st_mtime) / 60)
            return f"无回测进程。最近结果: {results[0].name}（{age_min} 分钟前完成）"
        return "无回测进程，data/ 下也没有结果文件。"
    pid_str = pid if pid != "unknown" else "（未能获取）"
    lines = [f"回测进行中 PID {pid_str}"]
    if logs:
        content = logs[0].read_bytes().decode("utf-8", errors="replace")
        periods = [l for l in content.splitlines() if "Period" in l and "/" in l]
        if periods:
            lines.append(f"进度: {periods[-1].strip()}")
    return "\n".join(lines)


def _h_backtest_result() -> str:
    results = sorted((ROOT / "data").glob("backtest_*.json"),
                     key=lambda f: f.stat().st_mtime, reverse=True)
    if not results:
        return "data/ 下没有回测结果文件"
    f = results[0]
    age_min = int((time.time() - f.stat().st_mtime) / 60)
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
    except Exception as e:
        return f"❌ 读取 {f.name} 失败: {e}"
    lines = [f"{f.name}（{age_min}分钟前完成）\n"]
    stats = data.get("stats", {})
    if stats:
        lines.append("汇总统计")
        mapping = {
            "annualized_ret_pct":  "年化收益",
            "sharpe_ratio":        "Sharpe",
            "max_drawdown_pct":    "最大回撤",
            "win_rate_pct":        "胜率",
            "mean_alpha_pct":      "平均超额",
            "n_periods":           "期数",
        }
        for key, label in mapping.items():
            v = stats.get(key)
            if v is not None:
                lines.append(f"  {label}: {v}")
    periods = data.get("period_results", [])
    if periods:
        lines.append(f"\n各期收益（共{len(periods)}期）")
        for p in periods[-6:]:
            port  = p.get("portfolio_ret", 0)
            bench = p.get("benchmark_ret")
            alpha = p.get("alpha")
            bench_s = f"{bench:+.2f}%" if bench is not None else "N/A"
            alpha_s = f"{alpha:+.2f}%" if alpha is not None else "N/A"
            lines.append(f"  P{p['period']}: 策略{port:+.2f}%  基准{bench_s}  超额{alpha_s}")
    return "\n".join(lines)


def _h_backtest(periods: int = 16, universe: str = "main", workers: int = 8) -> str:
    if _find_backtest_pid():
        return "⚠️ 已有回测进程在运行，请等待完成或先 sc3 停止。"
    logs = sorted(LOGS.glob("backtest_*.log"), key=lambda f: f.stat().st_mtime, reverse=True)
    if logs:
        log = logs[0]
        out_json = ROOT / "data" / (log.stem + ".json")
        if time.time() - log.stat().st_mtime < 1800 and not out_json.exists():
            return f"⚠️ {log.name} 在 30 分钟内更新且无结果文件，回测可能仍在运行。"
    universe_file = ROOT / "data" / f"{universe}_universe.json"
    if not universe_file.exists():
        return f"❌ 股票池文件不存在: {universe_file}"
    out_file = ROOT / "data" / f"backtest_{universe}_{periods}p.json"
    log_path = LOGS / f"backtest_{universe}_{periods}p.log"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"--- Backtest started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        f.write(f"    universe={universe_file.name}, periods={periods}, workers={workers}\n\n")
    proc = subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "backtest.py"),
         "--periods", str(periods), "--universe", str(universe_file),
         "--out", str(out_file), "--workers", str(workers)],
        cwd=str(ROOT),
        stdout=open(log_path, "a"), stderr=subprocess.STDOUT,
    )
    return (
        f"✅ 回测已启动 (PID {proc.pid})\n"
        f"  股票池: {universe} | 期数: {periods} | Workers: {workers}\n"
        f"  输出: {out_file.name} | 日志: {log_path.name}\n"
        f"用 bs 跟踪进度（每期约 20 min）"
    )


def _h_backtest_etf(periods: int = 12, fwd: int = 10, workers: int = 4) -> str:
    if _find_backtest_pid():
        return "⚠️ 已有回测进程在运行，请等待完成或先 sc3 停止。"
    out_file = ROOT / "data" / f"backtest_etf_{periods}p.json"
    log_path = LOGS / f"backtest_etf_{periods}p.log"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"--- ETF backtest started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    proc = subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "etf_backtest.py"),
         "--periods", str(periods), "--fwd", str(fwd),
         "--workers", str(workers), "--out", str(out_file)],
        cwd=str(ROOT),
        stdout=open(log_path, "a"), stderr=subprocess.STDOUT,
    )
    return (
        f"✅ ETF 回测已启动 (PID {proc.pid})\n"
        f"  期数: {periods} | 前向: {fwd}d | Workers: {workers}\n"
        f"  输出: {out_file.name} | 日志: {log_path.name}"
    )


def _launch_chip(tier: str, mods: str = "") -> str:
    min_win, max_win = _CHIP_TIERS.get(tier, _CHIP_TIERS["1"])
    label = f"T{tier} {min_win}-{max_win}%" if max_win else f"T{tier} ≥{min_win}%"
    log_suffix = f"t{tier}" + (f"_{mods}" if mods else "")
    log_path = LOGS / f"chip_strategy_{log_suffix}.log"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"--- chip_strategy {label} started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    cmd = [sys.executable, "-X", "utf8", str(SCRIPTS / "chip_strategy.py"),
           "--min-win", str(min_win), "--max-today-pct", "5"]
    if max_win:              cmd += ["--max-win", str(max_win)]
    if "e" in mods:          cmd += ["--max-price", "50"]
    if "k" in mods:          cmd += ["--no-kcb"]
    if "h" in mods:          cmd += ["--max-6m-ratio", "0.9"]
    if "b" in mods:          cmd += ["--boll-near"]
    if "m" in mods:          cmd += ["--macd-conv"]
    if "z" in mods:          cmd += ["--macd-zero"]
    subprocess.Popen(cmd, cwd=str(ROOT),
                     stdout=open(log_path, "a", encoding="utf-8"),
                     stderr=subprocess.STDOUT)
    eta = "约2-4分钟" if any(x in mods for x in "bmz") else "约1-2分钟"
    return f"筹码策略 {label} 已启动（{eta}）✅\n结果推送到微信 📱"


def _h_chip(arg: str) -> str:
    arg = arg.strip()
    if not arg or arg == "help":
        return _CHIP_LIST
    if arg == "all" or arg.startswith("all"):
        rest = arg[3:].strip().replace(" ", "")
        log_path = LOGS / "daily_chip_scan.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"--- daily_chip_scan started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        cmd_args = [sys.executable, "-X", "utf8", str(SCRIPTS / "daily_chip_scan.py")]
        if "h" in rest: cmd_args += ["--high-filter"]
        if "b" in rest: cmd_args += ["--boll"]
        if "e" in rest: cmd_args += ["--max-price", "50"]
        if "k" in rest: cmd_args += ["--no-kcb"]
        subprocess.Popen(cmd_args, cwd=str(ROOT),
                         stdout=open(log_path, "a", encoding="utf-8"),
                         stderr=subprocess.STDOUT)
        return "筹码全档扫描（T1-T5）已启动，约2-3分钟后推微信 📱"
    parts = arg.split(None, 1)
    head  = parts[0]
    tail  = parts[1].lower().replace(" ", "") if len(parts) > 1 else ""
    if head and head[0].isdigit():
        tier_str = head[0]
        mods     = head[1:].lower() + tail
    else:
        return f"用法错误：c {arg}\n\n{_CHIP_LIST}"
    if tier_str not in _CHIP_TIERS:
        return f"档位 {tier_str} 不存在，有效范围 1-5\n\n{_CHIP_LIST}"
    return _launch_chip(tier_str, mods)


def _h_chip_data_driven(mods: str = "bekhm") -> str:
    log_path = LOGS / "chip_cad.log"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"--- chip_cad started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "chip_cad.py"), "--mods", mods],
        cwd=str(ROOT),
        stdout=open(log_path, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )
    return f"筹码数据驱动扫描（T4→T1→T2→T3→T5 {mods}）已启动 ✅\n约3-5分钟后推一条微信 📱"


def _h_restart() -> str:
    pid = _find_monitor_pid()
    killed = False
    if pid:
        try:
            os.kill(int(pid), signal.SIGTERM)
            killed = True
        except Exception:
            pass
        time.sleep(2)
    log_path = LOGS / "monitor_loop.log"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n--- Restarted by Feishu bot at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"), "--loop", "--interval", "5"],
        cwd=str(ROOT),
        stdout=open(log_path, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )
    note = f"（已终止旧进程 PID {pid}）" if killed else "（未找到旧进程，直接启动）"
    return f"monitor.py 已重启 ✅ {note}"


def _h_start_monitor() -> str:
    pid = _find_monitor_pid()
    if pid:
        return f"monitor.py 已在运行（PID {pid}）"
    monitor_log = LOGS / "monitor_loop.log"
    if monitor_log.exists() and time.time() - monitor_log.stat().st_mtime < 300:
        age = int((time.time() - monitor_log.stat().st_mtime) / 60)
        return f"monitor.py 看起来已在运行（日志 {age} 分钟前更新）"
    with open(monitor_log, "a", encoding="utf-8") as f:
        f.write(f"\n--- Started by Feishu bot at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"), "--loop", "--interval", "5"],
        cwd=str(ROOT),
        stdout=open(monitor_log, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )
    return "monitor.py 已启动 ✅"


def _h_kill_backtest() -> str:
    pid = _find_backtest_pid()
    if not pid:
        return "当前无回测进程在运行"
    try:
        PROCESS_TERMINATE = 1
        h = ctypes.windll.kernel32.OpenProcess(PROCESS_TERMINATE, False, int(pid))
        if h:
            ok = ctypes.windll.kernel32.TerminateProcess(h, 1)
            ctypes.windll.kernel32.CloseHandle(h)
            return f"✅ 已终止回测进程 PID {pid}" if ok else f"❌ TerminateProcess 失败（PID {pid}）"
        return f"❌ 无法打开进程 PID {pid}"
    except Exception as e:
        return f"❌ 终止失败: {e}"


def _h_shortcut(num: str) -> str:
    parts    = num.split(None, 1)
    main_num = parts[0] if parts else ""
    sub_arg  = parts[1].strip() if len(parts) > 1 else ""
    if not main_num:
        return _SC_LIST
    num = main_num
    if num == "1":
        return _h_start_monitor()
    elif num == "2":
        return _h_restart()
    elif num == "3":
        return _h_kill_backtest()
    elif num == "4":
        log_path = LOGS / "factor_ic_main.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"--- factor_analysis started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        subprocess.Popen(
            [sys.executable, "-X", "utf8", str(SCRIPTS / "factor_analysis.py"),
             "--rolling", "6", "--step", "20", "--out", str(ROOT / "data" / "factor_ic.json")],
            cwd=str(ROOT),
            stdout=open(log_path, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )
        return "因子IC回测已启动（后台运行，约1-2小时）✅"
    elif num == "5":
        log_path = LOGS / "batch_financials.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"--- batch_financials started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        subprocess.Popen(
            [sys.executable, "-X", "utf8", str(SCRIPTS / "tools" / "batch_financials.py")],
            cwd=str(ROOT),
            stdout=open(log_path, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )
        return "batch_financials.py 已启动（约1小时）✅"
    elif num == "6":
        log_path = LOGS / "build_universe.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"--- build_universe started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        subprocess.Popen(
            [sys.executable, "-X", "utf8", str(SCRIPTS / "tools" / "build_universe.py")],
            cwd=str(ROOT),
            stdout=open(log_path, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )
        return "build_universe.py 已启动（约5-10分钟）✅"
    elif num == "7":
        return _h_scan()
    elif num == "8":
        return _h_logs(20)
    elif num == "10":
        log_path = LOGS / "daily_chip_scan.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"--- daily_chip_scan started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        subprocess.Popen(
            [sys.executable, "-X", "utf8", str(SCRIPTS / "daily_chip_scan.py")],
            cwd=str(ROOT),
            stdout=open(log_path, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )
        return "筹码全档扫描（T1-T5）已启动，约1-2分钟后推微信 📱"
    elif num.startswith("9"):
        inline_mods = num[1:].lower()
        if sub_arg and sub_arg[0].isdigit():
            tier_str = sub_arg[0]
            sub_mods = sub_arg[1:].lower()
        else:
            tier_str = ""
            sub_mods = sub_arg.lower()
        tier = tier_str if tier_str in _CHIP_TIERS else "1"
        return _launch_chip(tier, inline_mods + sub_mods)
    else:
        return f"未知快捷命令 sc {num}\n\n{_SC_LIST}"


def _h_research(code: str) -> str:
    if not code:
        return "用法: fx600519 或 研究 600519"
    try:
        r = subprocess.run(
            [sys.executable, "-X", "utf8", str(SCRIPTS / "research.py"), code, "--text"],
            cwd=str(ROOT), capture_output=True, text=True, timeout=180,
            encoding="utf-8", errors="replace",
        )
        stdout = (r.stdout or "").strip()
        stderr_clean = "\n".join(
            l for l in (r.stderr or "").splitlines()
            if l.strip() and "\r" not in l and "%" not in l
        )
        if not stdout:
            return f"❌ {code} 无报告输出\n{stderr_clean[-500:]}" if stderr_clean else f"❌ {code} 无输出"
        return stdout[:3500] + ("\n...(已截断)" if len(stdout) > 3500 else "")
    except subprocess.TimeoutExpired:
        return f"❌ 分析 {code} 超时（>3min）"
    except Exception as e:
        return f"❌ 分析失败: {e}"


_FACTOR_ZH = {
    "accruals": "应计因子", "amihud_illiquidity": "Amihud非流动性",
    "ar_quality": "应收账款质量", "asset_growth": "资产增速",
    "atr_normalized": "ATR波动率", "bb_squeeze": "布林压缩",
    "bollinger_position": "布林位置", "cash_flow_quality": "现金流质量",
    "chip_distribution": "筹码分布", "div_yield": "股息率",
    "divergence": "背离信号", "gap_frequency": "跳空频率",
    "gross_margin_trend": "毛利率趋势", "growth": "成长",
    "hammer_bottom": "锤形底", "idiosyncratic_vol": "特质波动率",
    "intraday_vs_overnight": "日内/隔夜收益比", "limit_hits": "涨停次数",
    "limit_open_rate": "开板率", "low_volatility": "低波动",
    "ma60_deviation": "MA60偏离", "ma_alignment": "均线排列",
    "macd_signal": "MACD信号", "main_inflow": "主力净流入",
    "market_beta": "市场Beta", "market_relative_strength": "市场相对强度",
    "max_return": "最大单日涨幅", "medium_term_momentum": "中期动量",
    "momentum": "动量", "momentum_concavity": "动量凸性",
    "nearness_to_high": "接近历史高点", "northbound": "北向资金",
    "obv_trend": "OBV趋势", "piotroski": "Piotroski F分",
    "position_52w": "52周价格位置", "price_efficiency": "价格效率",
    "price_inertia": "价格惯性", "price_volume_corr": "量价相关性",
    "quality": "质量", "return_skewness": "收益偏度",
    "reversal": "反转", "roe_trend": "ROE趋势",
    "rsi_signal": "RSI信号", "short_interest": "融券做空",
    "size_factor": "规模因子", "trend_linearity": "趋势线性度",
    "turnover_acceleration": "换手加速", "turnover_percentile": "换手率分位",
    "upday_ratio": "上涨天占比", "value": "价值",
    "volume": "成交量", "volume_expansion": "放量信号",
    "volume_ratio": "量比", "overhead_resistance": "套牢盘压力",
    "upper_shadow_reversal": "上影线反转", "sector_sympathy": "板块共振",
}

_FACTOR_GLOSSARY: dict[str, str] = {
    "value":                 "估值因子：PE/PB 越低得分越高，便宜股票",
    "growth":                "成长因子：营收/利润增速越快得分越高",
    "momentum":              "短期动量：近20日涨幅，涨得多的继续涨",
    "quality":               "质量因子：ROE/ROA 越高越好，盈利能力",
    "northbound":            "北向资金：外资净买入，聪明钱流向",
    "volume":                "放量突破：成交量放大配合价格突破",
    "position_52w":          "52周位置：价格在一年高低点中的位置",
    "div_yield":             "股息率：股息/股价，越高越稳健",
    "volume_ratio":          "量比：当日量 vs 过去均量",
    "ma_alignment":          "均线排列：短中长期均线由上到下排列",
    "low_volatility":        "低波动：日涨跌幅越稳定得分越高",
    "reversal":              "短期反转：近期大跌后反弹，均值回归",
    "accruals":              "应计因子：现金利润 vs 会计利润",
    "asset_growth":          "资产增速：总资产增长越快反而得分低",
    "piotroski":             "Piotroski F分：9项财务健康指标，0-9分",
    "short_interest":        "融券：融券余额占比高=空头多=负面",
    "rsi_signal":            "RSI：相对强弱指数，超买/超卖",
    "macd_signal":           "MACD：均线差离值，金叉死叉",
    "turnover_percentile":   "换手率分位：当前换手 vs 历史分位",
    "chip_distribution":     "筹码分布：持筹成本集中度",
    "limit_hits":            "涨停次数：近期涨停频率",
    "price_inertia":         "价格惯性：连续同向运动",
    "divergence":            "背离：价格和指标方向不一致",
    "bollinger_position":    "布林位置：价格在布林带中的位置",
    "roe_trend":             "ROE趋势：净资产收益率是否持续改善",
    "cash_flow_quality":     "现金流质量：经营现金流 vs 净利润",
    "main_inflow":           "主力净流入：大单资金净流入",
    "turnover_acceleration": "换手加速：换手率增速",
    "momentum_concavity":    "动量凸性：动量是否在加速",
    "bb_squeeze":            "布林压缩：波动率收窄，即将爆发",
    "idiosyncratic_vol":     "特质波动率：剥离市场后的个股波动",
    "gross_margin_trend":    "毛利率趋势：毛利润率变化方向",
    "size_factor":           "规模因子：市值大小，小市值溢价",
    "amihud_illiquidity":    "Amihud非流动性：价格冲击/成交额",
    "medium_term_momentum":  "中期动量：60-250日趋势，机构持仓",
    "obv_trend":             "OBV趋势：能量潮，成交量累积方向",
    "market_beta":           "市场Beta：与大盘同涨同跌的程度",
    "atr_normalized":        "ATR：平均真实波幅",
    "ma60_deviation":        "MA60偏离：价格偏离60日均线程度",
    "max_return":            "最大单日涨幅：近期最大单日涨幅",
    "return_skewness":       "收益偏度：涨多跌少为正偏",
    "upday_ratio":           "上涨天占比：近期上涨天数比例",
    "volume_expansion":      "放量：成交量是否持续放大",
    "nearness_to_high":      "接近高点：价格接近历史高点",
    "price_volume_corr":     "量价相关：价涨量增为正，健康上涨",
    "trend_linearity":       "趋势线性：价格上涨的平滑程度",
    "gap_frequency":         "跳空频率：跳空缺口出现频率",
    "market_relative_strength": "市场相对强度：vs 大盘的相对表现",
    "price_efficiency":      "价格效率：趋势 vs 噪声",
    "intraday_vs_overnight": "日内/隔夜：散户/机构行为信号",
    "hammer_bottom":         "锤形底：K线锤形，下影线长=支撑强",
    "limit_open_rate":       "开板率：涨停后次日开板比例",
    "upper_shadow_reversal": "上影线反转：长上影线=卖压大",
    "sector_sympathy":       "板块共振：所在板块整体涨势",
    "overhead_resistance":   "套牢盘压力：历史成交密集区的卖压",
}


def _h_ic() -> str:
    ic_path = ROOT / "data" / "factor_ic.json"
    if not ic_path.exists():
        return "factor_ic.json 不存在"
    data = json.loads(ic_path.read_text(encoding="utf-8"))
    ic_table = data.get("ic_table", {})
    if not ic_table:
        return "ic_table 为空"
    def _safe_ic(item):
        v = item[1]
        return abs(v.get("mean_ic") or 0.0) if isinstance(v, dict) else 0.0
    buy_factors = [(n, v) for n, v in ic_table.items() if not n.startswith("sell_score_")]
    buy_factors.sort(key=_safe_ic, reverse=True)
    valid = [(n, v) for n, v in buy_factors if isinstance(v, dict) and v.get("mean_ic") is not None]
    def _fmt(name, v):
        ic = v.get("mean_ic") or 0.0
        zh = _FACTOR_ZH.get(name, name)
        return f"  {ic:+.3f}  {zh}"
    lines = [f"因子 IC（买入侧）有效 {len(valid)}/{len(buy_factors)} 个\nTop 10"]
    for name, v in valid[:10]:
        lines.append(_fmt(name, v))
    lines.append("Bottom 10")
    for name, v in valid[-10:]:
        lines.append(_fmt(name, v))
    return "\n".join(lines)


def _h_factor_info(name: str) -> str:
    name = name.strip().lower().replace("-", "_")
    if name in _FACTOR_GLOSSARY:
        zh = _FACTOR_ZH.get(name, name)
        return f"{name} （{zh}）\n{_FACTOR_GLOSSARY[name]}"
    matches = [(k, v) for k, v in _FACTOR_GLOSSARY.items()
               if name in k or name in _FACTOR_ZH.get(k, "")]
    if not matches:
        all_names = sorted(_FACTOR_GLOSSARY.keys())
        return (f"未找到因子 {name}\n可用: " +
                ", ".join(all_names[:20]) +
                (f" ...共{len(all_names)}个" if len(all_names) > 20 else ""))
    k, v = matches[0]
    zh = _FACTOR_ZH.get(k, k)
    return f"{k} （{zh}）\n{v}"


def _h_read_file(path: str) -> str:
    try:
        fp = (ROOT / path).resolve()
        if not str(fp).startswith(str(ROOT)):
            return "❌ 不允许读取项目目录之外的文件"
        if not fp.exists():
            return f"文件不存在: {path}"
        content = fp.read_text(encoding="utf-8", errors="replace")
        if len(content) > 2000:
            content = content[:2000] + "\n... (已截断)"
        return content
    except Exception as e:
        return f"读取失败: {e}"


_AI_BOT_NAME = "太子"  # cc-connect AI bot name for unknown commands


# ── Dispatch ──────────────────────────────────────────────────────────────────
def _dispatch_sync(t: str) -> str | None:
    """Return reply string for known commands, or None to hand off to Claude."""
    t = re.sub(r'^(sc)(\d)', r'\1 \2', t)
    t = re.sub(r'^(c)(\d)',  r'\1 \2', t)

    if t in ("帮助", "help", "h", "？", "?"):
        return _HELP
    if t == "t":
        return _h_tasks()
    if t == "fh":
        return _FACTOR_HELP
    if t in ("状态", "status", "z"):
        return _h_status()
    if t in ("ic", "因子ic", "因子IC"):
        try: return _h_ic()
        except Exception as e: return f"❌ ic 出错: {e}"
    if t.startswith("fx") and len(t) > 2 and (t[2:3].isdigit() or t[2:3] == " "):
        return _h_research(t[2:].strip())
    if t.startswith("研究 ") or t.startswith("分析 "):
        return _h_research(t.split(None, 1)[1].strip())
    if t in ("fx", "研究", "分析"):
        return "用法: fx600519 或 研究 贵州茅台"
    if t == "sc" or (t.startswith("sc ") and t[3:4] != ""):
        return _h_shortcut(t[2:].strip())
    if t in ("sch", "快捷列表"):
        return _SC_LIST
    if t == "ch":
        return _CHIP_LIST
    if t == "cad" or t.startswith("cad"):
        mods = (t[4:].strip().replace(" ", "") or "bekhm") if t.startswith("cadm") else \
               (t[3:].strip().replace(" ", "") or "bekh")
        return _h_chip_data_driven(mods)
    if t == "ca" or t.startswith("ca"):
        return _h_chip("all " + t[2:] if t[2:] else "all")
    if t == "c" or t.startswith("c "):
        return _h_chip(t[2:].strip() if t.startswith("c ") else "")
    if t in ("ich", "因子列表"):
        names = sorted(_FACTOR_GLOSSARY.keys())
        pairs = [f"{n} {_FACTOR_ZH.get(n, '')}" for n in names]
        return f"因子列表（共{len(names)}个）— 用 icf 因子名 查详情\n" + "  ".join(pairs)
    if t in ("icf", "因子介绍") or t.startswith("icf ") or t.startswith("因子介绍 "):
        name = t.split(None, 1)[1].strip() if " " in t else ""
        return _h_factor_info(name) if name else "用法: icf momentum"
    if t in ("br", "回测结果"):
        return _h_backtest_result()
    if t in ("bs", "回测状态"):
        try: return _h_backtest_status()
        except Exception as e: return f"❌ bs 出错: {e}"
    if t.startswith("bte") or t in ("etf回测",):
        raw = t[3:].strip() if t.startswith("bte") else ""
        m = re.match(r'^(\d+)', raw)
        return _h_backtest_etf(periods=int(m.group(1)) if m else 12)
    if t.startswith("回测") or t.startswith("bt") or t.startswith("backtest"):
        raw = t
        for prefix in ("backtest", "回测", "bt"):
            if raw.startswith(prefix):
                raw = raw[len(prefix):].strip()
                break
        periods, universe = 16, "main"
        r2 = raw.replace("期", "")
        m = re.match(r'^(\d+)([sm]?)$', r2) or re.match(r'^([sm])(\d+)$', r2)
        if m:
            g1, g2 = m.group(1), m.group(2)
            if g1.isdigit():
                periods  = int(g1)
                universe = "smallcap" if g2 == "s" else "main"
            else:
                universe = "smallcap" if g1 == "s" else "main"
                periods  = int(g2)
        return _h_backtest(periods=periods, universe=universe)

    return None  # → unknown command


# ── Feishu event handler ──────────────────────────────────────────────────────
def _on_message_receive(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    event  = data.event
    sender = event.sender
    msg    = event.message

    if sender.sender_type != "user":
        return

    allowed = _allowed_open_ids()
    open_id = sender.sender_id.open_id
    if allowed and open_id not in allowed:
        return

    if msg.message_type != "text":
        return

    try:
        content = json.loads(msg.content)
        text    = content.get("text", "").strip()
    except Exception:
        return

    # Strip @mention tags in group chats
    text = re.sub(r"@_user_\S*\s*", "", text).strip()
    if not text:
        return

    message_id = msg.message_id
    chat_id    = msg.chat_id
    print(f"[MSG] open_id={open_id} chat_id={chat_id} text={text!r}", flush=True)

    def handle():
        if text.lower() in ("chatid", "chat_id"):
            reply_text(message_id, f"chat_id: {chat_id}\nopen_id: {open_id}")
            return
        result = _dispatch_sync(text)
        if result is None:
            reply_text(message_id, f"❓ 未识别命令，发 h 查看帮助。\nAI 对话请找 @{_AI_BOT_NAME}")
        else:
            reply_text(message_id, result)

    threading.Thread(target=handle, daemon=True).start()


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    global _client
    cfg   = _cfg()
    fs    = _fs_cfg()
    app_id     = fs["app_id"]
    app_secret = fs["app_secret"]

    _client = lark.Client.builder().app_id(app_id).app_secret(app_secret).build()

    event_handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register(lark.im.v1.P2ImMessageReceiveV1, _on_message_receive)
        .build()
    )

    while True:
        try:
            ws = lark.ws.Client(
                app_id, app_secret,
                event_handler=event_handler,
                log_level=lark.LogLevel.WARNING,
            )
            print(f"[StockSage Feishu] starting (app_id={app_id})", flush=True)
            ws.start()
        except KeyboardInterrupt:
            print("[StockSage Feishu] stopped by user", flush=True)
            break
        except Exception as e:
            print(f"[StockSage Feishu] crashed: {e!r}, restarting in 10s…", flush=True)
            time.sleep(10)


if __name__ == "__main__":
    main()
