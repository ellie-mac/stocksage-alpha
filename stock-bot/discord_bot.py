"""
StockSage Discord Bot  (with Claude AI)
========================================
支持自然语言对话 + 固定命令两种模式。

配置 (stock-bot/config.json):
    discord.bot_token    - Discord Developer Portal → Bot → Token
    discord.allowed_ids  - 只接受这些用户 ID（留空=接受所有人）
    claude.api_key       - Anthropic API Key（留空=仅固定命令模式）

启动:
    python -X utf8 stock-bot/discord_bot.py

固定命令 (不消耗 Claude API):
    h                    帮助
    z                    系统状态（进程 + 日志）
    q                    全局概览（进程/回测/持仓/推荐）
    c                    持仓盈亏推送 📱微信
    hh                   持仓列表
    s                    扫盘推送 📱微信
    tn                   全市场扫描 (--test-now) 📱微信
    p                    今日推荐
    fx 600519            单股分析报告（~1min）
    l / l30              monitor 最近日志
    ic                   因子 IC 摘要（有效因子排序）
    r                    重启 monitor
    sm                   启动 monitor
    bs                   回测进度
    kb                   终止回测
    bt / bt16 / bt16s    启动回测（s=小盘）
    sug                  给出操作建议
    do                   执行上条建议

对话模式 (消耗 claude.api_key 额度):
    其他内容走 Claude AI 自然语言对话。
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import discord

# Ensure stdout/stderr handle UTF-8 on Windows
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT    = Path(__file__).resolve().parent.parent
BOT_DIR = Path(__file__).resolve().parent
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

_executor = ThreadPoolExecutor(max_workers=4)

# ── Config ────────────────────────────────────────────────────────────────────
def _cfg() -> dict:
    return json.loads((BOT_DIR / "config.json").read_text(encoding="utf-8"))

def _dc_cfg() -> dict:
    return _cfg().get("discord", {})

def _bot_token() -> str:
    t = _dc_cfg().get("bot_token", "")
    if not t or t.startswith("Discord"):
        raise RuntimeError("请先在 stock-bot/config.json 填入 discord.bot_token")
    return t

def _allowed_ids() -> set[int]:
    ids = _dc_cfg().get("allowed_ids", [])
    return {int(i) for i in ids} if ids else set()

def _claude_api_key() -> str:
    return _cfg().get("claude", {}).get("api_key", "")

# ── Command handlers ──────────────────────────────────────────────────────────
_HELP = """**StockSage 命令**
`z` 系统状态  |  `q` 全局概览
`c` 持仓推送 📱微信  |  `hh` 持仓列表
`s` 扫盘推送 📱微信  |  `tn` 全市场扫描 📱微信
`p` 今日推荐  |  `ic` 因子IC摘要
`fx 600519` 单股分析  |  `l` / `l30` 日志
`r` 重启monitor  |  `sm` 启动monitor
`bs` 回测进度  |  `kb` 终止回测
`bt` / `bt16` / `bt16s` 启动回测（s=小盘）
`sug` 给我建议  |  `do` 执行上条建议
`h` 帮助

💬 其他内容走AI对话（消耗token）"""

def _h_status() -> str:
    lines = [f"**系统状态** @ {datetime.now():%Y-%m-%d %H:%M:%S}\n"]
    r = subprocess.run(
        ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/V"],
        capture_output=True, text=True
    )
    procs = []
    for line in r.stdout.strip().splitlines()[1:]:
        parts = line.strip('"').split('","')
        if len(parts) >= 8:
            procs.append(f"  PID {parts[1]} | CPU {parts[7]} | {parts[4]}")
    if procs:
        lines.append("**Python 进程:**")
        lines.extend(procs)
    else:
        lines.append("无运行中的 Python 进程")

    log_path = SCRIPTS / "monitor_loop.log"
    if log_path.exists():
        tail = log_path.read_bytes()[-3000:].decode("utf-8", errors="replace")
        last = [l for l in tail.splitlines() if l.strip()][-5:]
        lines.append("\n**Monitor 最近日志:**")
        lines.append("```")
        lines.extend(last)
        lines.append("```")
    else:
        lines.append("\nmonitor_loop.log 不存在（monitor 可能未运行）")
    return "\n".join(lines)


def _h_holdings() -> str:
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--sell-only", "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return "已触发持仓推送，结果稍后发送到微信 📱"


def _h_scan() -> str:
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return "已触发扫盘，结果稍后发送到微信 📱"


def _h_test_now() -> str:
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--test-now"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return "已触发全市场扫描 (--test-now)，结果稍后发送到微信 📱"


def _h_research(code: str) -> str:
    """Run research.py for a single stock and return trimmed output."""
    if not code:
        return "用法: `fx 600519` 或 `研究 600519`"
    try:
        r = subprocess.run(
            [sys.executable, "-X", "utf8", str(SCRIPTS / "research.py"), code],
            cwd=str(ROOT), capture_output=True, text=True, timeout=120, encoding="utf-8"
        )
        out = (r.stdout or "") + (r.stderr or "")
        out = out.strip()
        if not out:
            return f"research.py 对 {code} 无输出（检查代码是否正确）"
        if len(out) > 1800:
            out = out[:1800] + "\n...(已截断)"
        return f"**{code} 分析报告**\n```\n{out}\n```"
    except subprocess.TimeoutExpired:
        return f"❌ 分析 {code} 超时（>120s）"
    except Exception as e:
        return f"❌ 分析失败: {e}"


def _h_picks() -> str:
    picks_path = ROOT / "data" / "latest_picks.json"
    if not picks_path.exists():
        return "latest_picks.json 不存在，今日可能尚未选股。"
    data  = json.loads(picks_path.read_text(encoding="utf-8"))
    items = data.get("results", [])
    ts    = data.get("timestamp") or data.get("date", "")
    date  = ts[:10] if ts else "?"
    lines = [f"**今日推荐** ({date})\n"]
    for i, s in enumerate(items[:10], 1):
        name  = s.get("name") or s.get("code", "?")
        score = s.get("composite", s.get("score", 0))
        lines.append(f"{i}. {name}  _(得分 {score:.3f})_")
    return "\n".join(lines)


def _h_logs(n: int = 20) -> str:
    log_path = SCRIPTS / "monitor_loop.log"
    if not log_path.exists():
        return "monitor_loop.log 不存在。"
    tail = log_path.read_bytes()[-8000:].decode("utf-8", errors="replace")
    last = [l for l in tail.splitlines() if l.strip()][-n:]
    body = "\n".join(last) or "(空)"
    if len(body) > 1800:
        body = "..." + body[-1800:]
    return f"**日志 -{n}**\n```\n{body}\n```"


def _find_monitor_pid() -> str | None:
    try:
        wmic = r"C:\Windows\System32\wbem\wmic.exe"
        r = subprocess.run([wmic, "process", "where", "name='python.exe'",
                            "get", "processid,commandline", "/format:csv"],
                           capture_output=True, text=True)
        for line in r.stdout.splitlines():
            if "monitor" in line.lower() and "--loop" in line.lower():
                parts = line.strip().split(",")
                return parts[-1].strip() if parts else None
    except Exception:
        pass
    return None


def _h_restart() -> str:
    import time, signal as _signal
    pid = _find_monitor_pid()
    killed = False
    if pid:
        try:
            os.kill(int(pid), _signal.SIGTERM)
            killed = True
        except Exception:
            pass
        time.sleep(2)

    log_path = SCRIPTS / "monitor_loop.log"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n--- Restarted by Discord bot at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    log_fh = open(log_path, "a", encoding="utf-8")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--loop", "--interval", "5"],
        cwd=str(ROOT),
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )
    note = f"（已终止旧进程 PID {pid}）" if killed else "（未找到旧进程，直接启动）"
    return f"monitor.py 已重启 ✅ {note}"


# Stores last suggestion's executable commands for `do`
_last_suggestion: dict = {}


def _h_suggest() -> str:
    import time
    actions: list[tuple[str | None, str]] = []  # (cmd_or_None, description)

    # Rule 1: monitor running?
    monitor_running = False
    try:
        wmic = r"C:\Windows\System32\wbem\wmic.exe"
        r = subprocess.run([wmic, "process", "where", "name='python.exe'",
                            "get", "commandline", "/format:csv"],
                           capture_output=True, text=True)
        for line in r.stdout.splitlines():
            if "monitor" in line.lower() and "--loop" in line.lower():
                monitor_running = True
    except Exception:
        pass

    if not monitor_running:
        actions.append(("sm", "monitor.py 未运行，需要启动"))

    # Rule 2: backtest running or done?
    logs = sorted(SCRIPTS.glob("backtest_*.log"), key=lambda f: f.stat().st_mtime, reverse=True)
    if logs:
        log = logs[0]
        log_age = time.time() - log.stat().st_mtime
        out_json = ROOT / "data" / (log.stem + ".json")
        content = log.read_bytes().decode("utf-8", errors="replace")
        periods = [l for l in content.splitlines() if "Period" in l and "/" in l]
        if not out_json.exists() and log_age < 3600:
            prog = periods[-1].strip() if periods else "进度未知"
            actions.append((None, f"回测进行中（{prog}），等待完成后更新权重"))
        elif out_json.exists():
            # Check if weights were updated after this backtest
            research_py = SCRIPTS / "research.py"
            if research_py.exists() and research_py.stat().st_mtime < out_json.stat().st_mtime:
                actions.append((None, "回测已完成但 research.py 权重未更新，建议手动运行权重更新"))

    # Rule 3: holdings need check?
    h_path = ROOT / "holdings.json"
    if h_path.exists():
        h = json.loads(h_path.read_text(encoding="utf-8"))
        if h:
            actions.append(("c", f"有 {len(h)} 只持仓，建议检查盈亏"))

    # Rule 4: last scan too old?
    sig_path = ROOT / "data" / "signals_log.json"
    if sig_path.exists():
        sig_age = time.time() - sig_path.stat().st_mtime
        if sig_age > 14400:  # >4 hours
            actions.append(("s", f"信号日志 {int(sig_age/3600):.0f}h 未更新，建议扫盘"))

    if not actions:
        actions.append((None, "系统运行正常，无需操作"))

    # Store executable commands for `do`
    _last_suggestion["cmds"] = [a[0] for a in actions if a[0]]
    _last_suggestion["time"] = time.time()

    lines = ["**建议（优先级排序）:**"]
    for i, (cmd, desc) in enumerate(actions, 1):
        suffix = f"  →  `{cmd}`" if cmd else ""
        lines.append(f"{i}. {desc}{suffix}")
    if _last_suggestion["cmds"]:
        lines.append(f"\n发 `do` 自动执行以上 {len(_last_suggestion['cmds'])} 条命令")
    return "\n".join(lines)


def _h_do() -> str:
    import time
    if not _last_suggestion.get("cmds"):
        return "没有待执行的建议，先发 `sug`"
    age = time.time() - _last_suggestion.get("time", 0)
    if age > 300:
        return "建议已超过5分钟，请重新发 `sug`"
    results = []
    for cmd in _last_suggestion["cmds"]:
        r = _dispatch_sync(cmd)
        if isinstance(r, str):
            results.append(f"`{cmd}` → {r[:120]}")
    _last_suggestion.clear()
    return "\n".join(results) if results else "没有可执行的命令"


def _h_overview() -> str:
    import time
    lines = [f"**全局概览** @ {datetime.now():%Y-%m-%d %H:%M:%S}\n"]

    # --- Processes (log-mtime based, no wmic needed) ---
    procs = []
    monitor_log = SCRIPTS / "monitor_loop.log"
    if monitor_log.exists() and time.time() - monitor_log.stat().st_mtime < 900:
        age = int((time.time() - monitor_log.stat().st_mtime) / 60)
        procs.append(f"  ✅ monitor.py 运行中（日志 {age}分钟前更新）")
    else:
        procs.append("  ❌ monitor.py 未运行（或超过15分钟无日志）")

    bt_pid = _find_backtest_pid()
    bt_logs = sorted(SCRIPTS.glob("backtest_*.log"), key=lambda f: f.stat().st_mtime, reverse=True)
    if bt_pid or (bt_logs and time.time() - bt_logs[0].stat().st_mtime < 1800
                  and not (ROOT / "data" / (bt_logs[0].stem + ".json")).exists()):
        procs.append("  ⏳ backtest.py 运行中")

    lines.append("**进程:**")
    lines.extend(procs)

    # --- Backtest progress ---
    if bt_logs:
        log = bt_logs[0]
        age_min = int((time.time() - log.stat().st_mtime) / 60)
        content = log.read_bytes().decode("utf-8", errors="replace")
        periods = [l for l in content.splitlines() if "Period" in l and "/" in l]
        out_json = ROOT / "data" / (log.stem + ".json")
        if out_json.exists():
            lines.append(f"\n**回测:** `{log.stem}` 已完成 ✅")
        elif age_min < 60:
            prog = periods[-1].strip() if periods else "进度未知"
            lines.append(f"\n**回测:** `{log.stem}` 进行中 — {prog}")

    # --- Today's picks ---
    picks_path = ROOT / "data" / "latest_picks.json"
    if picks_path.exists():
        data = json.loads(picks_path.read_text(encoding="utf-8"))
        ts = data.get("timestamp") or data.get("date", "")
        date_str = ts[:10] if ts else "未知"
        n = len(data.get("results", []))
        lines.append(f"\n**最近推荐:** {date_str}，共 {n} 只")

    # --- Holdings ---
    h_path = ROOT / "holdings.json"
    if h_path.exists():
        h = json.loads(h_path.read_text(encoding="utf-8"))
        lines.append(f"\n**持仓:** {len(h)} 只")

    return "\n".join(lines)


def _h_holdings_list() -> str:
    h_path = ROOT / "holdings.json"
    if not h_path.exists():
        return "holdings.json 不存在"
    data = json.loads(h_path.read_text(encoding="utf-8"))
    if not data:
        return "当前无持仓"
    lines = [f"**持仓列表** ({len(data)} 只)\n"]
    for s in data:
        lines.append(f"`{s['code']}` {s['name']}  成本 {s.get('cost_price', '?')}")
    return "\n".join(lines)


_FACTOR_ZH = {
    "accruals":                "应计因子（盈利质量）",
    "amihud_illiquidity":      "Amihud非流动性",
    "ar_quality":              "应收账款质量",
    "asset_growth":            "资产增速",
    "atr_normalized":          "ATR波动率",
    "bb_squeeze":              "布林压缩",
    "bollinger_position":      "布林位置",
    "cash_flow_quality":       "现金流质量",
    "chip_distribution":       "筹码分布",
    "div_yield":               "股息率",
    "divergence":              "背离信号",
    "gap_frequency":           "跳空频率",
    "gross_margin_trend":      "毛利率趋势",
    "growth":                  "成长",
    "hammer_bottom":           "锤形底",
    "idiosyncratic_vol":       "特质波动率",
    "intraday_vs_overnight":   "日内/隔夜收益比",
    "limit_hits":              "涨停次数",
    "limit_open_rate":         "开板率",
    "low_volatility":          "低波动",
    "ma60_deviation":          "MA60偏离",
    "ma_alignment":            "均线排列",
    "macd_signal":             "MACD信号",
    "main_inflow":             "主力净流入",
    "market_beta":             "市场Beta",
    "market_relative_strength":"市场相对强度",
    "max_return":              "最大单日涨幅",
    "medium_term_momentum":    "中期动量",
    "momentum":                "动量",
    "momentum_concavity":      "动量凸性",
    "nearness_to_high":        "接近历史高点",
    "northbound":              "北向资金",
    "obv_trend":               "OBV趋势",
    "piotroski":               "Piotroski F分",
    "position_52w":            "52周价格位置",
    "price_efficiency":        "价格效率",
    "price_inertia":           "价格惯性",
    "price_volume_corr":       "量价相关性",
    "quality":                 "质量",
    "return_skewness":         "收益偏度",
    "reversal":                "反转",
    "roe_trend":               "ROE趋势",
    "rsi_signal":              "RSI信号",
    "short_interest":          "融券做空",
    "size_factor":             "规模因子",
    "trend_linearity":         "趋势线性度",
    "turnover_acceleration":   "换手加速",
    "turnover_percentile":     "换手率分位",
    "upday_ratio":             "上涨天占比",
    "value":                   "价值",
    "volume":                  "成交量",
    "volume_expansion":        "放量信号",
    "volume_ratio":            "量比",
}

def _h_ic() -> str:
    ic_path = ROOT / "factor_ic.json"
    if not ic_path.exists():
        return "factor_ic.json 不存在"
    data = json.loads(ic_path.read_text(encoding="utf-8"))
    ic_table = data.get("ic_table", {})
    if not ic_table:
        return "ic_table 为空"

    def _safe_ic(item):
        v = item[1]
        if not isinstance(v, dict):
            return 0.0
        return abs(v.get("mean_ic") or 0.0)

    # Only show buy-side factors (exclude sell_score_ entries)
    buy_factors = [(n, v) for n, v in ic_table.items() if not n.startswith("sell_score_")]
    buy_factors = sorted(buy_factors, key=_safe_ic, reverse=True)
    valid = [(n, v) for n, v in buy_factors if isinstance(v, dict) and v.get("mean_ic") is not None]

    def _fmt(name, v):
        ic = v.get("mean_ic") or 0.0
        zh = _FACTOR_ZH.get(name, name)
        return f"  {ic:+.3f}  {zh}"

    lines = [f"**因子 IC（买入侧）** 有效 {len(valid)}/{len(buy_factors)} 个\n**Top 10 ↑**"]
    for name, v in valid[:10]:
        lines.append(_fmt(name, v))
    lines.append("\n**Bottom 10 ↓**")
    for name, v in valid[-10:]:
        lines.append(_fmt(name, v))
    return "\n".join(lines)


def _h_kill_backtest() -> str:
    import time
    pid = _find_backtest_pid()
    if not pid:
        logs = sorted(SCRIPTS.glob("backtest_*.log"), key=lambda f: f.stat().st_mtime, reverse=True)
        if logs:
            log = logs[0]
            out_json = ROOT / "data" / (log.stem + ".json")
            if out_json.exists():
                return f"当前无回测进程（{log.stem} 已完成）"
            age_min = int((time.time() - log.stat().st_mtime) / 60)
            if age_min < 30:
                return (f"wmic 未找到进程，但 {log.name} 在 {age_min} 分钟前更新，"
                        f"可能仍在运行。请用任务管理器手动终止 backtest.py")
        return "当前无回测进程在运行"
    try:
        import signal
        os.kill(int(pid), signal.SIGTERM)
        return f"✅ 已终止回测进程 PID {pid}"
    except Exception as e:
        return f"❌ 终止失败: {e}"


def _h_start_monitor() -> str:
    import time
    # Check via wmic first
    pid = _find_monitor_pid()
    if pid:
        return f"monitor.py 已在运行（PID {pid}）"
    # Fallback: recent log activity means it's likely running
    monitor_log = SCRIPTS / "monitor_loop.log"
    if monitor_log.exists() and time.time() - monitor_log.stat().st_mtime < 300:
        age = int((time.time() - monitor_log.stat().st_mtime) / 60)
        return f"monitor.py 看起来已在运行（日志 {age} 分钟前更新）"
    # Start it
    log_path = monitor_log
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n--- Started by Discord bot at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    log_fh = open(log_path, "a", encoding="utf-8")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--loop", "--interval", "5"],
        cwd=str(ROOT),
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )
    return "monitor.py 已启动 ✅"


def _find_backtest_pid() -> str | None:
    """Return PID string of running backtest.py process, or None."""
    try:
        wmic = r"C:\Windows\System32\wbem\wmic.exe"
        r = subprocess.run(
            [wmic, "process", "where", "name='python.exe'",
             "get", "processid,commandline", "/format:csv"],
            capture_output=True, text=True
        )
        for line in r.stdout.splitlines():
            if "backtest" in line.lower():
                parts = line.strip().split(",")
                if parts:
                    return parts[-1].strip()
    except Exception:
        pass
    return None


def _h_backtest_status() -> str:
    import time
    pid = _find_backtest_pid()

    # Find most recently modified backtest log
    logs = sorted(SCRIPTS.glob("backtest_*.log"), key=lambda f: f.stat().st_mtime, reverse=True)

    if not pid:
        # Fallback: if log modified <30min ago and output JSON missing, likely still running
        if logs:
            log = logs[0]
            log_age = time.time() - log.stat().st_mtime
            out_name = log.stem + ".json"  # e.g. backtest_main_16p.json
            out_file = ROOT / "data" / out_name
            if log_age < 1800 and not out_file.exists():
                pid = "unknown"  # treat as running

    if not pid:
        results = sorted(
            (ROOT / "data").glob("backtest_*.json"),
            key=lambda f: f.stat().st_mtime, reverse=True
        )
        if results:
            latest = results[0]
            age_min = int((time.time() - latest.stat().st_mtime) / 60)
            return f"无回测进程。最近结果: `{latest.name}`（{age_min} 分钟前完成）"
        return "无回测进程，data/ 下也没有结果文件。"

    pid_str = pid if (pid and pid != "unknown") else "（wmic未能获取）"
    lines = [f"**回测进行中** PID {pid_str}"]
    if logs:
        log = logs[0]
        lines.append(f"日志: `{log.name}`")
        content = log.read_bytes().decode("utf-8", errors="replace")
        periods_seen = [l for l in content.splitlines() if "Period" in l and "/" in l]
        if periods_seen:
            lines.append(f"进度: {periods_seen[-1].strip()}")
    return "\n".join(lines)


def _h_backtest(periods: int = 16, universe: str = "main", workers: int = 8) -> str:
    import time
    pid = _find_backtest_pid()
    if pid:
        return f"⚠️ 已有回测进程在运行（PID {pid}），请等待完成或先停止。"
    # Fallback: if log was updated recently and output JSON missing, treat as running
    if not pid:
        logs = sorted(SCRIPTS.glob("backtest_*.log"), key=lambda f: f.stat().st_mtime, reverse=True)
        if logs:
            log = logs[0]
            out_json = ROOT / "data" / (log.stem + ".json")
            if time.time() - log.stat().st_mtime < 1800 and not out_json.exists():
                return (f"⚠️ {log.name} 在 30 分钟内更新且无结果文件，"
                        f"回测可能仍在运行。如确认已停止请发 `bt` 重试。")

    universe_map = {
        "main":     SCRIPTS / "main_universe.json",
        "smallcap": SCRIPTS / "smallcap_universe.json",
    }
    universe_file = universe_map.get(universe, universe_map["main"])
    if not universe_file.exists():
        return f"❌ 股票池文件不存在: {universe_file}"

    out_file  = ROOT / "data" / f"backtest_{universe}_{periods}p.json"
    log_path  = SCRIPTS / f"backtest_{universe}_{periods}p.log"

    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"--- Backtest started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        f.write(f"    universe={universe_file.name}, periods={periods}, workers={workers}\n\n")

    proc = subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "backtest.py"),
         "--periods", str(periods),
         "--universe", str(universe_file),
         "--out", str(out_file),
         "--workers", str(workers)],
        cwd=str(ROOT),
        stdout=open(log_path, "a"),
        stderr=subprocess.STDOUT,
    )
    return (
        f"✅ 回测已启动 (PID {proc.pid})\n"
        f"• 股票池: {universe} ({universe_file.name})\n"
        f"• 期数: {periods}  Workers: {workers}\n"
        f"• 输出: {out_file.name}\n"
        f"• 日志: {log_path.name}\n"
        f"用 `日志` 命令或 `状态` 跟踪进度（预计每期 ~20 min）"
    )


def _h_read_file(path: str) -> str:
    """Read a project file for Claude to answer questions."""
    try:
        fp = (ROOT / path).resolve()
        # Safety: only allow files inside the project root
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


# ── Simple command dispatch (no Claude needed) ────────────────────────────────
def _dispatch_sync(text: str) -> str | None:
    """Return a reply string for known commands, or None to hand off to Claude."""
    t = text.strip().lstrip("/")
    try:
        return _dispatch_inner(t)
    except Exception as e:
        return f"❌ 命令执行出错: {e}"


def _dispatch_inner(t: str) -> str | None:

    if t in ("帮助", "help", "h", "？", "?"):
        return _HELP
    elif t in ("状态", "status", "z"):
        return _h_status()
    elif t in ("q", "全局概览", "当前状态", "overview"):
        return _h_overview()
    elif t in ("sug", "建议", "suggest", "你觉得呢"):
        return _h_suggest()
    elif t in ("do", "执行", "执行建议", "按照你说的做"):
        return _h_do()
    elif t in ("持仓", "c"):
        return _h_holdings()
    elif t in ("hh", "持仓列表"):
        return _h_holdings_list()
    elif t in ("ic", "因子ic", "因子IC"):
        try:
            return _h_ic()
        except Exception as e:
            return f"❌ ic 出错: {e}"
    elif t in ("信号", "扫盘", "scan", "s"):
        return _h_scan()
    elif t in ("tn", "test-now", "testnow", "全量扫描"):
        return _h_test_now()
    elif t in ("今日推荐", "推荐", "picks", "p"):
        return _h_picks()
    elif t.startswith("fx ") or t.startswith("研究 ") or t.startswith("分析 "):
        code = t.split(None, 1)[1].strip()
        return _h_research(code)
    elif t in ("fx", "研究", "分析"):
        return "用法: `fx 600519` 或 `研究 贵州茅台`"
    elif t.startswith("日志") or t.startswith("l"):
        raw = t[1:] if t.startswith("l") else t[2:]
        raw = raw.strip().replace("期", "")
        n = int(raw) if raw.isdigit() else 20
        return _h_logs(n)
    elif t in ("重启 monitor", "重启monitor", "restart monitor", "重启", "r"):
        return _h_restart()
    elif t in ("sm", "启动monitor", "start monitor"):
        return _h_start_monitor()
    elif t in ("kb", "终止回测", "kill backtest"):
        return _h_kill_backtest()
    elif t in ("回测状态", "bs", "backtest status"):
        try:
            return _h_backtest_status()
        except Exception as e:
            return f"❌ bs 出错: {e}"
    elif t.startswith("回测") or t.startswith("bt") or t.startswith("backtest"):
        # bt / bt16 / bt16s / 回测 16期 smallcap
        raw = t
        for prefix in ("backtest", "回测", "bt"):
            if raw.startswith(prefix):
                raw = raw[len(prefix):].strip()
                break
        parts    = raw.split()
        periods  = 16
        universe = "main"
        for p in parts:
            p2 = p.replace("期", "")
            if p2.isdigit():
                periods = int(p2)
            elif p in ("s", "small", "smallcap", "小盘"):
                universe = "smallcap"
            elif p in ("m", "main", "主"):
                universe = "main"
        # also handle compact form like "bt16s" or "bt8m"
        import re
        m = re.match(r'^(\d+)([sm]?)$', raw.replace("期",""))
        if m:
            periods  = int(m.group(1))
            universe = "smallcap" if m.group(2) == "s" else "main"
        return _h_backtest(periods=periods, universe=universe)

    return None  # hand off to Claude


# ── Claude AI tool executor ───────────────────────────────────────────────────
_CLAUDE_TOOLS = [
    {
        "name": "get_status",
        "description": "获取系统状态：Python进程列表和monitor最近日志",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_scan",
        "description": "立即触发买卖信号扫描（结果发微信）",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_holdings",
        "description": "触发持仓盈亏推送（结果发微信）",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_picks",
        "description": "获取今日选股推荐列表",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_logs",
        "description": "获取monitor运行日志",
        "input_schema": {
            "type": "object",
            "properties": {
                "n": {"type": "integer", "description": "显示行数，默认15"}
            },
            "required": [],
        },
    },
    {
        "name": "restart_monitor",
        "description": "重启monitor.py循环",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_backtest",
        "description": "启动股票回测任务（后台运行，需要数小时）",
        "input_schema": {
            "type": "object",
            "properties": {
                "periods":  {"type": "integer", "description": "回测期数，默认16"},
                "universe": {"type": "string",  "description": "股票池：main（主策略705股）或 smallcap（小盘股），默认main"},
                "workers":  {"type": "integer", "description": "并行worker数，默认8"},
            },
            "required": [],
        },
    },
    {
        "name": "read_file",
        "description": "读取项目文件内容，用于回答关于代码、配置、数据的问题",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "相对于项目根目录的文件路径，如 scripts/research.py"}
            },
            "required": ["path"],
        },
    },
]

_CLAUDE_SYSTEM = f"""你是 StockSage Alpha 的智能助手，运行在用户的 Windows 量化交易服务器上。
你可以控制 StockSage 系统、回答代码和策略问题、分析数据。

项目根目录: {ROOT}
主要脚本: scripts/  数据目录: data/  配置: stock-bot/config.json

规则：
- 内部充分分析，但最终回复极度精简：只给结论，1-3句话，不解释过程
- 需要文件内容时先用 read_file 读取再回答，不要凭记忆猜
- 回测任务耗时数小时，启动后只报告 PID 和日志文件名即可
"""

# Per-channel conversation history: channel_id -> list of messages
_history: dict[int, list[dict]] = {}
_MAX_HISTORY = 10  # keep last 10 messages (~5 turns)


_TOOL_RESULT_LIMIT = 1500  # max chars fed back to Claude per tool call

def _execute_tool(name: str, inputs: dict) -> str:
    if name == "get_status":
        result = _h_status()
    elif name == "run_scan":
        result = _h_scan()
    elif name == "get_holdings":
        result = _h_holdings()
    elif name == "get_picks":
        result = _h_picks()
    elif name == "get_logs":
        result = _h_logs(inputs.get("n", 15))
    elif name == "restart_monitor":
        result = _h_restart()
    elif name == "run_backtest":
        result = _h_backtest(
            periods=inputs.get("periods", 16),
            universe=inputs.get("universe", "main"),
            workers=inputs.get("workers", 8),
        )
    elif name == "read_file":
        result = _h_read_file(inputs.get("path", ""))
    else:
        return f"未知工具: {name}"
    # Trim oversized results before sending back to Claude
    if len(result) > _TOOL_RESULT_LIMIT:
        result = result[:_TOOL_RESULT_LIMIT] + "\n...(已截断)"
    return result


def _claude_dispatch(channel_id: int, text: str) -> str:
    try:
        import anthropic
    except ImportError:
        return "❌ anthropic 包未安装，运行: pip install anthropic"

    api_key = _claude_api_key()
    if not api_key:
        return "❌ 未配置 claude.api_key，请在 stock-bot/config.json 填入 Anthropic API Key"

    client = anthropic.Anthropic(api_key=api_key)

    history = _history.setdefault(channel_id, [])
    history.append({"role": "user", "content": text})

    messages = history.copy()

    # Agentic loop (tool use)
    for _ in range(5):  # max 5 rounds
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            system=_CLAUDE_SYSTEM,
            tools=_CLAUDE_TOOLS,
            messages=messages,
        )

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    print(f"[TOOL] {block.name}({block.input})", flush=True)
                    result = _execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
        else:
            # Final text response
            reply = "".join(
                block.text for block in response.content if hasattr(block, "text")
            ) or "(无回复)"

            # Save to history (keep last N turns)
            history.append({"role": "assistant", "content": reply})
            if len(history) > _MAX_HISTORY:
                _history[channel_id] = history[-_MAX_HISTORY:]

            return reply

    return "❌ Claude 未能在限定轮次内完成回复，请重试"


# ── Discord client ────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
client  = discord.Client(intents=intents)


@client.event
async def on_ready():
    print(f"[StockSage] Logged in as {client.user} (id={client.user.id})", flush=True)
    claude_ok = bool(_claude_api_key())
    print(f"[StockSage] Claude AI: {'enabled' if claude_ok else 'disabled (no api_key)'}", flush=True)
    if not _allowed_ids():
        print("[StockSage] allowed_ids empty — accepting all users", flush=True)


@client.event
async def on_message(message: discord.Message):
    if message.author == client.user:
        return

    text    = (message.content or "").strip()
    user_id = message.author.id
    allowed = _allowed_ids()

    if not text:
        return

    print(f"[MSG] user={message.author} id={user_id} text={text!r}", flush=True)

    if allowed and user_id not in allowed:
        print(f"  -> unauthorized (add {user_id} to discord.allowed_ids)", flush=True)
        await message.reply("❌ 未授权")
        return

    if not allowed:
        print(f"  -> tip: add {user_id} to discord.allowed_ids to restrict access", flush=True)

    async with message.channel.typing():
        loop = asyncio.get_event_loop()

        # Try fixed commands first (fast, no API call)
        result = await loop.run_in_executor(_executor, _dispatch_sync, text)

        # Fall back to Claude AI
        if result is None:
            if _claude_api_key():
                channel_id = message.channel.id
                result = await loop.run_in_executor(
                    _executor, _claude_dispatch, channel_id, text
                )
            else:
                result = f"未知命令: `{text}`\n\n发送 `帮助` 查看可用命令。\n💡 配置 `claude.api_key` 后可直接用自然语言对话。"

    MAX = 1990
    for chunk in [result[i:i+MAX] for i in range(0, len(result), MAX)]:
        await message.reply(chunk)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    client.run(_bot_token())
