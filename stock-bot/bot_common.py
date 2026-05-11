"""
StockSage Bot — shared logic for lark_bot.py and discord_bot.py
"""
from __future__ import annotations

import ctypes
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
BOT_DIR = Path(__file__).resolve().parent
SCRIPTS = ROOT / "src"
LOGS    = SCRIPTS / "logs"
LOGS.mkdir(exist_ok=True)

# ── Text constants ────────────────────────────────────────────────────────────
HELP = """\
  cmh 筹码策略 | hot 热榜策略 | gc 金叉共振 | pos 高低位 | p 今日推荐 | z 状态 | t 定时任务 | sch 快捷命令 | fh 因子/回测 | 其他走AI对话"""

FACTOR_HELP = """\
因子 & 分析
  ic 因子IC摘要 | ich 因子列表
  icf 因子名 因子说明 | fx600519 单股分析

回测
  bs 进度 | br 结果摘要
  bt 主板 | bts 小盘 | bte ETF
  数字=期数（bt默认16，bte默认12），s=小盘，如 bts24、bte6"""

SC_LIST = """\
快捷命令 sc N
  sc1 启动 monitor | sc2 重启 monitor
  sc3 终止回测 | sc4 因子IC回测
  sc5 预热财务缓存 | sc6 重建股票池
  sc7 扫盘推送 | sc8 monitor日志
  sc9[T][修饰] 筹码单档 | sc10 筹码全档

数据查询快捷
  p 今日推荐  hr 热榜结果  cr 筹码结果
  sg 近期信号  perf 策略表现"""

CHIP_LIST = """\
筹码命令
  cad  数据驱动 T1-T3（bekh）⭐
  cadm 同上 + MACD绿柱（bekhm）⭐
  ca  全档T1-T3 | cah 全档排高位 | cabekh 全档+全修饰

  c1 T1≥95%  c2 T2 90-95%  c3 T3 85-90%

修饰符（可叠加）
  b BOLL  e ≤50元  k 排科创  h 排高位  m MACD绿柱  z MACD近零
  示例：c1bmz  c2mz  c3kh  cad  cadm"""

HOT_LIST = """\
热榜策略（东方财富实时热榜）
  hs    热度扫描 top5%（动量过滤）⭐
  hsh   热度扫描 top5% + 排高位（距6月高点≥10%）⭐
  hs10  热度扫描 top10%
  hs10h 热度扫描 top10% + 排高位

热度来源：东方财富热榜，每2小时刷新"""

# ── Agent tool definitions ────────────────────────────────────────────────────
TOOLS: list[dict] = [
    {
        "name": "get_system_status",
        "description": "获取系统状态：StockSage各进程、今日推荐股票。适用于'z/状态/进程/系统怎么样'等查询。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_today_picks",
        "description": "获取今日主策略推荐股票列表（latest_picks.json）。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_golden_cross",
        "description": "获取金叉共振扫描结果（G0=7信号 G1=6信号 G2=5信号）。适用于'gc/金叉/今天有什么金叉'。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_stock_position",
        "description": "分析单只股票的高低位：52周位置%、距高点距离、MA20/MA60偏离、布林带位置、趋势方向。快速（<30s）。",
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {"type": "string", "description": "6位股票代码，如 '000001' 或 '600519'"},
            },
            "required": ["code"],
        },
    },
    {
        "name": "analyze_stock",
        "description": "对单只股票做全面因子分析报告，含动量/价值/质量/技术等多维评分。详细但较慢（1-3分钟）。",
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {"type": "string", "description": "6位股票代码或名称，如 '600519' 或 '贵州茅台'"},
            },
            "required": ["code"],
        },
    },
    {
        "name": "get_factor_ic",
        "description": "获取各因子的IC（信息系数）摘要，评估每个因子的预测能力强弱。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_factor_info",
        "description": "查询某个具体因子的详细说明（定义、逻辑、用途）。",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "因子名称（英文），如 'momentum'、'accruals'、'roe_trend'"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "get_backtest_result",
        "description": "获取最新回测结果摘要：年化收益、Sharpe、最大回撤、胜率、各期超额收益。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_backtest_status",
        "description": "查询回测进程是否在运行及当前进度。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_tasks",
        "description": "查询Windows计划任务执行情况（今日是否已运行、下次运行时间）。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_logs",
        "description": "获取monitor日志的最后N行，用于排查问题。",
        "input_schema": {
            "type": "object",
            "properties": {
                "n": {"type": "integer", "description": "获取最后N行，默认20，最大100", "default": 20},
            },
            "required": [],
        },
    },
    {
        "name": "run_hot_scan",
        "description": "启动热榜扫描策略（东方财富热榜+动量过滤），结果推送微信。",
        "input_schema": {
            "type": "object",
            "properties": {
                "top_pct": {"type": "number", "description": "热榜前N%，默认5.0", "default": 5.0},
                "cah": {"type": "boolean", "description": "是否过滤高位股（距6月高点≥10%才保留），默认false", "default": False},
            },
            "required": [],
        },
    },
    {
        "name": "run_full_scan",
        "description": "触发主策略全市场扫盘，结果推送微信（约5-10分钟）。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_holdings_check",
        "description": "只检查当前持仓股票的信号（不扫全市场），快速推送微信。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "start_monitor",
        "description": "启动monitor.py定时循环扫描进程（若未运行）。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "restart_monitor",
        "description": "重启monitor.py循环扫描进程（先停旧进程再启新进程）。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_chip_scan",
        "description": "启动筹码策略扫描，指定档位和修饰符。结果推微信。",
        "input_schema": {
            "type": "object",
            "properties": {
                "tier": {
                    "type": "string",
                    "description": "筹码档位：'1'(≥95%) '2'(90-95%) '3'(85-90%) '4'(75-85%) '5'(65-75%)",
                    "enum": ["1", "2", "3", "4", "5"],
                },
                "mods": {
                    "type": "string",
                    "description": "修饰符：b=布林, e=≤50元, k=排科创, h=排高位, m=MACD绿柱, z=MACD近零。可叠加如'bekh'",
                    "default": "",
                },
            },
            "required": ["tier"],
        },
    },
    {
        "name": "run_chip_cad",
        "description": "启动筹码CAD数据驱动扫描（T4→T1→T2→T3→T5顺序），推送微信。",
        "input_schema": {
            "type": "object",
            "properties": {
                "mods": {"type": "string", "description": "修饰符组合，默认'bekhm'", "default": "bekhm"},
            },
            "required": [],
        },
    },
    {
        "name": "run_backtest",
        "description": "启动因子回测（后台运行，每期约20分钟）。",
        "input_schema": {
            "type": "object",
            "properties": {
                "periods": {"type": "integer", "description": "回测期数，默认16", "default": 16},
                "universe": {
                    "type": "string",
                    "description": "股票池：'main'（主板）或 'smallcap'（小盘）",
                    "enum": ["main", "smallcap"],
                    "default": "main",
                },
            },
            "required": [],
        },
    },
    {
        "name": "run_factor_ic",
        "description": "启动因子IC回测分析（约1-2小时），分析各因子的预测能力。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "show_holdings",
        "description": "显示当前持仓列表：股票代码、名称、股数、成本价、估算市值。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_hot_scan_result",
        "description": "获取最近一次热榜扫描的结果缓存（不重新扫描），包含股票排名、动量、综合得分。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_chip_scan_result",
        "description": "获取最近一次筹码扫描结果缓存，按档位分组显示。",
        "input_schema": {
            "type": "object",
            "properties": {
                "mode": {
                    "type": "string",
                    "description": "结果类型：'cad'（CAD扫描）、'cadm'（CAD+MACD）、'cah'（全档排高位）、'scan'（普通全档）",
                    "enum": ["cad", "cadm", "cah", "scan"],
                    "default": "cad",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_recent_signals",
        "description": "获取最近N天的买卖信号历史记录（signals_log.json），包含买卖只数、市场状态。",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "查询最近几天，默认7", "default": 7},
            },
            "required": [],
        },
    },
    {
        "name": "get_strategy_perf",
        "description": "获取主策略/筹码策略/金叉策略的近期胜率和收益表现统计。",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "read_file",
        "description": "读取项目目录内的文件内容，用于调试或查看配置。只能读取项目目录内的文件。",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "相对于项目根目录的文件路径，如 'data/factor_ic.json' 或 'alert_config.json'"},
            },
            "required": ["path"],
        },
    },
]

# ── Config dicts ──────────────────────────────────────────────────────────────
_CHIP_TIERS = {
    "1": (95, None),
    "2": (90, 95),
    "3": (85, 90),
}

# (name, sched_time, desc) — 按调度时间排序
_TASK_LIST = [
    ("weekly_PerfReport",      "00:00", "周度绩效报告 📱"),
    ("sync_Knowledge",         "02:00", "知识库同步"),
    ("factor_Analysis",        "03:00", "因子IC分析"),
    ("chip_Premarket",         "07:00", "盘前筹码兜底"),
    ("main_Morning",           "07:10", "主策略盘前兜底"),
    ("integrity_Check",        "08:00", "数据完整性"),
    ("concept_Warm",           "08:30", "概念map预热"),
    ("institution_Scan",       "08:30", "机构扫盘 📱"),
    ("watchlist_Monitor",      "09:15", "自选股监控 📱"),
    ("report_Morning",         "09:25", "盘前报告 📱"),
    ("watchlist_Scan",         "09:30", "自选股扫描 📱"),
    ("report_Midday",          "11:35", "午间报告 📱"),
    ("closing_Batch",          "15:05", "收盘批处理"),
    ("report_Evening",         "15:30", "收盘报告 📱"),
    ("market_Warm",            "15:35", "市场数据预热"),
    ("marketcap_Scan",         "15:35", "市值策略扫盘 📱"),
    ("daily_PerfLog",          "16:05", "胜率统计 📱"),
    ("price_Prefetch",         "17:00", "价格缓存预热"),
    ("fundflow_Prefetch",      "17:30", "资金流向预取"),
    ("chip_Night",             "18:00", "筹码缓存"),
    ("hot_Scan",               "19:00", "热榜扫描 📱"),
    ("merge_Sessions",         "19:14", "Lark会话合并"),
    ("golden_Scan",            "19:30", "金叉扫描 📱"),
    ("watchlist_Updater",      "20:00", "自选股更新"),
    ("chip_CadScan",           "21:00", "筹码扫描 📱"),
    ("nightly_Scan",           "22:10", "夜间选股 📱"),
    ("main_Night",             "22:30", "夜间预热"),
]

_HOT_RANK_NAMES = [
    "hot_Rank_0935", "hot_Rank_1000", "hot_Rank_1100",
    "hot_Rank_1330", "hot_Rank_1430",
]

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
    "ar_quality":            "应收账款质量：应收款增速 vs 营收增速",
}

# ── Process helpers ───────────────────────────────────────────────────────────
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


def _describe_cmdline(cmd: str) -> str:
    rules = [
        ("monitor.py",         "--loop",              "Monitor 循环"),
        ("monitor.py",         "--sell-only",         "Monitor 持仓检查"),
        ("monitor.py",         "--test-now",          "Monitor 全市场扫描"),
        ("monitor.py",         "",                    "Monitor（单次）"),
        ("lark_bot.py",        "",                    "Lark Bot"),
        ("discord_bot.py",     "",                    "Discord Bot"),
        ("analysis.py",        "--universe.*smallcap","IC回测 小盘"),
        ("analysis.py",        "--universe.*etf",     "IC回测 ETF"),
        ("analysis.py",        "",                    "IC回测 主策略"),
        ("main.py",            "--smallcap",          "回测 小盘"),
        ("main.py",            "",                    "回测 主策略"),
        ("etf.py",             "",                    "回测 ETF"),
        ("batch_financials.py","",                    "财务数据预热"),
        ("build_screener_universe.py", "",            "重建股票池"),
        ("strategy.py",        "--cad",               "筹码 CAD 扫描"),
        ("strategy.py",        "",                    "筹码策略扫描"),
        ("daily_scan.py",      "",                    "筹码全档扫描"),
        ("pipeline.py",        "",                    "筹码流水线"),
        ("prefetch.py",        "--price",             "价格缓存预热"),
        ("prefetch.py",        "--market",            "市场数据预热"),
        ("prefetch.py",        "--concept",           "概念 map 预热"),
        ("integrity_check.py", "",                    "完整性检查"),
        ("research.py",        "",                    "单股分析"),
        ("golden_cross_scan.py","",                   "金叉扫描"),
        ("hot_scan.py",        "",                    "热榜扫描"),
        ("pos_check.py",       "",                    "位置分析"),
        ("nightly_scan.py",    "",                    "夜间主策略选股"),
        ("watchlist_scan.py",  "",                    "自选股扫描"),
        ("small_strategy.py",  "",                    "小盘策略"),
        ("etf_strategy.py",    "",                    "ETF策略"),
        ("marketcap_strategy.py", "",                 "市值策略"),
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


# ── Command handlers ──────────────────────────────────────────────────────────
def h_status() -> str:
    lines = [f"系统状态 @ {datetime.now():%Y-%m-%d %H:%M:%S}\n"]
    proc_list = _get_python_procs()
    root_str = str(ROOT).replace("\\", "/").lower()
    _SS = {
        "monitor.py", "lark_bot.py", "discord_bot.py",
        "analysis.py", "main.py", "etf.py",
        "batch_financials.py", "build_screener_universe.py", "strategy.py",
        "daily_scan.py", "pipeline.py",
        "prefetch.py", "research.py", "integrity_check.py", "hot_scan.py", "pos_check.py",
        "institution_scan.py", "golden_cross_scan.py",
        "nightly_scan.py", "watchlist_scan.py", "small_strategy.py", "etf_strategy.py",
        "marketcap_strategy.py",
    }
    ss_procs, other_procs = [], []
    for pid, cmd in proc_list:
        c = cmd.replace("\\", "/").lower()
        if root_str in c or any(s in c for s in _SS):
            ss_procs.append((pid, cmd))
        else:
            other_procs.append((pid, cmd))

    if ss_procs:
        lines.append("暴富进程:")
        for pid, cmd in ss_procs:
            lines.append(f"  ✅ {_describe_cmdline(cmd)}  PID {pid}")
    else:
        lines.append("❌ 无暴富进程运行")

    if other_procs:
        lines.append("\n其他 Python 进程:")
        for pid, cmd in other_procs:
            name = cmd.split()[-1].replace("\\", "/").split("/")[-1][:40]
            lines.append(f"  ⚪ {name}  PID {pid}")

    lines.append("")
    lines.append(h_picks())
    return "\n".join(lines)


def h_scan() -> str:
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"), "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return "已触发扫盘，结果稍后发送到微信 📱"


def h_holdings() -> str:
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"),
         "--sell-only", "--always-send"],
        cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return "已触发持仓推送，结果稍后发送到微信 📱"


def h_picks() -> str:
    picks_path = ROOT / "data" / "latest_picks.json"
    if not picks_path.exists():
        return "latest_picks.json 不存在，今日可能尚未选股。"
    data  = json.loads(picks_path.read_text(encoding="utf-8"))
    items = data.get("results", [])
    ts    = data.get("timestamp") or data.get("date", "")
    date_ = ts[:10] if ts else "?"
    today = datetime.now().strftime("%Y-%m-%d")
    if date_ != today:
        return f"今日暂无推荐信号 (上次: {date_})"
    lines = [f"今日推荐 ({date_})\n"]
    for i, s in enumerate(items[:10], 1):
        name  = s.get("name") or s.get("code", "?")
        score = s.get("composite", s.get("score", 0))
        lines.append(f"{i}. {name}  (得分 {score:.3f})")
    return "\n".join(lines)


def h_gc() -> str:
    gc_path = ROOT / "data" / "golden_cross_latest.json"
    if not gc_path.exists():
        return "golden_cross_latest.json 不存在。"
    data   = json.loads(gc_path.read_text(encoding="utf-8"))
    tiers  = data.get("tiers", {})
    date_  = data.get("date", "?")
    date_s = f"{date_[4:6]}/{date_[6:]}" if len(date_) == 8 else date_
    _SIG_SHORT = {
        "MACD金叉": "MACD", "KDJ金叉": "KDJ", "RSI金叉": "RSI",
        "MA5/10金叉": "MA5/10", "MA10/20金叉": "MA10/20",
        "量能金叉": "量", "OBV金叉": "OBV", "布林中轨金叉": "布林",
    }
    TIERS = {"G0": "7信号", "G1": "6信号", "G2": "5信号"}
    total = sum(len(tiers.get(t, [])) for t in TIERS)
    lines = [f"金叉共振 {date_s}  共{total}只\n"]
    for t, label in TIERS.items():
        picks = tiers.get(t, [])
        if not picks:
            continue
        lines.append(f"{t} {label}  {len(picks)}只")
        for p in picks:
            sig_s = "·".join(_SIG_SHORT.get(s, s) for s in p.get("signals", []))
            lines.append(f"  {p['code']} {p['name']} ¥{p['close']:.2f}  {sig_s}")
    if not total:
        lines.append("今日无金叉共振信号")
    return "\n".join(lines)


def h_logs(n: int = 20) -> str:
    log_path = LOGS / "monitor_loop.log"
    if not log_path.exists():
        return "monitor_loop.log 不存在。"
    tail = log_path.read_bytes()[-8000:].decode("utf-8", errors="replace")
    last = [l for l in tail.splitlines() if l.strip()][-n:]
    body = "\n".join(last) or "(空)"
    if len(body) > 3500:
        body = "..." + body[-3500:]
    return f"日志 -{n}\n```\n{body}\n```"


def h_tasks() -> str:
    all_names = [n for n, _, _ in _TASK_LIST] + _HOT_RANK_NAMES
    names_list = "','".join(all_names)
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
        by_name: dict[str, tuple] = {}
        for line in (r.stdout or "").splitlines():
            parts = line.strip().split("|||")
            if len(parts) == 4:
                name, status, last_run, next_run = parts
                by_name[name] = (status, last_run, next_run)

        if not by_name:
            return "❌ 无任务数据"

        today_mdd    = date.today().strftime("%m/%d")
        tomorrow_mdd = (date.today() + timedelta(days=1)).strftime("%m/%d")

        def _tick(status: str, next_run: str) -> str:
            if status == "OK":
                return "✅"
            if next_run != "--" and next_run.startswith(tomorrow_mdd):
                return "❌"
            return "⬜"

        lines = []
        for name, sched_time, desc in _TASK_LIST:
            status, _, next_run = by_name.get(name, ("--", "--", "--"))
            t = sched_time if sched_time else "--:--"
            lines.append(f"{_tick(status, next_run)} {t} {name} / {desc}")

        hot_ok  = sum(1 for n in _HOT_RANK_NAMES if by_name.get(n, ("--",))[0] == "OK")
        hot_tot = len(_HOT_RANK_NAMES)
        hot_tick = "✅" if hot_ok == hot_tot else ("🟡" if hot_ok > 0 else "⬜")
        lines.append(f"\n后台: {hot_tick}热榜快照({hot_ok}/{hot_tot})")

        return "\n".join(lines)
    except Exception as e:
        return f"❌ 查询失败: {e}"


def h_backtest_status() -> str:
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


def h_backtest_result() -> str:
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
        lines.append(f"\n最近6期收益（共{len(periods)}期）")
        for p in periods[-6:]:
            port  = p.get("portfolio_ret", 0)
            bench = p.get("benchmark_ret")
            alpha = p.get("alpha")
            bench_s = f"{bench:+.2f}%" if bench is not None else "N/A"
            alpha_s = f"{alpha:+.2f}%" if alpha is not None else "N/A"
            lines.append(f"  P{p['period']}: 策略{port:+.2f}%  基准{bench_s}  超额{alpha_s}")
    return "\n".join(lines)


def h_backtest(periods: int = 16, universe: str = "main", workers: int = 8) -> str:
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
        [sys.executable, "-X", "utf8", str(SCRIPTS / "backtest" / "main.py"),
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


def h_backtest_etf(periods: int = 12, fwd: int = 10, workers: int = 4) -> str:
    if _find_backtest_pid():
        return "⚠️ 已有回测进程在运行，请等待完成或先 sc3 停止。"
    out_file = ROOT / "data" / f"backtest_etf_{periods}p.json"
    log_path = LOGS / f"backtest_etf_{periods}p.log"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"--- ETF backtest started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    proc = subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "backtest" / "etf.py"),
         "--periods", str(periods), "--fwd", str(fwd),
         "--workers", str(workers), "--out", str(out_file)],
        cwd=str(ROOT),
        stdout=open(log_path, "a", encoding="utf-8"), stderr=subprocess.STDOUT,
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
    cmd = [sys.executable, "-X", "utf8", str(SCRIPTS / "chip" / "strategy.py"),
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


def h_chip(arg: str) -> str:
    arg = arg.strip()
    if not arg or arg == "help":
        return CHIP_LIST
    if arg == "all" or arg.startswith("all"):
        rest = arg[3:].strip().replace(" ", "")
        log_path = LOGS / "daily_chip_scan.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"--- daily_chip_scan started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        cmd_args = [sys.executable, "-X", "utf8", str(SCRIPTS / "chip" / "daily_scan.py")]
        if "h" in rest: cmd_args += ["--high-filter"]
        if "b" in rest: cmd_args += ["--boll"]
        if "e" in rest: cmd_args += ["--max-price", "50"]
        if "k" in rest: cmd_args += ["--no-kcb"]
        subprocess.Popen(cmd_args, cwd=str(ROOT),
                         stdout=open(log_path, "a", encoding="utf-8"),
                         stderr=subprocess.STDOUT)
        return "筹码全档扫描（T1-T3）已启动，约2-3分钟后推微信 📱"
    parts = arg.split(None, 1)
    head  = parts[0]
    tail  = parts[1].lower().replace(" ", "") if len(parts) > 1 else ""
    if head and head[0].isdigit():
        tier_str = head[0]
        mods     = head[1:].lower() + tail
    else:
        return f"用法错误：c {arg}\n\n{CHIP_LIST}"
    if tier_str not in _CHIP_TIERS:
        return f"档位 {tier_str} 不存在，有效范围 1-3\n\n{CHIP_LIST}"
    return _launch_chip(tier_str, mods)


def h_chip_data_driven(mods: str = "bekhm") -> str:
    log_path = LOGS / "chip_cad.log"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"--- chip_cad started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "chip" / "strategy.py"), "--cad", "--mods", mods],
        cwd=str(ROOT),
        stdout=open(log_path, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )
    return f"筹码数据驱动扫描（T1-T3 {mods}）已启动 ✅\n约3-5分钟后推飞书卡片+微信 📱"


def h_hot_scan(top_pct: float = 5.0, cah: bool = False) -> str:
    pct_s = str(int(top_pct)) if top_pct == int(top_pct) else str(top_pct)
    label = f"top{pct_s}%" + ("排高位" if cah else "")
    log_path = LOGS / "hot_scan.log"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"--- hot_scan {label} started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    cmd = [sys.executable, "-X", "utf8", str(SCRIPTS / "strategies" / "hot_scan.py"),
           "--top-pct", str(top_pct), "--push"]
    if cah:
        cmd += ["--cah"]
    subprocess.Popen(cmd, cwd=str(ROOT),
                     stdout=open(log_path, "a", encoding="utf-8"),
                     stderr=subprocess.STDOUT)
    return f"热榜扫描 {label} 已启动（约1-2分钟）✅\n结果推送到微信 📱"


def h_pos(code: str) -> str:
    code = re.sub(r'\D', '', code)[:6].zfill(6)
    if not code.isdigit() or len(code) != 6:
        return "用法: pos000001 或 pos 600519"
    try:
        r = subprocess.run(
            [sys.executable, "-X", "utf8", str(SCRIPTS / "pos_check.py"), code],
            cwd=str(ROOT), capture_output=True, text=True, timeout=60,
            encoding="utf-8", errors="replace",
        )
        out = (r.stdout or "").strip()
        if not out:
            err = (r.stderr or "").strip()
            return f"❌ {code} 无输出\n{err[-300:]}" if err else f"❌ {code} 无数据"
        return out
    except subprocess.TimeoutExpired:
        return f"❌ {code} 查询超时（>60s）"
    except Exception as e:
        return f"❌ 位置查询失败: {e}"


def h_show_holdings() -> str:
    path = ROOT / "holdings.json"
    if not path.exists():
        return "holdings.json 不存在"
    data = json.loads(path.read_text(encoding="utf-8"))
    if not data:
        return "持仓为空"
    lines = [f"当前持仓 ({len(data)} 只)\n"]
    total_cost = 0.0
    for h in data:
        code  = h.get("code", "?")
        name  = h.get("name", code)
        shares = h.get("shares", 0)
        cost  = h.get("cost_price", 0)
        if cost:
            mv = shares * cost
            total_cost += mv
            lines.append(f"  {code} {name}  {shares}股  成本¥{cost:.3f}  市值≈{mv:,.0f}")
        else:
            lines.append(f"  {code} {name}  {shares}股  (成本未录入)")
    if total_cost:
        lines.append(f"\n总成本估算: ¥{total_cost:,.0f}")
    return "\n".join(lines)


def h_main_picks() -> str:
    picks_path = ROOT / "data" / "latest_picks.json"
    if not picks_path.exists():
        return "latest_picks.json 不存在，主策略尚未运行。"
    data  = json.loads(picks_path.read_text(encoding="utf-8"))
    items = data.get("results", [])
    if not items:
        return "主策略暂无选股结果。"
    ts    = data.get("timestamp") or data.get("date", "")
    date_ = ts[:10] if ts else "?"
    today = datetime.now().strftime("%Y-%m-%d")
    if date_ != today:
        return f"今日暂无推荐信号 (上次: {date_})"
    lines = [f"主策略今日推荐 ({date_})\n"]
    for i, s in enumerate(items[:5], 1):
        name  = s.get("name") or s.get("code", "?")
        code  = s.get("code", "")
        score = s.get("composite", s.get("score", 0))
        lines.append(f"{i}. {code} {name}  得分 {score:.3f}")
    return "\n".join(lines)


def h_hot_scan_result() -> str:
    path = ROOT / "data" / "hot_scan_latest.json"
    if not path.exists():
        return "hot_scan_latest.json 不存在，先运行 hs 触发扫描"
    data  = json.loads(path.read_text(encoding="utf-8"))
    picks = data.get("picks", [])
    date_s   = data.get("date", "?")
    top_pct  = data.get("top_pct", 5)
    suffix   = "（排高位）" if data.get("cah") else ""
    lines = [f"热榜扫描 {date_s} top{top_pct}%{suffix}  共{len(picks)}只\n"]
    for p in picks[:20]:
        chg = f"+{p['change_pct']}%" if p.get("change_pct", 0) >= 0 else f"{p['change_pct']}%"
        lines.append(
            f"  {p['code']} {p['name']}  ¥{p['close']}  {chg}"
            f"  热度#{p['rank']}  动量{p['momentum']:.0f}  综合{p['score']:.0f}"
        )
    return "\n".join(lines)


def h_chip_scan_result(mode: str = "cad") -> str:
    fname_map = {
        "cad":  "chip_cad_latest.json",
        "cadm": "chip_cadm_latest.json",
        "cah":  "chip_cah_latest.json",
        "scan": "chip_scan_latest.json",
    }
    fname = fname_map.get(mode, "chip_scan_latest.json")
    path  = ROOT / "data" / fname
    if not path.exists():
        return f"{fname} 不存在"
    data   = json.loads(path.read_text(encoding="utf-8"))
    raw_date = data.get("date", "?")
    date_s = f"{raw_date[4:6]}/{raw_date[6:]}" if len(raw_date) == 8 else raw_date
    tiers  = data.get("tiers", {})
    if not tiers:
        picks = data.get("picks", data.get("results", []))
        lines = [f"筹码扫描 {date_s} {mode.upper()}  共{len(picks)}只\n"]
        for p in picks[:20]:
            lines.append(f"  {p.get('code','?')} {p.get('name','?')}  ¥{p.get('close',0):.2f}  赢家率{p.get('winner_rate',0):.1f}%")
        return "\n".join(lines)
    top3 = ("T1", "T2", "T3")
    total = sum(len(v) for k, v in tiers.items() if k in top3)
    lines = [f"筹码扫描 {date_s} {mode.upper()}  共{total}只\n"]
    for tier, items in tiers.items():
        if tier not in top3:
            continue
        lines.append(f"{tier}  {len(items)}只")
        for p in items[:5]:
            lines.append(f"  {p.get('code','?')} {p.get('name','?')}  ¥{p.get('close',0):.2f}  赢家率{p.get('winner_rate',0):.1f}%")
    return "\n".join(lines)


def h_recent_signals(days: int = 7) -> str:
    path = ROOT / "data" / "signals_log.json"
    if not path.exists():
        return "signals_log.json 不存在"
    data = json.loads(path.read_text(encoding="utf-8"))
    if not data:
        return "无信号记录"
    from datetime import timedelta
    cutoff = (datetime.now().date() - timedelta(days=days)).isoformat()
    recent = [e for e in data if e.get("date", "") >= cutoff]
    if not recent:
        return f"过去 {days} 天无信号"
    lines = [f"近 {days} 天信号（{len(recent)} 次扫描）\n"]
    for entry in recent[-10:]:
        d      = entry.get("date", "?")
        buys   = entry.get("buy_signals", [])
        sells  = entry.get("sell_signals", [])
        regime = entry.get("regime_score", "?")
        lines.append(f"{d}  市场{regime}  买{len(buys)}只 卖{len(sells)}只")
        for b in buys[:3]:
            lines.append(f"  ↑ {b.get('code','')} {b.get('name','')}  买分{b.get('buy_score',0):.0f}")
        for s in sells[:3]:
            lines.append(f"  ↓ {s.get('code','')} {s.get('name','')}  卖分{s.get('sell_score',0):.0f}")
    return "\n".join(lines)


def h_strategy_perf() -> str:
    perf_files = {
        "主策略": ROOT / "data" / "main_daily_perf.json",
        "筹码策略": ROOT / "data" / "chip_daily_perf.json",
        "金叉策略": ROOT / "data" / "gc_daily_perf.json",
    }
    lines = [f"策略近期表现 @ {datetime.now():%m/%d}\n"]
    any_data = False
    for label, path in perf_files.items():
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        if not data:
            continue
        recent = [e for e in data if e.get("n", 0) > 0][-10:]
        if not recent:
            continue
        any_data = True
        n_total = sum(e.get("n", 0) for e in recent)
        wins    = sum(e.get("n", 0) * e.get("win_rate", 0) / 100 for e in recent)
        wr      = wins / n_total * 100 if n_total else 0
        avg_rets = [e["avg_ret"] for e in recent if e.get("avg_ret") is not None]
        avg_ret  = sum(avg_rets) / len(avg_rets) if avg_rets else 0
        latest   = recent[-1]
        lines.append(
            f"{label}: 近{len(recent)}次  {n_total}只  胜率{wr:.0f}%  均收益{avg_ret:+.2f}%"
            f"  (最近: {latest.get('date','?')[:8]})"
        )
    return "\n".join(lines) if any_data else "暂无策略表现数据"


def h_restart() -> str:
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
        f.write(f"\n--- Restarted by bot at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"), "--loop", "--interval", "5"],
        cwd=str(ROOT),
        stdout=open(log_path, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )
    note = f"（已终止旧进程 PID {pid}）" if killed else "（未找到旧进程，直接启动）"
    return f"monitor.py 已重启 ✅ {note}"


def h_start_monitor() -> str:
    pid = _find_monitor_pid()
    if pid:
        return f"monitor.py 已在运行（PID {pid}）"
    monitor_log = LOGS / "monitor_loop.log"
    if monitor_log.exists() and time.time() - monitor_log.stat().st_mtime < 300:
        age = int((time.time() - monitor_log.stat().st_mtime) / 60)
        return f"monitor.py 看起来已在运行（日志 {age} 分钟前更新）"
    with open(monitor_log, "a", encoding="utf-8") as f:
        f.write(f"\n--- Started by bot at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
    subprocess.Popen(
        [sys.executable, "-X", "utf8", str(SCRIPTS / "monitor.py"), "--loop", "--interval", "5"],
        cwd=str(ROOT),
        stdout=open(monitor_log, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )
    return "monitor.py 已启动 ✅"


def h_kill_backtest() -> str:
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


def h_shortcut(num: str) -> str:
    parts    = num.split(None, 1)
    main_num = parts[0] if parts else ""
    sub_arg  = parts[1].strip() if len(parts) > 1 else ""
    if not main_num:
        return SC_LIST
    num = main_num
    if num == "1":
        return h_start_monitor()
    elif num == "2":
        return h_restart()
    elif num == "3":
        return h_kill_backtest()
    elif num == "4":
        log_path = LOGS / "factor_ic_main.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"--- factor_analysis started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        subprocess.Popen(
            [sys.executable, "-X", "utf8", str(SCRIPTS / "factors" / "analysis.py"),
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
        log_path = LOGS / "build_screener_universe.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"--- build_screener_universe started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        subprocess.Popen(
            [sys.executable, "-X", "utf8", str(SCRIPTS / "tools" / "build_screener_universe.py")],
            cwd=str(ROOT),
            stdout=open(log_path, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )
        return "build_universe.py 已启动（约5-10分钟）✅"
    elif num == "7":
        return h_scan()
    elif num == "8":
        return h_logs(20)
    elif num == "10":
        log_path = LOGS / "daily_chip_scan.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"--- daily_chip_scan started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
        subprocess.Popen(
            [sys.executable, "-X", "utf8", str(SCRIPTS / "chip" / "daily_scan.py")],
            cwd=str(ROOT),
            stdout=open(log_path, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
        )
        return "筹码全档扫描（T1-T3）已启动，约1-2分钟后推微信 📱"
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
        return f"未知快捷命令 sc {num}\n\n{SC_LIST}"


def h_research(code: str) -> str:
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


def h_ic() -> str:
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


def h_factor_info(name: str) -> str:
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


def h_read_file(path: str) -> str:
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


# ── Agent tool executor ───────────────────────────────────────────────────────
def call_tool(name: str, args: dict) -> str:
    try:
        if name == "get_system_status":   return h_status()
        if name == "get_today_picks":     return h_picks()
        if name == "get_golden_cross":    return h_gc()
        if name == "get_stock_position":  return h_pos(args.get("code", ""))
        if name == "analyze_stock":       return h_research(args.get("code", ""))
        if name == "get_factor_ic":       return h_ic()
        if name == "get_factor_info":     return h_factor_info(args.get("name", ""))
        if name == "get_backtest_result": return h_backtest_result()
        if name == "get_backtest_status": return h_backtest_status()
        if name == "get_tasks":           return h_tasks()
        if name == "get_logs":            return h_logs(min(int(args.get("n", 20)), 100))
        if name == "run_hot_scan":
            return h_hot_scan(top_pct=float(args.get("top_pct", 5.0)), cah=bool(args.get("cah", False)))
        if name == "run_full_scan":       return h_scan()
        if name == "run_holdings_check":  return h_holdings()
        if name == "start_monitor":       return h_start_monitor()
        if name == "restart_monitor":     return h_restart()
        if name == "run_chip_scan":
            return _launch_chip(str(args.get("tier", "1")), str(args.get("mods", "")))
        if name == "run_chip_cad":
            return h_chip_data_driven(str(args.get("mods", "bekhm")))
        if name == "run_backtest":
            return h_backtest(periods=int(args.get("periods", 16)), universe=str(args.get("universe", "main")))
        if name == "run_factor_ic":
            log_path = LOGS / "factor_ic_main.log"
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"--- factor_analysis started at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
            subprocess.Popen(
                [sys.executable, "-X", "utf8", str(SCRIPTS / "factors" / "analysis.py"),
                 "--rolling", "6", "--step", "20", "--out", str(ROOT / "data" / "factor_ic.json")],
                cwd=str(ROOT), stdout=open(log_path, "a", encoding="utf-8"), stderr=subprocess.STDOUT,
            )
            return "因子IC回测已启动（约1-2小时）✅"
        if name == "show_holdings":           return h_show_holdings()
        if name == "get_hot_scan_result":     return h_hot_scan_result()
        if name == "get_chip_scan_result":
            return h_chip_scan_result(str(args.get("mode", "cad")))
        if name == "get_recent_signals":
            return h_recent_signals(int(args.get("days", 7)))
        if name == "get_strategy_perf":       return h_strategy_perf()
        if name == "read_file":               return h_read_file(args.get("path", ""))
        return f"❌ 未知工具: {name}"
    except Exception as e:
        return f"❌ {name} 执行失败: {e}"


# ── AI dispatch (GitHub Models API, tool-calling) ────────────────────────────
_AI_SYSTEM = (
    "你是 StockSage 量化交易系统的助手。用中文简洁回答。"
    "遇到需要数据或执行操作的请求，直接调用工具，不要先询问确认。"
    "可以连续调用多个工具来组合回答复杂问题（如先查持仓再查每只股票位置）。"
    "对于无关问题，正常回答即可。"
)

# Convert Anthropic tool format → OpenAI function calling format
_OAI_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": t["name"],
            "description": t["description"],
            "parameters": t.get("input_schema", {"type": "object", "properties": {}}),
        },
    }
    for t in TOOLS
]

_GH_MODELS_URL = "https://models.inference.ai.azure.com/chat/completions"
_GH_MODEL = "gpt-4o-mini"


def _gh_token() -> str:
    import subprocess
    try:
        r = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=10)
        return r.stdout.strip()
    except Exception:
        return ""


def dispatch_ai(text: str, api_key: str) -> str:
    import json, httpx
    token = _gh_token()
    if not token:
        return "❓ 未识别命令，发 h 查看帮助。"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    messages: list[dict] = [
        {"role": "system", "content": _AI_SYSTEM},
        {"role": "user", "content": text},
    ]
    try:
        client = httpx.Client(timeout=120)
        for _ in range(5):
            resp = client.post(_GH_MODELS_URL, headers=headers, json={
                "model": _GH_MODEL,
                "messages": messages,
                "tools": _OAI_TOOLS,
                "max_tokens": 1024,
            })
            resp.raise_for_status()
            data = resp.json()
            choice = data["choices"][0]
            msg = choice["message"]
            finish = choice["finish_reason"]

            if finish == "stop":
                return (msg.get("content") or "（无回复）").strip()

            if finish == "tool_calls":
                messages.append(msg)
                for tc in msg.get("tool_calls", []):
                    fn = tc["function"]
                    result = call_tool(fn["name"], json.loads(fn.get("arguments") or "{}"))
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc["id"],
                        "content": str(result),
                    })
            else:
                break

        final = client.post(_GH_MODELS_URL, headers=headers, json={
            "model": _GH_MODEL,
            "messages": messages,
            "max_tokens": 512,
        })
        final.raise_for_status()
        return (final.json()["choices"][0]["message"].get("content") or "（无回复）").strip()
    except Exception as e:
        print(f"[AI dispatch error] {e}", flush=True)
        return f"❌ AI 调用失败: {e}\n发 h 查看快捷命令。"


# ── Shared command dispatch ───────────────────────────────────────────────────
def dispatch_command(t: str) -> str | None:
    """Return reply for known commands, or None to hand off to AI/bot-specific handler."""
    t = re.sub(r'^(sc)(\d)', r'\1 \2', t)
    t = re.sub(r'^(c)(\d)',  r'\1 \2', t)

    if t in ("帮助", "help", "h", "？", "?"):
        return HELP
    if t == "t":
        return h_tasks()
    if t == "fh":
        return FACTOR_HELP
    if t in ("状态", "status", "z"):
        return h_status()
    if t in ("ic", "因子ic", "因子IC"):
        try: return h_ic()
        except Exception as e: return f"❌ ic 出错: {e}"
    if t.startswith("fx") and len(t) > 2 and (t[2:3].isdigit() or t[2:3] == " "):
        return h_research(t[2:].strip())
    if t.startswith("研究 ") or t.startswith("分析 "):
        return h_research(t.split(None, 1)[1].strip())
    if t in ("fx", "研究", "分析"):
        return "用法: fx600519 或 研究 贵州茅台"
    if t == "sc" or (t.startswith("sc ") and t[3:4] != ""):
        return h_shortcut(t[2:].strip())
    if t in ("sch", "快捷列表"):
        return SC_LIST
    if t in ("cmh", "ch"):
        return CHIP_LIST
    if t in ("gc", "金叉"):
        return h_gc()
    if t == "hot":
        return HOT_LIST
    if t in ("p", "持仓", "portfolio"):
        return h_main_picks()
    if t in ("hr", "热榜结果"):
        return h_hot_scan_result()
    if t in ("cr", "筹码结果"):
        return h_chip_scan_result("cad")
    if t in ("sg", "信号", "signals"):
        return h_recent_signals(7)
    if t in ("perf", "表现", "策略表现"):
        return h_strategy_perf()
    if t.startswith("hs"):
        rest = t[2:]
        top_pct = 20.0 if "20" in rest else (10.0 if "10" in rest else 5.0)
        cah = "h" in rest.replace("10", "").replace("20", "")
        return h_hot_scan(top_pct=top_pct, cah=cah)
    if t.startswith("pos"):
        code = t[3:].strip()
        return h_pos(code) if code else "用法: pos000001 或 pos 600519"
    if t.startswith("位置") or t.startswith("高低位"):
        code = t.split(None, 1)[1].strip() if " " in t else ""
        return h_pos(code) if code else "用法: 位置 000001"
    if t == "cad" or t.startswith("cad"):
        mods = (t[4:].strip().replace(" ", "") or "bekhm") if t.startswith("cadm") else \
               (t[3:].strip().replace(" ", "") or "bekh")
        return h_chip_data_driven(mods)
    if t == "ca" or t.startswith("ca"):
        return h_chip("all " + t[2:] if t[2:] else "all")
    if t == "c" or t.startswith("c "):
        return h_chip(t[2:].strip() if t.startswith("c ") else "")
    if t in ("ich", "因子列表"):
        names = sorted(_FACTOR_GLOSSARY.keys())
        pairs = [f"{n} {_FACTOR_ZH.get(n, '')}" for n in names]
        return f"因子列表（共{len(names)}个）— 用 icf 因子名 查详情\n" + "  ".join(pairs)
    if t in ("icf", "因子介绍") or t.startswith("icf ") or t.startswith("因子介绍 "):
        name = t.split(None, 1)[1].strip() if " " in t else ""
        return h_factor_info(name) if name else "用法: icf momentum"
    if t in ("br", "回测结果"):
        return h_backtest_result()
    if t in ("bs", "回测状态"):
        try: return h_backtest_status()
        except Exception as e: return f"❌ bs 出错: {e}"
    if t.startswith("bte") or t in ("etf回测",):
        raw = t[3:].strip() if t.startswith("bte") else ""
        m = re.match(r'^(\d+)', raw)
        return h_backtest_etf(periods=int(m.group(1)) if m else 12)
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
        return h_backtest(periods=periods, universe=universe)
    if t in ("r", "重启monitor", "重启 monitor", "restart monitor"):
        return h_restart()
    if t in ("sm", "启动monitor", "start monitor"):
        return h_start_monitor()

    return None
