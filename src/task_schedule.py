"""Single source of truth for all StockSage scheduled tasks.

三个 consumer 之前各维护一份清单容易漂：
  - src/setup_scheduler.py 的 TASKS / DISABLED_TASKS（注册 + 生成 bat）
  - src/notify/notify.py 的 _SCHEDULE（任务完成后"剩余任务"提示）
  - stock-bot/bot_common.py 的 _TASK_LIST（lark_bot 的 h_tasks 命令）

合并到这里：每个任务一条 dict 记录，含所有需要的字段；三个 consumer
通过 setup_scheduler_tasks() / notify_schedule() / bot_task_list() 派生
出各自需要的视图。

字段：
  name      —— 任务名（schtasks /TN 用）
  time      —— HH:MM
  desc      —— 简短描述（不带 📱 后缀；display 函数按 push flag 拼）
  push      —— 是否推送 WeChat（display 时加 📱 后缀）
  slot      —— bat 生成 key；None = 不由 setup_scheduler 管（如 institution_Scan
               / sync_Knowledge / watchlist_Monitor 等通过 lark-agent 或别处注册）
  disabled  —— 当前是否 disable（setup_scheduler 创建后立即 /Change /DISABLE）
  display   —— 是否在 bot/notify 显示（hot_Rank 这种快照型可隐藏）
"""
from __future__ import annotations

from typing import Any


ALL_TASKS: list[dict[str, Any]] = [
    # ── 凌晨/盘前外 ──────────────────────────────────────────────────────────
    {"name": "weekly_PerfReport", "time": "00:00", "desc": "周度绩效报告",       "push": True,  "slot": None,             "disabled": False, "display": True},
    {"name": "sync_Knowledge",    "time": "02:00", "desc": "知识库同步",         "push": False, "slot": None,             "disabled": True,  "display": False},
    {"name": "factor_Analysis",   "time": "03:00", "desc": "因子IC分析",         "push": False, "slot": None,             "disabled": False, "display": True},
    {"name": "institution_Scan",  "time": "04:00", "desc": "机构扫盘",           "push": True,  "slot": None,             "disabled": False, "display": True},

    # ── 盘前 (07-09) ────────────────────────────────────────────────────────
    {"name": "integrity_Check",   "time": "08:00", "desc": "数据完整性检查",     "push": False, "slot": "integrity_check","disabled": False, "display": True},
    {"name": "cffex_CiticAM",     "time": "19:00", "desc": "中信期货空单跟踪",   "push": True,  "slot": "cffex_citic",    "disabled": False, "display": True},
    {"name": "concept_Warm",      "time": "08:30", "desc": "概念map预热",        "push": False, "slot": "concept_warm",   "disabled": False, "display": True},
    {"name": "watchlist_Monitor", "time": "09:15", "desc": "自选股监控",         "push": True,  "slot": None,             "disabled": True,  "display": True},
    {"name": "report_Morning",    "time": "09:25", "desc": "盘前选股报告",       "push": True,  "slot": "chip_morning",   "disabled": True,  "display": True},
    {"name": "watchlist_Scan",    "time": "09:30", "desc": "自选股扫描",         "push": True,  "slot": None,             "disabled": False, "display": True},

    # ── 盘中热榜快照（hidden from display，仅注册）────────────────────────────
    {"name": "hot_Rank_0935",     "time": "09:35", "desc": "热榜快照 09:35",     "push": False, "slot": "hot_rank",       "disabled": False, "display": False},
    {"name": "hot_Rank_1000",     "time": "10:00", "desc": "热榜快照 10:00",     "push": False, "slot": "hot_rank",       "disabled": False, "display": False},
    {"name": "hot_Rank_1100",     "time": "11:00", "desc": "热榜快照 11:00",     "push": False, "slot": "hot_rank",       "disabled": False, "display": False},

    # ── 盘中 ────────────────────────────────────────────────────────────────
    {"name": "report_Midday",     "time": "11:35", "desc": "午间行情报告",       "push": True,  "slot": "chip_midday",    "disabled": True,  "display": True},
    {"name": "hot_Rank_1330",     "time": "13:30", "desc": "热榜快照 13:30",     "push": False, "slot": "hot_rank",       "disabled": False, "display": False},
    {"name": "hot_Rank_1430",     "time": "14:30", "desc": "热榜快照 14:30",     "push": False, "slot": "hot_rank",       "disabled": False, "display": False},

    # ── 收盘 (15-16) ────────────────────────────────────────────────────────
    {"name": "closing_Batch",     "time": "15:05", "desc": "收盘批处理",         "push": False, "slot": None,             "disabled": False, "display": True},
    {"name": "signal_Tracker",    "time": "15:25", "desc": "信号绩效跟踪",       "push": False, "slot": None,             "disabled": True,  "display": True},
    {"name": "report_Evening",    "time": "15:30", "desc": "收盘报告",           "push": True,  "slot": "chip_evening",   "disabled": True,  "display": True},
    {"name": "market_Warm",       "time": "15:35", "desc": "市场数据预热",       "push": False, "slot": "market_warm",    "disabled": False, "display": True},
    {"name": "marketcap_Scan",    "time": "16:30", "desc": "市值策略扫盘",       "push": True,  "slot": "marketcap_scan", "disabled": False, "display": True},
    {"name": "daily_PerfLog",     "time": "16:05", "desc": "胜率统计",           "push": True,  "slot": "daily_perf_log", "disabled": False, "display": True},
    {"name": "escalator_PerfLog", "time": "16:15", "desc": "扶梯策略胜率分析",   "push": True,  "slot": "escalator_perf_log","disabled": True, "display": True},
    {"name": "strategy_Compare",  "time": "16:20", "desc": "多策略胜率对比",     "push": True,  "slot": "strategy_compare","disabled": True, "display": True},
    {"name": "hot_Scan",          "time": "16:35", "desc": "热榜策略扫描",       "push": True,  "slot": "hot_scan",       "disabled": False, "display": True},

    # ── 收盘后扫描/预热 (17-22) ───────────────────────────────────────────
    {"name": "price_Prefetch",    "time": "17:00", "desc": "价格历史预热",       "push": False, "slot": "price_prefetch", "disabled": False, "display": True},
    {"name": "fundflow_Prefetch", "time": "17:30", "desc": "资金流向预热",       "push": False, "slot": "fundflow_prefetch","disabled": False, "display": True},
    {"name": "chip_Night",        "time": "18:00", "desc": "筹码缓存预取",       "push": False, "slot": "chip_night",     "disabled": False, "display": True},
    {"name": "main_Scan",         "time": "18:30", "desc": "主/小/ETF 扫盘",     "push": True,  "slot": "monitor_scan",   "disabled": False, "display": True},
    {"name": "quality_Prefetch",  "time": "19:10", "desc": "质量指标预热",       "push": False, "slot": "quality_prefetch","disabled": False, "display": True},
    {"name": "merge_Sessions",    "time": "23:14", "desc": "Lark会话合并",       "push": False, "slot": None,             "disabled": False, "display": True},
    {"name": "golden_Scan",       "time": "19:30", "desc": "金叉策略扫描",       "push": True,  "slot": "gc_scan",        "disabled": False, "display": True},
    {"name": "sideways_Scan",     "time": "20:00", "desc": "横盘策略扫描",       "push": True,  "slot": "sideways_scan",  "disabled": False, "display": True},
    {"name": "escalator_Scan",    "time": "20:15", "desc": "扶梯策略扫描",       "push": True,  "slot": "escalator_scan", "disabled": False, "display": True},
    {"name": "chip_CadScan",      "time": "21:00", "desc": "筹码扫描",           "push": True,  "slot": "cad_scan",       "disabled": False, "display": True},
    {"name": "evening_Strategy",  "time": "22:00", "desc": "多策略汇总·晚间",    "push": True,  "slot": "evening_strategy","disabled": False, "display": True},
    {"name": "main_Night",        "time": "22:30", "desc": "财务缓存预热",       "push": False, "slot": None,             "disabled": False, "display": True},
    {"name": "watchlist_Updater", "time": "23:40", "desc": "自选股更新",         "push": False, "slot": None,             "disabled": False, "display": True},

    # ── 每日 1 次任务汇报（只推飞书，23:00 收尾汇总）────────────────────────────
    # 替代每个任务的 started/ok feishu 噪音；每天晚上汇总一次今日任务状态。
    {"name": "task_Summary_Evening", "time": "23:00", "desc": "任务汇报·晚上",     "push": False, "slot": "task_summary",   "disabled": False, "display": False},
]


def _desc_with_emoji(task: dict[str, Any]) -> str:
    """Display description, append 📱 for push tasks."""
    return f"{task['desc']} 📱" if task["push"] else task["desc"]


def setup_scheduler_tasks() -> list[tuple[str, str, str, str, bool]]:
    """5-tuple list for src/setup_scheduler.py TASKS — 仅 setup_scheduler 管理的（slot != None）。
    Returns: [(name, time, slot, desc_with_emoji, push), ...]
    """
    return [
        (t["name"], t["time"], t["slot"], _desc_with_emoji(t), t["push"])
        for t in ALL_TASKS if t["slot"] is not None
    ]


def setup_scheduler_disabled() -> set[str]:
    """set of task names that should be schtasks /Change /DISABLE after creation."""
    return {t["name"] for t in ALL_TASKS if t["disabled"]}


def notify_schedule() -> list[tuple[str, str, str]]:
    """3-tuple list for src/notify/notify.py _SCHEDULE — 所有 display=True 且未 disabled 的任务。
    Returns: [(name, time, desc_with_emoji), ...]
    """
    return [
        (t["name"], t["time"], _desc_with_emoji(t))
        for t in ALL_TASKS if t["display"] and not t["disabled"]
    ]


def bot_task_list() -> list[tuple[str, str, str]]:
    """3-tuple list for stock-bot/bot_common.py _TASK_LIST — 按 push / 不 push 分组带 __SEP__ 分隔符。
    Returns: [("__SEP__", "", "── X ──"), (name, time, desc), ...]
    """
    push_tasks = [t for t in ALL_TASKS if t["push"] and t["display"] and not t["disabled"]]
    nopush_tasks = [t for t in ALL_TASKS if not t["push"] and t["display"] and not t["disabled"]]
    push_tasks.sort(key=lambda x: x["time"])
    nopush_tasks.sort(key=lambda x: x["time"])

    out: list[tuple[str, str, str]] = [("__SEP__", "", "── 推送微信 ──")]
    for t in push_tasks:
        out.append((t["name"], t["time"], _desc_with_emoji(t)))
    out.append(("__SEP__", "", "── 不推微信 ──"))
    for t in nopush_tasks:
        out.append((t["name"], t["time"], t["desc"]))
    return out


def hot_rank_names() -> list[str]:
    """For bot_common._HOT_RANK_NAMES — auxiliary task names hidden from display
    but still queried in h_tasks() for status summary."""
    return [t["name"] for t in ALL_TASKS if t["name"].startswith("hot_Rank_")]
