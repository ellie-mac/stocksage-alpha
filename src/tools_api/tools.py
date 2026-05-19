"""
10 个工具实现 — 通过 @tool 装饰器注册到 TOOL_REGISTRY。

每个工具：
  • 接受命名参数（kwargs）
  • 返回 dict（可序列化为 JSON）
  • 不触发推送或写盘（只读或返回 dry_run 结果）
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

_HERE = Path(__file__).resolve().parent   # src/tools_api/
_ROOT = _HERE.parent.parent               # repo root
sys.path.insert(0, str(_HERE.parent))     # src/
sys.path.insert(0, str(_HERE))            # src/tools_api/

from tools_api import tool  # noqa: E402  — tools_api已在sys.modules中(由__init__触发)

_DATA = _ROOT / "data"
_PY   = sys.executable


# ─────────────────────────────────────────────────────────────────────────────
# 1. run_strategy_scan
# ─────────────────────────────────────────────────────────────────────────────

@tool(
    name="run_strategy_scan",
    description="运行指定策略扫描，返回买入信号列表（不推送）",
    parameters={
        "strategy_name": {
            "type": "string",
            "description": "策略名称：main | small | etf | hot_scan",
        },
        "dry_run": {
            "type": "boolean",
            "description": "是否仅返回结果而不写文件",
            "optional": True,
        },
    },
)
def run_strategy_scan(strategy_name: str = "main", dry_run: bool = True) -> dict:
    from strategies.base import get_strategy, _STRATEGY_CLASSES

    if strategy_name not in _STRATEGY_CLASSES:
        return {"error": f"未知策略: {strategy_name}。可选: {list(_STRATEGY_CLASSES)}"}

    from common import load_alert_config
    config = load_alert_config()

    result = get_strategy(strategy_name).run(config, dry_run=dry_run)
    d = result.to_dict()
    # BaseStrategy.run() 吞掉了异常并把 failed=True 放进 metadata；
    # 这里把业务失败浮到 ToolResult 层，让调用方能感知。
    if d.get("metadata", {}).get("failed"):
        raise RuntimeError(d["metadata"].get("error", "strategy failed"))
    return d


# ─────────────────────────────────────────────────────────────────────────────
# 2. get_factor_snapshot
# ─────────────────────────────────────────────────────────────────────────────

@tool(
    name="get_factor_snapshot",
    description="对单只股票计算所有因子得分，返回买入分/卖出分及各因子明细",
    parameters={
        "code": {
            "type": "string",
            "description": "6位股票代码，如 '000001'",
        },
    },
)
def get_factor_snapshot(code: str) -> dict:
    try:
        from report.utils import score_one_buy
        from factors import DEFAULT_WEIGHTS
        result = score_one_buy(code.zfill(6), weights=DEFAULT_WEIGHTS)
        return result
    except Exception as e:
        return {"error": str(e), "code": code}


# ─────────────────────────────────────────────────────────────────────────────
# 3. run_backtest
# ─────────────────────────────────────────────────────────────────────────────

@tool(
    name="run_backtest",
    description="运行策略回测（调用 backtest/main.py），返回胜率/收益摘要",
    parameters={
        "strategy_name": {
            "type": "string",
            "description": "策略名称：main | small | etf",
        },
        "start_date": {
            "type": "string",
            "description": "回测起始日 YYYY-MM-DD，如 '2025-01-01'",
            "optional": True,
        },
        "end_date": {
            "type": "string",
            "description": "回测截止日 YYYY-MM-DD，默认今天",
            "optional": True,
        },
    },
)
def run_backtest(
    strategy_name: str = "main",
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    bt_script = _ROOT / "src" / "backtest" / "main.py"
    if not bt_script.exists():
        return {"error": f"回测脚本不存在: {bt_script}"}

    cmd = [_PY, "-X", "utf8", str(bt_script), "--strategy", strategy_name]
    if start_date:
        cmd += ["--start", start_date]
    if end_date:
        cmd += ["--end", end_date]

    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           encoding="utf-8", timeout=300)
        return {
            "strategy": strategy_name,
            "returncode": r.returncode,
            "stdout": r.stdout[-2000:] if r.stdout else "",
            "stderr": r.stderr[-500:] if r.stderr else "",
        }
    except subprocess.TimeoutExpired:
        return {"error": "回测超时 (>300s)"}
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# 4. analyze_factor_ic
# ─────────────────────────────────────────────────────────────────────────────

@tool(
    name="analyze_factor_ic",
    description="读取因子 IC 回测结果，返回排名前 N 的因子及其 IC/ICIR",
    parameters={
        "top_n": {
            "type": "integer",
            "description": "返回 IC 最高的前 N 个因子（默认 20）",
            "optional": True,
        },
        "min_quality": {
            "type": "string",
            "description": "最低质量过滤：strong | moderate | weak（默认不过滤）",
            "optional": True,
        },
        "ic_file": {
            "type": "string",
            "description": "IC 文件路径（默认 data/factor_ic_main.json）",
            "optional": True,
        },
    },
)
def analyze_factor_ic(
    top_n: int = 20,
    min_quality: str | None = None,
    ic_file: str | None = None,
) -> dict:
    path = Path(ic_file) if ic_file else _DATA / "factor_ic_main.json"
    if not path.exists():
        return {"error": f"IC 文件不存在: {path}"}

    data = json.loads(path.read_text(encoding="utf-8"))
    meta = data.get("meta", {})
    table = data.get("ic_table", {})

    quality_order = {"strong": 3, "moderate": 2, "weak": 1, "noise": 0}
    min_q = quality_order.get(min_quality or "", -1)

    rows = []
    for factor, stats in table.items():
        q = quality_order.get(stats.get("quality", "noise"), 0)
        if q < min_q:
            continue
        rows.append({
            "factor":    factor,
            "mean_ic":   stats.get("mean_ic"),
            "icir":      stats.get("icir"),
            "quality":   stats.get("quality"),
            "direction": stats.get("direction"),
        })

    rows.sort(key=lambda r: abs(r["mean_ic"] or 0), reverse=True)
    return {"meta": meta, "factors": rows[:top_n], "total": len(rows)}


# ─────────────────────────────────────────────────────────────────────────────
# 5. track_signal_performance
# ─────────────────────────────────────────────────────────────────────────────

@tool(
    name="track_signal_performance",
    description="统计历史信号的远期收益表现（T+1/T+5/T+20 胜率和均值）",
    parameters={
        "strategy_name": {
            "type": "string",
            "description": "策略筛选：main | small | etf | all（默认 all）",
            "optional": True,
        },
        "lookback_days": {
            "type": "integer",
            "description": "回看天数（默认 90）",
            "optional": True,
        },
    },
)
def track_signal_performance(
    strategy_name: str = "all",
    lookback_days: int = 90,
) -> dict:
    perf_file = _DATA / "signal_performance.json"
    if not perf_file.exists():
        # fallback: 从 signals_log 读原始信号，不做远期收益计算
        sig_file = _DATA / "signals_log.json"
        if not sig_file.exists():
            return {"error": "signal_performance.json 和 signals_log.json 均不存在"}
        log = json.loads(sig_file.read_text(encoding="utf-8"))
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        entries = [e for e in log if e.get("date", "") >= cutoff]
        if strategy_name != "all":
            entries = [e for e in entries if e.get("source", "") == strategy_name]
        return {
            "source": "signals_log (no forward returns yet)",
            "entries": len(entries),
            "signals": sum(len(e.get("buy_signals", [])) for e in entries),
        }

    perf = json.loads(perf_file.read_text(encoding="utf-8"))
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    if strategy_name != "all":
        perf = [p for p in perf if p.get("source", p.get("strategy", "")) == strategy_name]
    perf = [p for p in perf if p.get("signal_date", "") >= cutoff]

    windows: dict[str, list[float]] = {}
    for entry in perf:
        for key, val in entry.items():
            if key.startswith("fwd_ret_") and val is not None:
                windows.setdefault(key, []).append(float(val))

    summary: dict[str, dict] = {}
    for key, vals in windows.items():
        summary[key] = {
            "n":        len(vals),
            "win_rate": round(sum(1 for v in vals if v > 0) / len(vals) * 100, 1),
            "avg_ret":  round(sum(vals) / len(vals), 2),
            "max_ret":  round(max(vals), 2),
            "min_ret":  round(min(vals), 2),
        }

    return {
        "strategy":      strategy_name,
        "lookback_days": lookback_days,
        "entries":       len(perf),
        "windows":       summary,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 6. check_data_integrity
# ─────────────────────────────────────────────────────────────────────────────

_KEY_FILES = {
    "universe_main":  "data/universe_main.json",
    "latest_picks":   "data/latest_picks.json",
    "signals_log":    "data/signals_log.json",
    "factor_ic_main": "data/factor_ic_main.json",
    "chip_scan":      "data/chip_scan_latest.json",
    "hot_scan":       "data/hot_scan_latest.json",
    "alert_config":   "alert_config.json",
}

_STALE_HOURS = {
    "latest_picks": 26,
    "chip_scan":    26,
    "hot_scan":     4,
    "signals_log":  72,
}


@tool(
    name="check_data_integrity",
    description="检查关键数据文件是否存在、是否过期，返回健康状态报告",
    parameters={},
)
def check_data_integrity() -> dict:
    now = time.time()
    report: list[dict] = []

    for key, rel in _KEY_FILES.items():
        path = _ROOT / rel
        if not path.exists():
            report.append({"file": key, "status": "missing", "path": rel})
            continue

        age_h = (now - path.stat().st_mtime) / 3600
        stale_h = _STALE_HOURS.get(key)
        stale = (stale_h is not None) and (age_h > stale_h)
        report.append({
            "file":    key,
            "status":  "stale" if stale else "ok",
            "age_h":   round(age_h, 1),
            "path":    rel,
        })

    ok    = sum(1 for r in report if r["status"] == "ok")
    miss  = sum(1 for r in report if r["status"] == "missing")
    stale = sum(1 for r in report if r["status"] == "stale")
    return {
        "summary": {"ok": ok, "missing": miss, "stale": stale},
        "files":   report,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 7. build_strategy_report
# ─────────────────────────────────────────────────────────────────────────────

_PERF_FILES = {
    "main":  "data/main_daily_perf.json",
    "small": "data/sc_daily_perf.json",
    "chip":  "data/chip_daily_perf.json",
    "gc":    "data/gc_daily_perf.json",
    "hot":   "data/hot_daily_perf.json",
    "etf":   "data/etf_daily_perf.json",
}


@tool(
    name="build_strategy_report",
    description="汇总策略胜率/收益表现，返回近 N 日统计摘要",
    parameters={
        "strategy_name": {
            "type": "string",
            "description": "策略名称：main | small | chip | gc | hot | etf | all",
            "optional": True,
        },
        "days": {
            "type": "integer",
            "description": "统计近 N 天数据（默认 30）",
            "optional": True,
        },
    },
)
def build_strategy_report(strategy_name: str = "all", days: int = 30) -> dict:
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    targets = (
        {strategy_name: _PERF_FILES[strategy_name]}
        if strategy_name in _PERF_FILES
        else _PERF_FILES
    )
    if strategy_name == "all":
        targets = _PERF_FILES

    result: dict[str, dict] = {}
    for name, rel in targets.items():
        path = _ROOT / rel
        if not path.exists():
            result[name] = {"status": "no_data"}
            continue
        records = json.loads(path.read_text(encoding="utf-8"))
        recent = [r for r in records if r.get("date", "") >= cutoff]
        if not recent:
            result[name] = {"status": "no_recent_data", "total_records": len(records)}
            continue

        win_rates = [r["win_rate"] for r in recent if r.get("win_rate") is not None]
        avg_rets  = [r["avg_ret"]  for r in recent if r.get("avg_ret")  is not None]
        result[name] = {
            "days":          len(recent),
            "avg_win_rate":  round(sum(win_rates) / len(win_rates), 1) if win_rates else None,
            "avg_ret":       round(sum(avg_rets) / len(avg_rets), 2)   if avg_rets  else None,
            "latest_date":   recent[-1].get("date"),
        }

    return {"period_days": days, "cutoff": cutoff, "strategies": result}


# ─────────────────────────────────────────────────────────────────────────────
# 8. optimize_factor_weights
# ─────────────────────────────────────────────────────────────────────────────

@tool(
    name="optimize_factor_weights",
    description="运行因子权重自动调优（auto_tune.py），返回建议的权重变更",
    parameters={
        "apply": {
            "type": "boolean",
            "description": "是否将建议权重写入配置（默认 False，仅预览）",
            "optional": True,
        },
    },
)
def optimize_factor_weights(apply: bool = False) -> dict:
    script = _ROOT / "src" / "jobs" / "auto_tune.py"
    if not script.exists():
        return {"error": f"auto_tune.py 不存在: {script}"}

    cmd = [_PY, "-X", "utf8", str(script)]
    if apply:
        cmd.append("--apply")

    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           encoding="utf-8", timeout=120)
        return {
            "applied":    apply,
            "returncode": r.returncode,
            "stdout":     r.stdout[-2000:] if r.stdout else "",
            "stderr":     r.stderr[-500:] if r.stderr else "",
        }
    except subprocess.TimeoutExpired:
        return {"error": "auto_tune 超时 (>120s)"}
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# 9. get_chip_profile
# ─────────────────────────────────────────────────────────────────────────────

@tool(
    name="get_chip_profile",
    description="查询单只股票在最新筹码扫描中的分层结果（tier/winner_rate/spread_pct）",
    parameters={
        "code": {
            "type": "string",
            "description": "6位股票代码，如 '000001'",
        },
    },
)
def get_chip_profile(code: str) -> dict:
    code = code.zfill(6)
    scan_file = _DATA / "chip_scan_latest.json"
    if not scan_file.exists():
        return {"error": "chip_scan_latest.json 不存在，请先运行 chip/daily_scan.py"}

    scan = json.loads(scan_file.read_text(encoding="utf-8"))
    date = scan.get("date", "")

    # 在各 tier 中搜索
    for tier_name, picks in scan.get("tiers", {}).items():
        for p in (picks if isinstance(picks, list) else []):
            if str(p.get("code", "")).zfill(6) == code:
                return {
                    "code":        code,
                    "date":        date,
                    "tier":        tier_name,
                    "name":        p.get("name", ""),
                    "winner_rate": p.get("winner_rate"),
                    "spread_pct":  p.get("spread_pct"),
                    "close":       p.get("close"),
                    "pct_chg":     p.get("pct_chg"),
                    "industry":    p.get("industry"),
                }

    # 不在任何 tier 中
    return {
        "code":   code,
        "date":   date,
        "tier":   None,
        "status": "not_in_scan",
    }


# ─────────────────────────────────────────────────────────────────────────────
# 10. query_watchlist
# ─────────────────────────────────────────────────────────────────────────────

@tool(
    name="query_watchlist",
    description="查询当前 watchlist（自选股）列表，含最新因子得分（可选）",
    parameters={
        "with_scores": {
            "type": "boolean",
            "description": "是否实时计算每只股票的因子得分（较慢）",
            "optional": True,
        },
    },
)
def query_watchlist(with_scores: bool = False) -> dict:
    from common import load_alert_config
    config = load_alert_config()
    watchlist = config.get("watchlist", config.get("screener_universe", []))

    if not watchlist:
        return {"watchlist": [], "count": 0}

    if not with_scores:
        return {"watchlist": watchlist, "count": len(watchlist)}

    # 实时打分
    from report.utils import score_one_buy
    from factors import DEFAULT_WEIGHTS

    scored = []
    for code in watchlist:
        try:
            s = score_one_buy(str(code).zfill(6), weights=DEFAULT_WEIGHTS)
            scored.append({
                "code":       s.get("code", code),
                "name":       s.get("name", ""),
                "buy_score":  s.get("buy_score"),
                "sell_score": s.get("sell_score"),
                "change_pct": s.get("change_pct"),
            })
        except Exception as e:
            scored.append({"code": code, "error": str(e)})

    scored.sort(key=lambda x: -(x.get("buy_score") or 0))
    return {"watchlist": scored, "count": len(scored)}
