# signal_tracker.py — 收盘后追踪买卖信号的远期收益，写入 signal_performance.json
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

# ── 路径定义 ────────────────────────────────────────────────────────────────────
_ROOT          = Path(__file__).parent.parent
SIGNALS_LOG    = _ROOT / "data" / "signals_log.json"
WATCHLIST_LOG  = _ROOT / "data" / "watchlist_log.json"
PERF_LOG       = _ROOT / "data" / "signal_performance.json"
SCAN_CACHE     = _ROOT / "data" / "watchlist_scan_latest.json"
FACTOR_CONFIG  = _ROOT / "scripts" / "factor_config.py"

# 远期收益窗口（交易日）
FWD_WINDOWS = [1, 5, 20]

# ── 辅助函数 ────────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> list | dict:
    """安全读取 JSON；文件不存在时返回空列表。"""
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, obj, dry_run: bool = False) -> None:
    """写入 JSON；dry_run 时只打印不写盘。"""
    if dry_run:
        print(f"  [dry-run] 跳过写入 {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _get_closes(code: str, start_date: str, end_date: str) -> dict[str, float]:
    """
    用 akshare 获取复权收盘价，返回 {"YYYY-MM-DD": close, ...}。
    失败时返回空字典并打印 WARN。
    """
    try:
        import akshare as ak
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust="qfq",
        )
        if df is None or df.empty:
            return {}
        # 列名: 日期, 收盘
        return {str(row["日期"])[:10]: float(row["收盘"]) for _, row in df.iterrows()}
    except Exception as e:
        print(f"  WARN fetch {code}: {e}")
        return {}


def _fwd_return(closes: dict[str, float], signal_date: str,
                entry_price: float, n: int) -> float | None:
    """
    计算 T+N 交易日的远期收益率（%）。
    若数据不足则返回 None。
    """
    if not closes or entry_price <= 0:
        return None
    sorted_dates = sorted(closes.keys())
    # 找 signal_date 之后的第一个交易日索引
    try:
        first_after = next(i for i, d in enumerate(sorted_dates) if d > signal_date)
    except StopIteration:
        return None
    target_idx = first_after + n - 1
    if target_idx >= len(sorted_dates):
        return None
    target_date = sorted_dates[target_idx]
    target_close = closes[target_date]
    return round((target_close - entry_price) / entry_price * 100, 4)


def _enough_time_passed(signal_date: str, n: int, today: str,
                         closes: dict[str, float]) -> bool:
    """
    判断从 signal_date 起是否已经过了 N 个交易日（用 closes 的日期计数）。
    """
    sorted_dates = sorted(closes.keys())
    try:
        first_after = next(i for i, d in enumerate(sorted_dates) if d > signal_date)
    except StopIteration:
        return False
    return (first_after + n - 1) < len(sorted_dates)


# ── 主要步骤 ────────────────────────────────────────────────────────────────────

def fill_signal_returns(signals_log: list, today: str, dry_run: bool) -> list:
    """
    遍历 signals_log 中的 BUY 信号，填写尚未计算的远期收益字段。
    返回更新后的 signals_log（保留原始结构）。
    """
    print("\n=== 填写买入信号远期收益 ===")
    updated = 0

    # 为了减少 API 调用，按 code 聚合需要查询的日期范围
    # 先扫描需要填写的信号
    needed: dict[str, dict] = {}  # code -> {signal_date, entry_price, ref_to_signal}

    for run_entry in signals_log:
        run_date = run_entry.get("date", "")
        for sig in run_entry.get("buy_signals", []):
            code = sig.get("code", "")
            price = float(sig.get("signal_price") or sig.get("price") or 0)
            # 检查是否有未填写的窗口
            missing = [n for n in FWD_WINDOWS
                       if sig.get(f"fwd_ret_{n}d") is None]
            if missing and code and price > 0:
                if code not in needed:
                    needed[code] = {"min_date": run_date, "max_date": run_date}
                else:
                    if run_date < needed[code]["min_date"]:
                        needed[code]["min_date"] = run_date
                    if run_date > needed[code]["max_date"]:
                        needed[code]["max_date"] = run_date

    if not needed:
        print("  没有需要填写的买入信号。")
        return signals_log

    # 每个 code 获取一次行情
    for run_entry in signals_log:
        run_date = run_entry.get("date", "")
        for sig in run_entry.get("buy_signals", []):
            code = sig.get("code", "")
            price = float(sig.get("signal_price") or sig.get("price") or 0)
            if code not in needed or price <= 0:
                continue

            missing = [n for n in FWD_WINDOWS
                       if sig.get(f"fwd_ret_{n}d") is None]
            if not missing:
                continue

            # 获取行情：signal_date 往后多留 60 个自然日保证覆盖 20 交易日
            fetch_end = (datetime.strptime(run_date, "%Y-%m-%d")
                         + timedelta(days=60)).strftime("%Y-%m-%d")
            fetch_end = min(fetch_end, today)
            closes = _get_closes(code, run_date, fetch_end)

            for n in missing:
                if _enough_time_passed(run_date, n, today, closes):
                    ret = _fwd_return(closes, run_date, price, n)
                    if ret is not None:
                        sig[f"fwd_ret_{n}d"] = ret
                        updated += 1
                        print(f"  {code} {run_date} fwd_ret_{n}d = {ret:+.2f}%")

    if not dry_run:
        print(f"  已更新 {updated} 个收益字段。")
    else:
        print(f"  [dry-run] 将更新 {updated} 个收益字段。")

    return signals_log


def snapshot_watchlist(today: str, dry_run: bool) -> None:
    """
    读取 watchlist_scan_latest.json，追加到 watchlist_log.json。
    若当天已有记录则跳过。
    """
    print("\n=== 快照今日自选股扫描结果 ===")
    if not SCAN_CACHE.exists():
        print("  WARN: watchlist_scan_latest.json 不存在，跳过快照。")
        return

    scan = _load_json(SCAN_CACHE)
    scan_date = scan.get("date", "") if isinstance(scan, dict) else ""
    if scan_date != today:
        print(f"  WARN: scan_cache 日期 {scan_date!r} 不是今天 {today}，跳过。")
        return

    wl_log: list = _load_json(WATCHLIST_LOG) if WATCHLIST_LOG.exists() else []  # type: ignore[assignment]

    # 检查今天是否已存在
    if any(e.get("date") == today for e in wl_log):
        print(f"  今天 {today} 已有记录，跳过。")
        return

    # 构建新条目：为每条 scored 预留 fwd_ret 字段
    entries = []
    for item in scan.get("scored", []):
        entry = dict(item)
        for n in FWD_WINDOWS:
            entry.setdefault(f"fwd_ret_{n}d", None)
        entries.append(entry)

    wl_log.append({"date": today, "entries": entries})
    _save_json(WATCHLIST_LOG, wl_log, dry_run)
    print(f"  已追加 {len(entries)} 条自选股记录（{today}）。")


def fill_watchlist_returns(today: str, dry_run: bool) -> None:
    """
    为 watchlist_log.json 中的历史条目填写远期收益。
    """
    print("\n=== 填写自选股远期收益 ===")
    if not WATCHLIST_LOG.exists():
        print("  watchlist_log.json 不存在，跳过。")
        return

    wl_log: list = _load_json(WATCHLIST_LOG)  # type: ignore[assignment]
    updated = 0

    for day_entry in wl_log:
        entry_date = day_entry.get("date", "")
        for item in day_entry.get("entries", []):
            code = str(item.get("code", ""))
            price = float(item.get("price") or 0)
            if not code or price <= 0:
                continue
            missing = [n for n in FWD_WINDOWS if item.get(f"fwd_ret_{n}d") is None]
            if not missing:
                continue

            fetch_end = (datetime.strptime(entry_date, "%Y-%m-%d")
                         + timedelta(days=60)).strftime("%Y-%m-%d")
            fetch_end = min(fetch_end, today)
            closes = _get_closes(code, entry_date, fetch_end)

            for n in missing:
                if _enough_time_passed(entry_date, n, today, closes):
                    ret = _fwd_return(closes, entry_date, price, n)
                    if ret is not None:
                        item[f"fwd_ret_{n}d"] = ret
                        updated += 1

    _save_json(WATCHLIST_LOG, wl_log, dry_run)
    print(f"  自选股远期收益已更新 {updated} 个字段。")


def compute_performance(signals_log: list) -> dict:
    """
    汇总信号绩效：
      - 每个窗口的平均/中位/胜率
      - 每个因子的胜率（基于 20d 窗口的买入信号）
    返回结构化的 performance 字典。
    """
    from statistics import median

    # 收集所有有效买入信号
    buy_rets: dict[int, list[float]] = {n: [] for n in FWD_WINDOWS}
    factor_stats: dict[str, dict] = {}  # factor -> {wins, total}

    for run_entry in signals_log:
        for sig in run_entry.get("buy_signals", []):
            for n in FWD_WINDOWS:
                v = sig.get(f"fwd_ret_{n}d")
                if v is not None:
                    buy_rets[n].append(v)

            # 因子胜率：只看 fwd_ret_20d 有值的信号
            ret20 = sig.get("fwd_ret_20d")
            if ret20 is None:
                continue
            won = ret20 > 0
            for bf in sig.get("bullish", []):
                fac = bf.get("factor", "")
                if not fac:
                    continue
                if fac not in factor_stats:
                    factor_stats[fac] = {"wins": 0, "total": 0}
                factor_stats[fac]["total"] += 1
                if won:
                    factor_stats[fac]["wins"] += 1

    # 汇总每个窗口统计
    window_stats = {}
    for n in FWD_WINDOWS:
        rets = buy_rets[n]
        if rets:
            wins = sum(1 for r in rets if r > 0)
            window_stats[f"fwd_{n}d"] = {
                "n": len(rets),
                "mean_pct": round(sum(rets) / len(rets), 4),
                "median_pct": round(median(rets), 4),
                "win_rate": round(wins / len(rets), 4),
                "wins": wins,
            }
        else:
            window_stats[f"fwd_{n}d"] = {"n": 0}

    # 整理因子统计，加入胜率
    factor_perf = {}
    for fac, st in factor_stats.items():
        total = st["total"]
        wins = st["wins"]
        factor_perf[fac] = {
            "total": total,
            "wins": wins,
            "hit_rate": round(wins / total, 4) if total > 0 else None,
        }

    return {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "window_stats": window_stats,
        "factor_hit_rates": factor_perf,
    }


def print_report(perf: dict) -> None:
    """打印人类可读的绩效报告。"""
    print("\n" + "=" * 55)
    print("  StockSage Alpha — 信号绩效报告")
    print(f"  数据截至: {perf.get('updated_at', 'N/A')}")
    print("=" * 55)

    ws = perf.get("window_stats", {})
    print("\n── 买入信号远期收益 ──────────────────────────────────")
    for key in [f"fwd_{n}d" for n in FWD_WINDOWS]:
        st = ws.get(key, {})
        n_samples = st.get("n", 0)
        if n_samples == 0:
            print(f"  {key:10s}: 暂无足够数据")
        else:
            print(f"  {key:10s}: N={n_samples:3d}  "
                  f"均值={st['mean_pct']:+6.2f}%  "
                  f"中位={st['median_pct']:+6.2f}%  "
                  f"胜率={st['win_rate']:.1%}")

    fh = perf.get("factor_hit_rates", {})
    if fh:
        print("\n── 因子胜率（20d 买入信号，N≥3）───────────────────")
        rows = [(f, v) for f, v in fh.items()
                if isinstance(v.get("hit_rate"), float) and v["total"] >= 3]
        rows.sort(key=lambda x: x[1]["hit_rate"], reverse=True)
        for fac, v in rows:
            bar_len = int(v["hit_rate"] * 20)
            bar = "█" * bar_len + "░" * (20 - bar_len)
            print(f"  {fac:28s} {bar} {v['hit_rate']:.1%}  (N={v['total']})")
    print("=" * 55)


# ── CLI 入口 ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="追踪信号远期收益")
    parser.add_argument("--report", action="store_true",
                        help="只打印现有绩效报告，不更新数据")
    parser.add_argument("--dry-run", action="store_true",
                        help="计算但不写文件")
    args = parser.parse_args()

    # --report 模式：直接从磁盘加载并打印
    if args.report:
        perf = _load_json(PERF_LOG) if PERF_LOG.exists() else {}
        if not perf:
            print("signal_performance.json 不存在，请先运行一次不带 --report 的命令。")
            return
        print_report(perf)  # type: ignore[arg-type]
        return

    today = datetime.now().strftime("%Y-%m-%d")
    print(f"signal_tracker 运行日期: {today}  dry_run={args.dry_run}")

    # 1. 读取并更新 signals_log
    signals_log: list = _load_json(SIGNALS_LOG)  # type: ignore[assignment]
    if not isinstance(signals_log, list):
        signals_log = []

    signals_log = fill_signal_returns(signals_log, today, args.dry_run)

    # 写回 signals_log
    if not args.dry_run and signals_log:
        _save_json(SIGNALS_LOG, signals_log)
        print("  signals_log.json 已更新。")

    # 2. 快照今日自选股扫描
    snapshot_watchlist(today, args.dry_run)

    # 3. 填写自选股远期收益
    fill_watchlist_returns(today, args.dry_run)

    # 4. 计算汇总绩效
    print("\n=== 计算汇总绩效 ===")
    perf = compute_performance(signals_log)
    _save_json(PERF_LOG, perf, args.dry_run)

    # 5. 打印报告
    print_report(perf)
    print("\n完成。")


if __name__ == "__main__":
    main()
