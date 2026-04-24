#!/usr/bin/env python3
"""
Chip strategy backtest (CAH, T1–T4): winner-rate tiers with 6m-high filter.

Tiers  : T1 ≥95%, T2 90-95%, T3 85-90%, T4 75-85%
Filter : CAH — close/6m_high < 0.9 (距半年高点 ≥ 10%)

For each sampled trade date:
  1. Fetch chip data (cyq_perf + daily) — Tushare, cached 23h
  2. Fetch 6-month high (bi-weekly sampling, cached 90d)
  3. Apply screen() for each tier with h-filter
  4. Fetch forward daily prices at D+5 and D+10 (trading days)
  5. Compute per-pick and per-period forward returns

Output: summary table (stdout) + data/chip_backtest_result.json

Usage:
    python -X utf8 scripts/chip_backtest.py              # last 6 months, weekly
    python -X utf8 scripts/chip_backtest.py --months 3
    python -X utf8 scripts/chip_backtest.py --step 10   # every 10 trading days
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
DATA    = ROOT / "data"
sys.path.insert(0, str(SCRIPTS))

import cache as _cache
import fetcher as _fetcher
from chip_strategy import _get_pro, fetch_chip_data, screen, load_names, fetch_6m_high, _chip_cache_key
from common import send_wechat, configure_pushplus

# ---------------------------------------------------------------------------
# Combos
# ---------------------------------------------------------------------------

TIERS = {
    "T1": (95, None),
    "T2": (90, 95),
    "T3": (85, 90),
    "T4": (75, 85),
}

MODS = {
    "": dict(max_price=None, exclude_kcb=False),
}

COMBOS: list[dict] = []
for tier_name, (min_w, max_w) in TIERS.items():
    for mod_name, mod_kw in MODS.items():
        COMBOS.append(dict(
            key=f"{tier_name}{'+' + mod_name if mod_name else ''}",
            tier=tier_name,
            mod=mod_name,
            min_win=min_w,
            max_win=max_w,
            **mod_kw,
        ))


# ---------------------------------------------------------------------------
# Rate-limit-aware chip fetch
# ---------------------------------------------------------------------------

def _fetch_chip_retry(date: str, pro, max_retries: int = 0) -> "pd.DataFrame":
    """Wrap fetch_chip_data; skip on rate-limit (max_retries=0 means skip immediately)."""
    attempts = 0
    while True:
        try:
            return fetch_chip_data(date, pro)
        except Exception as e:
            msg = str(e)
            if "每小时" in msg or "每分钟" in msg or "最多访问" in msg or "频率超限" in msg or "次/天" in msg or "次/小时" in msg:
                if attempts >= max_retries:
                    print(f"  [rate-limit] {date} 限流，跳过", flush=True)
                    import pandas as pd
                    return pd.DataFrame()
                attempts += 1
                now = datetime.now()
                next_hour = now.replace(minute=0, second=0, microsecond=0)
                next_hour = next_hour.replace(hour=next_hour.hour + 1) if now.hour < 23 \
                    else now.replace(day=now.day + 1, hour=0, minute=0, second=0)
                wait = int((next_hour - now).total_seconds()) + 300
                resume_at = datetime.fromtimestamp(now.timestamp() + wait)
                print(f"  [rate-limit] cyq_perf 触发限流（第{attempts}次），等待至 {resume_at:%H:%M:%S} 再重试...",
                      flush=True)
                time.sleep(wait)
                print(f"  [rate-limit] 恢复，继续 {date}", flush=True)
            else:
                raise


# ---------------------------------------------------------------------------
# Completion notifications
# ---------------------------------------------------------------------------

def _push_notify(title: str, body: str) -> None:
    """Send completion notification to WeChat + Discord webhook."""
    cfg = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    sendkey = cfg.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
    send_wechat(title, body, sendkey)

    webhook_url = cfg.get("discord", {}).get("webhook_url", "")
    if webhook_url:
        try:
            import urllib.request
            discord_text = f"**{title}**\n{body[:1900]}"
            data = json.dumps({"content": discord_text}).encode("utf-8")
            req = urllib.request.Request(
                webhook_url, data=data,
                headers={"Content-Type": "application/json",
                         "User-Agent": "DiscordBot (stocksage-alpha, 1.0)"},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                print(f"[discord] webhook 已发送 (HTTP {r.status})")
        except Exception as e:
            print(f"[discord] webhook 失败: {e}")


# ---------------------------------------------------------------------------
# Trade calendar helpers
# ---------------------------------------------------------------------------


def _get_trade_dates_simple(months: int, step: int, pro) -> tuple[list[str], list[str]]:
    """
    Returns (sampled_dates, full_calendar) both in YYYYMMDD format,
    filtered to actual past trading days with room for forward returns.
    """
    print("[calendar] 获取交易日历（BaoStock/AKShare）...")
    raw: list[str] = _fetcher.get_trade_calendar()
    # Normalize: strip dashes, keep YYYYMMDD only
    all_dates: list[str] = sorted(set(d.replace("-", "") for d in raw))

    from datetime import date
    today_str = date.today().strftime("%Y%m%d")
    year  = date.today().year
    month = date.today().month - months
    while month <= 0:
        month += 12
        year  -= 1
    cutoff = f"{year}{month:02d}01"

    # Past dates only (≤ today)
    past_dates = [d for d in all_dates if d <= today_str]

    # safe_end: leave 10 forward trading days from the last sampled date
    safe_end = past_dates[-11] if len(past_dates) > 11 else past_dates[0]

    window  = [d for d in past_dates if cutoff <= d <= safe_end]
    sampled = window[::step]
    return sampled, all_dates


def _fwd_date(all_dates: list[str], base_date: str, fwd: int) -> str | None:
    """Return the trading date `fwd` days after `base_date`."""
    try:
        idx = all_dates.index(base_date)
        if idx + fwd < len(all_dates):
            return all_dates[idx + fwd]
    except ValueError:
        pass
    return None


# ---------------------------------------------------------------------------
# Forward return fetch
# ---------------------------------------------------------------------------

_FWD_CACHE: dict[str, dict[str, float]] = {}   # {trade_date: {ts_code: pct_chg_cumulative}}


def _fetch_fwd_close(date: str, pro) -> dict[str, float]:
    """Return {ts_code: close} for a trade date (cached in memory)."""
    if date in _FWD_CACHE:
        return _FWD_CACHE[date]
    cache_key = f"fwd_close_{date}"
    cached = _cache.get(cache_key, 30 * 24 * 3600)
    if cached:
        _FWD_CACHE[date] = cached
        return cached
    try:
        df = pro.daily(trade_date=date, fields="ts_code,close")
        if df is None or df.empty:
            return {}
        result = dict(zip(df["ts_code"], pd.to_numeric(df["close"], errors="coerce")))
        _cache.set(cache_key, result)
        _FWD_CACHE[date] = result
        return result
    except Exception as e:
        print(f"  [warn] fwd_close {date}: {e}")
        return {}


# ---------------------------------------------------------------------------
# Push helper
# ---------------------------------------------------------------------------

WIN_RANGE = {"T1": "≥95%", "T2": "90-95%", "T3": "85-90%", "T4": "75-85%"}

def _do_push(output: list[dict], months: int, n_dates: int, step: int = 20) -> None:
    """Build clean markdown push with main-strategy comparison and send."""
    bare = {r["combo"]: r for r in output if r.get("mod", "") == "" and r.get("n_picks", 0) > 0}
    n_periods = max((r.get("n_periods", 0) for r in bare.values()), default=0)

    lines = [
        f"## 筹码 CAH 回测 · {months}个月 · {n_periods}期",
        "",
        "| 层 | 获利盘 | 样本 | 期组合胜率 | 均涨10日 |",
        "|---|---|---:|---:|---:|",
    ]
    best_tier, best_ret = None, -999
    for tier in ["T1", "T2", "T3", "T4"]:
        r = bare.get(tier)
        if not r:
            continue
        pw = r.get("port_win10d", 0)
        pr = r.get("port_avg10d", 0)
        np_ = r.get("n_picks", 0)
        nper = r.get("n_periods", 1) or 1
        avg_n = round(np_ / nper)
        lines.append(f"| {tier} | {WIN_RANGE[tier]} | ~{avg_n}只/期 | {pw:.0f}% | {pr:+.2f}% |")
        if pr > best_ret:
            best_ret, best_tier = pr, tier

    if best_tier:
        bw = bare[best_tier].get("port_win10d", 0)
        lines += [
            "",
            f"> 🏆 **{best_tier}（{WIN_RANGE[best_tier]}）** 最优：{n_periods}期胜率 {bw:.0f}%，均涨10日 {best_ret:+.2f}%",
            f"> 过滤：CAH（距半年高点≥10%）；步长{step}日，共{n_periods}期",
        ]

    # Main strategy comparison
    main_stats: dict = {}
    main_path = DATA / "backtest_main_16p.json"
    if main_path.exists():
        try:
            md = json.loads(main_path.read_text(encoding="utf-8"))
            main_stats = md.get("stats", {})
        except Exception:
            pass

    if main_stats and best_tier:
        mw = main_stats.get("win_rate_pct", 0)
        mr = main_stats.get("mean_period_ret_pct", 0)
        mn = main_stats.get("n_periods", 0)
        chip_bw = bare[best_tier].get("port_win10d", 0)
        best_np = bare[best_tier].get("n_picks", 0)
        best_nper = bare[best_tier].get("n_periods", 1) or 1
        best_avg_n = round(best_np / best_nper)
        lines += [
            "",
            "### vs 主策略",
            "| 策略 | 期组合胜率 | 均涨10日 | 期数 | 均选股/期 |",
            "|---|---:|---:|---:|---:|",
            f"| 筹码 {best_tier}（CAH） | {chip_bw:.0f}% | {best_ret:+.2f}% | {n_periods} | ~{best_avg_n}只 |",
            f"| 主策略 | {mw:.0f}% | {mr:+.3f}% | {mn} | ~141只 |",
        ]

    push_body  = "\n".join(lines)
    push_title = f"筹码CAH回测 {months}个月 最优{best_tier} {best_ret:+.2f}%"
    print(f"\n{push_body}\n")
    _push_notify(push_title, push_body)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=6,
                        help="回测月数，默认6")
    parser.add_argument("--step",   type=int, default=5,
                        help="采样间隔（交易日），默认5（每周）")
    parser.add_argument("--fwd5",   type=int, default=5,
                        help="短期持有天数，默认5")
    parser.add_argument("--fwd10",  type=int, default=10,
                        help="中期持有天数，默认10")
    parser.add_argument("--out",    type=str,
                        default=str(DATA / "chip_backtest_result.json"))
    parser.add_argument("--push-only", action="store_true",
                        help="只重新推送已有 JSON，不重跑回测")
    args = parser.parse_args()

    if args.push_only:
        out_path = Path(args.out)
        if not out_path.exists():
            print(f"[bt] 找不到 {out_path}，请先正常跑一次")
            return
        output = json.loads(out_path.read_text(encoding="utf-8"))
        _do_push(output, months=args.months, n_dates=0, step=args.step)
        return

    pro = _get_pro()

    # ── Trade calendar ────────────────────────────────────────────────────
    print("[bt] 获取交易日历...")
    all_dates, full_cal = _get_trade_dates_simple(args.months, args.step, pro)
    print(f"[bt] 采样日期 {len(all_dates)} 个: {all_dates[0]} → {all_dates[-1]}")

    # ── Load names once ──────────────────────────────────────────────────
    names = load_names()

    # ── Per-combo accumulators ────────────────────────────────────────────
    # stock-level: {combo_key: [(ts_code, c0, c5, c10), ...]}
    records: dict[str, list[tuple]] = {c["key"]: [] for c in COMBOS}
    # period-level: {combo_key: {date: [(ts_code, c0, c5, c10), ...]}}
    period_records: dict[str, dict[str, list]] = {c["key"]: {} for c in COMBOS}

    # ── Loop over dates ───────────────────────────────────────────────────
    for i, date in enumerate(all_dates):
        d5  = _fwd_date(full_cal, date, args.fwd5)
        d10 = _fwd_date(full_cal, date, args.fwd10)
        if not d5 or not d10:
            print(f"  [{i+1}/{len(all_dates)}] {date}: 缺前向日期，跳过")
            continue

        print(f"  [{i+1}/{len(all_dates)}] {date}  fwd5={d5}  fwd10={d10}", flush=True)

        # Historical chip data never changes — use 30d TTL
        _BT_TTL = 30 * 24 * 3600
        new_key = _chip_cache_key(date, "ts")   # what fetch_chip_data actually looks for
        old_key = f"chip_data_{date}"           # legacy format written by earlier runs

        # Migrate old-format cache → new format so fetch_chip_data finds it on first pass
        if _cache.get(new_key, _BT_TTL) is None:
            old_data = _cache.get(old_key, _BT_TTL)
            if old_data is not None:
                _cache.set(new_key, old_data)
                print(f"  [cache] migrated {old_key} → {new_key}", flush=True)

        cache_hit = _cache.get(new_key, _BT_TTL) is not None
        df = _fetch_chip_retry(date, pro)
        if not cache_hit and not df.empty:
            # cyq_perf: 10 calls/hour hard limit → throttle to ~7/hour to be safe
            time.sleep(520)
        if df.empty:
            print(f"    no chip data, skip")
            continue

        # Fetch 6-month high for CAH filter (cached 90d, cheap on re-runs)
        six_m = fetch_6m_high(df["ts_code"].tolist(), date, pro)

        # Merge names
        if names:
            df["name"]     = df["ts_code"].map(lambda c: names.get(c, {}).get("name", ""))
            df["industry"] = df["ts_code"].map(lambda c: names.get(c, {}).get("industry", ""))
        else:
            df["name"] = df["industry"] = ""

        # Forward closes
        time.sleep(0.2)
        close5  = _fetch_fwd_close(d5, pro)
        time.sleep(0.2)
        close10 = _fetch_fwd_close(d10, pro)

        if not close5 or not close10:
            print(f"    no forward data, skip")
            continue

        # Apply each combo's screen with CAH h-filter
        for combo in COMBOS:
            result = screen(
                df,
                min_win=combo["min_win"],
                max_win=combo["max_win"],
                max_today_pct=5.0,
                max_6m_ratio=0.9,         # CAH: 距半年高点 ≥ 10%
                six_month_high=six_m,
                max_price=combo["max_price"],
                exclude_kcb=combo["exclude_kcb"],
            )
            day_picks = []
            for _, row in result.iterrows():
                ts = row["ts_code"]
                c0 = row.get("close")
                c5  = close5.get(ts)
                c10 = close10.get(ts)
                if pd.notna(c0) and c0 > 0 and c5 and c10:
                    tup = (ts, float(c0), float(c5), float(c10))
                    records[combo["key"]].append(tup)
                    day_picks.append(tup)
            if day_picks:
                period_records[combo["key"]][date] = day_picks

        time.sleep(0.3)

    # ── Compute stats ─────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    header = f"{'组合':<12}{'picks':>7}{'win5d%':>8}{'ret5d%':>9}{'win10d%':>9}{'ret10d%':>10}"
    print(header)
    print("-" * 80)

    output: list[dict] = []
    for combo in COMBOS:
        key  = combo["key"]
        recs = records[key]
        n    = len(recs)
        if n == 0:
            print(f"{key:<12}{'0':>7}{'—':>8}{'—':>9}{'—':>9}{'—':>10}")
            output.append(dict(combo=key, tier=combo["tier"], mod=combo["mod"],
                               n_picks=0))
            continue

        ret5d  = [(c5 / c0 - 1) * 100  for _, c0, c5, _   in recs]
        ret10d = [(c10 / c0 - 1) * 100 for _, c0, _, c10   in recs]

        win5d_pct  = sum(r > 0 for r in ret5d)  / n * 100
        win10d_pct = sum(r > 0 for r in ret10d) / n * 100
        avg5d  = sum(ret5d)  / n
        avg10d = sum(ret10d) / n

        # Per-period equal-weight portfolio returns
        port5d, port10d = [], []
        for day_picks in period_records[key].values():
            r5  = [(c5  / c0 - 1) * 100 for _, c0, c5, _   in day_picks]
            r10 = [(c10 / c0 - 1) * 100 for _, c0, _, c10  in day_picks]
            port5d.append(sum(r5)   / len(r5))
            port10d.append(sum(r10) / len(r10))
        n_periods   = len(port10d)
        port_win5d  = sum(r > 0 for r in port5d)  / n_periods * 100 if n_periods else 0
        port_win10d = sum(r > 0 for r in port10d) / n_periods * 100 if n_periods else 0
        port_avg5d  = sum(port5d)  / n_periods if n_periods else 0
        port_avg10d = sum(port10d) / n_periods if n_periods else 0

        print(f"{key:<12}{n:>7}{win5d_pct:>7.1f}%{avg5d:>+8.2f}%{win10d_pct:>8.1f}%{avg10d:>+9.2f}%"
              f"  |期组合: {port_win10d:.0f}%胜 {port_avg10d:+.2f}%")
        output.append(dict(
            combo=key, tier=combo["tier"], mod=combo["mod"],
            n_picks=n,
            win5d=round(win5d_pct, 2),       avg_ret5d=round(avg5d, 3),
            win10d=round(win10d_pct, 2),     avg_ret10d=round(avg10d, 3),
            med_ret5d=round(sorted(ret5d)[n // 2], 3),
            med_ret10d=round(sorted(ret10d)[n // 2], 3),
            n_periods=n_periods,
            port_win5d=round(port_win5d, 1),   port_avg5d=round(port_avg5d, 3),
            port_win10d=round(port_win10d, 1), port_avg10d=round(port_avg10d, 3),
        ))

    footer = (f"注：CAH（距半年高点≥10%）；max_today_pct=5%\n"
              f"采样 {len(all_dates)} 个交易日  步长 {args.step}d  回测区间 {args.months}个月")
    print("=" * 80)
    print(f"\n{footer}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n结果已写入: {out_path}")

    _do_push(output, months=args.months, n_dates=len(all_dates), step=args.step)


if __name__ == "__main__":
    main()
