#!/usr/bin/env python3
"""
Chip strategy backtest: test all tier × modifier combos over historical dates.

Tiers  : T1 ≥95%, T2 90-95%, T3 85-90%, T4 75-85%, T5 65-75%
Modifiers: none / e (price≤50) / k (no 科创板) / ek (both)
= 20 combos total

For each sampled trade date:
  1. Fetch chip data (cyq_perf + daily) — Tushare, cached 23h
  2. Apply screen() for each combo  (6m_high skipped for speed)
  3. Fetch forward daily prices at D+5 and D+10 (trading days)
  4. Compute per-pick forward returns

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
from chip_strategy import _get_pro, fetch_chip_data, screen, load_names
from common import send_wechat, configure_pushplus

# ---------------------------------------------------------------------------
# Combos
# ---------------------------------------------------------------------------

TIERS = {
    "T1": (95, None),
    "T2": (90, 95),
    "T3": (85, 90),
    "T4": (75, 85),
    "T5": (65, 75),
}

MODS = {
    "":   dict(max_price=None, exclude_kcb=False),
    "e":  dict(max_price=50.0, exclude_kcb=False),
    "k":  dict(max_price=None, exclude_kcb=True),
    "ek": dict(max_price=50.0, exclude_kcb=True),
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
            if "每小时" in msg or "每分钟" in msg or "最多访问" in msg:
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

def _push_discord_dm(user_id: int, bot_token: str, text: str) -> None:
    """Send a DM via Discord REST API."""
    import urllib.request
    headers = {"Authorization": f"Bot {bot_token}",
               "Content-Type": "application/json"}
    # 1. Create / get DM channel
    payload = json.dumps({"recipient_id": str(user_id)}).encode()
    req = urllib.request.Request(
        "https://discord.com/api/v10/users/@me/channels",
        data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            channel = json.loads(resp.read())
        channel_id = channel["id"]
    except Exception as e:
        print(f"[discord] 建立DM频道失败: {e}")
        return
    # 2. Send message (split if > 1900 chars)
    for chunk in [text[i:i+1900] for i in range(0, len(text), 1900)]:
        payload = json.dumps({"content": chunk}).encode()
        req = urllib.request.Request(
            f"https://discord.com/api/v10/channels/{channel_id}/messages",
            data=payload, headers=headers, method="POST")
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            print(f"[discord] 发送失败: {e}")


def _push_notify(title: str, body: str) -> None:
    """Send completion notification to WeChat + Discord."""
    cfg = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    sendkey = cfg.get("serverchan", {}).get("sendkey", "")
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
    send_wechat(title, body, sendkey)

    bot_cfg = json.loads((ROOT / "stock-bot" / "config.json").read_text(encoding="utf-8"))
    bot_token  = bot_cfg.get("discord", {}).get("bot_token", "")
    allowed_ids = bot_cfg.get("discord", {}).get("allowed_ids", [])
    if bot_token and allowed_ids:
        discord_text = f"**{title}**\n```\n{body[:1500]}\n```"
        for uid in allowed_ids:
            _push_discord_dm(int(uid), bot_token, discord_text)
        print("[notify] Discord DM 已发送")


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
    args = parser.parse_args()

    pro = _get_pro()

    # ── Trade calendar ────────────────────────────────────────────────────
    print("[bt] 获取交易日历...")
    all_dates, full_cal = _get_trade_dates_simple(args.months, args.step, pro)
    print(f"[bt] 采样日期 {len(all_dates)} 个: {all_dates[0]} → {all_dates[-1]}")

    # ── Load names once ──────────────────────────────────────────────────
    names = load_names()

    # ── Per-combo accumulators ────────────────────────────────────────────
    # {combo_key: {"picks": [(ts_code, close, fwd5_close, fwd10_close), ...]}}
    records: dict[str, list[tuple]] = {c["key"]: [] for c in COMBOS}

    # ── Loop over dates ───────────────────────────────────────────────────
    for i, date in enumerate(all_dates):
        d5  = _fwd_date(full_cal, date, args.fwd5)
        d10 = _fwd_date(full_cal, date, args.fwd10)
        if not d5 or not d10:
            print(f"  [{i+1}/{len(all_dates)}] {date}: 缺前向日期，跳过")
            continue

        print(f"  [{i+1}/{len(all_dates)}] {date}  fwd5={d5}  fwd10={d10}", flush=True)

        # Historical data never changes — refresh cache TTL if file exists but 23h expired
        _BT_TTL = 30 * 24 * 3600
        cache_key = f"chip_data_{date}"
        stale = _cache.get(cache_key, _BT_TTL)
        if stale is not None and _cache.get(cache_key, 23 * 3600) is None:
            _cache.set(cache_key, stale)  # refresh timestamp so fetch_chip_data sees a hit
        cache_hit = _cache.get(cache_key, 23 * 3600) is not None
        df = _fetch_chip_retry(date, pro)
        if not cache_hit and not df.empty:
            # cyq_perf: 10 calls/hour hard limit → throttle to ~7/hour to be safe
            time.sleep(520)
        if df.empty:
            print(f"    no chip data, skip")
            continue

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

        # Apply each combo's screen
        for combo in COMBOS:
            result = screen(
                df,
                min_win=combo["min_win"],
                max_win=combo["max_win"],
                max_today_pct=5.0,
                max_6m_ratio=None,        # skip 6m_high for speed
                six_month_high=None,
                max_price=combo["max_price"],
                exclude_kcb=combo["exclude_kcb"],
            )
            for _, row in result.iterrows():
                ts = row["ts_code"]
                c0 = row.get("close")
                c5  = close5.get(ts)
                c10 = close10.get(ts)
                if pd.notna(c0) and c0 > 0 and c5 and c10:
                    records[combo["key"]].append((ts, float(c0), float(c5), float(c10)))

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

        print(f"{key:<12}{n:>7}{win5d_pct:>7.1f}%{avg5d:>+8.2f}%{win10d_pct:>8.1f}%{avg10d:>+9.2f}%")
        output.append(dict(
            combo=key, tier=combo["tier"], mod=combo["mod"],
            n_picks=n,
            win5d=round(win5d_pct, 2),   avg_ret5d=round(avg5d, 3),
            win10d=round(win10d_pct, 2), avg_ret10d=round(avg10d, 3),
            med_ret5d=round(sorted(ret5d)[n // 2], 3),
            med_ret10d=round(sorted(ret10d)[n // 2], 3),
        ))

    footer = (f"注：6m_high 过滤已跳过（回测提速）；max_today_pct=5%\n"
              f"采样 {len(all_dates)} 个交易日  步长 {args.step}d  回测区间 {args.months}个月")
    print("=" * 80)
    print(f"\n{footer}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n结果已写入: {out_path}")

    # ── Push notification ─────────────────────────────────────────────────
    top_rows = [r for r in output if r.get("n_picks", 0) > 0]
    table_lines = [f"{'组合':<10}{'picks':>6}{'win5d':>7}{'ret5d':>8}{'win10d':>8}{'ret10d':>8}"]
    for r in top_rows:
        table_lines.append(
            f"{r['combo']:<10}{r['n_picks']:>6}"
            f"{r['win5d']:>6.1f}%{r['avg_ret5d']:>+7.2f}%"
            f"{r['win10d']:>7.1f}%{r['avg_ret10d']:>+7.2f}%"
        )
    table_lines.append(footer)
    push_body = "\n".join(table_lines)
    push_title = f"筹码策略回测完成 ({args.months}个月 {len(all_dates)}日)"
    _push_notify(push_title, push_body)


if __name__ == "__main__":
    main()
