#!/usr/bin/env python3
"""
一次性预取过去 N 个月所有交易日的筹码数据（cyq_perf + daily），写入本地缓存。

预取完成后，chip_backtest.py / chip_strategy.py 运行时均可直接命中缓存，无需等待 API。

用法：
    python -X utf8 scripts/prefetch_chip.py              # 默认 6 个月
    python -X utf8 scripts/prefetch_chip.py --months 12  # 回溯 12 个月
    python -X utf8 scripts/prefetch_chip.py --dry-run    # 仅打印待拉取日期，不实际调用

限流说明：
    cyq_perf 每小时 10 次（含失败请求）。
    本脚本每次成功拉取后等待 520 秒（约 7 次/小时），已在限制内。
    已缓存的日期直接跳过，无需等待。
    中途中断可重新运行，会自动续拍（跳过已缓存日期）。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

import cache as _cache
import fetcher as _fetcher
from chip_strategy import fetch_chip_data, _chip_cache_key, _get_pro
from common import send_wechat, configure_pushplus

_PREFETCH_TTL = 30 * 24 * 3600   # 30 天：历史数据不变，长期有效
_THROTTLE_SEC = 520               # 每次新 API 调用后等待，控制在 ~7 次/小时


def _get_trade_dates(months: int) -> list[str]:
    """返回过去 months 个月（含今天）所有交易日，YYYYMMDD 格式，升序。"""
    print("[calendar] 获取交易日历...")
    raw = _fetcher.get_trade_calendar()
    all_dates = sorted(set(d.replace("-", "") for d in raw))

    today = date.today().strftime("%Y%m%d")
    yr    = date.today().year
    mo    = date.today().month - months
    while mo <= 0:
        mo += 12
        yr -= 1
    cutoff = f"{yr}{mo:02d}01"

    return [d for d in all_dates if cutoff <= d <= today]


def _is_cached(trade_date: str) -> bool:
    """用 30 天 TTL 检查缓存是否存在（历史数据不变，宽松 TTL）。"""
    return _cache.get(_chip_cache_key(trade_date), _PREFETCH_TTL) is not None


def _refresh_ttl(trade_date: str) -> None:
    """把已有的缓存条目时间戳刷新到当前，延长 23h 窗口。"""
    data = _cache.get(_chip_cache_key(trade_date), _PREFETCH_TTL)
    if data is not None:
        _cache.set(_chip_cache_key(trade_date), data)


def _push_notify(title: str, body: str) -> None:
    try:
        cfg = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
        send_wechat(title, body, sendkey)
    except Exception as e:
        print(f"[notify] 推送失败: {e}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months",  type=int, default=6,  help="回溯月数，默认 6")
    parser.add_argument("--dry-run", action="store_true",  help="仅列出待拉取日期，不调用 API")
    args = parser.parse_args()

    dates = _get_trade_dates(args.months)
    total = len(dates)
    print(f"[prefetch] 共 {total} 个交易日: {dates[0]} → {dates[-1]}")

    cached_dates  = [d for d in dates if _is_cached(d)]
    missing_dates = [d for d in dates if not _is_cached(d)]

    print(f"[prefetch] 已缓存: {len(cached_dates)}  待拉取: {len(missing_dates)}")

    if not missing_dates:
        print("[prefetch] 全部已缓存，刷新时间戳...")
        for d in cached_dates:
            _refresh_ttl(d)
        print("[prefetch] 完成，无需 API 调用。")
        return

    if args.dry_run:
        print("[dry-run] 待拉取日期:")
        for d in missing_dates:
            print(f"  {d}")
        return

    pro = _get_pro()

    # 刷新已有缓存的时间戳（延长 30 天有效期）
    for d in cached_dates:
        _refresh_ttl(d)
    if cached_dates:
        print(f"[prefetch] 已刷新 {len(cached_dates)} 个缓存条目的时间戳")

    fetched = 0
    failed: list[str] = []

    for i, trade_date in enumerate(missing_dates, 1):
        eta_calls = len(missing_dates) - i
        eta_hours = eta_calls * _THROTTLE_SEC / 3600
        print(f"\n[{i}/{len(missing_dates)}] 拉取 {trade_date}  "
              f"(剩余约 {eta_hours:.1f}h)", flush=True)
        try:
            df = fetch_chip_data(trade_date, pro)
            if df.empty:
                print(f"  [skip] {trade_date} 无数据（非交易日或 Tushare 缺档）")
                failed.append(trade_date)
            else:
                # 刷新时间戳，使 30 天 TTL 从现在起算
                _refresh_ttl(trade_date)
                fetched += 1
                print(f"  [OK] {trade_date} 已缓存 {len(df)} 条")
                if i < len(missing_dates):
                    resume_at = datetime.fromtimestamp(time.time() + _THROTTLE_SEC)
                    print(f"  [throttle] 等待至 {resume_at:%H:%M:%S} ...", flush=True)
                    time.sleep(_THROTTLE_SEC)
        except Exception as e:
            msg = str(e)
            if "每小时" in msg or "每分钟" in msg or "最多访问" in msg:
                # 限流：等到下一个整点 + 5 分钟再继续
                now       = datetime.now()
                next_hour = now.replace(minute=0, second=0, microsecond=0)
                next_hour = next_hour.replace(hour=now.hour + 1) if now.hour < 23 \
                            else now.replace(day=now.day + 1, hour=0, minute=0, second=0)
                wait      = int((next_hour - now).total_seconds()) + 300
                resume_at = datetime.fromtimestamp(now.timestamp() + wait)
                print(f"  [rate-limit] 触发限流，等待至 {resume_at:%H:%M:%S} 再继续...",
                      flush=True)
                time.sleep(wait)
                # 重试当前日期（不推进 i）
                try:
                    df = fetch_chip_data(trade_date, pro)
                    if not df.empty:
                        _refresh_ttl(trade_date)
                        fetched += 1
                        print(f"  [OK] {trade_date} 重试成功，已缓存 {len(df)} 条")
                    else:
                        failed.append(trade_date)
                except Exception as e2:
                    print(f"  [error] {trade_date} 重试失败: {e2}")
                    failed.append(trade_date)
            else:
                print(f"  [error] {trade_date}: {e}")
                failed.append(trade_date)

    summary = (
        f"预取完成\n"
        f"回测区间: {dates[0]} → {dates[-1]}（{total} 个交易日）\n"
        f"本次新拉: {fetched} 个\n"
        f"原有缓存: {len(cached_dates)} 个\n"
        f"失败/跳过: {len(failed)} 个"
        + (f"\n失败日期: {', '.join(failed)}" if failed else "")
    )
    print("\n" + "=" * 60)
    print(summary)
    print("=" * 60)

    _push_notify("筹码数据预取完成", summary)


if __name__ == "__main__":
    main()
