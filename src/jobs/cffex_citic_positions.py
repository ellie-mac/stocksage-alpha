#!/usr/bin/env python3
"""中信期货代客在中金所 IF/IH/IC/IM 持仓追踪（机构对冲信号代理）。

每个交易日晚 19:00 跑（推送当日盘后数据 / 非交易日推上一交易日）：抓
ak.get_cffex_rank_table 取中信期货行，保存到 cffex_citic_latest.json +
累积到 cffex_citic_history.json（最近 180 天）。
带 --push 时同时推 wechat 文本 + Feishu 折线图（2×2 panel 看趋势）。
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any

import akshare as ak
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
SRC = Path(__file__).resolve().parent.parent
DATA = ROOT / 'data'
LATEST = DATA / 'cffex_citic_latest.json'
HISTORY = DATA / 'cffex_citic_history.json'
CHART = DATA / 'cffex_citic_chart.png'

sys.path.insert(0, str(SRC))
from common import push_wechat, write_json

PRODUCTS = ['IF', 'IH', 'IC', 'IM']
PRODUCT_LABEL = {
    "IF": "IF 沪深300",
    "IH": "IH 上证50",
    "IC": "IC 中证500",
    "IM": "IM 中证1000",
}
TARGET_NAMES = ('中信期货', '中信期货有限公司')


def _now() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip().replace(',', '')
    if not s or s in {'--', '-'}:
        return None
    m = re.search(r'-?\d+', s)
    return int(m.group()) if m else None


def _load_json(path: Path, default: Any):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def _save_json(path: Path, data: Any) -> None:
    write_json(path, data)


def _prev_trade_dates(n: int, end_date: str | None = None) -> list[str]:
    if end_date:
        d = datetime.strptime(end_date, '%Y%m%d').date()
    else:
        d = datetime.now().date()
    out = []
    while len(out) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            out.append(d.strftime('%Y%m%d'))
    out.reverse()
    return out


def _get_prev_trade_date_str() -> str:
    """默认追踪日期：晚 19:00 跑时若今天是交易日已收盘，用今天；否则用上一交易日。

    CFFEX 持仓数据通常 17:00 左右发布，19:00 跑时今日数据已就绪。
    若 ak.get_cffex_rank_table 拿不到今日数据（极少数情况），caller 会
    见到 error；可通过 --date YYYYMMDD 手动指定上一交易日重跑。
    """
    now = datetime.now()
    if now.weekday() < 5 and (now.hour, now.minute) >= (15, 30):
        return now.strftime('%Y%m%d')
    return _prev_trade_dates(1)[0]


def _pick_contract_keys(rank_map: dict[str, pd.DataFrame], prefix: str) -> list[str]:
    """按持仓量降序排（主力 = 持仓最大），并列时按月份升序兜底。

    之前按月份升序取 candidates[0]，会在合约**实际换月前几天**仍误选
    将到期 dying near-month（持仓已转移到下月主力）。改成按 OI 降序后
    自动跟随真正的主力合约。
    """
    keys = [k for k in rank_map.keys() if str(k).upper().startswith(prefix)]
    def _total_oi(k: str) -> int:
        df = rank_map.get(k)
        if df is None or getattr(df, 'empty', True):
            return 0
        col = 'short_open_interest' if 'short_open_interest' in df.columns else None
        if col is None:
            return 0
        try:
            return int(pd.to_numeric(df[col], errors='coerce').fillna(0).sum())
        except Exception:
            return 0
    def _month(x: str) -> int:
        m = re.search(r'^(IF|IH|IC|IM)(\d+)$', str(x).upper())
        return int(m.group(2)) if m else 99999
    # 主力优先（OI desc）；持仓为 0 或并列时按月份升序兜底
    return sorted(keys, key=lambda k: (-_total_oi(k), _month(k)))


def _find_target_row(df: pd.DataFrame) -> pd.Series | None:
    if df is None or df.empty:
        return None
    short_name_col = 'short_party_name' if 'short_party_name' in df.columns else None
    long_name_col = 'long_party_name' if 'long_party_name' in df.columns else None
    for _, row in df.iterrows():
        short_name = str(row.get(short_name_col) if short_name_col else '').strip()
        long_name = str(row.get(long_name_col) if long_name_col else '').strip()
        if any(name in short_name for name in TARGET_NAMES) or any(name in long_name for name in TARGET_NAMES):
            return row
    return None


def _build_item_from_rank_map(rank_map: dict[str, pd.DataFrame], prefix: str, trade_date: str) -> dict:
    candidates = _pick_contract_keys(rank_map, prefix)
    if not candidates:
        return {'symbol': prefix, 'trade_date': trade_date, 'error': f'no_contract_found_for_{prefix}'}
    contract = candidates[0]
    df = rank_map.get(contract)
    row = _find_target_row(df)
    if row is None:
        return {'symbol': prefix, 'contract': contract, 'trade_date': trade_date, 'error': f'target_not_found_in_{contract}'}
    short_qty = _safe_int(row.get('short_open_interest'))
    short_chg = _safe_int(row.get('short_open_interest_chg'))
    long_qty = _safe_int(row.get('long_open_interest'))
    long_chg = _safe_int(row.get('long_open_interest_chg'))
    return {
        'symbol': prefix,
        'contract': contract,
        'member': str(row.get('short_party_name') or row.get('long_party_name') or '中信期货').strip(),
        'trade_date': trade_date,
        'short_qty': short_qty,
        'short_change': short_chg,
        'long_qty': long_qty,
        'long_change': long_chg,
        'net_short': (short_qty - long_qty) if (short_qty is not None and long_qty is not None) else None,
        'source': 'akshare.get_cffex_rank_table',
    }


def _calc_percentile(series: list[int], value: int | None) -> float | None:
    if not series or value is None:
        return None
    less_equal = sum(1 for x in series if x <= value)
    return round(less_equal / len(series) * 100, 1)


def _calc_stats(values: list[int], current: int | None, current_change: int | None) -> dict:
    stats = {
        'hist_days': len(values),
        'avg_5': None,
        'avg_10': None,
        'percentile_20': None,
        'change_avg_abs_10': None,
        'change_zscore_10': None,
        'change_anomaly': 'NA',
    }
    if values:
        stats['avg_5'] = round(mean(values[-5:]), 1)
        stats['avg_10'] = round(mean(values[-10:]), 1)
        stats['percentile_20'] = _calc_percentile(values[-20:], current)
    if len(values) >= 2:
        diffs = [values[i] - values[i - 1] for i in range(1, len(values))]
        if diffs:
            avg_abs = mean(abs(x) for x in diffs[-10:])
            stats['change_avg_abs_10'] = round(avg_abs, 1)
            window = diffs[-10:]
            mu = mean(window)
            variance = mean((x - mu) ** 2 for x in window)
            std = math.sqrt(variance)
            if current_change is not None and std > 0:
                z = (current_change - mu) / std
                stats['change_zscore_10'] = round(z, 2)
                if abs(z) >= 2:
                    stats['change_anomaly'] = '异常'
                elif abs(z) >= 1:
                    stats['change_anomaly'] = '偏大'
                else:
                    stats['change_anomaly'] = '正常'
            elif current_change is not None:
                stats['change_anomaly'] = '正常'
    return stats


def _dedup_history(history: list[dict]) -> list[dict]:
    by_date = {}
    for report in history:
        if isinstance(report, dict) and report.get('trade_date'):
            by_date[str(report['trade_date'])] = report
    return [by_date[d] for d in sorted(by_date.keys())]


def _load_history_series(prefix: str, current_contract: str, current_date: str, current_short_qty: int | None) -> list[int]:
    """加载该 symbol 的历史 short_qty 序列。

    关键：**按 contract 精确过滤**，避免跨合约换月时拼接产生 5000→25000
    级别的假跳变污染均值/百分位/zscore。current_contract 是今日的主力合约，
    只保留 history 里同一 contract 的记录。
    """
    history = _load_json(HISTORY, [])
    series: list[tuple[str, int]] = []
    if isinstance(history, list):
        for report in history:
            if not isinstance(report, dict):
                continue
            d = str(report.get('trade_date') or '')
            for item in report.get('items', []):
                if not isinstance(item, dict):
                    continue
                if item.get('symbol') != prefix or item.get('short_qty') is None:
                    continue
                # 同 contract 才纳入统计；current_contract 为空则不过滤（兼容旧脚本写的无 contract 记录）
                if current_contract and item.get('contract') and item.get('contract') != current_contract:
                    continue
                series.append((d, int(item['short_qty'])))
    dedup = {}
    for d, v in series:
        if d:
            dedup[d] = v
    if current_date and current_short_qty is not None:
        dedup[current_date] = current_short_qty
    ordered = [dedup[d] for d in sorted(dedup.keys())]
    return ordered[-20:]


def _add_baseline(report: dict) -> dict:
    for item in report['items']:
        if 'error' in item:
            continue
        values = _load_history_series(
            item['symbol'],
            item.get('contract', ''),  # 按合约过滤，避免跨合约污染
            report['trade_date'],
            item.get('short_qty'),
        )
        stats = _calc_stats(values, item.get('short_qty'), item.get('short_change'))
        item.update(stats)
    return report


def build_report(trade_date: str | None = None) -> dict:
    trade_date = trade_date or _get_prev_trade_date_str()
    try:
        rank_map = ak.get_cffex_rank_table(date=trade_date)
    except Exception as e:
        items = [{'symbol': s, 'trade_date': trade_date, 'error': f'get_cffex_rank_table failed: {type(e).__name__}: {e}'} for s in PRODUCTS]
        return {'generated_at': _now(), 'trade_date': trade_date, 'ok_count': 0, 'fail_count': len(items), 'items': items}

    items = []
    prev = _load_json(LATEST, {})
    prev_map = {str(x.get('symbol')): x for x in prev.get('items', []) if isinstance(x, dict)}

    for prefix in PRODUCTS:
        item = _build_item_from_rank_map(rank_map, prefix, trade_date)
        prev_item = prev_map.get(prefix, {})
        item['vs_prev_file_short_change'] = None
        # 跨合约不算 vs_prev——合约换月时持仓量级跳变是结构性的不是信号
        prev_contract = prev_item.get('contract')
        cur_contract  = item.get('contract')
        if prev_contract and cur_contract and prev_contract != cur_contract:
            item['rollover_from'] = prev_contract
        elif item.get('short_qty') is not None and prev_item.get('short_qty') is not None:
            item['vs_prev_file_short_change'] = item['short_qty'] - prev_item['short_qty']
        items.append(item)

    ok_count = sum(1 for x in items if 'error' not in x)
    report = {'generated_at': _now(), 'trade_date': trade_date, 'ok_count': ok_count, 'fail_count': len(items) - ok_count, 'items': items}
    return _add_baseline(report)


def save_history(report: dict) -> None:
    history = _load_json(HISTORY, [])
    if not isinstance(history, list):
        history = []
    history.append(report)
    history = _dedup_history(history)[-180:]
    _save_json(HISTORY, history)


def backfill_history(days: int = 20, end_date: str | None = None) -> dict:
    dates = _prev_trade_dates(days, end_date=end_date)
    existing = _load_json(HISTORY, [])
    if not isinstance(existing, list):
        existing = []
    existing_dates = {str(x.get('trade_date')) for x in existing if isinstance(x, dict) and x.get('trade_date')}
    added = 0
    skipped = 0
    failed = 0
    for d in dates:
        if d in existing_dates:
            skipped += 1
            continue
        report = build_report(d)
        if report.get('ok_count', 0) > 0:
            existing.append(report)
            added += 1
        else:
            failed += 1
    existing = _dedup_history(existing)[-180:]
    _save_json(HISTORY, existing)
    return {
        'requested_days': days,
        'added': added,
        'skipped': skipped,
        'failed': failed,
        'history_size': len(existing),
        'start_date': dates[0] if dates else None,
        'end_date': dates[-1] if dates else None,
    }


def _overall_view(items: list[dict]) -> str:
    ok_items = [x for x in items if 'error' not in x and x.get('short_change') is not None]
    if not ok_items:
        return '无有效数据'
    total_change = sum(int(x['short_change']) for x in ok_items)
    im_ic_change = sum(int(x['short_change']) for x in ok_items if x['symbol'] in {'IM', 'IC'})
    if total_change >= 3000 and im_ic_change > 0:
        return f"空仓加速堆积（单日 +{total_change} 手），压力主要在中小盘"
    if total_change <= -3000:
        return f"空仓减压（单日 {total_change:+d} 手），对冲压力下降"
    return '整体中性波动，先看是否连续两三天同方向累积'


def format_body(report: dict) -> str:
    """按净空降序、显示空/多双边、加索引名。"""
    items = list(report.get('items', []))
    # ok 排前，按 net_short 降序；error 后排
    ok = [x for x in items if 'error' not in x]
    bad = [x for x in items if 'error' in x]
    ok.sort(key=lambda x: -(x.get('net_short') or 0))
    items = ok + bad

    lines = [
        f"[期指·中信] {report.get('trade_date', 'NA')} 机构对冲跟踪<br>",
        f"📌 {_overall_view(report['items'])}<br>",
    ]
    # 换月提示：本日如有 contract 切换，标注（同合约样本会从 0 重新积累）
    rolled = [(it['symbol'], it.get('rollover_from'), it.get('contract'))
              for it in report.get('items', []) if it.get('rollover_from')]
    if rolled:
        msg = " / ".join(f"{s} {old}→{new}" for s, old, new in rolled)
        lines.append(f"🔄 **换月**: {msg}（同合约样本重新积累，今日 P20/动量 暂不可信）<br>")
    lines.append("<br>")
    for it in items:
        if 'error' in it:
            lines.append(f"{it['symbol']}｜❌ {it['error']}<br>")
            continue
        label = PRODUCT_LABEL.get(it['symbol'], it['symbol'])
        sq = it.get('short_qty') or 0
        lq = it.get('long_qty') or 0
        sc = it.get('short_change')
        lc = it.get('long_change')
        sc_s = '?' if sc is None else f"{sc:+d}"
        lc_s = '?' if lc is None else f"{lc:+d}"
        ratio = (sq / lq) if lq else 0
        ratio_s = f"{ratio:.2f}" if ratio else 'NA'
        p20 = it.get('percentile_20')
        p20_s = f"{p20:.0f}%" if p20 is not None else 'NA'
        anomaly = it.get('change_anomaly', 'NA')
        # A股惯例：🔴=多/涨，🟢=空/跌。净空时打绿，净多时打红。
        net = (it.get('net_short') or 0)
        side_emoji = '🟢' if net > 0 else ('🔴' if net < 0 else '⚪')
        lines.append(
            f"{side_emoji} **{label}**｜空 {sq}({sc_s}) / 多 {lq}({lc_s})｜空多比 {ratio_s}｜P20 {p20_s}｜动量 {anomaly}<br>"
        )
    lines.append("<br>")
    lines.append("📖 空多比>1.3 偏空 / 1.0-1.1 接近平手｜P20=20日分位｜动量基于10日波动 z-score<br>")
    sample = items[0].get('hist_days') if items and 'error' not in items[0] else 0
    if sample and sample < 30:
        lines.append(f"（样本 {sample} 天，待累积至 30 天指标更稳）<br>")
    return ''.join(lines)


def _render_chart(report: dict) -> Path | None:
    """2×2 panel 折线图：IM/IC/IF/IH，每格画空单+多单+净空填充。"""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from matplotlib.dates import DateFormatter
    except Exception as e:
        print(f"[cffex_citic] matplotlib 不可用，跳过图: {e}", flush=True)
        return None

    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    history = _load_json(HISTORY, [])
    if not isinstance(history, list):
        history = []

    # build {symbol: [(date, short_qty, long_qty), ...]} sorted ascending
    series: dict[str, list[tuple[str, int, int]]] = {s: [] for s in PRODUCTS}
    for rep in history:
        if not isinstance(rep, dict):
            continue
        d = str(rep.get('trade_date') or '')
        if not d:
            continue
        for it in rep.get('items', []):
            sym = it.get('symbol')
            sq = it.get('short_qty')
            lq = it.get('long_qty')
            if sym in series and sq is not None and lq is not None:
                series[sym].append((d, int(sq), int(lq)))

    # ensure today's report is included (in case save_history hasn't run yet)
    today = str(report.get('trade_date') or '')
    today_map = {it['symbol']: it for it in report.get('items', []) if 'error' not in it}
    for sym, it in today_map.items():
        sq = it.get('short_qty')
        lq = it.get('long_qty')
        if sym in series and sq is not None and lq is not None and today:
            existing = {d for d, _, _ in series[sym]}
            if today not in existing:
                series[sym].append((today, int(sq), int(lq)))

    for sym in series:
        # dedup by date keeping latest, then sort
        by_date: dict[str, tuple[int, int]] = {}
        for d, s, l in series[sym]:
            by_date[d] = (s, l)
        series[sym] = sorted([(d, s, l) for d, (s, l) in by_date.items()])

    if not any(series[s] for s in PRODUCTS):
        print("[cffex_citic] 历史为空，跳过图", flush=True)
        return None

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    panel_order = ['IM', 'IC', 'IF', 'IH']  # 中小盘 left/top（更受关注的放前面）
    for ax, sym in zip(axes.flat, panel_order):
        data = series[sym]
        if not data:
            ax.set_title(f"{PRODUCT_LABEL[sym]}（无数据）")
            ax.set_xticks([])
            ax.set_yticks([])
            continue
        dates = [datetime.strptime(d, "%Y%m%d") for d, _, _ in data]
        shorts = [s for _, s, _ in data]
        longs = [l for _, _, l in data]
        # A股惯例：红多绿空。多单红、空单绿；净多区浅红、净空区浅绿。
        ax.fill_between(dates, longs, shorts,
                        where=[s >= l for s, l in zip(shorts, longs)],
                        color='#a5d6a7', alpha=0.35, label='净空区')
        ax.fill_between(dates, longs, shorts,
                        where=[s < l for s, l in zip(shorts, longs)],
                        color='#ffcdd2', alpha=0.35, label='净多区')
        ax.plot(dates, longs, label='多单', color='#d62728', linewidth=2, marker='o', markersize=3)
        ax.plot(dates, shorts, label='空单', color='#2ca02c', linewidth=2, marker='o', markersize=3)
        # 今日标注
        if shorts and longs:
            net = shorts[-1] - longs[-1]
            ax.annotate(f"净空 {net:+d}",
                        xy=(dates[-1], shorts[-1]),
                        xytext=(5, 5), textcoords='offset points',
                        fontsize=9, color='#444')
        ax.set_title(PRODUCT_LABEL[sym], fontsize=11, fontweight='bold')
        ax.set_ylabel("持仓 (手)")
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left', fontsize=8, ncol=2)
        ax.xaxis.set_major_formatter(DateFormatter("%m-%d"))
        for tick in ax.get_xticklabels():
            tick.set_rotation(45)
            tick.set_ha('right')

    title_date = f"{today[:4]}-{today[4:6]}-{today[6:]}" if len(today) == 8 else today
    fig.suptitle(f"中信期货 持仓趋势  {title_date}", fontsize=13, fontweight='bold')
    plt.tight_layout(rect=(0, 0, 1, 0.96))

    CHART.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(CHART, dpi=120, bbox_inches='tight')
    plt.close(fig)
    print(f"[cffex_citic] 折线图已生成 → {CHART.name}", flush=True)
    return CHART


def main() -> int:
    parser = argparse.ArgumentParser(description='抓取中金所中信期货股指空单持仓')
    parser.add_argument('--push', action='store_true', help='推送微信 + 飞书折线图')
    parser.add_argument('--dry-run', action='store_true', help='仅打印，不落盘')
    parser.add_argument('--date', type=str, default=None, help='交易日 YYYYMMDD，默认上一交易日')
    parser.add_argument('--backfill', type=int, default=0, help='回补最近N个交易日历史样本')
    parser.add_argument('--no-chart', action='store_true', help='不生成/推送折线图')
    args = parser.parse_args()

    DATA.mkdir(exist_ok=True)

    if args.backfill > 0:
        result = backfill_history(days=args.backfill, end_date=args.date)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    report = build_report(args.date)
    body = format_body(report)

    if args.dry_run:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        print(body)
        if not args.no_chart:
            chart_path = _render_chart(report)
            if chart_path:
                print(f"[dry-run] 图已存到 {chart_path}")
        return 0 if report['ok_count'] > 0 else 1

    _save_json(LATEST, report)
    save_history(report)

    if args.push:
        # 仅飞书：文字解读 + 折线图（不再推微信）
        # 抽关键数字给解读用
        items = [x for x in report.get('items', []) if 'error' not in x]
        items.sort(key=lambda x: -(x.get('net_short') or 0))
        ok_items = [x for x in items if x.get('short_change') is not None]
        total_chg = sum(int(x['short_change']) for x in ok_items)
        im_ic_chg = sum(int(x['short_change']) for x in ok_items if x['symbol'] in {'IM', 'IC'})
        long_total = sum(int(x.get('long_change') or 0) for x in ok_items)

        def _line(it):
            sq, lq = it.get('short_qty', 0), it.get('long_qty', 0)
            sc, lc = it.get('short_change', 0), it.get('long_change', 0)
            ratio = (sq / lq) if lq else 0
            return (f"{PRODUCT_LABEL.get(it['symbol'], it['symbol'])}：空 {sq}({sc:+d}) / 多 {lq}({lc:+d})"
                    f"，净空 {it.get('net_short', 0):+d}，空多比 {ratio:.2f}")

        # 自动结论：tone + 衍生洞察
        if total_chg >= 3000 and im_ic_chg > 0:
            tone = "🔻 空仓加速堆积，中小盘是主要压力"
        elif total_chg <= -3000:
            tone = "🟢 空仓减压，对冲压力下降"
        elif abs(total_chg) < 1500 and long_total > 2000:
            tone = "🟡 多空两边都在加 = long-short 策略扩张，不是单向看空"
        else:
            tone = "⚪ 整体中性，看后续连续累积"

        # 衍生洞察（基于数据自动判断）
        insights = []
        # IH 多增 > 空增？(罕见，蓝筹偏正面信号)
        ih = next((it for it in items if it["symbol"] == "IH"), None)
        if ih and (ih.get("long_change") or 0) > (ih.get("short_change") or 0):
            insights.append(f"IH (上证50) 多增 {ih.get('long_change', 0):+d} > 空增 {ih.get('short_change', 0):+d} → 机构对大盘蓝筹偏正面")
        # 哪个合约空多比最大（结构性偏空最强）
        ratios = [(it["symbol"], (it.get("short_qty") or 0) / max(it.get("long_qty") or 1, 1)) for it in items]
        ratios.sort(key=lambda x: -x[1])
        if ratios:
            sym0, r0 = ratios[0]
            if r0 >= 1.4:
                insights.append(f"{PRODUCT_LABEL.get(sym0, sym0)} 空多比 {r0:.2f} 最高，结构性偏空（IM 长期高位通常是雪球对冲，不一定方向性）")
        # 单日 z-score 是否触发"异常"
        anom = [it for it in items if it.get("change_anomaly") == "异常"]
        if anom:
            insights.append(f"⚠️ {len(anom)} 个合约今日变动 z-score ≥ 2（{'/'.join(it['symbol'] for it in anom)}），关注是否连续累积")

        insights_block = ""
        if insights:
            insights_block = "\n💡 衍生洞察：\n  • " + "\n  • ".join(insights) + "\n"

        explanation = (
            f"[期指·中信] {report.get('trade_date', 'NA')} 机构对冲跟踪\n"
            "==========\n"
            f"📌 结论：{tone}\n"
            f"  • 4 路空单合计变化 {total_chg:+d} 手 / 多单 {long_total:+d} 手\n"
            f"  • IM+IC 空单变化 {im_ic_chg:+d} 手\n"
            f"{insights_block}"
            "==========\n"
            "📊 各合约（按净空降序）：\n  • " + "\n  • ".join(_line(it) for it in items)
        )

        try:
            sys.path.insert(0, str(SRC))
            from notify.notify import push_feishu_content, push_feishu_image
            push_feishu_content(explanation)
            print("[cffex_citic] 飞书文字解读推送成功", flush=True)
            if not args.no_chart:
                chart_path = _render_chart(report)
                if chart_path:
                    push_feishu_image(chart_path)
                    print("[cffex_citic] 飞书图推送成功", flush=True)
        except Exception as e:
            print(f"[cffex_citic] 飞书推送失败: {e}", flush=True)

    print(f"[cffex_citic] date={report.get('trade_date')} ok={report['ok_count']} fail={report['fail_count']}")
    return 0 if report['ok_count'] > 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
