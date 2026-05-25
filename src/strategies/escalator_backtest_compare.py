#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fetcher as _fetcher
from strategies.escalator_scan import _classify as classify_v1, _ma_bullish as ma_stack_ok_v1
from strategies.escalator_scan_v2 import _classify as classify_v2, _ma_stack_ok as ma_stack_ok_v2
from strategies._quality import load_name_industry_map, load_universe, compute_metrics, passes_quality, is_blacklisted

OUT = ROOT / 'data' / 'escalator_compare_backtest.json'
HORIZONS = [1, 3, 5, 10]
HOLD_DAYS = max(HORIZONS)
LOOKBACK_DAYS = 180
PRICE_DAYS = LOOKBACK_DAYS + 50
MIN_PRE = 35
MIN_POST = HOLD_DAYS
SAMPLE_STEP = 10
MAX_WORKERS = 8
UNIVERSE_LIMIT = int(os.getenv('ESC_BT_LIMIT', '600'))


def _pick_one(df: pd.DataFrame, classify_fn, ma_fn, code6: str, name: str, industry: str):
    out = []
    if df is None or len(df) < MIN_PRE + MIN_POST + 1:
        return out
    closes = df['close'].astype(float).to_numpy()
    highs = df['high'].astype(float).to_numpy()
    lows = df['low'].astype(float).to_numpy()
    dates = pd.to_datetime(df['date']).dt.strftime('%Y%m%d').to_numpy()

    for i in range(MIN_PRE - 1, len(df) - MIN_POST, SAMPLE_STEP):
        sub_c = closes[:i + 1]
        sub_h = highs[:i + 1]
        sub_l = lows[:i + 1]
        if not ma_fn(sub_c):
            continue
        cls = classify_fn(sub_c, sub_h, sub_l)
        if not cls:
            continue
        hist_df = df.iloc[max(0, i - 34): i + 1].copy()
        try:
            metrics = compute_metrics(hist_df)
        except Exception:
            continue
        if not passes_quality(metrics):
            continue
        entry = float(closes[i])
        future = closes[i + 1: i + 1 + HOLD_DAYS]
        if len(future) < HOLD_DAYS:
            continue
        rec = {
            'date': str(dates[i]),
            'code': code6,
            'name': name,
            'industry': industry,
            'tier': cls['tier'],
        }
        for h in HORIZONS:
            rec[f'ret_{h}d'] = float((closes[i + h] / entry - 1.0) * 100)
        path = future / entry - 1.0
        rec['max_up_10d'] = float(np.max(path) * 100)
        rec['max_dd_10d'] = float(np.min(path) * 100)
        out.append(rec)
    return out


def _summary(records):
    if not records:
        return {'samples': 0}
    df = pd.DataFrame(records)
    res = {'samples': int(len(df))}
    for h in HORIZONS:
        s = df[f'ret_{h}d']
        res[f'avg_{h}d'] = round(float(s.mean()), 3)
        res[f'median_{h}d'] = round(float(s.median()), 3)
        res[f'win_{h}d'] = round(float((s > 0).mean() * 100), 2)
    res['avg_max_up_10d'] = round(float(df['max_up_10d'].mean()), 3)
    res['avg_max_dd_10d'] = round(float(df['max_dd_10d'].mean()), 3)
    by_tier = {}
    for tier, g in df.groupby('tier'):
        by_tier[tier] = {
            'samples': int(len(g)),
            'avg_5d': round(float(g['ret_5d'].mean()), 3),
            'win_5d': round(float((g['ret_5d'] > 0).mean() * 100), 2),
            'avg_10d': round(float(g['ret_10d'].mean()), 3),
            'win_10d': round(float((g['ret_10d'] > 0).mean() * 100), 2),
            'avg_max_dd_10d': round(float(g['max_dd_10d'].mean()), 3),
        }
    res['by_tier'] = by_tier
    return res


def run():
    name_map, ind_map = load_name_industry_map()
    universe = sorted(load_universe())[:UNIVERSE_LIMIT]
    rows_v1 = []
    rows_v2 = []

    def worker(code: str):
        code6 = code[-6:]
        industry = ind_map.get(code6, '')
        if is_blacklisted(industry):
            return [], []
        try:
            df = _fetcher.get_price_history(code, days=PRICE_DAYS)
        except Exception:
            return [], []
        if df is None or len(df) < MIN_PRE + MIN_POST + 1:
            return [], []
        name = name_map.get(code6, code6)
        return (
            _pick_one(df, classify_v1, ma_stack_ok_v1, code6, name, industry),
            _pick_one(df, classify_v2, ma_stack_ok_v2, code6, name, industry),
        )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(worker, code): code for code in universe}
        for fut in tqdm(as_completed(futs), total=len(futs), desc='escalator_bt_fast'):
            a, b = fut.result()
            rows_v1.extend(a)
            rows_v2.extend(b)

    result = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'config': {
            'horizons': HORIZONS,
            'lookback_days': LOOKBACK_DAYS,
            'price_days': PRICE_DAYS,
            'sample_step': SAMPLE_STEP,
            'universe_limit': UNIVERSE_LIMIT,
            'max_workers': MAX_WORKERS,
        },
        'v1': _summary(rows_v1),
        'v2': _summary(rows_v2),
        'delta': {},
    }
    if result['v1'].get('samples') and result['v2'].get('samples'):
        for k in ['avg_1d','avg_3d','avg_5d','avg_10d','win_1d','win_3d','win_5d','win_10d','avg_max_up_10d','avg_max_dd_10d']:
            if k in result['v1'] and k in result['v2']:
                result['delta'][k] = round(result['v2'][k] - result['v1'][k], 3)
    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    return result


if __name__ == '__main__':
    res = run()
    print(json.dumps(res, ensure_ascii=False, indent=2))
