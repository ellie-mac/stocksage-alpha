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
from strategies.escalator_scan import _classify as classify_base, _ma_bullish
from strategies._quality import load_name_industry_map, load_universe, compute_metrics, passes_quality, is_blacklisted

OUT = ROOT / 'data' / 'escalator_ab_backtest.json'
HORIZONS = [1, 3, 5, 10]
HOLD_DAYS = 10
PRICE_DAYS = 230
MIN_PRE = 25
MIN_POST = HOLD_DAYS
SAMPLE_STEP = 10
MAX_WORKERS = 8
UNIVERSE_LIMIT = int(os.getenv('ESC_BT_LIMIT', '400'))
RUNUP_LEVELS = [8.0, 10.0, 12.0]


def pick_records(df: pd.DataFrame, code6: str, name: str, industry: str, max_5d_runup: float | None):
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
        if not _ma_bullish(sub_c):
            continue
        cls = classify_base(sub_c, sub_h, sub_l)
        if not cls:
            continue
        hist_df = df.iloc[max(0, i - 34): i + 1].copy()
        try:
            metrics = compute_metrics(hist_df)
        except Exception:
            continue
        if not passes_quality(metrics):
            continue

        if max_5d_runup is not None:
            if i < 5:
                continue
            runup_5d = (closes[i] / closes[i - 5] - 1.0) * 100
            if runup_5d > max_5d_runup:
                continue

        entry = float(closes[i])
        future = closes[i + 1: i + 1 + HOLD_DAYS]
        if len(future) < HOLD_DAYS:
            continue
        rec = {'date': str(dates[i]), 'code': code6, 'name': name, 'industry': industry, 'tier': cls['tier']}
        for h in HORIZONS:
            rec[f'ret_{h}d'] = float((closes[i + h] / entry - 1.0) * 100)
        path = future / entry - 1.0
        rec['max_up_10d'] = float(np.max(path) * 100)
        rec['max_dd_10d'] = float(np.min(path) * 100)
        out.append(rec)
    return out


def summary(records):
    if not records:
        return {'samples': 0}
    df = pd.DataFrame(records)
    res = {'samples': int(len(df))}
    for h in HORIZONS:
        s = df[f'ret_{h}d']
        res[f'avg_{h}d'] = round(float(s.mean()), 3)
        res[f'win_{h}d'] = round(float((s > 0).mean() * 100), 2)
    res['avg_max_up_10d'] = round(float(df['max_up_10d'].mean()), 3)
    res['avg_max_dd_10d'] = round(float(df['max_dd_10d'].mean()), 3)
    return res


def build_delta(base: dict, other: dict):
    out = {}
    for k in ['avg_1d','avg_3d','avg_5d','avg_10d','win_1d','win_3d','win_5d','win_10d','avg_max_up_10d','avg_max_dd_10d']:
        if k in base and k in other:
            out[k] = round(other[k] - base[k], 3)
    return out


def run():
    name_map, ind_map = load_name_industry_map()
    universe = sorted(load_universe())[:UNIVERSE_LIMIT]
    buckets = {'A_base': []}
    for lv in RUNUP_LEVELS:
        buckets[f'runup_le_{lv:g}'] = []

    def worker(code: str):
        code6 = code[-6:]
        industry = ind_map.get(code6, '')
        if is_blacklisted(industry):
            return {k: [] for k in buckets.keys()}
        try:
            df = _fetcher.get_price_history(code, days=PRICE_DAYS)
        except Exception:
            return {k: [] for k in buckets.keys()}
        if df is None or len(df) < MIN_PRE + MIN_POST + 1:
            return {k: [] for k in buckets.keys()}
        name = name_map.get(code6, code6)
        res = {'A_base': pick_records(df, code6, name, industry, None)}
        for lv in RUNUP_LEVELS:
            res[f'runup_le_{lv:g}'] = pick_records(df, code6, name, industry, lv)
        return res

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(worker, code): code for code in universe}
        for fut in tqdm(as_completed(futs), total=len(futs), desc='escalator_runup_grid'):
            res = fut.result()
            for k, v in res.items():
                buckets[k].extend(v)

    base = summary(buckets['A_base'])
    result = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'config': {
            'sample_step': SAMPLE_STEP,
            'universe_limit': UNIVERSE_LIMIT,
            'runup_levels': RUNUP_LEVELS,
        },
        'A_base': base,
    }
    for lv in RUNUP_LEVELS:
        key = f'runup_le_{lv:g}'
        s = summary(buckets[key])
        result[key] = s
        result[f'delta_{key}_vs_A'] = build_delta(base, s)

    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    run()
