#!/usr/bin/env python3
"""
金叉策略扫描 — 多维技术指标金叉共振筛选（MA60 趋势门控）

7项金叉信号：
  1. MACD金叉    — DIF向上穿越DEA（EMA导数，趋势动量）
  2. KDJ金叉     — K线向上穿越D线（随机指标，超卖反弹）
  3. RSI金叉     — RSI(6)向上穿越RSI(12)（动量恢复）
  4. MA10/20金叉 — 10日均线穿越20日均线（中期趋势确认）
  5. 量能金叉    — 量MA5穿越量MA10且价格上涨（成交量驱动）
  6. OBV金叉     — OBV的MA5穿越MA10（资金累积/分配）
  7. 布林中轨金叉 — 价格向上穿越MA20（突破均线确认）

前置条件：price > MA60 OR MA60 slope > 0（震荡市全部过滤）

档位（共振数量）：G0=7 · G1=6 · G2=5

用法：
    python -X utf8 scripts/golden_cross_scan.py [--push] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).parent))

UNIVERSE_PATH = ROOT / "data" / "universe_main.json"
OUT_LATEST    = ROOT / "data" / "golden_cross_latest.json"

_TIER_MIN = {"G0": 7, "G1": 6, "G2": 5}
_MIN_BARS = 45   # EMA26 + EMA9 预热需要的最少 K 线数


# ── 技术指标 ──────────────────────────────────────────────────────────────────

def _ema(arr: np.ndarray, period: int) -> np.ndarray:
    alpha = 2.0 / (period + 1)
    out = np.empty(len(arr), dtype=float)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = alpha * arr[i] + (1.0 - alpha) * out[i - 1]
    return out


def _sma(arr: np.ndarray, period: int) -> np.ndarray:
    return pd.Series(arr).rolling(period, min_periods=period).mean().values


def _macd(closes: np.ndarray, fast=12, slow=26, sig=9):
    """Return (DIF, DEA)."""
    dif = _ema(closes, fast) - _ema(closes, slow)
    dea = _ema(dif, sig)
    return dif, dea



def _kdj(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, n=9):
    """Return (K, D)."""
    total = len(closes)
    K = np.full(total, np.nan)
    D = np.full(total, np.nan)
    kp = dp = 50.0
    for i in range(n - 1, total):
        hi = np.max(highs[i - n + 1 : i + 1])
        lo = np.min(lows[i - n + 1 : i + 1])
        rsv = (closes[i] - lo) / (hi - lo) * 100 if hi != lo else 50.0
        kp = 2 / 3 * kp + 1 / 3 * rsv
        dp = 2 / 3 * dp + 1 / 3 * kp
        K[i] = kp
        D[i] = dp
    return K, D


def _rsi(closes: np.ndarray, period: int) -> np.ndarray:
    n = len(closes)
    rsi = np.full(n, np.nan)
    if n <= period:
        return rsi
    deltas = np.diff(closes)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    ag = np.mean(gains[:period])
    al = np.mean(losses[:period])
    for i in range(period, n - 1):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        if al > 0:
            rsi[i + 1] = 100.0 - 100.0 / (1.0 + ag / al)
        else:
            rsi[i + 1] = 100.0
    return rsi


def _obv(closes: np.ndarray, volumes: np.ndarray) -> np.ndarray:
    obv = np.empty(len(closes), dtype=float)
    obv[0] = volumes[0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            obv[i] = obv[i - 1] + volumes[i]
        elif closes[i] < closes[i - 1]:
            obv[i] = obv[i - 1] - volumes[i]
        else:
            obv[i] = obv[i - 1]
    return obv



def _cross_days_ago(a: np.ndarray, b: np.ndarray, lookback: int = 5) -> Optional[int]:
    """Return bars ago of the most recent upward cross (0 = today's bar), or None."""
    n = len(a)
    for lag in range(lookback):
        i = n - 1 - lag
        if i < 1:
            continue
        ap, bp = a[i - 1], b[i - 1]
        ai, bi = a[i],     b[i]
        if any(not np.isfinite(x) for x in (ap, bp, ai, bi)):
            continue
        if ap <= bp and ai > bi:
            return lag
    return None


# ── 单股评分 ─────────────────────────────────────────────────────────────────

def _score_stock(df: pd.DataFrame) -> tuple[int, list[str], float]:
    if len(df) < _MIN_BARS:
        return 0, [], 0.0

    c  = df["close"].values.astype(float)
    h  = df["high"].values.astype(float)
    lo = df["low"].values.astype(float)
    v  = df["volume"].values.astype(float)

    if not np.isfinite(c[-1]) or c[-1] <= 0:
        return 0, [], 0.0

    # MA60 gate: reject only when BOTH conditions fail (price below MA60 AND MA60 declining)
    ma60 = _sma(c, 60)
    if not (np.isfinite(ma60[-1]) and np.isfinite(ma60[-6]) and
            (c[-1] > ma60[-1] or ma60[-1] > ma60[-6])):
        return 0, [], 0.0

    fired: list[str] = []
    recency: list[int] = []

    def _try(a: np.ndarray, b: np.ndarray, label: str, lookback: int = 5) -> None:
        d = _cross_days_ago(a, b, lookback)
        if d is not None:
            fired.append(label)
            recency.append(d)

    # 1. MACD金叉 —— DIF 穿越 DEA（EMA导数，趋势动量）
    dif, dea = _macd(c)
    _try(dif, dea, "MACD金叉")

    # 2. KDJ金叉 —— K 穿越 D（随机指标，超卖反弹）
    K, D = _kdj(h, lo, c)
    _try(K, D, "KDJ金叉")

    # 3. RSI金叉 —— RSI(6) 穿越 RSI(12)（动量恢复）
    rsi6  = _rsi(c, 6)
    rsi12 = _rsi(c, 12)
    _try(rsi6, rsi12, "RSI金叉")

    # 4. MA10/20金叉 —— 中期趋势确认
    ma10 = _sma(c, 10)
    ma20 = _sma(c, 20)
    _try(ma10, ma20, "MA10/20金叉")

    # 5. 量能金叉 —— VolMA5 穿越 VolMA10，且近3日价格上涨
    vma5  = _sma(v, 5)
    vma10 = _sma(v, 10)
    price_up = len(c) >= 4 and np.isfinite(c[-1]) and np.isfinite(c[-4]) and c[-1] > c[-4]
    if price_up:
        _try(vma5, vma10, "量能金叉")

    # 6. OBV金叉 —— OBV MA(5) 穿越 MA(10)（资金累积/分配）
    obv_arr  = _obv(c, v)
    obv_ma5  = _sma(obv_arr, 5)
    obv_ma10 = _sma(obv_arr, 10)
    _try(obv_ma5, obv_ma10, "OBV金叉")

    # 7. 布林中轨金叉 —— 价格向上穿越 MA20（突破均线确认）
    _try(c, ma20, "布林中轨金叉")

    freshness = float(np.mean([max(0.0, 1.0 - d * 0.35) for d in recency])) if recency else 0.0
    return len(fired), fired, freshness


# ── 主扫描 ────────────────────────────────────────────────────────────────────

def _load_universe() -> list[str]:
    raw = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    return raw if isinstance(raw, list) else list(raw.keys())


def _build_name_maps() -> tuple[dict[str, str], dict[str, str]]:
    """直接读 stock_names.json 缓存，构建 6位代码 → (name, industry) 映射。"""
    names_file = ROOT / "scripts" / "cache" / "stock_names.json"
    names: dict[str, str] = {}
    inds:  dict[str, str] = {}
    try:
        raw = json.loads(names_file.read_text(encoding="utf-8"))
        for ts_code, info in raw.items():
            code6 = ts_code.split(".")[0]
            names[code6] = info.get("name", code6) if isinstance(info, dict) else str(info)
            inds[code6]  = info.get("industry", "")  if isinstance(info, dict) else ""
        print(f"[golden_cross] 名称缓存 {len(names)} 条", flush=True)
    except Exception as e:
        print(f"[golden_cross] 名称加载失败: {e}", flush=True)
    return names, inds


def run_scan(push: bool = False, dry_run: bool = False, as_of_date: str = "") -> dict:
    import fetcher as _fetcher

    if not as_of_date:  # only gate on live runs, not backfill
        from prefetch import wait_for_fresh_prices
        wait_for_fresh_prices()

    universe  = _load_universe()
    name_map, ind_map = _build_name_maps()
    date = as_of_date or datetime.now().strftime("%Y%m%d")

    def _fetch_and_score(code: str) -> Optional[dict]:
        try:
            df = _fetcher.get_price_history(code, days=90)
            if df is None or df.empty:
                return None
            if as_of_date:
                df = df[df["date"] <= as_of_date]
                if df.empty:
                    return None
            score, signals, freshness = _score_stock(df)
            if score < 3:
                return None
            name = name_map.get(code, code)
            if "ST" in name.upper():
                return None
            close = float(df["close"].iloc[-1])
            if not (3.0 <= close <= 500.0):
                return None
            # P0-L: 涨跌停/一字板过滤 — 无法成交，信号失真
            if len(df) >= 2:
                prev_c = float(df["close"].iloc[-2])
                if prev_c > 0 and abs(close - prev_c) / prev_c * 100 >= 9.5:
                    return None
            if float(df["high"].iloc[-1]) == float(df["low"].iloc[-1]):
                return None
            return {
                "code":      code,
                "name":      name,
                "industry":  ind_map.get(code, ""),
                "close":     round(close, 2),
                "gc_score":  score,
                "freshness": round(freshness, 3),
                "signals":   signals,
            }
        except Exception:
            return None

    print(f"[golden_cross] 扫描 {len(universe)} 只股票...")
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_and_score, c): c for c in universe}
        for fut in tqdm(as_completed(futs), total=len(futs)):
            res = fut.result()
            if res:
                results.append(res)

    results.sort(key=lambda x: (-x["gc_score"], -x.get("freshness", 0.0), x["code"]))

    # event_log — log every scan result for IC analysis and audit
    try:
        import event_log as _elog
        _rows = [{"date": date, "strategy": "golden_cross", "code": r["code"],
                  "signal_type": "gc_scan",
                  "price": r.get("close"),
                  "score": float(r.get("gc_score", 0)),
                  "details": {"name": r.get("name"), "gc_score": r.get("gc_score"),
                               "freshness": r.get("freshness"),
                               "signals": r.get("signals", []),
                               "industry": r.get("industry", "")}}
                 for r in results]
        if _rows:
            _elog.log_events(_rows)
    except Exception:
        pass

    tiers: dict[str, list] = {t: [] for t in _TIER_MIN}
    for r in results:
        sc = r["gc_score"]
        if sc == 7:   tiers["G0"].append(r)
        elif sc == 6: tiers["G1"].append(r)
        elif sc == 5: tiers["G2"].append(r)

    total = sum(len(v) for v in tiers.values())
    print(f"[golden_cross] 共 {total} 只："
          f"G0(7/7)={len(tiers['G0'])} G1(6/7)={len(tiers['G1'])} G2(5/7)={len(tiers['G2'])}")

    output = {"date": date, "tiers": tiers, "all_picks": results}

    if not dry_run:
        OUT_LATEST.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        dated = ROOT / "data" / f"golden_cross_{date}.json"
        dated.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[golden_cross] 已保存 → golden_cross_latest.json")

    if push and not dry_run:
        _push_results(output)

    return output


def _push_results(data: dict) -> None:
    from common import push_wechat

    date   = data["date"]
    tiers  = data["tiers"]
    total  = sum(len(v) for v in tiers.values())
    date_s = f"{date[4:6]}/{date[6:]}"

    _SIG_SHORT = {
        "MACD金叉":    "MACD",
        "KDJ金叉":     "KDJ",
        "RSI金叉":     "RSI",
        "MA10/20金叉": "MA10/20",
        "量能金叉":    "量",
        "OBV金叉":     "OBV",
        "布林中轨金叉": "布林",
    }

    # 只推 G0（7信号全共振）和 G1（6信号），G2（5信号）数量过多不推送
    blocks = []

    for t, label in [("G0", "7信号"), ("G1", "6信号")]:
        picks = tiers.get(t, [])
        if not picks:
            continue
        stock_lines = []
        for p in picks:
            sig_s = "·".join(_SIG_SHORT.get(s, s) for s in p["signals"])
            stock_lines.append(f"**{p['code']} {p['name']}** ¥{p['close']:.2f}<br>`{sig_s}`  ")
        blocks.append(f"**【{t} {label}  {len(picks)}只】**  \n" + "\n".join(stock_lines))

    blocks.append("⚠️ 仅供参考，不构成投资建议")

    title = f"金叉共振 {date_s}"
    body  = "\n\n".join(blocks)
    print(f"\n{title}\n{body}")
    push_wechat(title, body)
    print("[notify] 推送成功")


def load_gc_results(max_age_days: int = 3) -> dict:
    """Load golden cross scan results from disk.

    Returns empty dict if the file is missing or older than max_age_days.
    """
    if not OUT_LATEST.exists():
        return {}
    data = json.loads(OUT_LATEST.read_text(encoding="utf-8"))
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=max_age_days)).strftime("%Y%m%d")
    if data.get("date", "") < cutoff:
        return {}
    return data


def main() -> None:
    ap = argparse.ArgumentParser(description="金叉策略扫描")
    ap.add_argument("--push",      action="store_true",  help="推送微信")
    ap.add_argument("--no-push",   dest="push", action="store_false")
    ap.add_argument("--dry-run",   action="store_true",  help="不写文件、不推送")
    ap.add_argument("--push-only", action="store_true",  help="仅推送已保存的结果，不重新扫描")
    ap.add_argument("--date",      default="",           help="回填日期 YYYYMMDD，默认当日")
    ap.set_defaults(push=False)
    args = ap.parse_args()
    if args.push_only:
        if not OUT_LATEST.exists():
            print("[golden_cross] 找不到 golden_cross_latest.json，请先运行扫描")
            return
        data = json.loads(OUT_LATEST.read_text(encoding="utf-8"))
        _push_results(data)
        return
    run_scan(push=args.push, dry_run=args.dry_run, as_of_date=args.date)


if __name__ == "__main__":
    main()
