#!/usr/bin/env python3
"""
金叉策略扫描 — 多维技术指标金叉共振筛选

8项金叉信号：
  1. MACD金叉     — DIF向上穿越DEA（近3日内）
  2. KDJ金叉      — K线向上穿越D线（近3日内）
  3. RSI金叉      — RSI(6)向上穿越RSI(12)（近3日内）
  4. MA5/10金叉   — 5日均线穿越10日均线（近3日内）
  5. MA10/20金叉  — 10日均线穿越20日均线（近5日内）
  6. 量能金叉     — 量MA(5)穿越量MA(10)且价格上涨（近3日内）
  7. OBV金叉      — OBV的MA(5)穿越MA(10)（近3日内）
  8. 布林中轨金叉  — 价格向上穿越20日均线（近3日内）

档位（共振数量）：G1≥6 · G2=5 · G3=4 · G4=3

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

_TIER_MIN = {"G0": 8, "G1": 7, "G2": 6, "G3": 5, "G4": 4, "G5": 3}
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


def _crossed_up(a: np.ndarray, b: np.ndarray, lookback: int = 3) -> bool:
    """True if `a` crossed above `b` within the last `lookback` bars."""
    n = len(a)
    for i in range(max(1, n - lookback), n):
        ap, bp = a[i - 1], b[i - 1]
        ai, bi = a[i],     b[i]
        if any(not np.isfinite(x) for x in (ap, bp, ai, bi)):
            continue
        if ap <= bp and ai > bi:
            return True
    return False


# ── 单股评分 ─────────────────────────────────────────────────────────────────

def _score_stock(df: pd.DataFrame) -> tuple[int, list[str]]:
    if len(df) < _MIN_BARS:
        return 0, []

    c  = df["close"].values.astype(float)
    h  = df["high"].values.astype(float)
    lo = df["low"].values.astype(float)
    v  = df["volume"].values.astype(float)

    if not np.isfinite(c[-1]) or c[-1] <= 0:
        return 0, []

    fired: list[str] = []

    # 1. MACD金叉 —— DIF 穿越 DEA
    dif, dea = _macd(c)
    if _crossed_up(dif, dea, 3):
        fired.append("MACD金叉")

    # 2. KDJ金叉 —— K 穿越 D
    K, D = _kdj(h, lo, c)
    if _crossed_up(K, D, 3):
        fired.append("KDJ金叉")

    # 3. RSI金叉 —— RSI(6) 穿越 RSI(12)
    rsi6  = _rsi(c, 6)
    rsi12 = _rsi(c, 12)
    if _crossed_up(rsi6, rsi12, 3):
        fired.append("RSI金叉")

    # 4. MA5/10金叉
    ma5  = _sma(c, 5)
    ma10 = _sma(c, 10)
    if _crossed_up(ma5, ma10, 3):
        fired.append("MA5/10金叉")

    # 5. MA10/20金叉
    ma20 = _sma(c, 20)
    if _crossed_up(ma10, ma20, 5):
        fired.append("MA10/20金叉")

    # 6. 量能金叉 —— VolMA5 穿越 VolMA10，且近3日价格上涨
    vma5  = _sma(v, 5)
    vma10 = _sma(v, 10)
    price_up = len(c) >= 4 and np.isfinite(c[-1]) and np.isfinite(c[-4]) and c[-1] > c[-4]
    if _crossed_up(vma5, vma10, 3) and price_up:
        fired.append("量能金叉")

    # 7. OBV金叉 —— OBV MA(5) 穿越 MA(10)
    obv_arr  = _obv(c, v)
    obv_ma5  = _sma(obv_arr, 5)
    obv_ma10 = _sma(obv_arr, 10)
    if _crossed_up(obv_ma5, obv_ma10, 3):
        fired.append("OBV金叉")

    # 8. 布林中轨金叉 —— 价格向上穿越 MA20
    if _crossed_up(c, ma20, 3):
        fired.append("布林中轨金叉")

    return len(fired), fired


# ── 主扫描 ────────────────────────────────────────────────────────────────────

def _load_universe() -> list[str]:
    raw = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    return raw if isinstance(raw, list) else list(raw.keys())


def _build_name_maps() -> tuple[dict[str, str], dict[str, str]]:
    """从 load_names() 构建 6位代码 → (name, industry) 映射。"""
    from chip_strategy import load_names
    raw = load_names()
    names = {}
    inds  = {}
    for ts_code, info in raw.items():
        code6 = ts_code.split(".")[0]
        names[code6] = info.get("name", code6) if isinstance(info, dict) else str(info)
        inds[code6]  = info.get("industry", "")  if isinstance(info, dict) else ""
    return names, inds


def run_scan(push: bool = False, dry_run: bool = False) -> dict:
    import fetcher as _fetcher

    universe  = _load_universe()
    name_map, ind_map = _build_name_maps()
    date = datetime.now().strftime("%Y%m%d")

    def _fetch_and_score(code: str) -> Optional[dict]:
        try:
            df = _fetcher.get_price_history(code, days=90)
            if df is None or df.empty:
                return None
            score, signals = _score_stock(df)
            if score < 3:
                return None
            name = name_map.get(code, code)
            if "ST" in name.upper():
                return None
            close = float(df["close"].iloc[-1])
            if not (3.0 <= close <= 500.0):
                return None
            return {
                "code":     code,
                "name":     name,
                "industry": ind_map.get(code, ""),
                "close":    round(close, 2),
                "gc_score": score,
                "signals":  signals,
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

    results.sort(key=lambda x: (-x["gc_score"], x["code"]))

    tiers: dict[str, list] = {t: [] for t in _TIER_MIN}
    for r in results:
        sc = r["gc_score"]
        if sc == 8:   tiers["G0"].append(r)
        elif sc == 7: tiers["G1"].append(r)
        elif sc == 6: tiers["G2"].append(r)
        elif sc == 5: tiers["G3"].append(r)
        elif sc == 4: tiers["G4"].append(r)
        elif sc == 3: tiers["G5"].append(r)

    total = sum(len(v) for v in tiers.values())
    print(f"[golden_cross] 共 {total} 只："
          f"G0={len(tiers['G0'])} G1={len(tiers['G1'])} G2={len(tiers['G2'])} "
          f"G3={len(tiers['G3'])} G4={len(tiers['G4'])} G5={len(tiers['G5'])}")

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
    import json as _json
    cfg     = _json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    sendkey = cfg.get("serverchan", {}).get("sendkey", "")
    from common import send_wechat, configure_pushplus
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))

    date   = data["date"]
    tiers  = data["tiers"]
    total  = sum(len(v) for v in tiers.values())
    date_s = f"{date[4:6]}/{date[6:]}"

    # 信号名缩写，放同一行不占空间
    _SIG_SHORT = {
        "MACD金叉":   "MACD",
        "KDJ金叉":    "KDJ",
        "RSI金叉":    "RSI",
        "MA5/10金叉": "5/10",
        "MA10/20金叉":"10/20",
        "量能金叉":   "量",
        "OBV金叉":    "OBV",
        "布林中轨金叉":"布林",
    }

    # G0-G2 列股票（每档最多20只）
    DETAIL_TIERS  = {"G0": "8信号", "G1": "7信号", "G2": "6信号"}
    # SUMMARY_TIERS = {"G3": "5信号", "G4": "4信号", "G5": "3信号"}

    lines = []

    for t, label in DETAIL_TIERS.items():
        picks = tiers.get(t, [])
        if not picks:
            continue
        lines.append(f"\n**【{t} {label}  {len(picks)}只】**  ")
        for p in picks:
            sig_s = "·".join(_SIG_SHORT.get(s, s) for s in p["signals"])
            lines.append(f"{p['code']} {p['name']} ¥{p['close']:.2f}  `{sig_s}`  ")

    # summary_parts = [f"{label} {len(tiers.get(t,[]))}只"
    #                  for t, label in SUMMARY_TIERS.items() if tiers.get(t)]
    # if summary_parts:
    #     lines.append("\n" + "  |  ".join(summary_parts) + "（未展开）  ")

    lines.append("\n⚠️ 仅供参考，不构成投资建议")
    lines.append("#量化记录 #技术指标 #金叉共振 #数据实验")

    title = f"金叉共振 {date_s}"
    body  = "\n".join(lines)
    print(f"\n{title}\n{body}")
    send_wechat(title, body, sendkey)
    print("[notify] 推送成功")


def main() -> None:
    ap = argparse.ArgumentParser(description="金叉策略扫描")
    ap.add_argument("--push",      action="store_true",  help="推送微信")
    ap.add_argument("--no-push",   dest="push", action="store_false")
    ap.add_argument("--dry-run",   action="store_true",  help="不写文件、不推送")
    ap.add_argument("--push-only", action="store_true",  help="仅推送已保存的结果，不重新扫描")
    ap.set_defaults(push=False)
    args = ap.parse_args()
    if args.push_only:
        if not OUT_LATEST.exists():
            print("[golden_cross] 找不到 golden_cross_latest.json，请先运行扫描")
            return
        data = json.loads(OUT_LATEST.read_text(encoding="utf-8"))
        _push_results(data)
        return
    run_scan(push=args.push, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
