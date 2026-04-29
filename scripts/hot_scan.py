#!/usr/bin/env python3
"""
热榜扫描策略 — 东方财富热度排名 + 动量过滤

用法：
    python -X utf8 scripts/hot_scan.py                # top 5%，不推送
    python -X utf8 scripts/hot_scan.py --top-pct 10   # top 10%
    python -X utf8 scripts/hot_scan.py --cah          # 排高位（距6月高点≥10%）
    python -X utf8 scripts/hot_scan.py --push         # 推微信
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

OUT_LATEST = ROOT / "data" / "hot_scan_latest.json"


def _momentum_score(df: pd.DataFrame) -> float:
    """0-100: price vs MA5/MA20/MA60 + risk-adjusted 5-day return - drawdown penalty."""
    c = df["close"].values
    ma5  = float(np.mean(c[-5:]))
    ma20 = float(np.mean(c[-20:]))
    ma60 = float(np.mean(c[-min(60, len(c)):])) if len(c) >= 10 else ma20
    score = 0.0
    if c[-1] > ma5:  score += 30
    if ma5  > ma20:  score += 30
    if ma20 > ma60:  score += 20
    if len(c) >= 6:
        atr = float(np.mean(np.abs(np.diff(c[-21:])))) if len(c) >= 21 else 0.0
        ret5 = (c[-1] - c[-6]) / c[-6]
        # Risk-adjusted: reward ret5 only if it exceeds noise (0.5×ATR/price)
        noise = 0.5 * atr / c[-1] if c[-1] > 0 and atr > 0 else 0.0
        if ret5 > noise: score += 20
    # Drawdown penalty: if price is >15% below its 20-day high, subtract 20 pts
    if len(c) >= 20:
        high_20d = float(np.max(c[-20:]))
        if high_20d > 0 and (c[-1] / high_20d - 1) < -0.15:
            score -= 20
    return max(0.0, min(score, 100.0))


def run_hot_scan(top_pct: float = 5.0, cah: bool = False, push: bool = False) -> dict:
    import fetcher as _fetcher

    hot_df = _fetcher._get_hot_rank_df()
    if hot_df is None or hot_df.empty:
        print("[hot_scan] 热榜数据获取失败", flush=True)
        return {}

    code_col = next((c for c in hot_df.columns if "代码" in c or c.lower() == "code"), None)
    rank_col  = next((c for c in hot_df.columns if "排名" in c or c.lower() == "rank"), None)
    name_col  = next((c for c in hot_df.columns if "名称" in c or c.lower() == "name"), None)

    if not code_col:
        print("[hot_scan] 找不到代码列，columns:", hot_df.columns.tolist(), flush=True)
        return {}

    total = len(hot_df)
    if rank_col:
        hot_df = hot_df.copy()
        hot_df[rank_col] = pd.to_numeric(hot_df[rank_col], errors="coerce")
        hot_df = hot_df.sort_values(rank_col).reset_index(drop=True)

    cutoff = max(1, int(total * top_pct / 100))
    top_df = hot_df.head(cutoff)

    codes    = top_df[code_col].astype(str).str.zfill(6).tolist()
    name_map = {str(r[code_col]).zfill(6): str(r[name_col]) for _, r in top_df.iterrows()} if name_col else {}
    rank_map = {str(r[code_col]).zfill(6): int(r[rank_col]) for _, r in top_df.iterrows()} if rank_col else {}

    print(f"[hot_scan] 热榜共 {total} 只，top {top_pct}% = {len(codes)} 只  cah={cah}", flush=True)

    results: list[dict] = []

    def _process(code: str):
        try:
            df = _fetcher.get_price_history(code, days=90)
            if df is None or df.empty or len(df) < 20:
                return None
            close = float(df["close"].iloc[-1])
            if not (3.0 <= close <= 500.0):
                return None
            name = name_map.get(code, code)
            if "ST" in name.upper():
                return None
            if len(df) >= 2:
                prev_close = float(df["close"].iloc[-2])
                if prev_close > 0 and abs(close - prev_close) / prev_close * 100 >= 9.5:
                    return None
            if "high" in df.columns and "low" in df.columns:
                if float(df["high"].iloc[-1]) == float(df["low"].iloc[-1]):
                    return None
            if cah:
                high_6m = float(df["high"].tail(120).max())
                if close > high_6m * 0.9:
                    return None
            momentum = _momentum_score(df)
            if momentum < 30:
                return None
            rank = rank_map.get(code, total)
            heat_score = max(0.0, 100.0 - rank / total * 100.0)
            score = heat_score * 0.4 + momentum * 0.6
            change_pct = round((close - float(df["close"].iloc[-2])) / float(df["close"].iloc[-2]) * 100, 2) if len(df) >= 2 else 0.0
            return {
                "code": code, "name": name, "close": round(close, 2),
                "change_pct": change_pct, "rank": rank,
                "rank_pct": round(rank / total * 100, 1),
                "momentum": round(momentum, 1), "score": round(score, 1),
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_process, c): c for c in codes}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="hot_scan"):
            res = fut.result()
            if res:
                results.append(res)

    results.sort(key=lambda x: -x["score"])
    date_str = datetime.now().strftime("%Y%m%d")

    # event_log — log top picks for IC analysis and audit
    try:
        import sys as _sys; _sys.path.insert(0, str(SCRIPTS))
        import event_log as _elog
        _date = datetime.now().strftime("%Y-%m-%d")
        _rows = [{"date": _date, "strategy": "hot_scan", "code": r["code"],
                  "signal_type": "hot_scan",
                  "price": r.get("close"),
                  "score": r.get("score"),
                  "details": {"name": r.get("name"), "rank": r.get("rank"),
                               "rank_pct": r.get("rank_pct"), "momentum": r.get("momentum"),
                               "change_pct": r.get("change_pct")}}
                 for r in results[:30]]
        if _rows:
            _elog.log_events(_rows)
    except Exception:
        pass
    output   = {"date": date_str, "top_pct": top_pct, "cah": cah, "picks": results[:30]}

    OUT_LATEST.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    (ROOT / "data" / f"hot_scan_{date_str}.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[hot_scan] 共 {len(results)} 只通过过滤 → hot_scan_latest.json", flush=True)

    if push:
        _push_results(output)
    return output


def _push_results(data: dict) -> None:
    cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    from common import send_wechat, configure_pushplus
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))

    picks   = data.get("picks", [])
    date_s  = data.get("date", "?")
    top_pct = data.get("top_pct", 5)
    suffix  = "（排高位）" if data.get("cah") else ""
    title   = f"热榜策略 {date_s}"

    if not picks:
        body = f"[热榜扫描 {date_s}] top{top_pct}%{suffix}\n无符合条件的股票"
    else:
        d = f"{date_s[:4]}/{date_s[4:6]}/{date_s[6:]}"
        lines = [f"🔥 热榜扫描 {d} top{top_pct}%{suffix}  共{len(picks)}只\n"]
        for p in picks[:15]:
            chg = f"+{p['change_pct']}%" if p["change_pct"] >= 0 else f"{p['change_pct']}%"
            lines.append(f"  {p['code']} {p['name']}  ¥{p['close']}  {chg}  热度#{p['rank']}  动量{p['momentum']:.0f}")
        body = "\n".join(lines)

    send_wechat(cfg.get("serverchan", {}).get("sendkey", ""), body, title=title)
    print(f"[OK] 微信推送: {body[:60]}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-pct", type=float, default=5.0)
    parser.add_argument("--cah",  action="store_true")
    parser.add_argument("--push", action="store_true")
    args = parser.parse_args()
    run_hot_scan(top_pct=args.top_pct, cah=args.cah, push=args.push)
