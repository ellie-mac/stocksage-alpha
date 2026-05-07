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


_LEGEND = (
    "· 短强  收盘>MA5  +30\n\n"
    "· 中强  MA5>MA20  +30\n\n"
    "· 长强  MA20>MA60  +20\n\n"
    "· 净涨  5日涨幅>0.5×ATR21/收盘  +20\n\n"
    "· 深调  距20日高点<-15%  -20\n\n"
    "· 量增  5日均量>20日均量×1.2  +10\n\n"
    "· 缩量  5日均量<20日均量×0.7  -10\n\n"
    "· 连热  昨日热榜前50%  +15"
)


def _momentum_score(df: pd.DataFrame) -> tuple[float, list[str]]:
    """Returns (score 0-100, tags). Tags are short labels for conditions that fired."""
    c = df["close"].values
    ma5  = float(np.mean(c[-5:]))
    ma20 = float(np.mean(c[-20:]))
    ma60 = float(np.mean(c[-min(60, len(c)):])) if len(c) >= 10 else ma20
    score = 0.0
    tags: list[str] = []
    if c[-1] > ma5:
        score += 30
        tags.append("短强")
    if ma5 > ma20:
        score += 30
        tags.append("中强")
    if ma20 > ma60:
        score += 20
        tags.append("长强")
    if len(c) >= 6:
        atr = float(np.mean(np.abs(np.diff(c[-21:])))) if len(c) >= 21 else 0.0
        ret5 = (c[-1] - c[-6]) / c[-6]
        noise = 0.5 * atr / c[-1] if c[-1] > 0 and atr > 0 else 0.0
        if ret5 > noise:
            score += 20
            tags.append(f"净涨(+{ret5*100:.1f}%)")
    if len(c) >= 20:
        high_20d = float(np.max(c[-20:]))
        if high_20d > 0 and (c[-1] / high_20d - 1) < -0.15:
            score -= 20
            dd = (c[-1] / high_20d - 1) * 100
            tags.append(f"深调({dd:.1f}%)")
    return max(0.0, min(score, 100.0)), tags


def _load_snapshot() -> tuple[list[dict], str]:
    """从 hot_rank_log 加载当日最新快照，返回 (stocks, fetch_time)。
    优先用今日最新时间戳的文件，fallback 到 latest.json。
    """
    log_dir = ROOT / "data" / "hot_rank_log"
    today   = datetime.now().strftime("%Y%m%d")

    # 找今日快照中最新的一个
    today_files = sorted(log_dir.glob(f"{today}_????.json"), reverse=True)
    path = today_files[0] if today_files else log_dir / "latest.json"

    if not path.exists():
        return [], ""
    snap = json.loads(path.read_text(encoding="utf-8"))
    stocks = snap.get("stocks", [])
    # 兼容旧格式：代码可能带 SZ/SH 前缀
    import re as _re
    cleaned = []
    for s in stocks:
        c = _re.sub(r"^(SZ|SH|sz|sh)", "", str(s.get("code", ""))).zfill(6)
        cleaned.append({**s, "code": c})
    return cleaned, snap.get("fetch_time", "")


def _load_prev_rank_map() -> dict[str, int]:
    """加载最近一次非今日快照，返回 {code: rank}，用于热度持续性判断。"""
    import re as _re
    log_dir = ROOT / "data" / "hot_rank_log"
    today   = datetime.now().strftime("%Y%m%d")
    for p in sorted(log_dir.glob("????????_????.json"), reverse=True):
        if p.name.startswith(today):
            continue
        try:
            snap = json.loads(p.read_text(encoding="utf-8"))
            result: dict[str, int] = {}
            for s in snap.get("stocks", []):
                c = _re.sub(r"^(SZ|SH|sz|sh)", "", str(s.get("code", ""))).zfill(6)
                result[c] = int(s.get("rank", 9999))
            if result:
                print(f"[hot_scan] 前日快照: {p.name}  {len(result)}只", flush=True)
                return result
        except Exception:
            continue
    return {}


def run_hot_scan(top_pct: float = 100.0, cah: bool = True, push: bool = False) -> dict:
    import fetcher as _fetcher

    stocks, fetch_time = _load_snapshot()
    if not stocks:
        print("[hot_scan] 热榜快照不可用，请先运行 hot_rank_logger.py", flush=True)
        return {}

    total   = len(stocks)
    cutoff  = max(1, int(total * top_pct / 100))
    top     = sorted(stocks, key=lambda r: r.get("rank", 9999))[:cutoff]
    codes    = [r["code"] for r in top]
    name_map = {r["code"]: r.get("name", "") for r in top}
    rank_map = {r["code"]: r.get("rank", total) for r in top}

    prev_rank_map = _load_prev_rank_map()

    print(f"[hot_scan] 热榜共 {total} 只，top {top_pct}% = {len(codes)} 只  cah={cah}", flush=True)

    results: list[dict] = []

    def _process(code: str):
        try:
            df = _fetcher.get_price_history(code, days=200)
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
            momentum, breakdown = _momentum_score(df)
            if momentum < 30:
                return None
            rank = rank_map.get(code, total)
            heat_score = max(0.0, 100.0 - rank / total * 100.0)
            bonus = 0.0

            # 量价配合：近5日均量 vs 近20日均量
            if "volume" in df.columns and len(df) >= 20:
                vol = df["volume"].values
                vol5  = float(np.mean(vol[-5:]))
                vol20 = float(np.mean(vol[-20:]))
                if vol20 > 0:
                    vol_ratio = vol5 / vol20
                    if vol_ratio > 1.2:
                        bonus += 10
                        breakdown.append(f"量增({vol_ratio:.1f}x)")
                    elif vol_ratio < 0.7:
                        bonus -= 10
                        breakdown.append(f"缩量({vol_ratio:.1f}x)")

            # 热度持续性：前日快照中排名前50%
            prev_rank = prev_rank_map.get(code)
            if prev_rank is not None and prev_rank <= total // 2:
                bonus += 15
                breakdown.append("连热")

            score = heat_score * 0.4 + momentum * 0.6 + bonus
            change_pct = round((close - float(df["close"].iloc[-2])) / float(df["close"].iloc[-2]) * 100, 2) if len(df) >= 2 else 0.0
            return {
                "code": code, "name": name, "close": round(close, 2),
                "change_pct": change_pct, "rank": rank,
                "rank_pct": round(rank / total * 100, 1),
                "momentum": round(momentum, 1), "score": round(score, 1),
                "breakdown": breakdown,
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
    output   = {"date": date_str, "top_pct": top_pct, "cah": cah,
                "snapshot_time": fetch_time, "picks": results[:30]}

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

    picks    = data.get("picks", [])
    date_s   = data.get("date", "?")
    top_pct  = data.get("top_pct", 5)
    snap_t   = data.get("snapshot_time", "")
    suffix   = "·排高位" if data.get("cah") else ""
    d        = f"{date_s[4:6]}/{date_s[6:]}" if len(date_s) == 8 else date_s
    title    = f"🔥 热榜策略 {d}{suffix}  {len(picks)}只"

    # 微信
    if not picks:
        body = f"热榜扫描无符合条件的股票"
    else:
        items = [f"快照: {snap_t[:16] if snap_t else '未知'}\n\n{_LEGEND}"]
        for p in picks[:15]:
            chg = f"+{p['change_pct']:.1f}%" if p["change_pct"] >= 0 else f"{p['change_pct']:.1f}%"
            tags = "  ".join(p.get("breakdown", []))
            items.append(
                f"**{p['code']} {p['name']}**  ¥{p['close']}  {chg}  热度#{p['rank']}\n\n{tags}"
            )
        body = "\n\n".join(items)
    send_wechat(title, body, cfg.get("serverchan", {}).get("sendkey", ""))
    print(f"[hot_scan] 微信推送完成", flush=True)

    # 飞书卡片
    try:
        from notify import push_feishu_card
        card_lines = [f"快照: {snap_t[:16] if snap_t else '未知'}  共{len(picks)}只", ""]
        card_lines += _LEGEND.splitlines()
        card_lines.append("")
        if picks:
            for p in picks[:15]:
                chg_s = f"+{p['change_pct']:.1f}%" if p["change_pct"] >= 0 else f"{p['change_pct']:.1f}%"
                tags = "  ".join(p.get("breakdown", []))
                card_lines.append(f"{p['code']} {p['name']}  ¥{p['close']}  {chg_s}  热度#{p['rank']}")
                card_lines.append(f"  {tags}")
                card_lines.append("")
        else:
            card_lines.append("无符合条件的股票")
        card_lines.append("仅供参考，不构成投资建议")
        push_feishu_card(title, card_lines)
    except Exception as e:
        print(f"[hot_scan] 飞书推送失败: {e}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-pct", type=float, default=100.0)
    parser.add_argument("--no-cah", action="store_true", help="不排高位（默认排除距半年高点<10%的高位股）")
    parser.add_argument("--push", action="store_true")
    args = parser.parse_args()
    run_hot_scan(top_pct=args.top_pct, cah=not args.no_cah, push=args.push)
