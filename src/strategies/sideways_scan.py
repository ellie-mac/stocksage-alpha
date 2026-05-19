#!/usr/bin/env python3
"""
横盘策略扫描 — 滑动窗口振幅判定

档位（窗口长度 × 严格/宽松，窗口越短门槛越严）：
  HX0 30天±5% 严格 · HX1 20天±4% 严格 · HX2 10天±3% 严格 · HX3 5天±2% 严格
  HS0 30天±5% 宽松 · HS1 20天±4% 宽松 · HS2 10天±3% 宽松 · HS3 5天±2% 宽松

严格 (HX)：窗口内 max/mid ≤ +pct 且 min/mid ≥ -pct（mid = (max+min)/2，全程稳定）
宽松 (HS)：窗口首尾两点 |chg_pct| ≤ pct（仅首尾偶合，可能有中段波动）

为什么窗口越短门槛越严：A 股日波动 1-2% 是常态，5 天内振幅 ±5% 几乎是 80%
股票的默认状态——那不算"横盘"，只算"最近没大事"。短窗口要更严才有"窄幅整理"信号价值。

归属规则：取最强档（窗口越长越强；同窗口严格 > 宽松）。一只股只归一档。

质量过滤（默认开启，排除"死水股"）：
  · 5 日均成交额 ≥ 0.5 亿  — 排除无人气、无成交的僵尸票
  · 量比 (5日量/60日量) ≥ 0.5 — 排除越来越冷的票

用法：
    python -X utf8 src/strategies/sideways_scan.py             # 全市场 + 质量过滤
    python -X utf8 src/strategies/sideways_scan.py --push      # + 推微信
    python -X utf8 src/strategies/sideways_scan.py --tech-only # 仅科技 TMT
    python -X utf8 src/strategies/sideways_scan.py --dry-run   # 打印不落盘
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
from tqdm import tqdm

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

UNIVERSE_PATH = ROOT / "data" / "universe_main.json"
OUT_LATEST    = ROOT / "data" / "sideways_latest.json"

# 档位优先级：窗口越长越强，同窗口严格优先。窗口越短，门槛越严。
_TIER_ORDER = ["HX0", "HS0", "HX1", "HS1", "HX2", "HS2", "HX3", "HS3"]
_TIER_SPEC: dict[str, tuple[int, str, float]] = {
    "HX0": (30, "strict", 0.05), "HS0": (30, "loose", 0.05),
    "HX1": (20, "strict", 0.04), "HS1": (20, "loose", 0.04),
    "HX2": (10, "strict", 0.03), "HS2": (10, "loose", 0.03),
    "HX3": (5,  "strict", 0.02), "HS3": (5,  "loose", 0.02),
}
_MIN_BARS = 65         # 拉 65 天，含 60 天 volume 用于量比

# 科技 (TMT) 行业关键词 — substring 匹配 industry 字段
_TECH_KEYWORDS = (
    "半导体", "集成电路", "芯片",
    "软件", "计算机", "互联网", "信息",
    "通信",
    "元器件", "电子", "光电",
    "网络", "数据", "云", "操作系统",
    "智能", "人工智", "IT",
)


def _is_tech(industry: str) -> bool:
    if not industry:
        return False
    return any(kw in industry for kw in _TECH_KEYWORDS)


def _load_universe() -> list[str]:
    raw = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    return raw if isinstance(raw, list) else list(raw.keys())


def _build_name_maps() -> tuple[dict[str, str], dict[str, str]]:
    names_file = ROOT / "data" / "stock_names.json"
    names: dict[str, str] = {}
    inds:  dict[str, str] = {}
    try:
        raw = json.loads(names_file.read_text(encoding="utf-8"))
        for ts_code, info in raw.items():
            code6 = ts_code.split(".")[0]
            names[code6] = info.get("name", code6) if isinstance(info, dict) else str(info)
            inds[code6]  = info.get("industry", "")  if isinstance(info, dict) else ""
        print(f"[sideways] 名称缓存 {len(names)} 条", flush=True)
    except Exception as e:
        print(f"[sideways] 名称加载失败: {e}", flush=True)
    return names, inds


def _classify(closes: np.ndarray) -> Optional[dict]:
    """Return metrics for the strongest tier this series qualifies for, else None."""
    if len(closes) < 5:
        return None
    for tier in _TIER_ORDER:
        n, mode, pct = _TIER_SPEC[tier]
        if len(closes) < n:
            continue
        window = closes[-n:]
        hi = float(np.max(window))
        lo = float(np.min(window))
        if lo <= 0:
            continue
        mid = (hi + lo) / 2.0
        if mid <= 0:
            continue
        range_pct = (hi - lo) / mid * 100
        if mode == "strict":
            ok = (hi / mid - 1) <= pct and (1 - lo / mid) <= pct
        else:
            first = float(window[0])
            last  = float(window[-1])
            ok = first > 0 and abs(last / first - 1) <= pct
        if ok:
            return {"tier": tier, "window": n, "mode": mode,
                    "pct_threshold": pct,
                    "range_pct": round(range_pct, 2),
                    "hi": round(hi, 2), "lo": round(lo, 2)}
    return None


_TIER_LABEL = {
    "HX0": "30天±5% 严格", "HS0": "30天±5% 宽松",
    "HX1": "20天±4% 严格", "HS1": "20天±4% 宽松",
    "HX2": "10天±3% 严格", "HS2": "10天±3% 宽松",
    "HX3": "5天±2% 严格",  "HS3": "5天±2% 宽松",
}
_TIER_CAP = {"HX0": 12, "HS0": 8, "HX1": 12, "HS1": 8,
             "HX2": 6,  "HS2": 5, "HX3": 5,  "HS3": 5}


def _push_results(data: dict) -> None:
    from common import push_wechat

    date  = data.get("date", "?")
    tiers = data.get("tiers", {})
    total = sum(len(v) for v in tiers.values())
    date_s = f"{date[4:6]}/{date[6:]}" if len(date) == 8 else date
    title = f"📐 横盘策略 {date_s}  {total}只"

    if total == 0:
        push_wechat(title, "今日无横盘信号")
        print("[sideways] 微信推送完成（无信号）", flush=True)
        return

    sections: list[str] = []
    for tier in _TIER_ORDER:
        picks = tiers.get(tier, [])
        if not picks:
            continue
        cap = _TIER_CAP.get(tier, 5)
        shown = picks[:cap]
        omitted = len(picks) - len(shown)
        lines = []
        for p in shown:
            close = p.get("close", 0) or 0
            rp = p.get("range_pct") or 0
            amt = p.get("amt_5d_yi") or 0
            vr = p.get("vol_ratio") or 0
            ind = p.get("industry", "")
            lines.append(
                f"**{p['code']} {p['name']}** ({ind}) ¥{close:.2f}  振幅{rp:.1f}% / 额{amt:.1f}亿 / 量比{vr:.2f}  "
            )
        section = f"**【{tier} {_TIER_LABEL[tier]}】{len(picks)}只**  \n" + "\n".join(lines)
        if omitted > 0:
            section += f"\n_...还有{omitted}只_"
        sections.append(section)

    legend = (
        "```\n"
        "门槛：30天±5% / 20天±4% / 10天±3% / 5天±2%（窗口越短越严）\n"
        "HX 严格：窗口内 max/min 相对中价都在 ±pct 以内（全程稳定）\n"
        "HS 宽松：窗口首尾两点涨跌幅 ≤pct（仅首尾偶合）\n"
        "归属：取最强档（窗口越长越强，同窗口严格优先）\n"
        "已过滤：死水股(5日均额≥0.5亿+量比≥0.5) + 行业黑名单\n"
        "```"
    )
    body = legend + "\n\n" + "\n\n".join(sections)
    push_wechat(title, body)
    print("[sideways] 微信推送完成", flush=True)


def run_scan(push: bool = False, dry_run: bool = False, tech_only: bool = False) -> dict:
    import fetcher as _fetcher
    try:
        from jobs.prefetch import wait_for_fresh_prices
        wait_for_fresh_prices()
    except Exception:
        pass

    universe = _load_universe()
    name_map, ind_map = _build_name_maps()
    date = datetime.now().strftime("%Y%m%d")

    if tech_only:
        before = len(universe)
        universe = [c for c in universe if _is_tech(ind_map.get(c[-6:], ""))]
        print(f"[sideways] 科技行业过滤: {before} → {len(universe)} 只", flush=True)

    from strategies._quality import compute_metrics, passes_quality, is_blacklisted

    def _fetch_and_classify(code: str) -> Optional[dict]:
        try:
            code6 = code[-6:]
            if is_blacklisted(ind_map.get(code6, "")):
                return None
            df = _fetcher.get_price_history(code, days=_MIN_BARS + 5)
            if df is None or len(df) < 5:
                return None
            name = name_map.get(code6, code6)
            if "ST" in name.upper():
                return None
            close = float(df["close"].iloc[-1])
            if not (3.0 <= close <= 500.0):
                return None
            metrics = compute_metrics(df)
            if not passes_quality(metrics):
                return None
            sw = _classify(df["close"].values)
            if not sw:
                return None
            return {
                "code":       code6,
                "name":       name,
                "industry":   ind_map.get(code6, ""),
                "close":      round(close, 2),
                "tier":       sw["tier"],
                "window":     sw["window"],
                "mode":       sw["mode"],
                "range_pct":  sw["range_pct"],
                "hi":         sw["hi"],
                "lo":         sw["lo"],
                "amt_5d_yi":  metrics["amt_5d_yi"],
                "vol_ratio":  metrics["vol_ratio"],
            }
        except Exception:
            return None

    print(f"[sideways] 扫描 {len(universe)} 只股票...", flush=True)
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_and_classify, c): c for c in universe}
        for fut in tqdm(as_completed(futs), total=len(futs)):
            res = fut.result()
            if res:
                results.append(res)

    results.sort(key=lambda x: (_TIER_ORDER.index(x["tier"]), x["range_pct"], x["code"]))

    tiers: dict[str, list] = {t: [] for t in _TIER_ORDER}
    for r in results:
        tiers[r["tier"]].append(r)

    counts = " ".join(f"{t}={len(tiers[t])}" for t in _TIER_ORDER)
    print(f"[sideways] 共 {len(results)} 只：{counts}", flush=True)

    output = {"date": date, "tiers": tiers, "all_picks": results}

    if not dry_run:
        OUT_LATEST.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        dated = ROOT / "data" / f"sideways_{date}.json"
        dated.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[sideways] 已保存 → sideways_latest.json")

    if push and not dry_run:
        try:
            _push_results(output)
        except Exception as e:
            print(f"[sideways] 微信推送失败: {e}", flush=True)

    try:
        import event_log as _elog
        _rows = [{"date": date, "strategy": "sideways", "code": r["code"],
                  "signal_type": "sideways_scan",
                  "price": r.get("close"),
                  "score": -r.get("range_pct", 100.0),
                  "details": {"name": r.get("name"), "tier": r.get("tier"),
                              "window": r.get("window"), "mode": r.get("mode"),
                              "range_pct": r.get("range_pct"),
                              "amt_5d_yi": r.get("amt_5d_yi"),
                              "vol_ratio": r.get("vol_ratio"),
                              "industry": r.get("industry", "")}}
                 for r in results]
        if _rows:
            _elog.log_events(_rows)
    except Exception:
        pass

    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--push",      action="store_true", help="推送微信")
    parser.add_argument("--dry-run",   action="store_true", help="打印不落盘")
    parser.add_argument("--tech-only", action="store_true",
                        help="仅扫描科技 TMT（默认扫全市场，靠流动性+量比过滤死水股）")
    args = parser.parse_args()
    run_scan(push=args.push, dry_run=args.dry_run, tech_only=args.tech_only)


if __name__ == "__main__":
    main()
