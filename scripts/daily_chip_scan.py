#!/usr/bin/env python3
"""
每日筹码全档扫描：T1-T5 全部档位，默认带 BOLL中轨±8% + 绿柱离零轴>1% 过滤。
用法：python -X utf8 scripts/daily_chip_scan.py [--date YYYYMMDD] [--dry-run] [--ak]
  --ak  跳过 Tushare，直接用 akshare 自算筹码分布（无额度限制，约5-10分钟）
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from chip_strategy import fetch_chip_data, fetch_chip_data_ak, fetch_6m_high, screen, add_indicators, load_names, _get_pro, _latest_trade_date
from common import send_wechat, configure_pushplus

TIERS = [
    {"label": "T1 ≥95%",    "min_win": 95,  "max_win": None},
    {"label": "T2 90-95%",  "min_win": 90,  "max_win": 95},
    {"label": "T3 85-90%",  "min_win": 85,  "max_win": 90},
    {"label": "T4 75-85%",  "min_win": 75,  "max_win": 85},
    {"label": "T5 65-75%",  "min_win": 65,  "max_win": 75},
]


def _push(title: str, body: str) -> None:
    try:
        cfg     = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
        sendkey = cfg.get("serverchan", {}).get("sendkey", "")
        configure_pushplus(cfg.get("pushplus", {}).get("token", ""))
        send_wechat(title, body, sendkey)
        print(f"[notify] 微信推送成功")
    except Exception as e:
        print(f"[notify] 推送失败: {e}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",        type=str, default=None)
    parser.add_argument("--dry-run",     action="store_true")
    parser.add_argument("--high-filter", action="store_true",
                        help="剔除处于半年高位的股票（close/6m_high >= 90%）")
    parser.add_argument("--ak",          action="store_true",
                        help="强制使用 akshare 自算模式，不消耗 Tushare 额度")
    parser.add_argument("--boll",        action="store_true",
                        help="启用 BOLL中轨±8% 过滤（默认关闭）")
    parser.add_argument("--max-price",   type=float, default=None,
                        help="剔除股价高于此值的股票（如 --max-price 50）")
    parser.add_argument("--no-kcb",      action="store_true",
                        help="剔除科创板股票（688xxx）")
    parser.add_argument("--no-push",     action="store_true",
                        help="只预取缓存，不发微信通知（夜间预热用）")
    args = parser.parse_args()

    trade_date = args.date or _latest_trade_date()
    print(f"[scan] trade_date={trade_date}", flush=True)

    names = load_names()
    pro   = _get_pro()  # needed for fetch_6m_high even in --ak mode

    if args.ak:
        print("[scan] --ak 模式：akshare 自算筹码分布", flush=True)
        df = fetch_chip_data_ak(trade_date)
    else:
        df = fetch_chip_data(trade_date, pro)
        if df.empty:
            print("[scan] Tushare 无数据，自动降级到 akshare 模式", flush=True)
            df = fetch_chip_data_ak(trade_date)

    if df.empty:
        print("[scan] 无数据，退出")
        _push(f"筹码扫描 {trade_date}", "无数据，可能是非交易日或 API 限流")
        return

    if names:
        df["name"]     = df["ts_code"].map(lambda c: names.get(c, {}).get("name", ""))
        df["industry"] = df["ts_code"].map(lambda c: names.get(c, {}).get("industry", ""))
    else:
        df["name"] = df["industry"] = ""

    # 预筛：取所有档位候选（winner_rate >= 65），一次性拉指标
    min_tier_win = min(t["min_win"] for t in TIERS)
    candidates = screen(df, min_win=min_tier_win, max_today_pct=5.0,
                        max_price=None, exclude_kcb=False)

    # 半年高位过滤（可选，--high-filter）
    six_month_high: dict[str, float] = {}
    if args.high_filter and not candidates.empty:
        print(f"[scan] 拉取半年高位数据…", flush=True)
        six_month_high = fetch_6m_high(candidates["ts_code"].tolist(), trade_date, pro)

    print(f"[scan] 候选 {len(candidates)} 只，开始拉 BOLL/MACD 指标…", flush=True)
    if not candidates.empty:
        candidates = add_indicators(candidates)

    parts = ["MACD近零"]
    if args.boll:        parts.insert(0, "BOLL中轨")
    if args.high_filter: parts.append("排半年高位")
    if args.max_price:   parts.append(f"股价≤{args.max_price:.0f}")
    if args.no_kcb:      parts.append("排科创")
    filter_label = "＋".join(parts)

    sections: list[str] = []
    tier_data: dict[str, list] = {}
    all_picks: list[dict] = []

    for tier in TIERS:
        tier_key = tier["label"].split()[0]   # "T1", "T2", ...
        result = screen(
            candidates,
            min_win        = tier["min_win"],
            max_win        = tier["max_win"],
            max_today_pct  = 5.0,
            max_price      = args.max_price,
            exclude_kcb    = args.no_kcb,
            boll_near_mid  = args.boll,
            macd_near_zero = True,
            max_6m_ratio   = 0.9 if args.high_filter else None,
            six_month_high = six_month_high if args.high_filter else None,
        )
        picks = len(result)
        header = f"【{tier['label']}】{picks}只"
        tier_picks: list[dict] = []
        if result.empty:
            sections.append(f"{header}\n（无）")
            tier_data[tier_key] = []
            continue

        result = result.copy()
        result["_code"] = result["ts_code"].str.split(".").str[0]
        result["_name"] = result.get("name", result["_code"]).fillna(result["_code"])
        result["_ind"]  = result.get("industry", "").fillna("")
        result["_close"] = result.get("close", 0).fillna(0).astype(float)
        result["_wr"]    = result.get("winner_rate", 0).fillna(0).astype(float)
        rows = [
            f"{r['_code']} {r['_name']} {r['_ind']} ¥{r['_close']:.2f}  "
            for r in result[["_code", "_name", "_ind", "_close"]].to_dict("records")
        ]
        tier_picks = [
            {"code": r["_code"], "name": r["_name"], "industry": r["_ind"],
             "close": r["_close"], "winner_rate": r["_wr"], "tier": tier_key}
            for r in result[["_code", "_name", "_ind", "_close", "_wr"]].to_dict("records")
        ]
        all_picks.extend(tier_picks)
        sections.append(header + "  \n" + "\n".join(rows))
        tier_data[tier_key] = tier_picks

    title = f"筹码全档 {trade_date}（{filter_label}）"
    body  = "\n\n".join(sections)
    print(f"\n{title}\n{body}")

    # 保存结构化选股结果（供 xhs/chip_writer.py 使用）
    latest_trade = _latest_trade_date()
    if trade_date == latest_trade or args.date is None:
        scan_out = ROOT / "data" / "chip_scan_latest.json"
        scan_data = {
            "date":         trade_date,
            "generated_at": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "filter":       filter_label,
            "tiers":        tier_data,
            "all_picks":    all_picks,
        }
        scan_out.write_text(json.dumps(scan_data, ensure_ascii=False, indent=2), encoding="utf-8")
        dated_out = ROOT / "data" / f"chip_scan_{trade_date}.json"
        dated_out.write_text(json.dumps(scan_data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[scan] 已保存 chip_scan_latest.json + chip_scan_{trade_date}.json（{len(all_picks)} 只）")

    if not args.dry_run and not args.no_push:
        _push(title, body)


if __name__ == "__main__":
    main()
