#!/usr/bin/env python3
"""
机构策略 — 监测主动基金季度调仓，多家同时新增则推送

用法：
    python -X utf8 scripts/institution_scan.py           # 扫描，不推送
    python -X utf8 scripts/institution_scan.py --push    # 推送微信+飞书
    python -X utf8 scripts/institution_scan.py --min-funds 3  # 至少3家新增才输出
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

ROOT    = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

WATCHLIST_PATH = ROOT / "data" / "fund_watchlist.json"
OUT_LATEST     = ROOT / "data" / "institution_scan_latest.json"


def _fetch_holdings(fund_code: str, year: str) -> pd.DataFrame:
    """拉取某基金某年全部季度持仓，返回 DataFrame，失败返回空 DataFrame。"""
    import akshare as ak
    try:
        df = ak.fund_portfolio_hold_em(symbol=fund_code, date=year)
        if df is None or df.empty:
            return pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame()


def _get_two_latest_quarters(df: pd.DataFrame) -> tuple[str, str]:
    """从持仓 DataFrame 里取最近两个季度标签，返回 (latest, prev)。"""
    quarters = df["季度"].unique().tolist()
    quarters.sort()
    if len(quarters) < 2:
        return quarters[-1] if quarters else "", ""
    return quarters[-1], quarters[-2]


def _new_positions(df: pd.DataFrame, latest_q: str, prev_q: str) -> set[str]:
    """latest_q 有但 prev_q 没有的股票代码集合。"""
    if not latest_q:
        return set()
    latest_codes = set(df[df["季度"] == latest_q]["股票代码"].tolist())
    if not prev_q:
        return latest_codes
    prev_codes = set(df[df["季度"] == prev_q]["股票代码"].tolist())
    return latest_codes - prev_codes


def run_institution_scan(min_funds: int = 2, push: bool = False) -> dict:
    watchlist = json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
    funds = watchlist.get("funds", [])
    if not min_funds:
        min_funds = watchlist.get("min_funds", 2)

    today = datetime.now()
    cur_year  = str(today.year)
    prev_year = str(today.year - 1)

    print(f"[institution_scan] 监测 {len(funds)} 只基金，min_funds={min_funds}", flush=True)

    # 并发拉持仓
    holdings: dict[str, pd.DataFrame] = {}

    def _fetch(fund: dict):
        code = fund["code"]
        # 同时拉当年和上一年，合并后才能跨年做季度对比（如 Q1 2026 vs Q4 2025）
        df_cur  = _fetch_holdings(code, cur_year)
        df_prev = _fetch_holdings(code, prev_year)
        frames = [df for df in [df_cur, df_prev] if not df.empty]
        if not frames:
            return code, pd.DataFrame()
        df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["股票代码", "季度"])
        return code, df

    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(_fetch, f): f for f in funds}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="拉持仓"):
            code, df = fut.result()
            if not df.empty:
                holdings[code] = df

    print(f"[institution_scan] 成功拉取 {len(holdings)}/{len(funds)} 只基金持仓", flush=True)

    # 找新增持仓
    # new_by_stock[股票代码] = [(基金代码, 基金名, 最新季度, 上季度, 占净值比例), ...]
    new_by_stock: dict[str, list[dict]] = {}
    fund_name_map = {f["code"]: f["name"] for f in funds}
    quarter_info: dict[str, tuple[str, str]] = {}  # 记录每只基金的季度情况

    for code, df in holdings.items():
        latest_q, prev_q = _get_two_latest_quarters(df)
        quarter_info[code] = (latest_q, prev_q)
        if not latest_q or not prev_q:
            continue
        new_codes = _new_positions(df, latest_q, prev_q)
        latest_df = df[df["季度"] == latest_q]
        for stock_code in new_codes:
            row = latest_df[latest_df["股票代码"] == stock_code]
            ratio = float(row["占净值比例"].iloc[0]) if not row.empty else 0.0
            if stock_code not in new_by_stock:
                new_by_stock[stock_code] = []
            new_by_stock[stock_code].append({
                "fund_code": code,
                "fund_name": fund_name_map.get(code, code),
                "latest_q":  latest_q,
                "prev_q":    prev_q,
                "ratio":     ratio,
            })

    # 过滤：>=min_funds 家同时新增
    results = []
    for stock_code, buyers in new_by_stock.items():
        if len(buyers) < min_funds:
            continue
        # 尝试拿股票名称
        stock_name = ""
        for code, df in holdings.items():
            rows = df[df["股票代码"] == stock_code]
            if not rows.empty and "股票名称" in rows.columns:
                stock_name = str(rows["股票名称"].iloc[0])
                break
        buyers.sort(key=lambda x: -x["ratio"])
        results.append({
            "stock_code":  stock_code,
            "stock_name":  stock_name,
            "fund_count":  len(buyers),
            "buyers":      buyers,
        })

    results.sort(key=lambda x: -x["fund_count"])

    # 找各基金最新季度
    latest_quarters = sorted({v[0] for v in quarter_info.values() if v[0]}, reverse=True)
    scan_quarter = latest_quarters[0] if latest_quarters else "未知"

    output = {
        "scan_time":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "scan_quarter": scan_quarter,
        "fund_count":   len(holdings),
        "min_funds":    min_funds,
        "hits":         results,
    }

    OUT_LATEST.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    date_str = datetime.now().strftime("%Y%m%d")
    (ROOT / "data" / f"institution_scan_{date_str}.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"[institution_scan] {scan_quarter} 共 {len(results)} 只股票被 >={min_funds} 家新增 → institution_scan_latest.json", flush=True)
    for r in results:
        names = "、".join(b["fund_name"][:8] for b in r["buyers"])
        print(f"  {r['stock_code']} {r['stock_name']}  {r['fund_count']}家新增: {names}", flush=True)

    if push:
        _push_results(output)

    return output


def _push_results(data: dict) -> None:
    cfg = json.loads((ROOT / "alert_config.json").read_text(encoding="utf-8"))
    from common import send_wechat, configure_pushplus
    configure_pushplus(cfg.get("pushplus", {}).get("token", ""))

    hits       = data.get("hits", [])
    quarter    = data.get("scan_quarter", "?")
    min_funds  = data.get("min_funds", 2)
    fund_count = data.get("fund_count", 0)
    title      = f"机构策略 {quarter[:10]}  {len(hits)}只被{min_funds}+家新增"

    # 季度标签简化：2026年1季度股票投资明细 → 2026Q1
    def _fmt_q(q: str) -> str:
        import re
        m = re.match(r"(\d{4})年(\d)季度", q)
        return f"{m.group(1)}Q{m.group(2)}" if m else q[:10]

    if not hits:
        body = f"扫描 {fund_count} 只基金，{_fmt_q(quarter)}，无股票被 >={min_funds} 家同时新增"
    else:
        items = [f"季度: {_fmt_q(quarter)}  扫描基金: {fund_count}只  阈值: >={min_funds}家新增"]
        for r in hits:
            fund_lines = "\n\n".join(
                f"· {b['fund_name'][:10]}  占净值{b['ratio']:.2f}%  {_fmt_q(b['latest_q'])}新增（原{_fmt_q(b['prev_q'])}未持有）"
                for b in r["buyers"]
            )
            items.append(
                f"**{r['stock_code']} {r['stock_name']}**  {r['fund_count']}家新增\n\n{fund_lines}"
            )
        body = "\n\n".join(items)

    send_wechat(title, body, cfg.get("serverchan", {}).get("sendkey", ""))
    print(f"[institution_scan] 微信推送完成", flush=True)

    try:
        from notify import push_feishu_card
        card_lines = [f"季度: {_fmt_q(quarter)}  扫描{fund_count}只基金  阈值>={min_funds}家新增", ""]
        if hits:
            for r in hits:
                card_lines.append(f"{r['stock_code']} {r['stock_name']}  {r['fund_count']}家新增")
                for b in r["buyers"]:
                    card_lines.append(f"  · {b['fund_name'][:10]}  占净值{b['ratio']:.2f}%  {_fmt_q(b['latest_q'])}新增")
                card_lines.append("")
        else:
            card_lines.append("无股票被多家基金同时新增")
        card_lines.append("仅供参考，不构成投资建议")
        push_feishu_card(title, card_lines)
    except Exception as e:
        print(f"[institution_scan] 飞书推送失败: {e}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-funds", type=int, default=0, help="至少几家新增（0=读配置文件）")
    parser.add_argument("--push", action="store_true")
    args = parser.parse_args()
    run_institution_scan(min_funds=args.min_funds, push=args.push)
