#!/usr/bin/env python3
"""
Factor-based stock screener with numbered menu support.

Two usage modes:
  1. Menu mode  : python screener.py --menu "1,3,9"
  2. Query mode : python screener.py "<natural language query>"

Run without arguments to print the factor menu.
"""

import sys
import json
import os

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Factor menu — presented to users so they can pick by number
# ---------------------------------------------------------------------------
# Each entry: (number, category, label_cn, label_en, conditions, weight_hint)
#   conditions  — hard filters applied in Stage 1 (real-time or batch data)
#   weight_hint — natural-language string fed to parse_weights() for Stage 2 ranking

MENU_ITEMS: dict[int, dict] = {
    # ── Valuation ──────────────────────────────────────────────────────────
    1:  {"cat": "估值",  "label": "低估值",    "en": "Low Valuation",
         "conditions": {"pe_percentile_max": 30, "pb_percentile_max": 30},
         "weight_hint": "focus on value"},
    2:  {"cat": "估值",  "label": "高股息",    "en": "High Dividend Yield",
         "conditions": {"div_yield_min": 3.0},
         "weight_hint": "高股息"},
    # ── Growth ─────────────────────────────────────────────────────────────
    3:  {"cat": "成长",  "label": "高成长",    "en": "High Growth (>20%)",
         "conditions": {"revenue_growth_min": 20, "profit_growth_min": 20},
         "weight_hint": "focus on growth"},
    4:  {"cat": "成长",  "label": "稳健成长",  "en": "Steady Growth (>10%)",
         "conditions": {"revenue_growth_min": 10, "profit_growth_min": 10},
         "weight_hint": ""},
    # ── Technical / Momentum ───────────────────────────────────────────────
    5:  {"cat": "技术",  "label": "价格强势",  "en": "Price Momentum (60d top)",
         "conditions": {"return_3m_min": 10},
         "weight_hint": "focus on momentum"},
    6:  {"cat": "技术",  "label": "均线多头",  "en": "MA Bullish Alignment",
         "conditions": {},                          # ranking-only, needs price history
         "weight_hint": "均线 trend following"},
    7:  {"cat": "技术",  "label": "量能放大",  "en": "Volume Breakout (>1.5× MA20)",
         "conditions": {"volume_breakout_min": 1.5},
         "weight_hint": "放量 volume breakout"},
    8:  {"cat": "技术",  "label": "高量比",    "en": "High Volume Ratio 量比 >2",
         "conditions": {"volume_ratio_min": 2.0},
         "weight_hint": "量比 active"},
    # ── Quality ────────────────────────────────────────────────────────────
    9:  {"cat": "质量",  "label": "高ROE",     "en": "High ROE (>15%)",
         "conditions": {"roe_min": 15},
         "weight_hint": "focus on quality"},
    10: {"cat": "质量",  "label": "低负债",    "en": "Low Debt Ratio (<40%)",
         "conditions": {"debt_ratio_max": 40},
         "weight_hint": ""},
    11: {"cat": "质量",  "label": "高毛利率",  "en": "High Gross Margin (>30%)",
         "conditions": {"gross_margin_min": 30},
         "weight_hint": ""},
    12: {"cat": "质量",  "label": "低波动",    "en": "Low Volatility / Stable",
         "conditions": {},                          # ranking-only, needs price history
         "weight_hint": "低波动 defensive"},
    # ── Capital Flows ──────────────────────────────────────────────────────
    13: {"cat": "资金",  "label": "主力资金流入", "en": "Institutional Net Inflow",
         "conditions": {},                          # ranking-only via northbound score
         "weight_hint": "northbound 机构资金 smart money"},
    # ── Size ───────────────────────────────────────────────────────────────
    14: {"cat": "规模",  "label": "小市值",    "en": "Small Cap (<100亿)",
         "conditions": {"market_cap_max": 100},
         "weight_hint": ""},
    15: {"cat": "规模",  "label": "大盘蓝筹",  "en": "Large Cap Blue Chip (>500亿)",
         "conditions": {"market_cap_min": 500},
         "weight_hint": ""},
}

MENU_TEXT = """
📊 请选择筛选因子（输入编号，多个用逗号分隔，如 "1,3,9"）：

【估值类】
  1. 低估值        Low Valuation          — PE/PB行业内分位偏低
  2. 高股息        High Dividend Yield    — 股息率TTM ≥ 3%

【成长类】
  3. 高成长        High Growth            — 营收/利润增速 > 20%
  4. 稳健成长      Steady Growth          — 营收/利润增速 > 10%

【技术/动量类】
  5. 价格强势      Price Momentum         — 近60日涨幅居前（强动量）
  6. 均线多头      MA Bullish Alignment   — 5/10/20/60日均线多头排列
  7. 量能放大      Volume Breakout        — 今日成交量 > 20日均量1.5倍
  8. 高量比        High Volume Ratio      — 量比 > 2（短期活跃放量）

【质量类】
  9. 高ROE         High ROE               — ROE ≥ 15%（盈利能力强）
 10. 低负债        Low Debt               — 资产负债率 < 40%（财务稳健）
 11. 高毛利率      High Gross Margin      — 毛利率 > 30%（护城河宽）
 12. 低波动        Low Volatility         — 股价波动率低、走势稳健

【资金类】
 13. 主力资金流入  Institutional Flow     — 近5日大单净流入持续为正

【规模类】
 14. 小市值        Small Cap              — 总市值 < 100亿（高弹性）
 15. 大盘蓝筹      Large Cap Blue Chip    — 总市值 > 500亿（低风险）

注：6、12、13 无法在海量实时数据中逐一过滤，但会通过评分权重影响排名结果。
    3、4、9、10、11 需要运行 batch_financials.py 预热财务缓存后才能作为硬过滤条件。
"""


def print_menu() -> None:
    print(MENU_TEXT)


def parse_menu_selection(selection: str) -> tuple[dict, str]:
    """
    Parse a comma-separated list of menu numbers (e.g. "1,3,9").
    Returns (conditions_dict, weight_hint_string).
    """
    conditions: dict = {}
    weight_parts: list[str] = []

    for part in selection.split(","):
        try:
            num = int(part.strip())
        except ValueError:
            continue
        item = MENU_ITEMS.get(num)
        if item is None:
            continue
        conditions.update(item["conditions"])
        if item.get("weight_hint"):
            weight_parts.append(item["weight_hint"])

    return conditions, " ".join(weight_parts)


# ---------------------------------------------------------------------------
# Existing keyword-based templates (still used for query mode)
# ---------------------------------------------------------------------------

FILTER_TEMPLATES: dict[str, dict] = {
    # Chinese
    "低估值":       {"pe_percentile_max": 30, "pb_percentile_max": 30},
    "高成长":       {"revenue_growth_min": 20, "profit_growth_min": 20},
    "低估值高成长": {"pe_percentile_max": 40, "revenue_growth_min": 15, "profit_growth_min": 15},
    "高质量":       {"roe_min": 15, "gross_margin_min": 30, "debt_ratio_max": 60},
    "白马股":       {"roe_min": 15, "gross_margin_min": 30, "revenue_growth_min": 10},
    "高动量":       {"return_3m_min": 10},
    "强势股":       {"return_3m_min": 15},
    "低负债":       {"debt_ratio_max": 40},
    "高股息":       {"div_yield_min": 3.0},
    "小市值":       {"market_cap_max": 100},
    "大盘蓝筹":     {"market_cap_min": 500},
    "高量比":       {"volume_ratio_min": 2.0},
    # English
    "low pe":          {"pe_percentile_max": 30},
    "low pb":          {"pb_percentile_max": 30},
    "low valuation":   {"pe_percentile_max": 30, "pb_percentile_max": 30},
    "undervalued":     {"pe_percentile_max": 35},
    "high growth":     {"revenue_growth_min": 20, "profit_growth_min": 20},
    "growth stock":    {"revenue_growth_min": 15, "profit_growth_min": 15},
    "quality":         {"roe_min": 15, "gross_margin_min": 30},
    "blue chip":       {"roe_min": 15, "market_cap_min": 500},
    "high roe":        {"roe_min": 15},
    "momentum":        {"return_3m_min": 10},
    "strong":          {"return_3m_min": 15},
    "low debt":        {"debt_ratio_max": 40},
    "high dividend":   {"div_yield_min": 3.0},
    "small cap":       {"market_cap_max": 100},
    "large cap":       {"market_cap_min": 500},
    "value growth":    {"pe_percentile_max": 40, "revenue_growth_min": 15},
    "high volume":     {"volume_ratio_min": 2.0},
    "volume breakout": {"volume_breakout_min": 1.5},
}

_CN_KEYWORDS: list[tuple[list[str], dict]] = [
    (["低估", "低pe", "低市盈", "便宜"],     {"pe_percentile_max": 40}),
    (["高成长", "高增长", "快速增长"],         {"revenue_growth_min": 20, "profit_growth_min": 15}),
    (["高roe", "高盈利", "优质", "龙头"],      {"roe_min": 12}),
    (["低负债", "无负债", "稳健"],             {"debt_ratio_max": 50}),
    (["高毛利", "高利润率"],                   {"gross_margin_min": 35}),
    (["强势", "趋势向上", "上涨"],             {"return_3m_min": 5}),
    (["量比", "活跃"],                         {"volume_ratio_min": 1.5}),
]

_EN_KEYWORDS: list[tuple[list[str], dict]] = [
    (["cheap", "bargain", "value"],            {"pe_percentile_max": 40}),
    (["fast growth", "high growth rate"],      {"revenue_growth_min": 20, "profit_growth_min": 15}),
    (["profitable", "high quality"],           {"roe_min": 12}),
    (["low leverage", "conservative"],         {"debt_ratio_max": 50}),
    (["high margin"],                          {"gross_margin_min": 35}),
    (["trending", "breakout", "uptrend"],      {"return_3m_min": 5}),
    (["active trading", "volume ratio"],       {"volume_ratio_min": 1.5}),
]

# Keys that can be evaluated from real-time spot data
_REALTIME_KEYS = frozenset({
    "pe_percentile_max", "pb_percentile_max",
    "market_cap_min", "market_cap_max",
    "return_3m_min",
    "volume_ratio_min",
    "div_yield_min",
    "volume_breakout_min",  # uses spot 成交量 approximation
})

# Keys requiring batch financial pre-computation
_FINANCIAL_KEYS = frozenset({
    "roe_min", "gross_margin_min", "debt_ratio_max",
    "revenue_growth_min", "profit_growth_min",
})


def parse_conditions(query: str) -> dict:
    """Parse a free-text query (Chinese or English) into a filter conditions dict."""
    conditions: dict = {}
    q_lower = query.lower()

    for template_name, template_conds in sorted(FILTER_TEMPLATES.items(), key=lambda x: -len(x[0])):
        if template_name.lower() in q_lower:
            conditions.update(template_conds)

    for keywords, conds in _CN_KEYWORDS + _EN_KEYWORDS:
        if any(k in q_lower for k in keywords):
            for k, v in conds.items():
                conditions.setdefault(k, v)

    if not conditions:
        conditions = {"roe_min": 10, "revenue_growth_min": 10}

    return conditions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pct_rank(series: pd.Series, low_is_good: bool = False) -> pd.Series:
    r = series.rank(pct=True, na_option="bottom")
    if low_is_good:
        r = 1 - r
    return r.fillna(0.5)


def _build_industry_pe_lookup(df_full: pd.DataFrame) -> dict[str, float]:
    """
    Returns {code: industry_pe_percentile_0_to_100} using cached industry map.
    Higher percentile means more expensive within the industry.
    Returns empty dict if the map is not cached yet.
    """
    try:
        import cache as _cache
        industry_map = _cache.get("industry_map", 7 * 86400)
        if not industry_map:
            return {}

        tmp = df_full[["code", "pe_ttm"]].copy()
        tmp["code"] = tmp["code"].astype(str).str.zfill(6)
        tmp["industry"] = tmp["code"].map(industry_map)
        tmp["pe_ttm"] = pd.to_numeric(tmp["pe_ttm"], errors="coerce")
        tmp = tmp[(tmp["pe_ttm"] > 0)].dropna(subset=["industry"])
        if tmp.empty:
            return {}

        tmp["pe_pct"] = tmp.groupby("industry", group_keys=False)["pe_ttm"].rank(pct=True) * 100
        return dict(zip(tmp["code"], tmp["pe_pct"]))
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Main screening function
# ---------------------------------------------------------------------------

def screen_stocks(
    query: str = "",
    top_n: int = 10,
    conditions_override: dict | None = None,
    weight_hint_override: str = "",
) -> dict:
    """
    Two-stage screening pipeline.
    conditions_override and weight_hint_override are used by --menu mode.
    """
    from factors import parse_weights

    if conditions_override is not None:
        conditions = conditions_override
        weights = parse_weights(weight_hint_override or query)
    else:
        conditions = parse_conditions(query)
        weights = parse_weights(query)

    # Fetch real-time snapshot
    try:
        import fetcher
        df_full_raw = fetcher._get_spot_df()
    except Exception as e:
        return {"error": f"Failed to fetch market data: {e}"}

    df = df_full_raw[~df_full_raw["名称"].str.contains("ST|退", na=False)].copy()

    # Rename columns
    rename_map = {
        "代码": "code",          "名称": "name",
        "最新价": "price",       "涨跌幅": "change_pct",
        "市盈率-动态": "pe_ttm", "市净率": "pb",
        "总市值": "market_cap",  "换手率": "turnover_rate",
        "60日涨跌幅": "return_3m",
        "量比": "volume_ratio",
        "股息率-TTM": "div_yield",
        "成交量": "volume",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    for col in ["pe_ttm", "pb", "market_cap", "price", "change_pct",
                "return_3m", "turnover_rate", "volume_ratio", "div_yield", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Build industry PE lookup from full pre-filter market
    df_for_industry = df.rename(columns={})  # already renamed
    industry_pe_pct = _build_industry_pe_lookup(df)

    # Basic hygiene
    df = df[(df["price"] > 1) & (df["pe_ttm"] > 0)].copy()

    # -----------------------------------------------------------------------
    # Stage 1a: real-time filters
    # -----------------------------------------------------------------------
    if "market_cap_min" in conditions:
        df = df[df["market_cap"] >= conditions["market_cap_min"] * 1e8]
    if "market_cap_max" in conditions:
        df = df[df["market_cap"] <= conditions["market_cap_max"] * 1e8]

    if "pe_percentile_max" in conditions:
        threshold = float(df["pe_ttm"].quantile(conditions["pe_percentile_max"] / 100))
        df = df[df["pe_ttm"] <= threshold]

    if "pb_percentile_max" in conditions and "pb" in df.columns:
        threshold = float(df["pb"].dropna().quantile(conditions["pb_percentile_max"] / 100))
        df = df[df["pb"] <= threshold]

    if "return_3m_min" in conditions and "return_3m" in df.columns:
        df = df[df["return_3m"] >= conditions["return_3m_min"]]

    if "volume_ratio_min" in conditions and "volume_ratio" in df.columns:
        df = df[df["volume_ratio"] >= conditions["volume_ratio_min"]]

    if "div_yield_min" in conditions and "div_yield" in df.columns:
        df = df[df["div_yield"] >= conditions["div_yield_min"]]

    # Volume breakout: approximation using current 成交量 rank (no per-stock MA in screener)
    if "volume_breakout_min" in conditions and "volume" in df.columns and "turnover_rate" in df.columns:
        # Use turnover rate as a volume-activity proxy when per-stock MA20 is unavailable
        threshold = float(df["turnover_rate"].quantile(0.6))  # top 40% by turnover
        df = df[df["turnover_rate"] >= threshold]

    # -----------------------------------------------------------------------
    # Stage 1b: financial filters from nightly batch cache
    # -----------------------------------------------------------------------
    financial_applied: list[str] = []
    financial_unavailable: list[str] = []

    requested_fin = {k for k in conditions if k in _FINANCIAL_KEYS}
    fin_df = None
    if requested_fin:
        try:
            import batch_financials
            fin_df = batch_financials.load()
        except Exception:
            fin_df = None

    if fin_df is not None and not fin_df.empty and requested_fin:
        df = df.merge(fin_df[["code", "roe", "gross_margin", "debt_ratio",
                               "revenue_growth", "profit_growth"]],
                      on="code", how="left")
        _filter_map = [
            ("roe_min",            "roe",            ">="),
            ("gross_margin_min",   "gross_margin",   ">="),
            ("debt_ratio_max",     "debt_ratio",     "<="),
            ("revenue_growth_min", "revenue_growth", ">="),
            ("profit_growth_min",  "profit_growth",  ">="),
        ]
        for cond_key, col, op in _filter_map:
            if cond_key in conditions and col in df.columns:
                t = conditions[cond_key]
                if op == ">=":
                    df = df[df[col].isna() | (df[col] >= t)]
                else:
                    df = df[df[col].isna() | (df[col] <= t)]
                financial_applied.append(cond_key)
        financial_unavailable = [k for k in requested_fin if k not in financial_applied]
    elif requested_fin:
        financial_unavailable = list(requested_fin)

    df = df.reset_index(drop=True)

    # -----------------------------------------------------------------------
    # Stage 2: weight-aware composite score
    # -----------------------------------------------------------------------

    # Value: industry-relative PE percentile if map is cached, else market-wide
    if industry_pe_pct:
        ind_pe = df["code"].map(industry_pe_pct).fillna(50.0) / 100
        v_raw = 1 - ind_pe  # low PE pct -> high value score
    else:
        v_raw = _pct_rank(df["pe_ttm"], low_is_good=True)

    # Growth
    if "revenue_growth" in df.columns and "profit_growth" in df.columns:
        g_raw = (_pct_rank(df["revenue_growth"]) + _pct_rank(df["profit_growth"])) / 2
    elif "revenue_growth" in df.columns:
        g_raw = _pct_rank(df["revenue_growth"])
    else:
        g_raw = pd.Series(0.5, index=df.index)

    # Momentum
    if "return_3m" in df.columns:
        m_raw = _pct_rank(df["return_3m"])
    else:
        m_raw = pd.Series(0.5, index=df.index)

    # Quality
    if "roe" in df.columns:
        q_parts = [_pct_rank(df["roe"])]
        if "gross_margin" in df.columns:
            q_parts.append(_pct_rank(df["gross_margin"]))
        if "debt_ratio" in df.columns:
            q_parts.append(_pct_rank(df["debt_ratio"], low_is_good=True))
        q_raw = sum(q_parts) / len(q_parts)
    else:
        q_raw = pd.Series(0.5, index=df.index)

    # Dividend yield (from spot data)
    if "div_yield" in df.columns:
        dy_raw = _pct_rank(df["div_yield"])
    else:
        dy_raw = pd.Series(0.5, index=df.index)

    # Volume ratio 量比 (from spot data)
    if "volume_ratio" in df.columns:
        vr_raw = _pct_rank(df["volume_ratio"])
    else:
        vr_raw = pd.Series(0.5, index=df.index)

    w = weights
    w_total = (w.value + w.growth + w.momentum + w.quality
               + w.div_yield + w.volume_ratio)
    if w_total == 0:
        w_total = 1.0

    df["score"] = (
        (v_raw  * w.value  + g_raw * w.growth  + m_raw * w.momentum +
         q_raw  * w.quality + dy_raw * w.div_yield + vr_raw * w.volume_ratio)
        / w_total * 100
    ).round(1)

    df = df.sort_values("score", ascending=False)

    results = []
    for _, row in df.head(top_n).iterrows():
        item = {
            "code":               str(row.get("code", "")),
            "name":               str(row.get("name", "")),
            "price":              float(row.get("price", 0) or 0),
            "change_pct":         float(row.get("change_pct", 0) or 0),
            "pe_ttm":             float(row.get("pe_ttm", 0) or 0),
            "pb":                 float(row.get("pb", 0) or 0),
            "market_cap_billion": round(float(row.get("market_cap", 0) or 0) / 1e8, 1),
            "score":              float(row.get("score", 0)),
        }
        for col in ("roe", "gross_margin", "revenue_growth", "profit_growth",
                    "div_yield", "volume_ratio", "return_3m"):
            if col in df.columns and pd.notna(row.get(col)):
                item[col] = round(float(row[col]), 1)
        results.append(item)

    applied_rt  = {k: v for k, v in conditions.items()
                   if k in _REALTIME_KEYS and k not in financial_unavailable}
    applied_fin = {k: conditions[k] for k in financial_applied}

    result = {
        "query":              query or f"menu: {weight_hint_override}",
        "weights_used": {
            "value": w.value, "growth": w.growth,
            "momentum": w.momentum, "quality": w.quality,
            "div_yield": w.div_yield, "volume_ratio": w.volume_ratio,
        },
        "applied_conditions": {**applied_rt, **applied_fin},
        "total_matched":      len(df),
        "results":            results,
        "valuation_basis":    "industry-relative PE percentile" if industry_pe_pct
                              else "market-wide PE percentile",
        "note": (
            "Results based on real-time quotes"
            + (" + batch financial data" if financial_applied else "")
            + ". Use /stocksage <code> for a full deep-dive on any candidate."
        ),
    }

    if financial_unavailable:
        result["unapplied_conditions"] = {k: conditions[k] for k in financial_unavailable}
        result["note"] += (
            f" Financial filters {financial_unavailable} need batch data"
            " — run scripts/batch_financials.py first."
        )

    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="A-share factor screener")
    parser.add_argument("query", nargs="*",
                        help='Natural language query, e.g. "低估值高成长" or "low PE high growth"')
    parser.add_argument("--menu", type=str, default="",
                        help='Comma-separated menu item numbers, e.g. "1,3,9"')
    parser.add_argument("--top", type=int, default=10,
                        help="Number of results to return (default: 10)")
    parser.add_argument("--list", action="store_true",
                        help="Print the factor menu and exit")
    args = parser.parse_args()

    if args.list or (not args.query and not args.menu):
        print_menu()
        sys.exit(0)

    if args.menu:
        cond_override, weight_hint = parse_menu_selection(args.menu)
        result = screen_stocks(
            query="",
            top_n=args.top,
            conditions_override=cond_override,
            weight_hint_override=weight_hint,
        )
    else:
        result = screen_stocks(query=" ".join(args.query), top_n=args.top)

    print(json.dumps(result, ensure_ascii=False, indent=2))
