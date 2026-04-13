#!/usr/bin/env python3
"""
Build screener_universe for alert_config.json + update screener_universe.md.

Selection strategy per sector:
  Sort all constituent stocks by hybrid rank: average of 成交额-rank and 成交量-rank.
  成交额 (price × volume) captures high-priced blue chips.
  成交量 (share count) is price-neutral, capturing active mid/small-caps.
  The average rank ensures neither metric monopolises selection.

  Boards with ≤ top_n stocks: take all (no filtering needed).

Usage:
  python scripts/build_universe.py            # update alert_config.json + md
  python scripts/build_universe.py --preview  # dry-run, print counts only
  python scripts/build_universe.py --top-n 20 # larger per-sector budget
"""

from __future__ import annotations
import argparse, datetime, json, os, socket, sys, time
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Per-request timeout: prevents any single API call from hanging indefinitely
socket.setdefaulttimeout(45)

import akshare as ak

_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(_ROOT, "alert_config.json")
MD_PATH     = os.path.join(_ROOT, "docs", "screener_universe.md")

# ── Sector list ────────────────────────────────────────────────────────────────
# Organised by theme; used both for fetching and for MD section headers.
GROUPS: dict[str, list[str]] = {
    "🍶 食品饮料": [
        "白酒Ⅱ", "啤酒", "调味发酵品Ⅱ", "乳品",
        "食品加工", "休闲食品", "零食", "软饮料",
    ],
    "🏦 金融": [
        "银行Ⅱ", "保险Ⅱ", "证券Ⅱ", "多元金融", "金融信息服务",
    ],
    "💊 医药医疗": [
        "化学制药", "生物制品", "中药Ⅱ", "医疗器械", "医疗服务",
        "医疗研发外包", "体外诊断", "血液制品", "疫苗", "医疗耗材",
        "医疗美容", "医美服务", "医美耗材",
    ],
    "⚡ 新能源": [
        "锂电池", "电池", "光伏设备", "光伏主材", "光伏电池组件",
        "风电设备", "风电整机", "核力发电", "逆变器",
    ],
    "💾 半导体/芯片": [
        "半导体", "数字芯片设计", "模拟芯片设计",
        "半导体设备", "集成电路制造", "集成电路封测",
    ],
    "🖥️ 软件/IT/AI": [
        "软件开发", "计算机", "垂直应用软件", "横向通用软件",
    ],
    "📱 消费电子/家电": [
        "消费电子", "品牌消费电子", "家用电器", "白色家电", "面板",
    ],
    "🚗 汽车/智驾": [
        "乘用车", "电动乘用车", "汽车零部件", "汽车电子电气系统",
    ],
    "🛡️ 军工": [
        "国防军工", "航天装备Ⅱ", "航空装备Ⅱ",
        "航海装备Ⅱ", "地面兵装Ⅱ", "军工电子Ⅱ",
    ],
    "🏗️ 地产/建材/建筑": [
        "房地产开发", "住宅开发", "建筑材料", "装修建材",
        "玻璃玻纤", "房屋建设Ⅱ", "工程机械", "轨交设备Ⅱ", "定制家居",
    ],
    "🧪 化工/新材料": [
        "化学原料", "基础化工", "化学制品", "化学纤维", "氟化工",
    ],
    "⛏️ 有色/金属/资源": [
        "有色金属", "黄金", "稀土", "铜", "铝", "钢铁", "小金属", "磁性材料",
    ],
    "🔋 能源/电力": [
        "煤炭开采", "油气开采Ⅱ", "石油石化", "火力发电", "水力发电", "燃气Ⅱ",
    ],
    "🌾 农业": [
        "养殖业", "生猪养殖", "农药", "磷肥及磷化工", "种子", "饲料", "农产品加工",
    ],
    "🚚 物流/交通运输": [
        "快递", "港口", "航运", "航空运输", "铁路运输", "仓储物流",
    ],
    "🎮 传媒/娱乐/消费": [
        "传媒", "游戏Ⅱ", "旅游及景区", "酒店", "百货",
        "纺织服饰", "品牌化妆品", "美容护理",
    ],
    "📡 通信/光学": [
        "通信", "通信设备", "光学光电子", "通信线缆及配套",
    ],
    "🤖 机器人/自动化/机械": [
        "机器人", "自动化设备", "机床工具", "激光设备", "仪器仪表", "专用设备",
    ],
    "🔌 电子元件/PCB": [
        "印制电路板", "LED", "元件", "被动元件", "光学元件",
    ],
    "🔆 电力设备/储能/氢能": [
        "综合电力设备商", "电网设备", "输变电设备", "电力设备", "燃料电池",
    ],
    "🌿 环保/教育/其他": [
        "诊断服务", "教育", "环保", "水务及水治理",
        "包装印刷", "物业管理", "互联网电商",
    ],
}

# Hot concept boards — supplementary to industry boards.
# Uses ak.stock_board_concept_cons_em(); auto-skipped if board name doesn't match.
CONCEPT_BOARDS: list[str] = [
    "人工智能", "AI算力", "华为概念", "低空经济",
    "机器人概念", "国产替代", "核电概念", "量子计算",
    "数字经济", "卫星互联网", "固态电池", "ChatGPT",
    "算力基础设施", "DeepSeek", "具身智能",
]
_CONCEPT_LABEL = "🔥 热门概念"


# ── Core fetch ─────────────────────────────────────────────────────────────────

def fetch_universe(top_n: int = 15) -> tuple[list[str], dict[str, list[tuple[str, str]]]]:
    """
    Returns (sorted_code_list, sector_map).
    sector_map: {sector_name: [(code, name), ...]}
    """
    print("Fetching industry board list...")
    boards = ak.stock_board_industry_name_em()
    valid  = set(boards["板块名称"].tolist())

    all_codes: set[str] = set()
    sector_map: dict[str, list[tuple[str, str]]] = {}

    all_sectors = [s for group in GROUPS.values() for s in group]

    for sector in all_sectors:
        if sector not in valid:
            print(f"  [SKIP] {sector}")
            continue
        try:
            df = ak.stock_board_industry_cons_em(symbol=sector)
            if df.empty:
                continue

            df["成交额"] = df["成交额"].fillna(0)
            df["成交量"] = df["成交量"].fillna(0)

            # Hybrid rank: average of 成交额-rank and 成交量-rank (ascending = better)
            # 成交额 captures high-priced blue chips; 成交量 is price-neutral (active mid/small caps)
            df["_rank_val"] = df["成交额"].rank(ascending=False)
            df["_rank_vol"] = df["成交量"].rank(ascending=False)
            df["_rank_avg"] = (df["_rank_val"] + df["_rank_vol"]) / 2
            df_sorted = df.sort_values("_rank_avg", ascending=True).head(top_n)

            picked: list[tuple[str, str]] = []
            for _, row in df_sorted.iterrows():
                code = str(row["代码"]).zfill(6)
                name = str(row["名称"])
                if "ST" in name:  # skip *ST / ST (special treatment, delisting risk)
                    continue
                picked.append((code, name))
                all_codes.add(code)

            sector_map[sector] = picked
            print(f"  [OK] {sector}: {len(picked):2d} stocks  total={len(all_codes)}")
            time.sleep(0.2)
        except Exception as e:
            print(f"  [ERR] {sector}: {e}")

    # ── Concept boards ─────────────────────────────────────────────────────────
    print("Fetching concept board list...")
    try:
        concept_df = ak.stock_board_concept_name_em()
        valid_concepts = set(concept_df["板块名称"].tolist())
    except Exception as e:
        print(f"  [ERR] concept board list: {e}")
        valid_concepts = set()

    for concept in CONCEPT_BOARDS:
        if concept not in valid_concepts:
            print(f"  [SKIP concept] {concept}")
            continue
        try:
            df = ak.stock_board_concept_cons_em(symbol=concept)
            if df.empty:
                continue

            if "成交额" not in df.columns or "成交量" not in df.columns:
                print(f"  [SKIP concept] {concept}: missing 成交额/成交量 columns")
                continue
            df["成交额"] = df["成交额"].fillna(0)
            df["成交量"] = df["成交量"].fillna(0)

            df["_rank_val"] = df["成交额"].rank(ascending=False)
            df["_rank_vol"] = df["成交量"].rank(ascending=False)
            df["_rank_avg"] = (df["_rank_val"] + df["_rank_vol"]) / 2
            df_sorted = df.sort_values("_rank_avg", ascending=True).head(top_n)

            picked: list[tuple[str, str]] = []
            for _, row in df_sorted.iterrows():
                code = str(row["代码"]).zfill(6)
                name = str(row["名称"])
                if "ST" in name:
                    continue
                picked.append((code, name))
                all_codes.add(code)

            if picked:
                sector_map[concept] = picked
                print(f"  [OK concept] {concept}: {len(picked):2d} stocks  total={len(all_codes)}")
            time.sleep(0.2)
        except Exception as e:
            print(f"  [ERR concept] {concept}: {e}")

    return sorted(all_codes), sector_map


# ── Markdown writer ────────────────────────────────────────────────────────────

def write_markdown(sector_map: dict[str, list[tuple[str, str]]], total: int) -> None:
    date_str    = datetime.date.today().strftime("%Y-%m-%d")
    sector_count = len(sector_map)

    lines = [
        "# StockSage 选股宇宙对照表",
        "",
        f"> 生成日期: {date_str} | 板块数: {sector_count} | 总股票数: {total}",
        ">",
        "> **选股策略**: 每板块按「成交额排名 + 成交量排名」均值升序取前 N 只。",
        "> 成交额捕捉高价蓝筹（茅台等）；成交量与价格无关，捕捉活跃中小盘。",
        "> 双排名均值避免单一指标被高价股垄断，同时过滤流动性极差的微盘票。",
        "",
    ]

    for group_name, sectors in GROUPS.items():
        group_has_data = any(s in sector_map for s in sectors)
        if not group_has_data:
            continue

        lines.append(f"## {group_name}")
        lines.append("")

        for sector in sectors:
            stocks = sector_map.get(sector)
            if not stocks:
                continue
            lines.append(f"### {sector}  ({len(stocks)} 只)")
            lines.append("")
            lines.append("| 代码 | 名称 |")
            lines.append("|------|------|")
            for code, name in stocks:
                lines.append(f"| {code} | {name} |")
            lines.append("")

    # ── Concept section ────────────────────────────────────────────────────────
    concept_sectors = [c for c in CONCEPT_BOARDS if c in sector_map]
    if concept_sectors:
        lines.append(f"## {_CONCEPT_LABEL}")
        lines.append("")
        for concept in concept_sectors:
            stocks = sector_map[concept]
            lines.append(f"### {concept}  ({len(stocks)} 只)")
            lines.append("")
            lines.append("| 代码 | 名称 |")
            lines.append("|------|------|")
            for code, name in stocks:
                lines.append(f"| {code} | {name} |")
            lines.append("")

    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Markdown written to {MD_PATH}  ({len(lines)} lines)")


# ── ETF watchlist builder ──────────────────────────────────────────────────────

# Theme → keywords to match against ETF name.
# First match wins; order matters (more specific themes first).
_ETF_THEMES: list[tuple[str, list[str]]] = [
    # 宽基指数
    ("宽基_科创50",    ["科创50"]),
    ("宽基_科创100",   ["科创100"]),
    ("宽基_创业板50",  ["创业板50", "创50"]),
    ("宽基_创业板",    ["创业板", "创业ETF", "创ETF"]),
    ("宽基_上证50",    ["上证50", "50ETF"]),
    ("宽基_沪深300",   ["沪深300", "300ETF"]),
    ("宽基_中证500",   ["中证500", "500ETF"]),
    ("宽基_中证1000",  ["中证1000", "1000ETF"]),
    ("宽基_中证2000",  ["中证2000"]),
    ("宽基_全A",       ["全A", "全市场", "A500"]),
    # 行业主题
    ("行业_AI算力",    ["AI", "算力", "人工智能", "大模型", "数字经济"]),
    ("行业_半导体",    ["半导体", "芯片", "集成电路"]),
    ("行业_软件IT",    ["软件", "云计算", "信息技术"]),
    ("行业_消费电子",  ["消费电子", "电子"]),
    ("行业_机器人",    ["机器人", "自动化"]),
    ("行业_新能源车",  ["新能源车", "电动车", "智能汽车"]),
    ("行业_汽车",      ["汽车", "乘用车"]),
    ("行业_新能源",    ["新能源", "光伏", "风电", "储能"]),
    ("行业_锂电",      ["锂电", "电池"]),
    ("行业_医疗器械",  ["医疗器械", "器械"]),
    ("行业_医药",      ["医药", "医疗", "生物", "创新药", "CXO"]),
    ("行业_军工",      ["军工", "国防", "航天", "航空"]),
    ("行业_白酒",      ["白酒", "酒"]),
    ("行业_食品",      ["食品", "消费"]),
    ("行业_银行",      ["银行"]),
    ("行业_证券",      ["证券", "券商"]),
    ("行业_保险",      ["保险"]),
    ("行业_金融",      ["金融"]),
    ("行业_房地产",    ["地产", "房地产"]),
    ("行业_建材",      ["建材", "建筑"]),
    ("行业_有色",      ["有色", "铜", "铝", "稀土", "小金属"]),
    ("行业_黄金",      ["黄金", "Gold"]),
    ("行业_煤炭",      ["煤炭"]),
    ("行业_石油",      ["石油", "油气", "能源"]),
    ("行业_电力",      ["电力", "电网", "输配电"]),
    ("行业_农业",      ["农业", "养殖", "畜牧"]),
    ("行业_物流",      ["物流", "航运", "港口"]),
    ("行业_传媒",      ["传媒", "游戏", "文娱"]),
    ("行业_通信",      ["通信", "5G"]),
    ("行业_环保",      ["环保", "水务"]),
    # 跨境
    ("跨境_纳斯达克",  ["纳斯达克", "纳指", "NASDAQ"]),
    ("跨境_标普500",   ["标普", "SP500", "S&P"]),
    ("跨境_港股",      ["港股", "恒生", "香港"]),
    ("跨境_中概",      ["中概互联", "中概"]),
    ("跨境_日本",      ["日本", "日经", "日经225"]),
    ("跨境_德国",      ["德国", "DAX"]),
    ("跨境_东南亚",    ["东南亚", "越南", "印度"]),
    # 其他资产
    ("债券",           ["债券", "国债", "信用债", "可转债"]),
    ("货币",           ["货币"]),
    ("REITs",          ["REITs", "基础设施", "不动产"]),
    ("商品_黄金",      ["黄金"]),
    ("商品_原油",      ["原油", "石油"]),
]


def _etf_theme(name: str) -> str:
    """Return theme key for an ETF by name keyword matching. First match wins."""
    for theme, keywords in _ETF_THEMES:
        for kw in keywords:
            if kw in name:
                return theme
    return "其他"


def _etf_index_key(name: str) -> str:
    """
    Extract a normalised 'index identifier' from an ETF name.
    ETFs with the same index_key track the same index and have ~100% overlap.
    We keep only the most liquid one per index_key.
    Strategy: strip common fund-house prefixes and suffixes, keep the core index name.
    """
    import re
    # Remove common fund house names (prefixes)
    prefixes = [
        "华夏", "易方达", "华泰柏瑞", "华泰", "南方", "博时", "嘉实", "富国",
        "广发", "汇添富", "工银瑞信", "工银", "招商", "鹏华", "国泰",
        "平安", "天弘", "大成", "万家", "长城", "银华", "华安", "建信",
        "国联安", "浦银安盛", "海富通", "华宝", "泰达宏利", "上投摩根",
        "摩根", "永赢", "景顺长城", "光大", "中欧", "兴全", "前海开源",
    ]
    core = name
    for p in prefixes:
        core = core.replace(p, "")
    # Remove generic suffixes
    core = re.sub(r"(ETF|联接|C类|A类|指数|基金|LOF)\s*$", "", core).strip()
    return core


def build_etf_watchlist(max_per_theme: int = 3) -> list[dict]:
    """
    Fetch ALL exchange-listed T+0 ETFs, deduplicate by same-index overlap,
    keep at most max_per_theme per theme (sorted by liquidity).

    Returns list of {code, name, shares=0, cost_price=0} dicts.
    """
    import pandas as pd

    # Primary: fund_etf_spot_em is the authoritative SSE/SZSE ETF list
    # (only genuine exchange ETFs, no LOFs). Falls back to THS category list.
    print("Fetching ETF list (primary: EastMoney spot)...")
    df_ths = pd.DataFrame(columns=["code", "name", "amount"])
    try:
        df_em = ak.fund_etf_spot_em()
        code_col = next((c for c in df_em.columns if "代码" in c), None)
        name_col = next((c for c in df_em.columns if "名称" in c or "简称" in c), None)
        amt_col  = next((c for c in df_em.columns if "成交额" in c), None)
        if code_col and name_col:
            df_ths = pd.DataFrame({
                "code":   df_em[code_col].astype(str).str.zfill(6),
                "name":   df_em[name_col].astype(str),
                "amount": pd.to_numeric(df_em[amt_col], errors="coerce").fillna(0)
                          if amt_col else 0.0,
            })
            print(f"  EastMoney ETF list: {len(df_ths)} entries")
        else:
            raise ValueError(f"unexpected columns: {list(df_em.columns)}")
    except Exception as e:
        print(f"  [WARN] EastMoney ETF list failed ({e}); falling back to THS...")
        try:
            df_ths = ak.fund_etf_category_ths()
            df_ths = df_ths.rename(columns={"基金代码": "code", "基金名称": "name"})
            df_ths["code"] = df_ths["code"].astype(str).str.zfill(6)
            df_ths["amount"] = 0.0
            df_ths = df_ths[["code", "name", "amount"]]
            print(f"  THS ETF list: {len(df_ths)} entries")
        except Exception as e2:
            print(f"  [ERR] THS ETF list also failed: {e2}")

    print("Fetching ETF spot quotes (Sina) for liquidity ranking...")
    try:
        df_sina = ak.fund_etf_category_sina()
        # Sina codes look like "sh510050" / "sz159915"
        df_sina["code"] = df_sina["代码"].astype(str).str.replace(
            r"^(sh|sz)", "", regex=True).str.zfill(6)
        df_sina = df_sina.rename(columns={"名称": "name", "成交额": "amount"})[
            ["code", "name", "amount"]]
        df_sina["amount"] = pd.to_numeric(df_sina["amount"], errors="coerce").fillna(0)
    except Exception as e:
        print(f"  [ERR] Sina ETF quotes: {e}")
        df_sina = pd.DataFrame(columns=["code", "name", "amount"])

    # Merge: THS provides the full list, Sina adds liquidity data
    df = df_ths.copy()
    sina_map = dict(zip(df_sina["code"], df_sina["amount"]))
    df["amount"] = df["code"].map(sina_map).fillna(0)
    # For codes only in Sina (not in THS), add them too
    sina_only = df_sina[~df_sina["code"].isin(df["code"].values)]
    df = pd.concat([df, sina_only], ignore_index=True)

    # Filter to exchange-listed T+0 ETFs only
    def _is_exchange_etf(code: str, name: str = "") -> bool:
        """True for genuine T+0 ETFs. Excludes LOFs (T+1) by requiring 'ETF' in name."""
        c = str(code).zfill(6)
        in_clear  = c.startswith("51") or c.startswith("159") or c.startswith("588")
        in_ambig  = c.startswith("16")   # 16xxxx mixes ETF and LOF
        if not (in_clear or in_ambig):
            return False
        if name:
            return "ETF" in name.upper()
        return in_clear   # without name, trust only unambiguous ranges

    # If data came from fund_etf_spot_em (authoritative), skip code-range filter —
    # every entry is already a genuine exchange ETF. Otherwise apply conservative filter.
    _authoritative = len(df_ths) > 0 and df_ths is not None
    if not _authoritative:
        df = df[df.apply(lambda r: _is_exchange_etf(r["code"], r["name"]), axis=1)].copy()
    print(f"  Exchange ETFs after filter: {len(df)}")

    # Classify by theme and extract index key
    df["theme"]     = df["name"].apply(_etf_theme)
    df["index_key"] = df["name"].apply(_etf_index_key)

    # Sort by liquidity descending
    df = df.sort_values("amount", ascending=False)

    # Deduplication pass 1: same index_key → keep only the most liquid one
    seen_index_keys: set[str] = set()
    deduped: list[dict] = []
    for _, row in df.iterrows():
        key = (row["theme"], row["index_key"])
        if key in seen_index_keys:
            continue
        seen_index_keys.add(key)
        deduped.append(row.to_dict())

    # Deduplication pass 2: max max_per_theme per theme
    theme_count: dict[str, int] = {}
    watchlist: list[dict] = []
    for row in deduped:          # already sorted by amount desc
        theme = row["theme"]
        if theme_count.get(theme, 0) >= max_per_theme:
            continue
        theme_count[theme] = theme_count.get(theme, 0) + 1
        watchlist.append({
            "code":       row["code"],
            "name":       row["name"],
            "shares":     0,
            "cost_price": 0,
        })

    print(f"  ETF watchlist: {len(watchlist)} ETFs across "
          f"{len(theme_count)} themes (max {max_per_theme}/theme)")
    return watchlist


# ── Watchlist builder ──────────────────────────────────────────────────────────

def build_watchlist(
    sector_map: dict[str, list[tuple[str, str]]],
    top_n_hot: int = 200,
    max_per_sector: int = 10,
    max_total: int = 500,
) -> list[str]:
    """
    Build intraday watchlist from EastMoney / 同花顺 hot rank.

    Steps:
      1. Fetch top_n_hot stocks from hot-rank API.
      2. Cross-reference with sector_map to determine each stock's sector.
         Stocks with no known sector are placed in a catch-all bucket.
      3. Apply deduplication: keep at most max_per_sector per sector,
         preserving hot-rank order (most popular first).
      4. Return up to max_total stock codes.
    """
    print("Fetching hot-rank list...")
    hot_codes: list[str] = []
    try:
        df_hot = ak.stock_hot_rank_em()
        # Column may be '代码' or '股票代码' depending on akshare version
        code_col = next((c for c in df_hot.columns if "代码" in c), None)
        if code_col:
            hot_codes = [str(r).zfill(6) for r in df_hot[code_col].head(top_n_hot)]
            print(f"  Hot rank fetched: {len(hot_codes)} stocks")
        else:
            print(f"  [WARN] Unknown columns: {list(df_hot.columns)}")
    except Exception as e:
        print(f"  [ERR] Hot rank fetch failed: {e}")

    if not hot_codes:
        return []

    # Build reverse map: code -> sector name (first match wins)
    code_to_sector: dict[str, str] = {}
    for sector, stocks in sector_map.items():
        for code, _ in stocks:
            if code not in code_to_sector:
                code_to_sector[code] = sector

    # Deduplication: max max_per_sector per sector
    sector_count: dict[str, int] = {}
    watchlist: list[str] = []
    for code in hot_codes:
        if len(watchlist) >= max_total:
            break
        sector = code_to_sector.get(code, "__other__")
        if sector_count.get(sector, 0) >= max_per_sector:
            continue
        sector_count[sector] = sector_count.get(sector, 0) + 1
        watchlist.append(code)

    print(f"  Watchlist: {len(watchlist)} stocks "
          f"(from {len(hot_codes)} hot, max {max_per_sector}/sector)")
    return watchlist


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Build StockSage screener universe")
    parser.add_argument("--preview", action="store_true",
                        help="只打印统计，不写入文件")
    parser.add_argument("--top-n",  type=int, default=15,
                        help="每板块按成交额取前 N 只（默认 15）")
    args = parser.parse_args()

    universe, sector_map = fetch_universe(args.top_n)
    print(f"\nTotal unique stocks: {len(universe)}")

    if args.preview:
        for group, sectors in GROUPS.items():
            for s in sectors:
                stocks = sector_map.get(s, [])
                if stocks:
                    print(f"  {s}: {[c for c, _ in stocks[:5]]}...")
        return

    # Build stock watchlist from hot rank (max 10 per sector, max 500 total)
    watchlist = build_watchlist(sector_map, max_per_sector=10)

    # Build ETF watchlist (all exchange ETFs, max 3 per theme, same-index dedup)
    etf_watchlist = build_etf_watchlist(max_per_theme=3)

    # Update alert_config.json
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)
    config["screener_universe"] = universe
    config["watchlist"]         = watchlist
    config["etf_watchlist"]     = etf_watchlist
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"Config written to {CONFIG_PATH}  "
          f"(universe={len(universe)}, watchlist={len(watchlist)}, "
          f"etf_watchlist={len(etf_watchlist)})")

    # Update screener_universe.md
    write_markdown(sector_map, len(universe))


if __name__ == "__main__":
    main()
