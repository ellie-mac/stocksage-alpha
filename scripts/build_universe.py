#!/usr/bin/env python3
"""
Build screener_universe for alert_config.json + update screener_universe.md.

Selection strategy per sector:
  Sort all constituent stocks by 成交额 (trading value) descending, take top N.
  成交额 = price × volume — a stable proxy for both market cap and activity level.
  Naturally picks mega-cap blue chips first, then active mid-caps, while
  excluding illiquid micro-cap junk at the bottom.

  Boards with ≤ top_n stocks: take all (no filtering needed).

Usage:
  python scripts/build_universe.py            # update alert_config.json + md
  python scripts/build_universe.py --preview  # dry-run, print counts only
  python scripts/build_universe.py --top-n 20 # larger per-sector budget
"""

from __future__ import annotations
import argparse, datetime, json, os, sys, time
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import akshare as ak

_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(_ROOT, "alert_config.json")
MD_PATH     = os.path.join(_ROOT, "screener_universe.md")

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

            # Sort by 成交额 descending; take top_n (or all if board is small)
            df_sorted = df.sort_values("成交额", ascending=False).head(top_n)

            picked: list[tuple[str, str]] = []
            for _, row in df_sorted.iterrows():
                code = str(row["代码"]).zfill(6)
                name = str(row["名称"])
                picked.append((code, name))
                all_codes.add(code)

            sector_map[sector] = picked
            new_count = sum(1 for c, _ in picked if c in all_codes)
            print(f"  [OK] {sector}: {len(picked):2d} stocks  total={len(all_codes)}")
            time.sleep(0.2)
        except Exception as e:
            print(f"  [ERR] {sector}: {e}")

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
        "> **选股策略**: 每板块按成交额降序取前 N 只。",
        "> 成交额 = 价格 × 成交量，是市值和活跃度的综合代理，天然包含大盘龙头和活跃中盘股，",
        "> 同时过滤掉底部流动性极差的微盘票。具体筛选由因子模型完成，此处仅定义扫描范围。",
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

    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Markdown written to {MD_PATH}  ({len(lines)} lines)")


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

    # Update alert_config.json
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)
    config["screener_universe"] = universe
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"Config written to {CONFIG_PATH}")

    # Update screener_universe.md
    write_markdown(sector_map, len(universe))


if __name__ == "__main__":
    main()
