#!/usr/bin/env python3
"""
Build screener_universe for alert_config.json.

Strategy per sector:
  - top 3 by 成交额   (龙头/活跃大盘股)
  - top 12 by 换手率  (弹性中小盘)
  Combined, deduped → 15 per sector.

Usage:
  python scripts/build_universe.py            # writes alert_config.json in-place
  python scripts/build_universe.py --preview  # just print the list
"""

from __future__ import annotations
import argparse, json, os, sys, time
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import akshare as ak

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(_ROOT, "alert_config.json")

SECTORS = [
    # 食品饮料
    "白酒Ⅱ", "啤酒", "调味发酵品Ⅱ", "乳品", "食品加工", "休闲食品", "零食", "软饮料",
    # 金融
    "银行Ⅱ", "保险Ⅱ", "证券Ⅱ", "多元金融", "金融信息服务",
    # 医药医疗
    "化学制药", "生物制品", "中药Ⅱ", "医疗器械", "医疗服务", "医疗研发外包",
    "体外诊断", "血液制品", "疫苗", "医疗耗材",
    "医疗美容", "医美服务", "医美耗材",
    # 新能源
    "锂电池", "电池", "光伏设备", "光伏主材", "光伏电池组件",
    "风电设备", "风电整机", "核力发电", "逆变器",
    # 半导体
    "半导体", "数字芯片设计", "模拟芯片设计",
    "半导体设备", "集成电路制造", "集成电路封测",
    # 软件/IT/AI
    "软件开发", "计算机", "垂直应用软件", "横向通用软件",
    # 消费电子/家电
    "消费电子", "品牌消费电子", "家用电器", "白色家电", "面板",
    # 汽车
    "乘用车", "电动乘用车", "汽车零部件", "汽车电子电气系统",
    # 军工
    "国防军工", "航天装备Ⅱ", "航空装备Ⅱ", "航海装备Ⅱ", "地面兵装Ⅱ", "军工电子Ⅱ",
    # 地产/建材/建筑
    "房地产开发", "住宅开发", "建筑材料", "装修建材",
    "玻璃玻纤", "房屋建设Ⅱ", "工程机械", "轨交设备Ⅱ", "定制家居",
    # 化工
    "化学原料", "基础化工", "化学制品", "化学纤维", "氟化工",
    # 金属/资源
    "有色金属", "黄金", "稀土", "铜", "铝", "钢铁", "小金属", "磁性材料",
    # 能源
    "煤炭开采", "油气开采Ⅱ", "石油石化", "火力发电", "水力发电", "燃气Ⅱ",
    # 农业
    "养殖业", "生猪养殖", "农药", "磷肥及磷化工", "种子", "饲料", "农产品加工",
    # 物流/交通
    "快递", "港口", "航运", "航空运输", "铁路运输", "仓储物流",
    # 传媒/娱乐/消费
    "传媒", "游戏Ⅱ", "旅游及景区", "酒店", "百货",
    "纺织服饰", "品牌化妆品", "美容护理",
    # 通信/光学
    "通信", "通信设备", "光学光电子", "通信线缆及配套",
    # 自动化/机器人/机械
    "机器人", "自动化设备", "机床工具", "激光设备", "仪器仪表", "专用设备",
    # 电子元件
    "印制电路板", "LED", "元件", "被动元件", "光学元件",
    # 电力设备/储能
    "综合电力设备商", "电网设备", "输变电设备", "电力设备", "燃料电池",
    # 其他
    "诊断服务", "教育", "环保", "水务及水治理",
    "包装印刷", "物业管理", "互联网电商",
]


def fetch_universe(top_n: int = 15) -> list[str]:
    print("Fetching industry board list...")
    boards = ak.stock_board_industry_name_em()
    valid = set(boards["板块名称"].tolist())

    all_codes: set[str] = set()

    for sector in SECTORS:
        if sector not in valid:
            print(f"  [SKIP] {sector}")
            continue
        try:
            df = ak.stock_board_industry_cons_em(symbol=sector)
            if df.empty:
                continue
            df["换手率"] = df["换手率"].fillna(0)
            df["成交额"] = df["成交额"].fillna(0)

            # 龙头3（成交额最大）+ 活跃中小盘12（换手率最高），合并去重取前 top_n
            top_vol  = df.nlargest(3, "成交额")["代码"].tolist()
            top_turn = df.nlargest(top_n - 3, "换手率")["代码"].tolist()
            picked = list(dict.fromkeys(top_vol + top_turn))[:top_n]
            new = [str(c).zfill(6) for c in picked if str(c).zfill(6) not in all_codes]
            all_codes.update(new)
            print(f"  [OK] {sector}: +{len(new):2d}  total={len(all_codes)}")
            time.sleep(0.2)
        except Exception as e:
            print(f"  [ERR] {sector}: {e}")

    return sorted(all_codes)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--preview", action="store_true", help="只打印，不写入配置文件")
    parser.add_argument("--top-n", type=int, default=15, help="每板块取前 N 只（默认15）")
    args = parser.parse_args()

    universe = fetch_universe(args.top_n)
    print(f"\nTotal unique stocks: {len(universe)}")

    if args.preview:
        print(json.dumps(universe, ensure_ascii=False, indent=2))
        return

    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    config["screener_universe"] = universe

    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print(f"Written to {CONFIG_PATH}")


if __name__ == "__main__":
    main()
