#!/usr/bin/env python3
"""
概念板块轮动评分器 — 多因子综合打分，辅助判断强势概念轮入/轮出信号。

因子:
  1. 今日涨跌幅 (f3)           — 即时动量
  2. 主力净流入 (f62)          — 聪明钱方向（可靠性有限，降权）
  3. 上涨家数占比 (f104/(f104+f105)) — 板块广度（硬指标不可伪造）
  4. 近3日累计涨幅 (kline)     — 趋势延续性
  5. 超大单净流入 (f164)       — 机构级别资金（同主力，降权）
  6. 换手率 (f8)              — 资金活跃度/市场关注度
  7. 龙头溢价率 (f136/f3)     — 负向惩罚：龙头独秀说明跟风弱

评分逻辑:
  - 各因子在全板块中排名 → 百分位 (0~1)
  - 加权合成总分:
      动量 15% + 主力流入 15% + 广度 20% + 3日趋势 20% +
      超大单 10% + 换手率 10% + 龙头溢价 -10%（负向惩罚）
  - 附加信号:
      🔥 强势轮入: 总分Top10 且 3日趋势>0
      🌱 蓄势待发: 今日涨幅<2% 但 主力净流入Top20 且 超大单为正
      ⚠️ 获利了结: 3日涨幅>10% 但 今日主力净流出

用法:
    python -X utf8 src/concept/concept_rotation.py [--top 15] [--no-proxy] [--json]

网络: push2.eastmoney.com 需要国内IP，海外VM通过 mihomo 代理（127.0.0.1:7890）。
"""
from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent.parent
DATA = ROOT / "data"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# 权重配置（正向因子 + 负向惩罚因子，绝对值之和=1.0）
WEIGHTS = {
    "momentum": 0.15,       # 今日涨幅 — 即时动量，略降避免追高
    "net_inflow": 0.15,     # 主力净流入 — 东财数据可靠性有限，降权
    "breadth": 0.20,        # 上涨家数占比 — 硬指标不可伪造
    "trend_3d": 0.20,       # 近3日累计涨幅 — 趋势延续性
    "big_order": 0.10,      # 超大单净流入 — 同主力，降权
    "turnover": 0.10,       # 换手率 — 资金活跃度/关注度
    "leader_premium": -0.10,  # 龙头溢价率 — 负向惩罚，龙头独秀=不健康
}

NOISE_KEYWORDS = [
    "昨日", "热股", "多板", "百元", "千元", "融资融券",
    "沪股通", "深股通", "MSCI", "HS300", "中证", "转债标的",
    "涨停", "连板", "首板", "打板",
]

# API fields
BLOCK_FIELDS = "f2,f3,f4,f8,f12,f14,f62,f104,f105,f109,f128,f136,f140,f164,f166"


def _get_session(use_proxy: bool = True) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    if use_proxy:
        proxy = "http://127.0.0.1:7890"
        s.proxies = {"http": proxy, "https": proxy}
    return s


def _test_push2(session: requests.Session) -> bool:
    try:
        r = session.get(
            "https://push2.eastmoney.com/api/qt/clist/get",
            params={"fid": "f3", "po": "1", "pz": "1", "pn": "1",
                    "np": "1", "fltt": "2", "invt": "2",
                    "fs": "m:90+t:3", "fields": "f14"},
            timeout=10,
        )
        return r.status_code == 200 and r.text.startswith("{")
    except Exception:
        return False


def _fetch_all_concepts(session: requests.Session) -> list[dict]:
    """获取全部概念板块实时多因子数据"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "fid": "f3", "po": "1", "pz": "500", "pn": "1",
        "np": "1", "fltt": "2", "invt": "2",
        "fs": "m:90+t:3",
        "fields": BLOCK_FIELDS,
    }
    r = session.get(url, params=params, timeout=15)
    r.raise_for_status()
    items = r.json().get("data", {}).get("diff", [])
    results = []
    for item in items:
        name = item.get("f14", "")
        if any(kw in name for kw in NOISE_KEYWORDS):
            continue
        up = item.get("f104", 0) or 0
        down = item.get("f105", 0) or 0
        total = up + down
        results.append({
            "code": item.get("f12", ""),
            "name": name,
            "pct_chg": item.get("f3", 0) or 0,
            "net_inflow": item.get("f62", 0) or 0,
            "up_count": up,
            "down_count": down,
            "breadth": round(up / total, 3) if total > 0 else 0.5,
            "big_order": item.get("f164", 0) or 0,
            "turnover": item.get("f8", 0) or 0,
            "leader_name": item.get("f128", ""),
            "leader_code": item.get("f140", ""),
            "leader_pct": item.get("f136", 0) or 0,
            # 龙头溢价率: 龙头涨幅 / 板块涨幅，越高说明龙头独秀、跟风弱
            "leader_premium": (
                round((item.get("f136", 0) or 0) / (item.get("f3", 0) or 0.01), 2)
                if (item.get("f3", 0) or 0) > 0.5 else 1.0
            ),
        })
    return results


def _fetch_kline_3d(session: requests.Session, code: str) -> float | None:
    """获取近3个交易日累计涨幅"""
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": f"90.{code}",
        "fields1": "f1",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101", "fqt": "1",
        "beg": (datetime.now() - timedelta(days=10)).strftime("%Y%m%d"),
        "end": datetime.now().strftime("%Y%m%d"),
        "lmt": "5",
    }
    try:
        r = session.get(url, params=params, timeout=8)
        if r.status_code != 200:
            return None
        klines = r.json().get("data", {}).get("klines", [])
        if len(klines) < 3:
            return None
        # 取最近3天涨幅之和
        total = 0.0
        for k in klines[-3:]:
            p = k.split(",")
            if len(p) >= 9 and p[8]:
                total += float(p[8])
        return round(total, 2)
    except Exception:
        return None


def _percentile_rank(values: list[float]) -> list[float]:
    """将一组值转换为百分位排名 (0~1)"""
    n = len(values)
    if n == 0:
        return []
    indexed = sorted(enumerate(values), key=lambda x: x[1])
    ranks = [0.0] * n
    for rank, (idx, _) in enumerate(indexed):
        ranks[idx] = rank / (n - 1) if n > 1 else 0.5
    return ranks


def concept_rotation(top_n: int = 15, use_proxy: bool = True) -> dict:
    """
    概念板块轮动评分。
    返回 {
        "timestamp": str,
        "total_concepts": int,
        "strong_entry": [...],   # 强势轮入
        "preparing": [...],      # 蓄势待发
        "take_profit": [...],    # 获利了结
        "full_ranking": [...],   # 全部排名
    }
    """
    session = _get_session(use_proxy)

    if not _test_push2(session):
        if use_proxy:
            session = _get_session(use_proxy=False)
        if not _test_push2(session):
            print("[rotation] ❌ push2.eastmoney.com 不可达")
            return {}

    # Step 1: 获取实时多因子数据
    concepts = _fetch_all_concepts(session)
    if not concepts:
        print("[rotation] ❌ 获取概念板块数据失败")
        return {}

    print(f"[rotation] 获取 {len(concepts)} 个概念板块, 拉取3日K线...")

    # Step 2: 批量获取3日趋势
    batch_size = 20
    trend_map: dict[str, float] = {}
    codes = [(c["code"], c["name"]) for c in concepts]

    for i in range(0, len(codes), batch_size):
        batch = codes[i:i + batch_size]
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(_fetch_kline_3d, session, code): name for code, name in batch}
            for f in as_completed(futs):
                name = futs[f]
                result = f.result()
                if result is not None:
                    trend_map[name] = result
        if i + batch_size < len(codes):
            time.sleep(0.2)

    # Fill missing trends with 0
    for c in concepts:
        c["trend_3d"] = trend_map.get(c["name"], 0.0)

    # Step 3: 百分位排名
    momentum_vals = [c["pct_chg"] for c in concepts]
    inflow_vals = [c["net_inflow"] for c in concepts]
    breadth_vals = [c["breadth"] for c in concepts]
    trend_vals = [c["trend_3d"] for c in concepts]
    bigorder_vals = [c["big_order"] for c in concepts]
    turnover_vals = [c["turnover"] for c in concepts]
    premium_vals = [c["leader_premium"] for c in concepts]

    momentum_pct = _percentile_rank(momentum_vals)
    inflow_pct = _percentile_rank(inflow_vals)
    breadth_pct = _percentile_rank(breadth_vals)
    trend_pct = _percentile_rank(trend_vals)
    bigorder_pct = _percentile_rank(bigorder_vals)
    turnover_pct = _percentile_rank(turnover_vals)
    premium_pct = _percentile_rank(premium_vals)  # 越高=龙头越独秀=越差

    # Step 4: 加权合成（leader_premium 为负向因子）
    for i, c in enumerate(concepts):
        score = (
            WEIGHTS["momentum"] * momentum_pct[i] +
            WEIGHTS["net_inflow"] * inflow_pct[i] +
            WEIGHTS["breadth"] * breadth_pct[i] +
            WEIGHTS["trend_3d"] * trend_pct[i] +
            WEIGHTS["big_order"] * bigorder_pct[i] +
            WEIGHTS["turnover"] * turnover_pct[i] +
            WEIGHTS["leader_premium"] * premium_pct[i]  # 负权重：溢价率高则扣分
        )
        c["score"] = round(score * 100, 1)
        c["momentum_pct"] = round(momentum_pct[i] * 100, 1)
        c["inflow_pct"] = round(inflow_pct[i] * 100, 1)
        c["breadth_pct"] = round(breadth_pct[i] * 100, 1)
        c["trend_pct"] = round(trend_pct[i] * 100, 1)
        c["bigorder_pct"] = round(bigorder_pct[i] * 100, 1)
        c["turnover_pct"] = round(turnover_pct[i] * 100, 1)
        c["premium_pct"] = round(premium_pct[i] * 100, 1)

    # Sort by composite score
    concepts.sort(key=lambda x: -x["score"])

    # Step 5: 分类信号
    strong_entry = []   # 🔥 强势轮入
    preparing = []      # 🌱 蓄势待发
    take_profit = []    # ⚠️ 获利了结

    inflow_top20_threshold = sorted(inflow_vals, reverse=True)[min(19, len(inflow_vals) - 1)]

    for c in concepts:
        # 强势轮入: Top10综合分 + 3日趋势为正
        if c["score"] >= concepts[min(9, len(concepts) - 1)]["score"] and c["trend_3d"] > 0:
            strong_entry.append(c)

        # 蓄势待发: 今日涨幅<2% 但主力净流入Top20 且超大单为正
        if c["pct_chg"] < 2.0 and c["net_inflow"] >= inflow_top20_threshold and c["big_order"] > 0:
            preparing.append(c)

        # 获利了结: 3日涨幅>10% 但今日主力净流出
        if c["trend_3d"] > 10.0 and c["net_inflow"] < 0:
            take_profit.append(c)

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_concepts": len(concepts),
        "strong_entry": strong_entry[:top_n],
        "preparing": preparing[:10],
        "take_profit": take_profit[:10],
        "full_ranking": concepts[:top_n],
    }


def _format_inflow(val: float) -> str:
    """格式化资金流（亿）"""
    if abs(val) >= 1e8:
        return f"{val / 1e8:.1f}亿"
    elif abs(val) >= 1e4:
        return f"{val / 1e4:.0f}万"
    return f"{val:.0f}"


def main():
    parser = argparse.ArgumentParser(description="概念板块轮动评分器")
    parser.add_argument("--top", type=int, default=15, help="输出排名前N（默认15）")
    parser.add_argument("--no-proxy", action="store_true", help="不使用代理")
    parser.add_argument("--json", action="store_true", help="输出JSON到 data/concept_rotation.json")
    args = parser.parse_args()

    result = concept_rotation(top_n=args.top, use_proxy=not args.no_proxy)
    if not result:
        return

    if args.json:
        out_path = DATA / "concept_rotation.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str),
                            encoding="utf-8")
        print(f"[rotation] 已写入 {out_path}")

    ts = result["timestamp"]
    print(f"\n{'='*85}")
    print(f"  概念板块轮动评分  {ts}  (共{result['total_concepts']}个概念)")
    print(f"  权重: 动量{int(WEIGHTS['momentum']*100)}% | 主力流入{int(WEIGHTS['net_inflow']*100)}% | "
          f"广度{int(WEIGHTS['breadth']*100)}% | 3日趋势{int(WEIGHTS['trend_3d']*100)}% | "
          f"超大单{int(WEIGHTS['big_order']*100)}% | 换手{int(WEIGHTS['turnover']*100)}% | "
          f"龙头溢价{int(WEIGHTS['leader_premium']*100)}%")
    print(f"{'='*85}")

    # 强势轮入
    if result["strong_entry"]:
        print(f"\n🔥 强势轮入 (综合Top10 + 3日趋势向上)")
        print(f"{'─'*85}")
        print(f"  {'概念':<12} {'综合分':>5} {'今日':>6} {'3日':>6} {'主力流入':>10} {'广度':>5} {'龙头':<10}")
        print(f"{'─'*85}")
        for c in result["strong_entry"]:
            print(f"  {c['name']:<12} {c['score']:>5.1f} {c['pct_chg']:>+5.2f}% "
                  f"{c['trend_3d']:>+5.2f}% {_format_inflow(c['net_inflow']):>10} "
                  f"{c['breadth']*100:>4.0f}% {c['leader_name']:<10}")

    # 蓄势待发
    if result["preparing"]:
        print(f"\n🌱 蓄势待发 (涨幅<2% + 主力流入Top20 + 超大单为正)")
        print(f"{'─'*85}")
        for c in result["preparing"]:
            print(f"  {c['name']:<12} 分:{c['score']:>5.1f} 涨:{c['pct_chg']:>+5.2f}% "
                  f"主力:{_format_inflow(c['net_inflow'])} 超大单:{_format_inflow(c['big_order'])}")

    # 获利了结
    if result["take_profit"]:
        print(f"\n⚠️  获利了结 (3日涨>10% + 今日主力流出)")
        print(f"{'─'*85}")
        for c in result["take_profit"]:
            print(f"  {c['name']:<12} 3日:{c['trend_3d']:>+5.2f}% "
                  f"今日流出:{_format_inflow(c['net_inflow'])}")

    # 全排名
    print(f"\n📊 综合评分 Top{args.top}")
    print(f"{'─'*85}")
    print(f"  {'#':>2} {'概念':<12} {'分数':>5} {'今日%':>6} {'3日%':>6} "
          f"{'主力流入':>10} {'广度':>5} {'涨/跌':>5} {'龙头':<8}")
    print(f"{'─'*85}")
    for i, c in enumerate(result["full_ranking"], 1):
        print(f"  {i:>2} {c['name']:<12} {c['score']:>5.1f} {c['pct_chg']:>+5.2f} "
              f"{c['trend_3d']:>+5.2f} {_format_inflow(c['net_inflow']):>10} "
              f"{c['breadth']*100:>4.0f}% {c['up_count']:>2}/{c['down_count']:<2} "
              f"{c['leader_name']:<8}")
    print(f"{'='*85}")


if __name__ == "__main__":
    main()
