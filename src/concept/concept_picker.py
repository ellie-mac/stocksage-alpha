#!/usr/bin/env python3
"""
概念板块AI选股器 — 从轮动评分Top概念中精选入场标的。

流程:
  1. 调用 concept_rotation.py 获取 Top N 概念
  2. 拉取每个概念的成分股数据（价格、涨幅、换手、市值等）
  3. 调用 AI (Copilot CLI Claude Opus 4.7) 审核，每个概念选出5只

用法:
    python -X utf8 src/concept/concept_picker.py [--mode evening|intraday] [--top-concepts 5] [--picks-per 5]

网络: push2.eastmoney.com 需国内IP, 海外VM通过 mihomo 代理.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent.parent
DATA = ROOT / "data"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# 成分股字段: f2=价格 f3=涨幅 f5=成交量 f6=成交额 f7=振幅 f8=换手 f9=PE
# f10=量比 f12=代码 f14=名称 f15=最高 f16=最低 f17=开盘 f20=总市值 f21=流通市值
STOCK_FIELDS = "f2,f3,f5,f6,f7,f8,f10,f12,f14,f15,f16,f17,f20,f21"


def _get_session(use_proxy: bool = True) -> requests.Session:
    if use_proxy:
        from concept.relay_session import make_relay_session
        return make_relay_session(headers=HEADERS)
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_concept_stocks(session: requests.Session, block_code: str,
                         top_n: int = 30) -> list[dict]:
    """获取概念板块内个股详情（按涨幅降序）"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "fid": "f3", "po": "1",
        "pz": str(top_n), "pn": "1",
        "np": "1", "fltt": "2", "invt": "2",
        "fs": f"b:{block_code}",
        "fields": STOCK_FIELDS,
    }
    try:
        r = session.get(url, params=params, timeout=15)
        items = r.json().get("data", {}).get("diff", [])
        stocks = []
        for s_item in items:
            price = s_item.get("f2", 0) or 0
            if price <= 0:
                continue
            stocks.append({
                "code": s_item.get("f12", ""),
                "name": s_item.get("f14", ""),
                "price": price,
                "pct_chg": s_item.get("f3", 0) or 0,
                "volume": s_item.get("f5", 0) or 0,
                "amount": s_item.get("f6", 0) or 0,
                "amplitude": s_item.get("f7", 0) or 0,
                "turnover": s_item.get("f8", 0) or 0,
                "volume_ratio": s_item.get("f10", 0) or 0,
                "open": s_item.get("f17", 0) or 0,
                "high": s_item.get("f15", 0) or 0,
                "low": s_item.get("f16", 0) or 0,
                "market_cap": s_item.get("f20", 0) or 0,
                "float_cap": s_item.get("f21", 0) or 0,
            })
        return stocks
    except Exception as e:
        print(f"  [picker] 获取成分股失败: {e}")
        return []


def _build_ai_prompt(concept_name: str, concept_data: dict,
                     stocks: list[dict], picks_per: int, mode: str) -> str:
    """构建AI审核的prompt"""
    mode_cn = "盘后复盘（明日买入）" if mode == "evening" else "盘中实时（当天追入）"

    # Format stock table
    stock_lines = []
    for s in stocks[:25]:  # 最多给25只让AI选
        cap_yi = s["market_cap"] / 1e8 if s["market_cap"] else 0
        amount_yi = s["amount"] / 1e8 if s["amount"] else 0
        stock_lines.append(
            f"  {s['name']:<8} {s['code']} 价:{s['price']:.2f} "
            f"涨:{s['pct_chg']:+.2f}% 换手:{s['turnover']:.1f}% "
            f"量比:{s['volume_ratio']:.2f} 振幅:{s['amplitude']:.1f}% "
            f"成交:{amount_yi:.1f}亿 市值:{cap_yi:.0f}亿"
        )
    stock_table = "\n".join(stock_lines)

    prompt = f"""你是A股短线交易专家。当前任务：从概念板块成分股中精选{picks_per}只最适合入场的标的。

## 场景
模式: {mode_cn}
概念: {concept_name}
概念今日涨幅: {concept_data.get('pct_chg', 0):+.2f}%
概念3日趋势: {concept_data.get('trend_3d', 0):+.2f}%
概念广度(上涨占比): {concept_data.get('breadth', 0)*100:.0f}%
概念量比: {concept_data.get('volume_ratio', 0):.2f}

## 成分股数据
{stock_table}

## 选股标准
1. 位置: 优选刚启动或中继（非高位加速末端）; 今日涨幅适中(2-7%)优于涨停板(流动性差)
2. 辨识度: 该概念的核心标的（主营相关度高）优于边缘蹭概念股
3. 流动性: 成交额>1亿, 避免小票流动性陷阱
4. 量价配合: 量比>1 + 换手活跃 = 资金认可
5. 市值适中: 50-500亿为佳（太小波动大风险高, 太大弹性不足）
6. 风险排除: 连续涨停>2天的回避（接力风险）, ST/*ST回避

## 输出格式（严格JSON）
返回一个JSON数组, 恰好{picks_per}个元素:
[
  {{"code": "002185", "name": "华天科技", "reason": "CPO核心封装标的,换手活跃,位置中继"}},
  ...
]

只返回JSON, 不要其他文字。"""
    return prompt


def ai_review(prompt: str) -> list[dict]:
    """调用 Copilot CLI (Claude Opus 4.7) 进行AI审核"""
    try:
        # Use copilot CLI via subprocess
        result = subprocess.run(
            ["copilot", "-p", prompt, "--model", "claude-opus-4.7", "-s", "--yolo"],
            capture_output=True,
            text=True,
            timeout=120,
            encoding="utf-8",
        )
        output = result.stdout.strip()

        # Extract JSON from output (might have markdown fences)
        if "```json" in output:
            output = output.split("```json")[1].split("```")[0].strip()
        elif "```" in output:
            output = output.split("```")[1].split("```")[0].strip()

        # Find JSON array
        start = output.find("[")
        end = output.rfind("]") + 1
        if start >= 0 and end > start:
            return json.loads(output[start:end])
        return []
    except subprocess.TimeoutExpired:
        print("  [picker] AI审核超时")
        return []
    except Exception as e:
        print(f"  [picker] AI审核失败: {e}")
        return []


def concept_pick(mode: str = "evening", top_concepts: int = 5,
                 picks_per: int = 5, use_proxy: bool = True) -> dict:
    """
    完整流程: 轮动评分 → 拉成分股 → AI选股
    """
    # Step 1: 获取轮动评分
    from concept_rotation import concept_rotation
    rotation_result = concept_rotation(mode=mode, top_n=top_concepts,
                                       use_proxy=use_proxy)
    if not rotation_result:
        return {}

    top_list = rotation_result["full_ranking"][:top_concepts]
    print(f"\n[picker] Top{top_concepts}概念: "
          + " | ".join(c["name"] for c in top_list))

    session = _get_session(use_proxy)

    # Step 2 & 3: 对每个概念拉成分股 + AI审核
    all_picks = []
    for concept in top_list:
        print(f"\n[picker] 处理: {concept['name']} ({concept['code']})")

        # 拉成分股
        stocks = fetch_concept_stocks(session, concept["code"], top_n=30)
        if not stocks:
            print(f"  [picker] 跳过 {concept['name']}: 无成分股数据")
            continue

        print(f"  [picker] 获取到 {len(stocks)} 只成分股, 调用AI审核...")

        # AI审核
        prompt = _build_ai_prompt(concept["name"], concept, stocks, picks_per, mode)
        picks = ai_review(prompt)

        if not picks:
            print(f"  [picker] AI审核无结果, 回退到规则选股")
            picks = _fallback_rule_pick(stocks, picks_per)

        # Enrich picks with price data
        stock_map = {s["code"]: s for s in stocks}
        enriched_picks = []
        for p in picks[:picks_per]:
            code = p.get("code", "")
            stock_info = stock_map.get(code, {})
            enriched_picks.append({
                "code": code,
                "name": p.get("name", stock_info.get("name", "")),
                "reason": p.get("reason", ""),
                "price": stock_info.get("price", 0),
                "pct_chg": stock_info.get("pct_chg", 0),
                "turnover": stock_info.get("turnover", 0),
                "volume_ratio": stock_info.get("volume_ratio", 0),
                "market_cap": stock_info.get("market_cap", 0),
            })

        all_picks.append({
            "concept_name": concept["name"],
            "concept_code": concept["code"],
            "concept_score": concept["score"],
            "concept_pct_chg": concept["pct_chg"],
            "concept_trend_3d": concept.get("trend_3d", 0),
            "picks": enriched_picks,
            "all_stocks_count": len(stocks),
        })

        time.sleep(0.5)  # Rate limiting between concepts

    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "date": datetime.now().strftime("%Y-%m-%d"),
        "mode": mode,
        "mode_cn": "盘后复盘" if mode == "evening" else "盘中实时",
        "top_concepts": top_concepts,
        "picks_per_concept": picks_per,
        "concepts": all_picks,
    }

    return result


def _fallback_rule_pick(stocks: list[dict], n: int) -> list[dict]:
    """AI不可用时的规则回退选股"""
    candidates = []
    for s in stocks:
        # 基本过滤
        if s["price"] <= 0:
            continue
        if s.get("amount", 0) < 1e8:  # 成交额<1亿排除
            continue
        cap = s.get("market_cap", 0)
        if cap > 0 and (cap < 30e8 or cap > 800e8):  # 市值30-800亿
            continue
        if s["pct_chg"] > 9.8:  # 涨停板排除（流动性差）
            continue
        # 打分: 量比*0.3 + 换手*0.3 + 涨幅适中*0.4
        momentum_score = min(s["pct_chg"] / 5.0, 1.0) if s["pct_chg"] > 0 else 0
        vr_score = min(s.get("volume_ratio", 0) / 3.0, 1.0)
        turn_score = min(s["turnover"] / 10.0, 1.0)
        score = momentum_score * 0.4 + vr_score * 0.3 + turn_score * 0.3
        candidates.append({**s, "_score": score})

    candidates.sort(key=lambda x: -x["_score"])
    return [{"code": c["code"], "name": c["name"], "reason": "规则选股(AI不可用)"}
            for c in candidates[:n]]


def main():
    parser = argparse.ArgumentParser(description="概念板块AI选股器")
    parser.add_argument("--mode", choices=["evening", "intraday"], default="evening",
                        help="运行模式")
    parser.add_argument("--top-concepts", type=int, default=5,
                        help="取轮动评分Top N概念 (默认5)")
    parser.add_argument("--picks-per", type=int, default=5,
                        help="每个概念选几只 (默认5)")
    parser.add_argument("--no-proxy", action="store_true")
    parser.add_argument("--json", action="store_true",
                        help="输出JSON到 data/concept_picks_YYYYMMDD.json")
    args = parser.parse_args()

    result = concept_pick(mode=args.mode, top_concepts=args.top_concepts,
                          picks_per=args.picks_per, use_proxy=not args.no_proxy)
    if not result:
        print("[picker] ❌ 无结果")
        return

    if args.json:
        date_str = datetime.now().strftime("%Y%m%d")
        out_path = DATA / f"concept_picks_{date_str}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2),
                            encoding="utf-8")
        print(f"\n[picker] 已写入 {out_path}")

    # Display
    print(f"\n{'='*90}")
    print(f"  概念AI选股 [{result['mode_cn']}]  {result['timestamp']}")
    print(f"  Top{result['top_concepts']}概念 × {result['picks_per_concept']}只/概念")
    print(f"{'='*90}")

    for cdata in result["concepts"]:
        print(f"\n  📌 {cdata['concept_name']} "
              f"(分:{cdata['concept_score']:.1f} "
              f"涨:{cdata['concept_pct_chg']:+.2f}% "
              f"3日:{cdata['concept_trend_3d']:+.2f}%)")
        print(f"  {'─'*84}")
        for i, p in enumerate(cdata["picks"], 1):
            cap_yi = p["market_cap"] / 1e8 if p["market_cap"] else 0
            print(f"    {i}. {p['name']:<8} {p['code']}  "
                  f"价:{p['price']:.2f} 涨:{p['pct_chg']:+.2f}% "
                  f"换手:{p['turnover']:.1f}% 量比:{p['volume_ratio']:.2f} "
                  f"市值:{cap_yi:.0f}亿")
            if p.get("reason"):
                print(f"       → {p['reason']}")

    print(f"\n{'='*90}")


if __name__ == "__main__":
    main()
