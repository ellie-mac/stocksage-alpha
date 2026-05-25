#!/usr/bin/env python3
"""
概念/行业板块龙头追踪 — 对接东财 Open API，筛选龙头稳定的强势板块。

功能：
1. 拉取所有概念/行业板块列表
2. 获取板块当日龙头 + 资金 + 涨跌幅
3. 对比今日/昨日龙头，筛选龙头稳定（非一日游）的强势板块

用法：
    python -X utf8 src/concept/block_leader.py [--top N] [--history]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

# 东财开放平台 AppKey（从环境变量读取）
APPKEY = os.environ.get("EASTMONEY_APPKEY", "")

HISTORY_FILE = DATA / "concept_leaders_history.json"


# ── API 调用 ─────────────────────────────────────────────────────────────────

def get_all_blocks() -> list[dict]:
    """获取所有概念/行业板块列表。"""
    url = "https://openapi.eastmoney.com/data/v1/block/list"
    params = {"appkey": APPKEY}
    try:
        res = requests.get(url, params=params, timeout=15).json()
        return res.get("data", {}).get("list", [])
    except Exception as e:
        print(f"[concept] 获取板块列表失败: {e}")
        return []


def get_block_detail(block_id: str) -> list[dict]:
    """获取板块内个股列表（含龙头标记、涨跌幅、资金流）。"""
    url = "https://openapi.eastmoney.com/data/v1/block/stock"
    params = {"appkey": APPKEY, "blockId": block_id}
    try:
        res = requests.get(url, params=params, timeout=15).json()
        return res.get("data", {}).get("list", [])
    except Exception as e:
        print(f"[concept] 获取板块 {block_id} 详情失败: {e}")
        return []


def get_block_leader(block_id: str) -> dict | None:
    """获取板块当日龙头股（东财 tag 含'龙头'的第一只）。"""
    stocks = get_block_detail(block_id)
    leaders = [s for s in stocks if "龙头" in s.get("tag", "")]
    if leaders:
        return {
            "code": leaders[0].get("code", ""),
            "name": leaders[0].get("name", ""),
            "pct_chg": leaders[0].get("pctChg", 0),
            "net_inflow": leaders[0].get("netInflow", 0),
        }
    # fallback: 取涨幅最高的
    if stocks:
        top = max(stocks, key=lambda s: s.get("pctChg", 0))
        return {
            "code": top.get("code", ""),
            "name": top.get("name", ""),
            "pct_chg": top.get("pctChg", 0),
            "net_inflow": top.get("netInflow", 0),
        }
    return None


# ── 历史对比 ──────────────────────────────────────────────────────────────────

def _load_history() -> dict:
    if HISTORY_FILE.exists():
        try:
            return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_history(history: dict):
    DATA.mkdir(parents=True, exist_ok=True)
    HISTORY_FILE.write_text(
        json.dumps(history, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


# ── 主逻辑 ────────────────────────────────────────────────────────────────────

def scan_strong_blocks(top_n: int = 50) -> list[dict]:
    """
    扫描前 top_n 热门板块，返回强势板块列表。
    强势定义：龙头与昨日相同（稳定）或涨幅 > 3%。
    """
    if not APPKEY:
        print("[concept] ⚠️  未设置 EASTMONEY_APPKEY 环境变量，无法调用东财API")
        return []

    blocks = get_all_blocks()
    if not blocks:
        print("[concept] 未获取到板块数据")
        return []

    history = _load_history()
    today = _today_str()
    today_leaders: dict[str, dict] = {}
    strong_blocks: list[dict] = []

    # 获取昨日龙头（最近一天的记录）
    dates = sorted(history.keys())
    yesterday_leaders = history.get(dates[-1], {}) if dates else {}

    print(f"[concept] 扫描 {min(top_n, len(blocks))} 个板块...")
    for b in blocks[:top_n]:
        block_id = b.get("id", "")
        block_name = b.get("name", "")
        leader = get_block_leader(block_id)

        if not leader:
            continue

        today_leaders[block_name] = leader

        # 判断强势：龙头稳定 or 涨幅强
        prev_leader = yesterday_leaders.get(block_name, {})
        is_stable = prev_leader.get("code") == leader["code"] and prev_leader.get("code")
        is_strong_pct = leader["pct_chg"] > 3.0

        if is_stable or is_strong_pct:
            strong_blocks.append({
                "板块": block_name,
                "龙头": leader["name"],
                "代码": leader["code"],
                "涨幅": f"{leader['pct_chg']:.2f}%",
                "龙头稳定": "✅" if is_stable else "❌",
                "净流入": leader["net_inflow"],
            })

    # 保存今日龙头到历史
    history[today] = today_leaders
    # 只保留最近 30 天
    if len(history) > 30:
        for old_date in sorted(history.keys())[:-30]:
            del history[old_date]
    _save_history(history)

    # 按：稳定优先 → 涨幅排序
    strong_blocks.sort(key=lambda x: (x["龙头稳定"] != "✅", -float(x["涨幅"].rstrip("%"))))
    return strong_blocks


def main():
    parser = argparse.ArgumentParser(description="概念板块龙头追踪")
    parser.add_argument("--top", type=int, default=50, help="扫描前N个板块")
    parser.add_argument("--history", action="store_true", help="显示历史龙头")
    args = parser.parse_args()

    if args.history:
        history = _load_history()
        for date, leaders in sorted(history.items())[-5:]:
            print(f"\n── {date} ──")
            for block, info in list(leaders.items())[:10]:
                print(f"  {block}: {info.get('name', '?')} ({info.get('code', '?')})")
        return

    results = scan_strong_blocks(top_n=args.top)
    if not results:
        print("[concept] 未找到强势板块")
        return

    print(f"\n{'='*60}")
    print("明日优先关注板块（龙头稳定，非一日游）：")
    print(f"{'='*60}")
    for i, item in enumerate(results[:10], 1):
        stable = item["龙头稳定"]
        print(f"  {i}. {item['板块']} | 龙头={item['龙头']}({item['代码']}) "
              f"涨幅={item['涨幅']} {stable}")
    print(f"{'='*60}")
    print(f"共 {len(results)} 个强势板块，显示前10")


if __name__ == "__main__":
    main()
