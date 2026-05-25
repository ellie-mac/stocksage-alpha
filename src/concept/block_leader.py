#!/usr/bin/env python3
"""
概念/行业板块龙头追踪 — 基于东财 push2 行情API（免费无限次）。

功能：
1. 拉取概念板块涨幅排名（全市场 ~400 个概念板块）
2. 获取板块内领涨个股作为"实际龙头"
3. 对比今日/昨日龙头，筛选龙头稳定（非一日游）的强势板块

网络：push2.eastmoney.com 需要国内IP，海外VM通过 mihomo 代理（127.0.0.1:7890）。

用法：
    python -X utf8 src/concept/block_leader.py [--top N] [--history] [--no-proxy]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

HISTORY_FILE = DATA / "concept_leaders_history.json"

# push2 API 参数: f2=最新价 f3=涨跌幅 f4=涨跌额 f8=换手率 f12=代码 f14=名称
# f62=主力净流入 f184=换手率(板块) f136=领涨股名 f140=领涨股代码 f141=领涨股涨幅
BLOCK_FIELDS = "f2,f3,f4,f8,f12,f14,f62,f136,f140,f141"
STOCK_FIELDS = "f2,f3,f6,f8,f12,f14,f62"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _get_session(use_proxy: bool = True) -> requests.Session:
    """创建 requests session，自动检测是否需要代理。"""
    s = requests.Session()
    s.headers.update(HEADERS)
    if use_proxy:
        proxy = "http://127.0.0.1:7890"
        s.proxies = {"http": proxy, "https": proxy}
    return s


def _test_connectivity(session: requests.Session) -> bool:
    """测试 push2 是否可达。"""
    try:
        r = session.get(
            "https://push2.eastmoney.com/api/qt/clist/get",
            params={"fid": "f3", "po": "1", "pz": "1", "pn": "1",
                    "np": "1", "fltt": "2", "invt": "2",
                    "fs": "m:90+t:3", "fields": "f14"},
            timeout=8,
        )
        return r.status_code == 200 and r.text.startswith("{")
    except Exception:
        return False


# ── API 调用 ─────────────────────────────────────────────────────────────────

def get_concept_blocks(session: requests.Session, top_n: int = 50) -> list[dict]:
    """获取概念板块涨幅排名。返回 [{code, name, pct_chg, turnover, net_inflow, leader_name, leader_code, leader_pct}]"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "fid": "f3", "po": "1",  # 按涨跌幅降序
        "pz": str(top_n), "pn": "1",
        "np": "1", "fltt": "2", "invt": "2",
        "fs": "m:90+t:3",  # 概念板块
        "fields": BLOCK_FIELDS,
    }
    try:
        r = session.get(url, params=params, timeout=15)
        data = r.json()
        items = data.get("data", {}).get("diff", [])
        results = []
        for item in items:
            results.append({
                "code": item.get("f12", ""),
                "name": item.get("f14", ""),
                "pct_chg": item.get("f3", 0),
                "turnover": item.get("f8", 0),
                "net_inflow": item.get("f62", 0),
                "leader_name": item.get("f136", ""),
                "leader_code": item.get("f140", ""),
                "leader_pct": item.get("f141", 0),
            })
        return results
    except Exception as e:
        print(f"[concept] 获取板块列表失败: {e}")
        return []


def get_block_stocks(session: requests.Session, block_code: str, top_n: int = 5) -> list[dict]:
    """获取板块内个股涨幅前N。"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "fid": "f3", "po": "1",
        "pz": str(top_n), "pn": "1",
        "np": "1", "fltt": "2", "invt": "2",
        "fs": f"b:{block_code}",
        "fields": STOCK_FIELDS,
    }
    try:
        r = session.get(url, params=params, timeout=10)
        data = r.json()
        items = data.get("data", {}).get("diff", [])
        return [{"code": s.get("f12", ""), "name": s.get("f14", ""),
                 "pct_chg": s.get("f3", 0), "net_inflow": s.get("f62", 0)}
                for s in items]
    except Exception:
        return []


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

def scan_strong_blocks(top_n: int = 50, use_proxy: bool = True) -> list[dict]:
    """
    扫描涨幅前 top_n 概念板块，返回强势板块列表。
    强势定义：领涨股与昨日相同（龙头稳定）或板块涨幅 > 3%。
    """
    session = _get_session(use_proxy)

    # 先尝试直连，失败则尝试代理，再失败报错
    if not _test_connectivity(session):
        if use_proxy:
            print("[concept] 代理不通，尝试直连...")
            session = _get_session(use_proxy=False)
        if not _test_connectivity(session):
            print("[concept] ❌ push2.eastmoney.com 不可达（需要国内IP或启动mihomo代理）")
            return []

    blocks = get_concept_blocks(session, top_n)
    if not blocks:
        print("[concept] 未获取到板块数据")
        return []

    history = _load_history()
    today = _today_str()

    # 获取昨日龙头
    dates = sorted(history.keys())
    yesterday_leaders = history.get(dates[-1], {}) if dates else {}

    today_leaders: dict[str, dict] = {}
    strong_blocks: list[dict] = []

    print(f"[concept] 获取到 {len(blocks)} 个概念板块，分析龙头...")
    for b in blocks:
        block_name = b["name"]
        leader_code = b["leader_code"]
        leader_name = b["leader_name"]

        today_leaders[block_name] = {
            "code": leader_code,
            "name": leader_name,
            "pct_chg": b["leader_pct"],
        }

        # 判断强势
        prev = yesterday_leaders.get(block_name, {})
        is_stable = prev.get("code") == leader_code and leader_code
        is_strong_pct = b["pct_chg"] > 3.0

        if is_stable or is_strong_pct:
            strong_blocks.append({
                "板块": block_name,
                "板块代码": b["code"],
                "板块涨幅": f"{b['pct_chg']:.2f}%",
                "龙头": leader_name,
                "龙头代码": leader_code,
                "龙头涨幅": f"{b['leader_pct']:.2f}%",
                "龙头稳定": "✅" if is_stable else "❌",
                "主力净流入": b["net_inflow"],
            })

    # 保存今日龙头到历史
    history[today] = today_leaders
    if len(history) > 30:
        for old_date in sorted(history.keys())[:-30]:
            del history[old_date]
    _save_history(history)

    # 排序：稳定优先 → 涨幅排序
    strong_blocks.sort(key=lambda x: (x["龙头稳定"] != "✅", -float(x["板块涨幅"].rstrip("%"))))
    return strong_blocks


def main():
    parser = argparse.ArgumentParser(description="概念板块龙头追踪")
    parser.add_argument("--top", type=int, default=50, help="扫描涨幅前N个板块")
    parser.add_argument("--history", action="store_true", help="显示历史龙头")
    parser.add_argument("--no-proxy", action="store_true", help="不使用代理（本机有国内网络时）")
    args = parser.parse_args()

    if args.history:
        history = _load_history()
        for date, leaders in sorted(history.items())[-5:]:
            print(f"\n── {date} ──")
            for block, info in list(leaders.items())[:10]:
                print(f"  {block}: {info.get('name', '?')} ({info.get('code', '?')})")
        return

    results = scan_strong_blocks(top_n=args.top, use_proxy=not args.no_proxy)
    if not results:
        print("[concept] 未找到强势板块")
        return

    print(f"\n{'='*60}")
    print("明日优先关注板块（龙头稳定，非一日游）：")
    print(f"{'='*60}")
    for i, item in enumerate(results[:15], 1):
        stable = item["龙头稳定"]
        inflow = item["主力净流入"]
        inflow_str = f"{inflow/1e8:.1f}亿" if abs(inflow) > 1e8 else f"{inflow/1e4:.0f}万"
        print(f"  {i:2d}. {item['板块']:8s} {item['板块涨幅']:>7s} | "
              f"龙头={item['龙头']}({item['龙头代码']}) {item['龙头涨幅']:>7s} "
              f"{stable} 资金={inflow_str}")
    print(f"{'='*60}")
    print(f"共 {len(results)} 个强势板块，显示前15")


if __name__ == "__main__":
    main()
