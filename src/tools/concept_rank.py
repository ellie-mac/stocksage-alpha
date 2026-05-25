#!/usr/bin/env python3
"""
概念板块强势排名工具 — 统计最近N个交易日概念涨幅Top20入选频次，输出频次最高的概念及龙头股群。

用法:
    python -X utf8 src/tools/concept_rank.py [--days 40] [--top 20] [--leaders 3] [--no-proxy]

网络: push2.eastmoney.com 需要国内IP，海外VM通过 mihomo 代理（127.0.0.1:7890）。
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent.parent
DATA = ROOT / "data"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

NOISE_KEYWORDS = [
    "昨日", "热股", "多板", "百元", "千元", "融资融券",
    "沪股通", "深股通", "MSCI", "HS300", "中证", "转债标的",
    "涨停", "连板", "首板", "打板",
]


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


def _fetch_concept_blocks(session: requests.Session) -> list[dict]:
    """获取所有概念板块实时数据"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "fid": "f3", "po": "1", "pz": "500", "pn": "1",
        "np": "1", "fltt": "2", "invt": "2",
        "fs": "m:90+t:3",
        "fields": "f2,f3,f12,f14",
    }
    r = session.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json().get("data", {}).get("diff", [])


def _load_leader_history() -> dict:
    """加载 block_leader.py 积累的历史龙头数据"""
    history_file = DATA / "concept_leaders_history.json"
    if history_file.exists():
        try:
            return json.loads(history_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _get_frequent_leaders(history: dict, concept_name: str, top_n: int = 3) -> list[dict]:
    """从历史龙头记录中统计某概念最常被标记为龙头的股票（东财官方f128标记）。
    history 结构: {date: {concept_name: {code, name, pct_chg}}}
    """
    counter: Counter = Counter()
    for _date, concepts in history.items():
        if concept_name in concepts:
            info = concepts[concept_name]
            leader = info.get("name", "")
            code = info.get("code", "")
            if leader and code:
                counter[(leader, code)] += 1
    return [{"name": name, "code": code, "tag_count": cnt}
            for (name, code), cnt in counter.most_common(top_n)]


def _fetch_kline(session: requests.Session, code: str, days: int = 70) -> list[tuple[str, float]]:
    """获取概念板块日K线（日期, 涨跌幅%）"""
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": f"90.{code}",
        "fields1": "f1",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101", "fqt": "1",
        "beg": (datetime.now() - timedelta(days=days)).strftime("%Y%m%d"),
        "end": datetime.now().strftime("%Y%m%d"),
        "lmt": "80",
    }
    try:
        r = session.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return []
        klines = r.json().get("data", {}).get("klines", [])
        daily = []
        for k in klines:
            p = k.split(",")
            if len(p) >= 9 and p[8]:
                daily.append((p[0], float(p[8])))
        return daily
    except Exception:
        return []


def concept_rank(days: int = 40, top_n: int = 20, leaders_n: int = 3,
                 use_proxy: bool = True) -> list[dict]:
    """
    统计最近 days 个交易日内，概念板块每日涨幅排名前 top_n 的入选频次。
    龙头股来源：
      1. 东财实时 f128 官方领涨标记（当日快照）
      2. concept_leaders_history.json 历史标记频次（block_leader.py 积累）
    返回 [{name, code, count, pct, current_leader: {name,code}, history_leaders: [{name,code,tag_count}]}]
    """
    session = _get_session(use_proxy)

    if not _test_push2(session):
        if use_proxy:
            session = _get_session(use_proxy=False)
        if not _test_push2(session):
            print("[concept_rank] ❌ push2.eastmoney.com 不可达")
            return []

    blocks = _fetch_concept_blocks(session)
    if not blocks:
        print("[concept_rank] ❌ 获取概念板块列表失败")
        return []

    # Filter noise, record eastmoney official leaders (f128)
    valid_blocks: list[tuple[str, str]] = []  # (code, name)
    official_leaders: dict[str, dict] = {}  # name -> {leader_name, leader_code}
    for b in blocks:
        name = b.get("f14", "")
        if any(kw in name for kw in NOISE_KEYWORDS):
            continue
        valid_blocks.append((b.get("f12", ""), name))
        official_leaders[name] = {
            "name": b.get("f128", ""),
            "code": b.get("f140", ""),
        }

    print(f"[concept_rank] 获取 {len(valid_blocks)} 个概念板块K线...")

    # Fetch klines in batches
    kline_data: list[dict] = []  # [{name, code, daily}]
    code_map: dict[str, str] = {name: code for code, name in valid_blocks}
    batch_size = 20
    for i in range(0, len(valid_blocks), batch_size):
        batch = valid_blocks[i:i + batch_size]
        with ThreadPoolExecutor(max_workers=5) as ex:
            futs = {ex.submit(_fetch_kline, session, c, days + 30): (c, n) for c, n in batch}
            for f in as_completed(futs):
                code, name = futs[f]
                daily = f.result()
                if daily:
                    kline_data.append({"name": name, "code": code, "daily": daily})
        if i + batch_size < len(valid_blocks):
            time.sleep(0.3)

    if not kline_data:
        print("[concept_rank] ❌ 未获取到K线数据")
        return []

    # Get recent N trading days
    all_dates = sorted(set(d for item in kline_data for d, _ in item["daily"]))
    recent = all_dates[-days:]
    actual_days = len(recent)
    print(f"[concept_rank] 实际交易日数: {actual_days} ({recent[0]} ~ {recent[-1]})")

    # Count top_n frequency
    counter = Counter()
    for date in recent:
        day_data = []
        for item in kline_data:
            for d, pct in item["daily"]:
                if d == date:
                    day_data.append((item["name"], pct))
                    break
        day_data.sort(key=lambda x: -x[1])
        for name, _ in day_data[:top_n]:
            counter[name] += 1

    # Load history for multi-leader lookup
    history = _load_leader_history()

    # Build result
    top_concepts = counter.most_common(top_n)
    result = []
    for name, count in top_concepts:
        code = code_map.get(name, "")
        current = official_leaders.get(name, {"name": "", "code": ""})
        hist_leaders = _get_frequent_leaders(history, name, leaders_n)
        result.append({
            "name": name,
            "code": code,
            "count": count,
            "pct": round(count / actual_days * 100, 1),
            "current_leader": current,
            "history_leaders": hist_leaders,
        })

    return result


def main():
    parser = argparse.ArgumentParser(description="概念板块强势排名（按Top20入选频次）")
    parser.add_argument("--days", type=int, default=40, help="统计最近N个交易日（默认40≈2个月）")
    parser.add_argument("--top", type=int, default=20, help="每日取涨幅前N（默认20）")
    parser.add_argument("--leaders", type=int, default=3, help="每个概念取龙头股数量（默认3）")
    parser.add_argument("--no-proxy", action="store_true", help="不使用代理")
    parser.add_argument("--json", action="store_true", help="输出JSON到 data/concept_rank.json")
    args = parser.parse_args()

    results = concept_rank(days=args.days, top_n=args.top, leaders_n=args.leaders,
                           use_proxy=not args.no_proxy)
    if not results:
        return

    if args.json:
        out_path = DATA / "concept_rank.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n[concept_rank] 已写入 {out_path}")

    print(f"\n{'='*80}")
    print(f"  概念板块涨幅Top{args.top} 入选频次排名（最近{args.days}个交易日）")
    print(f"  龙头来源: 东财官方领涨标记 (f128)")
    print(f"{'='*80}")
    for rank, item in enumerate(results[:20], 1):
        # Current leader from today's eastmoney snapshot
        cur = item.get("current_leader", {})
        cur_str = f"{cur['name']}({cur['code']})" if cur.get("name") else "-"

        # Historical leaders from block_leader.py accumulated data
        hist = item.get("history_leaders", [])
        if hist:
            hist_str = " | ".join(
                f"{ld['name']}({ld['code']}) ×{ld['tag_count']}" for ld in hist
            )
        else:
            hist_str = "(无历史数据，需 block_leader.py 积累)"

        print(f"\n {rank:>2}. {item['name']:<12} 入选{item['count']}次 ({item['pct']}%)")
        print(f"     今日龙头: {cur_str}")
        print(f"     历史龙头: {hist_str}")
    print(f"\n{'='*80}")


if __name__ == "__main__":
    main()
