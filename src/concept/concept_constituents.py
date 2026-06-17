#!/usr/bin/env python3
"""
概念板块成分股缓存管理器。

成分股列表变化缓慢(通常数周才调整), 适合本地缓存。
盘后拉取一次, 凌晨/接口不可用时从缓存读取。

用法:
  更新缓存:  python -X utf8 src/concept/concept_constituents.py update [--top 15]
  查看缓存:  python -X utf8 src/concept/concept_constituents.py show --concept 存储芯片
  缓存状态:  python -X utf8 src/concept/concept_constituents.py status

数据存储: data/concept_constituents.json
格式: {
  "BK1137": {
    "name": "存储芯片",
    "updated": "2026-05-25 22:30:00",
    "stocks": [{"code": "002185", "name": "华天科技", ...}, ...]
  }, ...
}
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent.parent
DATA = ROOT / "data"
CACHE_DIR = ROOT / "src" / "cache" / "concept"
CACHE_FILE = CACHE_DIR / "constituents.json"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
STOCK_FIELDS = "f2,f3,f5,f6,f7,f8,f9,f10,f12,f14,f15,f16,f17,f20,f21"


def _get_session(use_proxy: bool = True) -> requests.Session:
    if use_proxy:
        from concept.relay_session import make_relay_session
        return make_relay_session(headers=HEADERS)
    s = requests.Session()
    s.headers.update(HEADERS)
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


def load_cache() -> dict:
    """加载本地缓存"""
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict):
    """保存缓存到磁盘"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def fetch_constituents(session: requests.Session, block_code: str,
                       max_stocks: int = 50) -> list[dict]:
    """从东财拉取概念板块成分股列表"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "fid": "f3", "po": "1",
        "pz": str(max_stocks), "pn": "1",
        "np": "1", "fltt": "2", "invt": "2",
        "fs": f"b:{block_code}",
        "fields": STOCK_FIELDS,
    }
    try:
        r = session.get(url, params=params, timeout=15)
        items = r.json().get("data", {}).get("diff", [])
        stocks = []
        for item in items:
            price = item.get("f2", 0)
            if not isinstance(price, (int, float)) or price <= 0:
                continue
            def _num(v):
                return v if isinstance(v, (int, float)) else 0

            stocks.append({
                "code": item.get("f12", ""),
                "name": item.get("f14", ""),
                "price": price,
                "pct_chg": _num(item.get("f3", 0)),
                "volume": _num(item.get("f5", 0)),
                "amount": _num(item.get("f6", 0)),
                "amplitude": _num(item.get("f7", 0)),
                "turnover": _num(item.get("f8", 0)),
                "pe": _num(item.get("f9", 0)),
                "volume_ratio": _num(item.get("f10", 0)),
                "open": _num(item.get("f17", 0)),
                "high": _num(item.get("f15", 0)),
                "low": _num(item.get("f16", 0)),
                "market_cap": _num(item.get("f20", 0)),
                "float_cap": _num(item.get("f21", 0)),
            })
        return stocks
    except Exception as e:
        print(f"  [constituents] 拉取失败 {block_code}: {e}")
        return []


def update_cache(concepts: list[dict] | None = None, use_proxy: bool = True,
                 top_n: int = 15) -> dict:
    """
    更新成分股缓存。
    concepts: [{code, name}, ...] 需要更新的概念列表。
              如果为None, 从 concept_rotation 缓存中读取 Top N。
    """
    session = _get_session(use_proxy)

    if not _test_push2(session):
        if use_proxy:
            session = _get_session(use_proxy=False)
        if not _test_push2(session):
            print("[constituents] ❌ push2 不可达, 无法更新缓存")
            return {}

    # 如果没传概念列表, 从 rotation 缓存读取
    if concepts is None:
        rotation_file = DATA / "concept_rotation_evening.json"
        if rotation_file.exists():
            rotation = json.loads(rotation_file.read_text(encoding="utf-8"))
            concepts = [{"code": c["code"], "name": c["name"]}
                        for c in rotation.get("full_ranking", [])[:top_n]]
        else:
            print("[constituents] ❌ 无 rotation 缓存, 请先运行 concept_rotation")
            return {}

    # 只保留当天 top 概念 + 已有缓存中仍在 top 里的
    # 不主动删旧缓存, 多存一些备用无害
    cache = load_cache()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated_count = 0

    for concept in concepts:
        code = concept["code"]
        name = concept["name"]
        print(f"  [constituents] 拉取 {name} ({code}) ...", end=" ")

        stocks = fetch_constituents(session, code)
        if stocks:
            # 如果换手/成交额为0(如刚开盘), 用旧缓存的值补充
            old_entry = cache.get(code, {})
            old_stocks_map = {s["code"]: s for s in old_entry.get("stocks", [])}
            FALLBACK_FIELDS = ["turnover", "volume", "amount", "volume_ratio", "amplitude"]
            for s in stocks:
                if s.get("turnover", 0) == 0 and s["code"] in old_stocks_map:
                    old = old_stocks_map[s["code"]]
                    for field in FALLBACK_FIELDS:
                        if s.get(field, 0) == 0 and old.get(field, 0) != 0:
                            s[field] = old[field]

            cache[code] = {
                "name": name,
                "updated": now,
                "stock_count": len(stocks),
                "stocks": stocks,
            }
            print(f"✓ {len(stocks)}只")
            updated_count += 1
        else:
            print("✗ 失败")

        time.sleep(0.3)  # Rate limit

    save_cache(cache)
    print(f"\n[constituents] 更新完成: {updated_count}/{len(concepts)} 个概念, "
          f"总缓存: {len(cache)} 个概念 | 文件: {CACHE_FILE}")
    return cache


def get_cached_stocks(block_code: str) -> list[dict]:
    """从缓存获取成分股(供 picker 调用)"""
    cache = load_cache()
    entry = cache.get(block_code, {})
    return entry.get("stocks", [])


def get_cache_age(block_code: str) -> float | None:
    """获取缓存年龄(小时), None表示无缓存"""
    cache = load_cache()
    entry = cache.get(block_code, {})
    updated = entry.get("updated")
    if not updated:
        return None
    try:
        dt = datetime.strptime(updated, "%Y-%m-%d %H:%M:%S")
        age_hours = (datetime.now() - dt).total_seconds() / 3600
        return age_hours
    except Exception:
        return None


def show_concept(name_or_code: str):
    """展示某个概念的缓存成分股"""
    cache = load_cache()

    # 支持按名称或代码查找
    entry = None
    block_code = None
    for code, data in cache.items():
        if code == name_or_code or data.get("name") == name_or_code:
            entry = data
            block_code = code
            break

    if not entry:
        print(f"[constituents] 未找到: {name_or_code}")
        print(f"  可用概念: {', '.join(d['name'] for d in cache.values())}")
        return

    stocks = entry["stocks"]
    print(f"\n  {entry['name']} ({block_code}) — 缓存时间: {entry['updated']} "
          f"| {len(stocks)}只成分股")
    print(f"  {'─'*85}")
    print(f"  {'名称':<8} {'代码':<8} {'价格':>6} {'涨幅':>7} {'换手':>6} "
          f"{'量比':>5} {'成交额':>8} {'市值':>8}")
    print(f"  {'─'*85}")
    for s in stocks[:30]:
        cap_yi = s["market_cap"] / 1e8 if s.get("market_cap") else 0
        amt_yi = s["amount"] / 1e8 if s.get("amount") else 0
        print(f"  {s['name']:<8} {s['code']:<8} {s['price']:>6.2f} "
              f"{s['pct_chg']:>+6.2f}% {s['turnover']:>5.1f}% "
              f"{s.get('volume_ratio',0):>5.2f} {amt_yi:>7.1f}亿 {cap_yi:>7.0f}亿")


def show_status():
    """展示缓存状态"""
    cache = load_cache()
    if not cache:
        print("[constituents] 缓存为空, 请先运行 update")
        return

    print(f"\n  概念成分股缓存状态 ({CACHE_FILE})")
    print(f"  {'─'*60}")
    print(f"  {'概念':<14} {'代码':<8} {'成分股':>5} {'缓存时间':<20} {'年龄'}")
    print(f"  {'─'*60}")
    for code, data in sorted(cache.items(), key=lambda x: x[1].get("updated", ""), reverse=True):
        age = get_cache_age(code)
        age_str = f"{age:.1f}h" if age is not None else "?"
        print(f"  {data['name']:<14} {code:<8} {data.get('stock_count',0):>5} "
              f"{data['updated']:<20} {age_str}")


def main():
    parser = argparse.ArgumentParser(description="概念板块成分股缓存管理")
    sub = parser.add_subparsers(dest="cmd")

    p_update = sub.add_parser("update", help="更新成分股缓存")
    p_update.add_argument("--top", type=int, default=15, help="更新Top N概念")
    p_update.add_argument("--no-proxy", action="store_true")

    p_show = sub.add_parser("show", help="查看概念成分股")
    p_show.add_argument("--concept", required=True, help="概念名称或代码")

    sub.add_parser("status", help="查看缓存状态")

    args = parser.parse_args()

    if args.cmd == "update":
        result = update_cache(use_proxy=not args.no_proxy, top_n=args.top)
        if not result:
            exit(1)
    elif args.cmd == "show":
        show_concept(args.concept)
    elif args.cmd == "status":
        show_status()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
