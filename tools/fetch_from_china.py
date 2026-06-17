#!/usr/bin/env python3
"""
fetch_from_china.py — 国内电脑一键拉取东财数据
零依赖，只用Python标准库，无需pip install任何东西。

用法：
  python3 fetch_from_china.py

输出：
  eastmoney_dump_YYYYMMDD_HHMM.json
"""

import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

BLOCK_FIELDS = "f2,f3,f4,f7,f8,f10,f12,f14,f22,f62,f104,f105,f128,f136,f140,f164,f166"

NOISE_KEYWORDS = [
    "昨日", "热股", "多板", "百元", "千元", "融资融券",
    "沪股通", "深股通", "MSCI", "HS300", "中证", "转债标的",
    "涨停", "连板", "首板", "打板",
]


def _get(url, params=None, timeout=15):
    """用标准库发GET请求，返回JSON"""
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_all_concepts():
    """分页拉取全量概念板块"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    all_items = []
    page = 1

    while True:
        params = {
            "fid": "f3", "po": "1", "pz": "500", "pn": str(page),
            "np": "1", "fltt": "2", "invt": "2",
            "fs": "m:90+t:3",
            "fields": BLOCK_FIELDS,
        }
        data = _get(url, params)
        items = data.get("data", {}).get("diff", [])
        if not items:
            break
        all_items.extend(items)
        total = data.get("data", {}).get("total", 0)
        print(f"  第{page}页: 获取{len(items)}条, 累计{len(all_items)}/{total}")
        if len(all_items) >= total or len(items) < 500:
            break
        page += 1
        time.sleep(0.1)

    results = []
    for item in all_items:
        name = item.get("f14", "")
        if any(kw in name for kw in NOISE_KEYWORDS):
            continue
        up = item.get("f104", 0) or 0
        down = item.get("f105", 0) or 0
        total_stocks = up + down
        results.append({
            "code": item.get("f12", ""),
            "name": name,
            "pct_chg": item.get("f3", 0) or 0,
            "volume_ratio": item.get("f10", 0) or 0,
            "speed": item.get("f22", 0) or 0,
            "turnover": item.get("f8", 0) or 0,
            "amplitude": item.get("f7", 0) or 0,
            "net_inflow": item.get("f62", 0) or 0,
            "big_order": item.get("f164", 0) or 0,
            "up_count": up,
            "down_count": down,
            "breadth": round((up / total_stocks) * min(total_stocks / 20, 1.0), 3) if total_stocks > 0 else 0,
            "leader_name": item.get("f128", ""),
            "leader_code": item.get("f140", ""),
            "leader_pct": item.get("f136", 0) or 0,
        })
    return results


def fetch_kline_3d(code):
    """获取单个概念近3个交易日累计涨幅"""
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
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
        data = _get(url, params, timeout=10)
        klines = data.get("data", {}).get("klines", [])
        if len(klines) < 3:
            return None
        total = 0.0
        for k in klines[-3:]:
            p = k.split(",")
            if len(p) >= 9 and p[8]:
                total += float(p[8])
        return round(total, 2)
    except Exception:
        return None


def fetch_all_klines(concepts):
    """批量拉取所有概念的3日K线"""
    trend_map = {}
    codes = [(c["code"], c["name"]) for c in concepts]
    batch_size = 30
    done = 0

    for i in range(0, len(codes), batch_size):
        batch = codes[i:i + batch_size]
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = {ex.submit(fetch_kline_3d, code): name for code, name in batch}
            for f in as_completed(futs):
                name = futs[f]
                result = f.result()
                if result is not None:
                    trend_map[name] = result
                done += 1

        if i + batch_size < len(codes):
            time.sleep(0.2)

        if (i // batch_size) % 5 == 0:
            print(f"  K线进度: {done}/{len(codes)}, 成功{len(trend_map)}条")

    return trend_map


def main():
    now = datetime.now()
    print(f"=== 东财数据拉取工具 === {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    print("[1/3] 拉取全量概念板块...")
    concepts = fetch_all_concepts()
    print(f"  ✅ 获取 {len(concepts)} 个概念板块（去噪后）")
    print()

    print("[2/3] 拉取3日K线数据...")
    trend_map = fetch_all_klines(concepts)
    print(f"  ✅ 获取 {len(trend_map)} 条3日趋势数据")
    print()

    daily_chg = {c["name"]: c["pct_chg"] for c in concepts}

    output = {
        "timestamp": now.isoformat(),
        "date": now.strftime("%Y%m%d"),
        "total_concepts": len(concepts),
        "concepts": concepts,
        "trend_3d": trend_map,
        "daily_change": daily_chg,
    }

    filename = f"eastmoney_dump_{now.strftime('%Y%m%d_%H%M')}.json"
    Path(filename).write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    size_kb = Path(filename).stat().st_size / 1024
    print(f"[3/3] ✅ 已保存: {filename}")
    print(f"     概念数: {len(concepts)}")
    print(f"     3日K线: {len(trend_map)}条")
    print(f"     文件大小: {size_kb:.0f} KB")
    print()
    print("把这个文件传回海外主机，放到 stocksage-alpha/data/ 目录。")


if __name__ == "__main__":
    main()
