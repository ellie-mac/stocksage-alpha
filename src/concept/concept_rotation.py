#!/usr/bin/env python3
"""
概念板块轮动评分器 — 双模式多因子综合打分。

两种模式:
  1. 盘后复盘版 (evening): 收盘后运行, 选明天可买的概念方向
  2. 盘中实时版 (intraday): 盘中运行, 发现正在启动的概念

盘后因子权重 (总和=100%):
  广度 25% | 3日趋势 22% | 量比 18% | 换手率 12% | 动量 10% | 主力流入 8% | 超大单 5%

盘中因子权重 (总和=100%):
  涨速 20% | 量比 20% | 广度 18% | 动量 15% | 3日趋势 12% | 主力流入 10% | 换手率 5%

信号输出:
  火 强势轮入: 综合Top10 + 3日趋势>0
  苗 蓄势待发: 涨幅<2% + 主力流入Top20 + 量比>1.5
  警 获利了结: 3日涨>8% + 今日主力净流出

用法:
    python -X utf8 src/concept/concept_rotation.py [--mode evening|intraday] [--top 15] [--json]

网络: push2.eastmoney.com 需国内IP, 海外VM通过 Tailscale relay (100.111.44.98:8765).
"""
from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests

try:
    import akshare as ak
    HAS_AKSHARE = True
except ImportError:
    HAS_AKSHARE = False

ROOT = Path(__file__).resolve().parent.parent.parent
DATA = ROOT / "data"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _safe_float(v) -> float:
    """Convert value to float safely, returning 0.0 on failure."""
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0

# ── 双模式权重 ─────────────────────────────────────────────────────────────────
WEIGHTS_EVENING = {
    "breadth": 0.20,        # 广度: 全面涨=延续概率高
    "trend_3d": 0.18,       # 3日趋势: 连续强=主线逻辑确认
    "net_inflow": 0.15,     # 主力流入: 主力进场次日大概率延续
    "volume_ratio": 0.15,   # 量比: 异常放量=新资金进场
    "momentum": 0.12,       # 动量: 当日强度
    "turnover": 0.10,       # 换手率: 持续关注度
    "speed": 0.05,          # 涨速: 尾盘加速信号
    "big_order": 0.05,      # 超大单: 补充信号
}

WEIGHTS_INTRADAY = {
    "speed": 0.20,          # 涨速: 正在加速=发动时刻
    "breadth": 0.18,        # 广度: 板块普涨确认
    "volume_ratio": 0.17,   # 量比: 突然放量=资金涌入
    "net_inflow": 0.15,     # 主力流入: 主力进场次日大概率延续
    "momentum": 0.15,       # 动量: 盘中需方向确认
    "trend_3d": 0.10,       # 3日趋势: 有基础的加速更可靠
    "turnover": 0.05,       # 换手率: 盘中不完整, 降权
}

NOISE_KEYWORDS = [
    "昨日", "热股", "多板", "百元", "千元", "融资融券",
    "沪股通", "深股通", "MSCI", "HS300", "中证", "转债标的",
    "涨停", "连板", "首板", "打板",
]

BLOCK_FIELDS = "f2,f3,f4,f7,f8,f10,f12,f14,f22,f62,f104,f105,f128,f136,f140,f164,f166"


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


def _fetch_all_concepts(session: requests.Session) -> list[dict]:
    """获取全部概念板块实时多因子数据（分页拉取确保全量）"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    all_items = []
    page = 1
    page_size = 500

    while True:
        params = {
            "fid": "f3", "po": "1", "pz": str(page_size), "pn": str(page),
            "np": "1", "fltt": "2", "invt": "2",
            "fs": "m:90+t:3",
            "fields": BLOCK_FIELDS,
        }
        r = session.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json().get("data", {})
        items = data.get("diff", [])
        if not items:
            break
        all_items.extend(items)
        total = data.get("total", 0)
        if len(all_items) >= total or len(items) < page_size:
            break
        page += 1

    results = []
    for item in all_items:
        name = item.get("f14", "")
        if any(kw in name for kw in NOISE_KEYWORDS):
            continue
        up = int(item.get("f104", 0) or 0)
        down = int(item.get("f105", 0) or 0)
        total = up + down
        results.append({
            "code": item.get("f12", ""),
            "name": name,
            "pct_chg": _safe_float(item.get("f3", 0)),
            "volume_ratio": _safe_float(item.get("f10", 0)),
            "speed": _safe_float(item.get("f22", 0)),
            "turnover": _safe_float(item.get("f8", 0)),
            "amplitude": _safe_float(item.get("f7", 0)),
            "net_inflow": _safe_float(item.get("f62", 0)),
            "big_order": _safe_float(item.get("f164", 0)),
            "up_count": up,
            "down_count": down,
            "breadth": round((up / total) * min(total / 20, 1.0), 3) if total > 0 else 0,
            "leader_name": item.get("f128", ""),
            "leader_code": item.get("f140", ""),
            "leader_pct": _safe_float(item.get("f136", 0)),
        })
    return results


def _fetch_concepts_akshare() -> list[dict]:
    """AkShare备用源：从同花顺拉概念列表+日K算涨幅。
    为避免太慢，只拉概念列表+用缓存中的概念名去拿日K。
    缺少：量比、涨速、资金流、龙头 — 这些字段填0。
    """
    if not HAS_AKSHARE:
        return []
    try:
        print("[rotation] 尝试AkShare备用源(同花顺)...")
        name_df = ak.stock_board_concept_name_ths()
        all_names = set(name_df["name"].tolist())
        print(f"[rotation] 同花顺概念列表: {len(all_names)}个")

        # 优先用本地缓存里有的概念名（减少请求量）
        cache_path = DATA / "concept_trend3d_cache.json"
        target_names = []
        if cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            target_names = [n for n in cached.get("data", {}).keys() if n in all_names]
        if len(target_names) < 30:
            # 缓存不够，取前100个概念
            target_names = name_df["name"].tolist()[:100]

        print(f"[rotation] 拉取 {len(target_names)} 个概念日K...")
        concepts = []
        today = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")
        done = 0
        failed = 0

        for name in target_names:
            try:
                hist = ak.stock_board_concept_index_ths(
                    symbol=name, start_date=start, end_date=today
                )
                if hist.empty or len(hist) < 2:
                    failed += 1
                    continue
                last = hist.iloc[-1]
                prev_close = hist.iloc[-2]["收盘价"]
                pct_chg = round((last["收盘价"] - prev_close) / prev_close * 100, 2) if prev_close else 0

                # 3日趋势
                trend_3d = 0.0
                if len(hist) >= 4:
                    base = hist.iloc[-4]["收盘价"]
                    trend_3d = round((last["收盘价"] - base) / base * 100, 2) if base else 0

                concepts.append({
                    "code": "",
                    "name": name,
                    "pct_chg": pct_chg,
                    "volume_ratio": 0,
                    "speed": 0,
                    "turnover": 0,
                    "amplitude": 0,
                    "net_inflow": 0,
                    "big_order": 0,
                    "up_count": 0,
                    "down_count": 0,
                    "breadth": 0,
                    "leader_name": "",
                    "leader_code": "",
                    "leader_pct": 0,
                    "trend_3d": trend_3d,
                })
                done += 1
            except Exception:
                failed += 1
                continue

        print(f"[rotation] AkShare完成: 成功{done}个, 失败{failed}个")
        return concepts
    except Exception as e:
        print(f"[rotation] AkShare也失败: {e}")
        return []


def _fetch_kline_3d(session: requests.Session, code: str) -> float | None:
    """获取近3个交易日累计涨幅"""
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
        r = session.get(url, params=params, timeout=8)
        if r.status_code != 200:
            return None
        klines = r.json().get("data", {}).get("klines", [])
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


def _calc_trend_3d_local(daily_dir: Path, concepts: list[dict]) -> dict[str, float]:
    """从本地每日涨幅文件累加最近3个交易日的趋势。
    文件格式: daily_dir/YYYYMMDD.json = {"概念名": 当日涨幅, ...}
    返回空字典表示数据不足3天。
    """
    files = sorted(daily_dir.glob("*.json"), reverse=True)
    if len(files) < 3:
        return {}
    # 取最近3天
    recent_3 = files[:3]
    day_data = []
    for f in recent_3:
        try:
            day_data.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            return {}
    # 累加3天涨幅
    trend_map: dict[str, float] = {}
    for c in concepts:
        name = c["name"]
        total = sum(d.get(name, 0.0) for d in day_data)
        trend_map[name] = round(total, 2)
    return trend_map


def save_daily_change(concepts: list[dict], date_str: str | None = None):
    """保存当日各概念涨幅，供后续3日趋势计算。
    每天只需调用一次（收盘后），date_str格式: YYYYMMDD。
    """
    daily_dir = DATA / "concept_daily_chg"
    daily_dir.mkdir(parents=True, exist_ok=True)
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    out = {c["name"]: c["pct_chg"] for c in concepts}
    p = daily_dir / f"{date_str}.json"
    p.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"[rotation] 已保存当日涨幅: {p.name} ({len(out)}条)")


def concept_rotation(mode: str = "evening", top_n: int = 15,
                     use_proxy: bool = True) -> dict:
    """
    概念板块轮动评分。
    mode: "evening" (盘后复盘) 或 "intraday" (盘中实时)
    """
    session = _get_session(use_proxy)

    use_akshare = False
    if not _test_push2(session):
        if use_proxy:
            session = _get_session(use_proxy=False)
        if not _test_push2(session):
            print("[rotation] ⚠️ push2.eastmoney.com 不可达，尝试备用源...")
            use_akshare = True

    weights = WEIGHTS_EVENING if mode == "evening" else WEIGHTS_INTRADAY

    # Step 1: 获取实时多因子数据
    if use_akshare:
        concepts = _fetch_concepts_akshare()
        if not concepts:
            print("[rotation] ❌ 东财+AkShare均失败")
            return {}
    else:
        concepts = _fetch_all_concepts(session)
        if not concepts:
            # 东财返回空，试AkShare
            concepts = _fetch_concepts_akshare()
            if not concepts:
                print("[rotation] ❌ 获取概念板块数据失败")
                return {}

    print(f"[rotation] 模式: {'盘后复盘' if mode == 'evening' else '盘中实时'} | "
          f"获取 {len(concepts)} 个概念板块")

    # Step 2: 获取3日趋势 — 优先使用本地累积数据，fallback到push2his
    # 如果AkShare源已经带了trend_3d，直接跳过
    daily_chg_dir = DATA / "concept_daily_chg"
    daily_chg_dir.mkdir(parents=True, exist_ok=True)

    has_trend = use_akshare and any(c.get("trend_3d", 0) != 0 for c in concepts)

    if not has_trend and weights.get("trend_3d", 0) > 0:
        trend_map: dict[str, float] = {}

        # 方法A: 从本地累积的每日涨幅数据计算3日趋势
        local_trend = _calc_trend_3d_local(daily_chg_dir, concepts)
        if local_trend:
            trend_map = local_trend
            print(f"[rotation] 3日趋势(本地累积): {len(trend_map)}条")
        else:
            # 方法B: fallback到push2his (过渡期,前3天数据不够时)
            print("[rotation] 本地数据不足3天，尝试push2his...")
            batch_size = 20
            codes = [(c["code"], c["name"]) for c in concepts]
            for i in range(0, len(codes), batch_size):
                batch = codes[i:i + batch_size]
                with ThreadPoolExecutor(max_workers=8) as ex:
                    futs = {ex.submit(_fetch_kline_3d, session, code): name
                            for code, name in batch}
                    for f in as_completed(futs):
                        name = futs[f]
                        result = f.result()
                        if result is not None:
                            trend_map[name] = result
                if i + batch_size < len(codes):
                    time.sleep(0.2)

            if trend_map:
                print(f"[rotation] 3日K线(push2his): {len(trend_map)}条")
            else:
                # 方法C: 最后fallback — 缓存文件
                cache_path = DATA / "concept_trend3d_cache.json"
                if cache_path.exists():
                    cached = json.loads(cache_path.read_text(encoding="utf-8"))
                    trend_map = cached.get("data", {})
                    print(f"[rotation] ⚠️ 使用旧缓存3日数据 (来自{cached.get('ts','?')})")
                else:
                    print("[rotation] ⚠️ 无3日数据来源，全部为0")

        for c in concepts:
            c["trend_3d"] = trend_map.get(c["name"], 0.0)
    elif not has_trend:
        for c in concepts:
            c["trend_3d"] = 0.0
    # else: AkShare已带trend_3d，无需处理

    # Step 3: 各因子百分位排名
    factor_values: dict[str, list[float]] = {
        "momentum": [c["pct_chg"] for c in concepts],
        "volume_ratio": [c["volume_ratio"] for c in concepts],
        "speed": [c["speed"] for c in concepts],
        "turnover": [c["turnover"] for c in concepts],
        "breadth": [c["breadth"] for c in concepts],
        "trend_3d": [c["trend_3d"] for c in concepts],
        "net_inflow": [c["net_inflow"] for c in concepts],
        "big_order": [c["big_order"] for c in concepts],
    }

    factor_pcts: dict[str, list[float]] = {
        k: _percentile_rank(v) for k, v in factor_values.items()
    }

    # Step 4: 加权合成 + 量价背离惩罚
    for i, c in enumerate(concepts):
        score = sum(
            weights.get(factor, 0) * factor_pcts[factor][i]
            for factor in factor_pcts
            if factor in weights
        )

        # 量价背离惩罚: 量比>1.5 且 主力净流出 → 放量出货信号, 扣15分
        # 普通流出惩罚: 主力净流出但量比不高 → 轻度扣分5分
        if c["volume_ratio"] > 1.5 and c["net_inflow"] < 0:
            score -= 0.15  # 放量出货, 严重
        elif c["net_inflow"] < 0:
            score -= 0.05  # 普通流出, 轻微惩罚

        # 过热惩罚: 3日涨幅大 且 今日主力净流出 → 获利了结信号
        # 仅在涨多+资金撤退同时满足时扣分，主力仍流入则不惩罚
        if c["trend_3d"] > 10.0 and c["net_inflow"] < 0:
            score -= 0.12  # 严重过热+资金撤退
        elif c["trend_3d"] > 8.0 and c["net_inflow"] < 0:
            score -= 0.06  # 中度过热+资金撤退

        c["score"] = round(score * 100, 1)
        # 保存各因子百分位供展示
        for factor in weights:
            if factor in factor_pcts:
                c[f"{factor}_pct"] = round(factor_pcts[factor][i] * 100, 1)

    # Sort by composite score
    concepts.sort(key=lambda x: -x["score"])

    # Step 5: 分类信号
    strong_entry = []   # 🔥 强势轮入
    preparing = []      # 🌱 蓄势待发
    take_profit = []    # ⚠️ 获利了结

    inflow_vals = factor_values["net_inflow"]
    inflow_top20 = sorted(inflow_vals, reverse=True)[min(19, len(inflow_vals) - 1)]
    top10_score = concepts[min(9, len(concepts) - 1)]["score"]

    for c in concepts:
        # 强势轮入: Top10综合分 + 3日趋势为正
        if c["score"] >= top10_score and c["trend_3d"] > 0:
            strong_entry.append(c)

        # 蓄势待发: 今日涨幅<2% + 主力净流入Top20 + 量比>1.5
        if (c["pct_chg"] < 2.0 and c["net_inflow"] >= inflow_top20
                and c["volume_ratio"] > 1.5):
            preparing.append(c)

        # 获利了结: 3日涨幅>8% + 今日主力净流出
        if c["trend_3d"] > 8.0 and c["net_inflow"] < 0:
            take_profit.append(c)

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": mode,
        "mode_cn": "盘后复盘" if mode == "evening" else "盘中实时",
        "weights": weights,
        "total_concepts": len(concepts),
        "strong_entry": strong_entry[:top_n],
        "preparing": preparing[:10],
        "take_profit": take_profit[:10],
        "full_ranking": concepts[:top_n],
    }


def _format_inflow(val: float) -> str:
    """格式化资金流（亿/万）"""
    if abs(val) >= 1e8:
        return f"{val / 1e8:+.1f}亿"
    elif abs(val) >= 1e4:
        return f"{val / 1e4:+.0f}万"
    return f"{val:+.0f}"


def recalc_from_cache(mode: str = "evening", top_n: int = 15) -> dict:
    """从缓存JSON读取原始因子数据，用当前权重重新打分。"""
    suffix = "evening" if mode == "evening" else "intraday"
    cache_path = DATA / f"concept_rotation_{suffix}.json"
    if not cache_path.exists():
        print(f"[recalc] ❌ 缓存文件不存在: {cache_path}")
        return {}

    cached = json.loads(cache_path.read_text(encoding="utf-8"))
    concepts = cached.get("full_ranking", [])
    if not concepts:
        print("[recalc] ❌ 缓存中无排名数据")
        return {}

    # 需要完整概念列表来做百分位排名，但缓存只存了 top_n
    # 如果缓存的 total_concepts > len(concepts)，百分位会不精确，但仍比旧权重好
    print(f"[recalc] 从缓存加载 {len(concepts)} 个概念 (原始时间: {cached.get('timestamp', '?')})")

    weights = WEIGHTS_EVENING if mode == "evening" else WEIGHTS_INTRADAY

    # 重新百分位排名
    factor_keys = ["momentum", "volume_ratio", "speed", "turnover",
                   "breadth", "trend_3d", "net_inflow", "big_order"]
    # 映射: momentum 来自 pct_chg
    factor_values: dict[str, list[float]] = {
        "momentum": [c.get("pct_chg", 0) for c in concepts],
        "volume_ratio": [c.get("volume_ratio", 0) for c in concepts],
        "speed": [c.get("speed", 0) for c in concepts],
        "turnover": [c.get("turnover", 0) for c in concepts],
        "breadth": [c.get("breadth", 0) for c in concepts],
        "trend_3d": [c.get("trend_3d", 0) for c in concepts],
        "net_inflow": [c.get("net_inflow", 0) for c in concepts],
        "big_order": [c.get("big_order", 0) for c in concepts],
    }

    factor_pcts: dict[str, list[float]] = {
        k: _percentile_rank(v) for k, v in factor_values.items()
    }

    # 加权合成 + 量价背离惩罚 + 过热惩罚
    for i, c in enumerate(concepts):
        score = sum(
            weights.get(factor, 0) * factor_pcts[factor][i]
            for factor in factor_pcts
            if factor in weights
        )
        if c.get("volume_ratio", 0) > 1.5 and c.get("net_inflow", 0) < 0:
            score -= 0.15
        elif c.get("net_inflow", 0) < 0:
            score -= 0.05

        # 过热惩罚
        if c.get("trend_3d", 0) > 10.0 and c.get("net_inflow", 0) < 0:
            score -= 0.12
        elif c.get("trend_3d", 0) > 8.0 and c.get("net_inflow", 0) < 0:
            score -= 0.06

        c["score"] = round(score * 100, 1)
        for factor in weights:
            if factor in factor_pcts:
                c[f"{factor}_pct"] = round(factor_pcts[factor][i] * 100, 1)

    concepts.sort(key=lambda x: -x["score"])

    # 分类信号
    strong_entry = []
    preparing = []
    take_profit = []

    inflow_vals = factor_values["net_inflow"]
    inflow_top20 = sorted(inflow_vals, reverse=True)[min(19, len(inflow_vals) - 1)] if inflow_vals else 0
    top10_score = concepts[min(9, len(concepts) - 1)]["score"] if len(concepts) >= 10 else 0

    for c in concepts:
        if c["score"] >= top10_score and c.get("trend_3d", 0) > 0:
            strong_entry.append(c)
        if (c.get("pct_chg", 0) < 2.0 and c.get("net_inflow", 0) >= inflow_top20
                and c.get("volume_ratio", 0) > 1.5):
            preparing.append(c)
        if c.get("trend_3d", 0) > 8.0 and c.get("net_inflow", 0) < 0:
            take_profit.append(c)

    return {
        "timestamp": cached.get("timestamp", "?") + " (recalc)",
        "mode": mode,
        "mode_cn": ("盘后复盘" if mode == "evening" else "盘中实时") + " [重算]",
        "weights": weights,
        "total_concepts": len(concepts),
        "strong_entry": strong_entry[:top_n],
        "preparing": preparing[:10],
        "take_profit": take_profit[:10],
        "full_ranking": concepts[:top_n],
    }


def main():
    parser = argparse.ArgumentParser(description="概念板块轮动评分器（双模式）")
    parser.add_argument("--mode", choices=["evening", "intraday"], default="evening",
                        help="运行模式: evening=盘后复盘, intraday=盘中实时")
    parser.add_argument("--top", type=int, default=15, help="输出排名前N（默认15）")
    parser.add_argument("--no-proxy", action="store_true", help="不使用代理")
    parser.add_argument("--json", action="store_true", help="输出JSON")
    parser.add_argument("--recalc", action="store_true",
                        help="从缓存数据用当前权重重新打分（不联网）")
    args = parser.parse_args()

    if args.recalc:
        result = recalc_from_cache(mode=args.mode, top_n=args.top)
    else:
        result = concept_rotation(mode=args.mode, top_n=args.top,
                                  use_proxy=not args.no_proxy)
    if not result:
        return

    if args.json:
        suffix = "evening" if args.mode == "evening" else "intraday"
        out_path = DATA / f"concept_rotation_{suffix}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str),
                            encoding="utf-8")
        print(f"\n[rotation] 已写入 {out_path}")

    ts = result["timestamp"]
    weights = result["weights"]
    mode_cn = result["mode_cn"]

    print(f"\n{'='*88}")
    print(f"  概念板块轮动评分 [{mode_cn}]  {ts}  (共{result['total_concepts']}个概念)")
    print(f"  权重: ", end="")
    weight_strs = [f"{_factor_cn(k)}{int(v*100)}%" for k, v in weights.items()]
    print(" | ".join(weight_strs))
    print(f"{'='*88}")

    # 强势轮入
    if result["strong_entry"]:
        print(f"\n🔥 强势轮入 (综合Top10 + 3日趋势向上)")
        print(f"{'─'*88}")
        if args.mode == "intraday":
            print(f"  {'概念':<12} {'分数':>5} {'涨幅':>6} {'涨速':>6} "
                  f"{'量比':>5} {'广度':>5} {'主力流入':>10} {'龙头':<10}")
            print(f"{'─'*88}")
            for c in result["strong_entry"]:
                print(f"  {c['name']:<12} {c['score']:>5.1f} {c['pct_chg']:>+5.2f}% "
                      f"{c['speed']:>+5.2f} {c['volume_ratio']:>5.2f} "
                      f"{c['breadth']*100:>4.0f}% {_format_inflow(c['net_inflow']):>10} "
                      f"{c['leader_name']:<10}")
        else:
            print(f"  {'概念':<12} {'分数':>5} {'涨幅':>6} {'3日':>6} "
                  f"{'量比':>5} {'广度':>5} {'主力流入':>10} {'龙头':<10}")
            print(f"{'─'*88}")
            for c in result["strong_entry"]:
                print(f"  {c['name']:<12} {c['score']:>5.1f} {c['pct_chg']:>+5.2f}% "
                      f"{c['trend_3d']:>+5.2f} {c['volume_ratio']:>5.2f} "
                      f"{c['breadth']*100:>4.0f}% {_format_inflow(c['net_inflow']):>10} "
                      f"{c['leader_name']:<10}")

    # 蓄势待发
    if result["preparing"]:
        print(f"\n🌱 蓄势待发 (涨幅<2% + 主力流入Top20 + 量比>1.5)")
        print(f"{'─'*88}")
        for c in result["preparing"]:
            print(f"  {c['name']:<12} 分:{c['score']:>5.1f} 涨:{c['pct_chg']:>+5.2f}% "
                  f"量比:{c['volume_ratio']:.2f} 主力:{_format_inflow(c['net_inflow'])} "
                  f"龙头:{c['leader_name']}")

    # 获利了结
    if result["take_profit"]:
        print(f"\n⚠️  获利了结 (3日涨>8% + 今日主力流出)")
        print(f"{'─'*88}")
        for c in result["take_profit"]:
            print(f"  {c['name']:<12} 3日:{c['trend_3d']:>+5.2f}% "
                  f"今日流出:{_format_inflow(c['net_inflow'])} 涨幅:{c['pct_chg']:>+.2f}%")

    # 全排名
    print(f"\n📊 综合评分 Top{args.top}")
    print(f"{'─'*88}")
    if args.mode == "intraday":
        print(f"  {'#':>2} {'概念':<12} {'分数':>5} {'涨幅':>6} {'涨速':>5} "
              f"{'量比':>5} {'广度':>5} {'涨/跌':>5} {'主力流入':>10} {'龙头':<8}")
    else:
        print(f"  {'#':>2} {'概念':<12} {'分数':>5} {'涨幅':>6} {'3日':>6} "
              f"{'量比':>5} {'换手':>5} {'广度':>5} {'涨/跌':>5} {'主力流入':>10} {'龙头':<8}")
    print(f"{'─'*88}")
    for i, c in enumerate(result["full_ranking"], 1):
        if args.mode == "intraday":
            print(f"  {i:>2} {c['name']:<12} {c['score']:>5.1f} {c['pct_chg']:>+5.2f} "
                  f"{c['speed']:>+4.1f} {c['volume_ratio']:>5.2f} "
                  f"{c['breadth']*100:>4.0f}% {c['up_count']:>2}/{c['down_count']:<2} "
                  f"{_format_inflow(c['net_inflow']):>10} {c['leader_name']:<8}")
        else:
            print(f"  {i:>2} {c['name']:<12} {c['score']:>5.1f} {c['pct_chg']:>+5.2f} "
                  f"{c['trend_3d']:>+5.2f} {c['volume_ratio']:>5.2f} "
                  f"{c['turnover']:>5.1f} {c['breadth']*100:>4.0f}% "
                  f"{c['up_count']:>2}/{c['down_count']:<2} "
                  f"{_format_inflow(c['net_inflow']):>10} {c['leader_name']:<8}")
    print(f"{'='*88}")


def _factor_cn(key: str) -> str:
    """因子英文key转中文"""
    mapping = {
        "momentum": "动量", "volume_ratio": "量比", "speed": "涨速",
        "turnover": "换手", "breadth": "广度", "trend_3d": "3日趋势",
        "net_inflow": "主力", "big_order": "超大单",
    }
    return mapping.get(key, key)


if __name__ == "__main__":
    main()
