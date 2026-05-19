"""统一的流动性 / 量能 / 价格质量指标。

供各 scanner 在 fetch 完 K 线后调用，输出口径一致的 amt_5d_yi 和 vol_ratio
字段，避免每条策略各自重新发明轮子（单位不同、口径不同、阈值不同）。

典型用法：
    from strategies._quality import compute_metrics, passes_quality

    df = fetcher.get_price_history(code, days=65)
    m  = compute_metrics(df)
    if not passes_quality(m):
        return None
    pick = {..., "amt_5d_yi": m["amt_5d_yi"], "vol_ratio": m["vol_ratio"]}
"""
from __future__ import annotations

import functools
import json
from pathlib import Path
from typing import Optional

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_DATA_DIR = _REPO_ROOT / "data"


# Default gate thresholds — strategies can override per-策略
DEFAULT_MIN_AMT_YI    = 0.5   # 5 日均成交额 ≥ 0.5 亿（排除死水股）
DEFAULT_MIN_VOL_RATIO = 0.5   # 5 日量 / 60 日量 ≥ 0.5（排除越来越冷）


# 行业黑名单 — 默认排除"不想看的赛道"（精确匹配整个 industry 字符串）
BLACKLIST_INDUSTRIES = frozenset([
    # 酒及饮料
    "白酒", "红黄酒", "啤酒", "软饮料", "乳制品",
    # 金融
    "证券", "银行", "保险", "多元金融",
    # 地产装修
    "区域地产", "全国地产", "房产服务", "园区开发", "装修装饰",
    # 消费品杂项
    "食品", "家用电器", "服饰", "家居用品", "百货", "超市连锁",
    "文教休闲", "酒店餐饮", "旅游景点", "旅游服务",
    # 农业
    "农药化肥", "农业综合", "饲料", "种植业", "渔业", "林业",
    # 交通运输
    "航空", "仓储物流", "运输设备", "水运", "港口", "铁路",
    "机场", "公路", "空运", "公共交通",
])


def is_blacklisted(industry: str) -> bool:
    """substring 匹配 — industry 含任意 BLACKLIST_INDUSTRIES 关键词即命中。

    精确匹配会漏 "白酒II" / "白酒制造" 这类变体；substring 能稳健覆盖。
    空字符串视为"未知行业"，不命中（防止 scanner 误剔无 industry 数据的票）。
    """
    if not industry:
        return False
    return any(kw in industry for kw in BLACKLIST_INDUSTRIES)


@functools.lru_cache(maxsize=1)
def load_name_industry_map() -> tuple[dict[str, str], dict[str, str]]:
    """从 data/stock_names.json 读出 (name_map, industry_map)，6 位 code 作 key。

    lru_cache 让进程内重复调用零成本。stock_names.json 每天 02:00 sync_Knowledge
    刷新，scanner 进程通常一次跑完就退出，缓存命中策略适用。
    """
    p = _DATA_DIR / "stock_names.json"
    if not p.exists():
        return {}, {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}, {}
    names: dict[str, str] = {}
    inds:  dict[str, str] = {}
    for ts_code, info in raw.items():
        code6 = ts_code.split(".")[0]
        if isinstance(info, dict):
            names[code6] = info.get("name", code6)
            inds[code6]  = info.get("industry", "")
        else:
            names[code6] = str(info)
            inds[code6]  = ""
    return names, inds


def load_universe(
    universe_path: Optional[Path] = None,
    drop_bj: bool = True,
    drop_st: bool = True,
) -> list[str]:
    """加载 universe 代码列表，默认排除北证 + ST/退市。

    universe_path 默认 data/universe_main.json；自动调 load_name_industry_map 拿 ST name。
    """
    if universe_path is None:
        universe_path = _DATA_DIR / "universe_main.json"
    raw = json.loads(Path(universe_path).read_text(encoding="utf-8"))
    codes = raw if isinstance(raw, list) else list(raw.keys())
    if not (drop_bj or drop_st):
        return codes
    name_map: dict[str, str] = {}
    if drop_st:
        name_map, _ = load_name_industry_map()
    out = []
    for c in codes:
        if drop_bj and is_bj_code(c):
            continue
        if drop_st:
            n = name_map.get(c[-6:], "")
            if "ST" in n.upper() or "退" in n:
                continue
        out.append(c)
    return out


def load_marketcap_cache(max_days: int = 7, verbose: bool = True) -> dict[str, float]:
    """读取 data/marketcap_cache.json，返回 {6位代码: 总市值(元)}。

    max_days 控制过期检查：缓存日期早于 max_days 天前时仍返回旧数据（带 warning）。
    文件缺失、IO 错误、或 data 字段为空均返回 {}。
    """
    p = _DATA_DIR / "marketcap_cache.json"
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        if verbose:
            print(f"[marketcap_cache] 读取失败: {e}")
        return {}
    data: dict[str, float] = raw.get("data", {}) or {}
    cache_date = raw.get("date", "")
    if cache_date:
        from datetime import date as _d, timedelta as _td
        cutoff = (_d.today() - _td(days=max_days)).strftime("%Y%m%d")
        if cache_date < cutoff:
            if data:
                if verbose:
                    print(f"[marketcap_cache] 已过期({cache_date})，仍使用旧数据({len(data)}只)")
                return data
            if verbose:
                print(f"[marketcap_cache] 过期({cache_date})且为空，跳过")
            return {}
    return data


def inject_marketcap(spot_df, cache: Optional[dict[str, float]] = None, verbose: bool = True):
    """给 Sina 行情 DataFrame 注入 "总市值" 列（从磁盘缓存映射）。

    Sina 代码带 sh/sz/bj 前缀，先规一化到 6 位再 map。cache=None 时自动加载。
    返回新的 DataFrame；如果缓存为空或 spot 为空，原样返回（可能仍无总市值列）。
    """
    if cache is None:
        cache = load_marketcap_cache(verbose=verbose)
    if not cache or spot_df is None or len(spot_df) == 0:
        return spot_df
    spot_df = spot_df.copy()
    # 代码规一化（"sh603486" / "sz301377" → "603486"）— 复用 fetcher.normalize_code 避免散落
    import sys as _sys
    _sys.path.insert(0, str(_REPO_ROOT / "src"))
    import fetcher as _f
    norm = spot_df["代码"].astype(str).apply(_f.normalize_code)
    spot_df["总市值"] = norm.map(cache)
    if verbose:
        print(f"[marketcap_cache] 已注入 {len(cache)} 只市值")
    return spot_df


@functools.lru_cache(maxsize=1)
def load_quality_cache() -> dict[str, dict]:
    """读取 data/quality_metrics_latest.json，返回 {code6: metrics(含 close)}。

    过期（非当日）或文件不存在时返回 {}。下游 scanner 优先查这个缓存避免
    重复 compute_metrics，缓存 miss 时再回退到现拉。
    """
    p = _DATA_DIR / "quality_metrics_latest.json"
    if not p.exists():
        return {}
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y%m%d")
    if d.get("date", "") != today:
        return {}
    return d.get("metrics", {})


def enrich_pick(code: str, days: int = 65) -> Optional[dict]:
    """5 路 scanner 公共流水线：优先 quality cache → fallback 现拉。

    Returns 含 metrics 字段（amt_5d_yi/vol_ratio/is_limit_today/is_yi_zi）+ close。
    fetch 失败或质量门槛不过返回 None。
    """
    code6 = code[-6:]
    # 优先读 quality_metrics_latest 缓存（当日 fresh）
    cache = load_quality_cache()
    if code6 in cache:
        m = cache[code6]
        if not passes_quality(m):
            return None
        return m
    # 缓存 miss — 现拉
    import sys
    sys.path.insert(0, str(_REPO_ROOT / "src"))
    try:
        import fetcher as _f
        df = _f.get_price_history(code6, days=days)
    except Exception:
        return None
    if df is None or len(df) < 5:
        return None
    m = compute_metrics(df, code6)
    if not passes_quality(m):
        return None
    return {**m, "close": float(df["close"].iloc[-1])}


def is_bj_code(code: str) -> bool:
    """识别北证 A 股代码 — 排除主板/创业/科创以外的票。

    支持三种格式：bj 前缀（'bj920000'）、sh/sz 前缀剥离后 8x/43 开头、纯数字 8x/43。
    注意：688/689 是科创板（上交所），不算北证。
    """
    if not code:
        return False
    if code[:2].lower() == "bj":
        return True
    digits = code[-6:] if len(code) >= 6 else code
    if not digits.isdigit():
        return False
    if digits.startswith("43"):
        return True
    if digits.startswith("8") and not digits.startswith("86"):
        return True
    return False


def _limit_threshold_pct(code: str) -> float:
    """日涨跌停阈值（百分比，留 0.5pp 浮点缓冲）。

    创业板 (300/301) + 科创板 (688/689) 是 20% → 19.0；其余主板/中小板 10% → 9.5。
    ST 股票名字含 "ST" 但代码无 prefix 区分——scanner 层已通过 name 检查剔除 ST，
    这里只按板块代码判断。
    """
    d = code[-6:] if code else ""
    if len(d) >= 3 and d[:3] in ("300", "301", "688", "689"):
        return 19.0
    return 9.5


def compute_metrics(df: Optional[pd.DataFrame], code: str = "") -> dict:
    """从价格历史 DataFrame 算质量指标，口径全 A 股统一。

    要求列：close, volume；可选：high, low（用于一字板判定）。
    单位约定：volume = 手（×100 = 股）；close = 元。
    可选 code：用于按板块判涨跌停阈值（主板 9.5% / 创业·科创 19%）。

    Returns dict with:
      - amt_5d_yi:      5 日均成交额（亿）
      - vol_ratio:      5 日均量 / 60 日均量（数据不够时给 1.0 neutral）
      - is_limit_today: 当日相对前日 |chg_pct| ≥ 板块阈值（涨跌停）
      - is_yi_zi:       当日 high == low（一字板）

    数据不全时返回 {} —— 调用方将这视为"质量不达标，剔除"。
    """
    if df is None or len(df) < 5 or "close" not in df.columns or "volume" not in df.columns:
        return {}
    closes = df["close"].values
    vols   = df["volume"].values
    close  = float(closes[-1])
    if close <= 0:
        return {}

    avg_vol_5d = float(vols[-5:].mean())
    amt_5d_yi  = avg_vol_5d * close * 100 / 1e8   # 手→股 ×100，元→亿 /1e8

    if len(vols) >= 20:
        n60 = min(60, len(vols))
        avg_vol_60d = float(vols[-n60:].mean())
        vol_ratio = avg_vol_5d / avg_vol_60d if avg_vol_60d > 0 else 0.0
    else:
        vol_ratio = 1.0   # 数据不够给 neutral，不卡 < 20 天上市的新股

    is_limit_today = False
    if len(closes) >= 2:
        prev = float(closes[-2])
        thr = _limit_threshold_pct(code)
        if prev > 0 and abs(close - prev) / prev * 100 >= thr:
            is_limit_today = True

    is_yi_zi = False
    if "high" in df.columns and "low" in df.columns:
        if float(df["high"].iloc[-1]) == float(df["low"].iloc[-1]):
            is_yi_zi = True

    return {
        "amt_5d_yi":      round(amt_5d_yi, 2),
        "vol_ratio":      round(vol_ratio, 2),
        "is_limit_today": is_limit_today,
        "is_yi_zi":       is_yi_zi,
    }


def passes_liquidity(metrics: dict,
                     min_amt_yi: float = DEFAULT_MIN_AMT_YI,
                     min_vol_ratio: float = DEFAULT_MIN_VOL_RATIO) -> bool:
    """流动性 + 量能门槛单测。"""
    if not metrics:
        return False
    return (metrics.get("amt_5d_yi", 0) >= min_amt_yi and
            metrics.get("vol_ratio", 0) >= min_vol_ratio)


def passes_quality(metrics: dict,
                   reject_limits: bool = True,
                   min_amt_yi: float = DEFAULT_MIN_AMT_YI,
                   min_vol_ratio: float = DEFAULT_MIN_VOL_RATIO) -> bool:
    """流动性 + 量能 + 当日涨跌停/一字板综合判断。"""
    if not passes_liquidity(metrics, min_amt_yi, min_vol_ratio):
        return False
    if reject_limits and (metrics.get("is_limit_today") or metrics.get("is_yi_zi")):
        return False
    return True


def pick_fields(metrics: dict) -> dict:
    """只取要 inline 进 pick 的展示字段（amt/vol_ratio）。"""
    return {
        "amt_5d_yi": metrics.get("amt_5d_yi", 0.0),
        "vol_ratio": metrics.get("vol_ratio", 0.0),
    }
