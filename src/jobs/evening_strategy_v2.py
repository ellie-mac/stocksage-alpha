#!/usr/bin/env python3
"""
多策略汇总（晚间）— 合并主策略/小盘/金叉/筹码/低市值/热榜/横盘/扶梯八路信号，统一推送。

22:00 跑，所有 scanner 跑完后汇总当日多策略共振结果。
股票按覆盖策略数从多到少排列；同策略内按原始得分排序。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

from common import push_wechat

DATA = ROOT / "data"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[evening_strategy] 读取 {path.name} 失败: {e}")
        return None


def _latest_trading_day(ref: datetime | None = None):
    """Return the latest trading day using weekday proxy (Mon-Fri)."""
    now = ref or datetime.now()
    d = now.date()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _trading_day_gap(file_dt, ref: datetime | None = None) -> int:
    """Trading-day gap from file date to latest trading day; weekends don't count."""
    latest = _latest_trading_day(ref)
    cur = file_dt.date() if hasattr(file_dt, "date") else file_dt
    if cur > latest:
        return 0
    gap = 0
    while cur < latest:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            gap += 1
    return gap


def _is_fresh(date_str: str, fmt: str = "%Y%m%d", max_days: int = 1) -> bool:
    """按交易日判断新鲜度：最新交易日=0，上一交易日=1；周末看到周五仍算最新有效。"""
    if not date_str:
        return False
    try:
        d = datetime.strptime(date_str[:10].replace("-", "")[:8], fmt)
        gap = _trading_day_gap(d)
        return 0 <= gap <= max_days
    except Exception:
        return False


def _norm_code(code: str) -> str:
    """Strip sh/sz exchange prefix; keep last 6 digits.

    gc_scan emits 'sh603486' / 'sz301377' while chip/hot/marketcap use pure 6-digit
    codes. Without normalization, set intersections silently return 0.
    """
    if not code:
        return code
    s = str(code)
    return s.lower().lstrip("shz")[-6:] if any(c.isalpha() for c in s) else s


def _filter_blacklist(picks: list[dict], ind_map: dict[str, str]) -> list[dict]:
    """剔除 industry 在 BLACKLIST_INDUSTRIES 中的 picks（兜底，scanner 已过滤过一次）。"""
    if not ind_map:
        return picks
    sys.path.insert(0, str(ROOT / "src"))
    from strategies._quality import is_blacklisted
    return [p for p in picks if not is_blacklisted(ind_map.get(p["code"], ""))]


# 科技 (TMT) 行业关键词 — 与 src/strategies/sideways_scan.py 保持同步
_TECH_KEYWORDS = (
    "半导体", "集成电路", "芯片",
    "软件", "计算机", "互联网", "信息",
    "通信",
    "元器件", "电子", "光电",
    "网络", "数据", "云", "操作系统",
    "智能", "人工智", "IT",
)


def _is_tech(industry: str) -> bool:
    if not industry:
        return False
    return any(kw in industry for kw in _TECH_KEYWORDS)


def _load_industry_map() -> dict[str, str]:
    """从 stock_names.json 读取 {6位code: industry}。"""
    p = DATA / "stock_names.json"
    if not p.exists():
        print(f"[evening_strategy] stock_names.json 不存在，无法过滤科技行业")
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        return {ts.split(".")[0]: (info.get("industry", "") if isinstance(info, dict) else "")
                for ts, info in raw.items()}
    except Exception as e:
        print(f"[evening_strategy] 读取 stock_names.json 失败: {e}")
        return {}


def _filter_tech(picks: list[dict], ind_map: dict[str, str]) -> list[dict]:
    """只保留 industry 在 _TECH_KEYWORDS 中匹配的 picks。"""
    if not ind_map:
        return picks
    return [p for p in picks if _is_tech(ind_map.get(p["code"], ""))]


# 流动性过滤 — 复用 _quality.compute_metrics 口径（amt_5d_yi 亿）
_LIQUIDITY_MIN_AMT_YI = 0.5


def _lookup_amt_yi(code: str, cache: dict[str, float | None]) -> float | None:
    """从 fetcher.get_price_history 拉 5 日均成交额（亿）。命中价格缓存几乎免费。"""
    if code in cache:
        return cache[code]
    try:
        sys.path.insert(0, str(ROOT / "src"))
        import fetcher as _f
        df = _f.get_price_history(code, days=10)
        if df is None or "volume" not in df.columns or len(df) < 5:
            cache[code] = None
            return None
        closes = df["close"].tail(5).values
        vols   = df["volume"].tail(5).values
        amt = float((closes * vols).mean()) * 100 / 1e8
        cache[code] = amt
        return amt
    except Exception:
        cache[code] = None
        return None


def _filter_liquid(picks: list[dict], min_amt_yi: float,
                   cache: dict[str, float | None]) -> list[dict]:
    """按 5 日均成交额下限过滤。若 pick 本身没有 amt_5d_yi 字段，从 fetcher 现算。

    Bulk-warms cache via ThreadPoolExecutor before per-pick filtering — turns
    a 21s serial fetch into ~2s parallel on first cold run.
    """
    missing = [p["code"] for p in picks
               if p.get("amt_5d_yi") is None and p["code"] not in cache]
    if missing:
        from concurrent.futures import ThreadPoolExecutor as _TPE
        with _TPE(max_workers=10) as _ex:
            list(_ex.map(lambda c: _lookup_amt_yi(c, cache), missing))

    out: list[dict] = []
    for p in picks:
        amt = p.get("amt_5d_yi")
        if amt is None:
            amt = cache.get(p["code"])
        if amt is not None and amt >= min_amt_yi:
            if "amt_5d_yi" not in p:
                p = dict(p, amt_5d_yi=amt)
            out.append(p)
    return out


# ── Source freshness check ────────────────────────────────────────────────────

_SOURCE_FILES = [
    ("主", "latest_picks.json",        "timestamp"),  # ISO timestamp w/ date prefix
    ("小", "latest_picks.json",        "timestamp"),  # same file as 主
    ("叉", "golden_cross_latest.json", "date"),       # YYYYMMDD
    ("筹", "chip_scan_latest.json",    "date"),
    ("市", "marketcap_latest.json",    "date"),
    ("热", "hot_scan_latest.json",     "date"),
    ("横", "sideways_latest.json",     "date"),
    ("扶", "escalator_latest.json",    "date"),       # 扶梯策略（活跃慢牛）
]


def _check_sources(max_days: int = 1) -> dict[str, dict]:
    """扫描 7 路 source 文件，返回 {tag: {"status": ..., "age_days": N, "file_date": "YYYYMMDD"}}.
    status: 'fresh' (<= max_days)、'stale' (older)、'missing' (file 不存在或日期字段缺失)
    age_days 按交易日口径计算，周末不计入过期。
    """
    out: dict[str, dict] = {}
    for tag, fname, key in _SOURCE_FILES:
        path = DATA / fname
        if not path.exists():
            out[tag] = {"status": "missing", "reason": "no file"}
            continue
        try:
            d = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            out[tag] = {"status": "missing", "reason": f"read error: {str(e)[:50]}"}
            continue
        raw = d.get(key, "")
        if not raw:
            out[tag] = {"status": "missing", "reason": f"empty {key}"}
            continue
        digits = "".join(c for c in str(raw)[:10] if c.isdigit())[:8]
        if len(digits) != 8:
            out[tag] = {"status": "missing", "reason": f"bad date: {raw[:20]}"}
            continue
        try:
            file_date = datetime.strptime(digits, "%Y%m%d").date()
        except Exception:
            out[tag] = {"status": "missing", "reason": f"unparseable: {digits}"}
            continue
        age = _trading_day_gap(file_date)
        status = "fresh" if 0 <= age <= max_days else "stale"
        out[tag] = {"status": status, "age_days": age, "file_date": digits}
    return out


# ── Data loaders ──────────────────────────────────────────────────────────────

def _load_main() -> tuple[list[dict], list[dict]]:
    """Returns (主策略picks, 小盘picks); each item: {code, name, score}."""
    d = _load(DATA / "latest_picks.json")
    if not d:
        return [], []
    ts = d.get("timestamp", "")
    if ts and not _is_fresh(ts[:10].replace("-", ""), "%Y%m%d"):
        print("[evening_strategy] latest_picks.json 数据已过期，跳过主/小盘策略")
        return [], []

    def _extract(lst: list) -> list[dict]:
        return [{"code": _norm_code(p["code"]), "name": p.get("name", ""), "score": p.get("score", 0)}
                for p in lst if p.get("code")]

    return _extract(d.get("results", [])), _extract(d.get("smallcap", []))


def _load_gc() -> list[dict]:
    """Returns [{code, name, score, tier, close}, ...] for all tiers in tiers dict.
    Codes are normalized to 6-digit form (gc_scan emits sh/sz prefixes)."""
    d = _load(DATA / "golden_cross_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[evening_strategy] golden_cross_latest.json 数据已过期，跳过金叉策略")
        return []
    picks = []
    for tier, tier_picks in d.get("tiers", {}).items():
        for p in tier_picks:
            raw = p.get("code")
            if raw:
                picks.append({"code": _norm_code(raw), "name": p.get("name", ""),
                               "score": p.get("gc_score", 0), "tier": tier,
                               "close": p.get("close")})
    return picks


def _load_chip() -> list[dict]:
    """Returns [{code, name, score, tier, close}, ...] for all tiers in all_picks."""
    d = _load(DATA / "chip_scan_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[evening_strategy] chip_scan_latest.json 数据已过期，跳过筹码策略")
        return []
    picks = []
    for p in d.get("all_picks", []):
        if p.get("code"):
            picks.append({"code": p["code"], "name": p.get("name", ""),
                          "score": p.get("winner_rate", 0), "tier": p.get("tier", ""),
                          "close": p.get("close")})
    return picks


def _load_hot() -> list[dict]:
    """Returns [{code, name, score, rank, close}, ...] from hot_scan_latest.json (top 30 by score)."""
    d = _load(DATA / "hot_scan_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[evening_strategy] hot_scan_latest.json 数据已过期，跳过热榜策略")
        return []
    picks = []
    for p in d.get("picks", []):
        if p.get("code"):
            picks.append({"code": p["code"], "name": p.get("name", ""),
                          "score": p.get("score", 0), "rank": p.get("rank"),
                          "close": p.get("close")})
    return picks


def _load_sideways() -> list[dict]:
    """Returns [{code, name, score, tier, close, range_pct, avg_amt_5d_yi}, ...]
    from sideways_latest.json (8 tiers HX0~HS3, 0-based)."""
    d = _load(DATA / "sideways_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[evening_strategy] sideways_latest.json 数据已过期，跳过横盘策略")
        return []
    picks = []
    for p in d.get("all_picks", []):
        if p.get("code"):
            picks.append({"code": p["code"], "name": p.get("name", ""),
                          "score": -float(p.get("range_pct", 100.0)),
                          "tier": p.get("tier", ""),
                          "close": p.get("close"),
                          "range_pct": p.get("range_pct"),
                          "avg_amt_5d_yi": p.get("avg_amt_5d_yi"),
                          "industry": p.get("industry", "")})
    return picks


def _load_marketcap() -> list[dict]:
    """Returns [{code, name, price, marketcap_yi, mv_rank}, ...] from marketcap_latest.json."""
    d = _load(DATA / "marketcap_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[evening_strategy] marketcap_latest.json 数据已过期，跳过市值策略")
        return []
    return [
        {"code": p["code"], "name": p.get("name", ""), "score": 0,
         "price": p.get("price"), "marketcap_yi": p.get("marketcap_yi"),
         "mv_rank": p.get("mv_rank")}
        for p in d.get("picks", []) if p.get("code")
    ]


def _load_regime() -> dict | None:
    """Read regime score from latest_picks.json (main_strategy writes it).
    Returns {"score": float, "signal": str} or None.
    """
    d = _load(DATA / "latest_picks.json")
    if not d:
        return None
    score = d.get("regime_score")
    if score is None:
        return None
    return {
        "score": float(score),
        "signal": d.get("regime", ""),
    }


def _regime_recommendation(score: float | None) -> list[str]:
    """根据回测得出的 regime × strategy 矩阵给当日策略建议。

    数据来源：80 天 marketcap + 20-22 天其他策略 backfill 结果（2026-05-22 跑出）：
      score ≤ 4 (caution/bear)：marketcap T+1 = 76%/+2.6% (caution) / 53.6%/+0.3% (bear)
      score 5-6 (neutral+)：各项接近 50%（最差 regime）
      score ≥ 7 (bull)：chip+marketcap 共振 T+5=55.2% / chip T+10=62.1% / 扶梯 T+10=61.5%
                       但 marketcap 单策略 bull regime T+1 只有 45.8%
    """
    if score is None:
        return []
    lines = []
    if score >= 7:
        lines.append(f"🎯 **大盘 {score:.0f}/10 (多头) — MA 多头排列**")
        lines.append("  ✅ 优先：**筹+市共振** / 筹 T+10 hold (62% win) / 扶梯 T+10 (61% win)")
        lines.append("  ❌ 避开：**市值单策略短打**（多头时小盘跑输大盘 T+1=46%）")
    elif score >= 5:
        lines.append(f"🎯 **大盘 {score:.0f}/10 (中性) — 方向未明**")
        lines.append("  ⚠️ 回测显示各策略此 regime **表现最差**（接近随机），建议谨慎")
        lines.append("  ⚪ 信号弱时优先观望，等明确多头或熊市信号")
    elif score >= 3:
        lines.append(f"🎯 **大盘 {score:.0f}/10 (走弱) — 跌破 MA20**")
        lines.append("  ✅ 优先：**市值 T+1 短打**（走弱期 T+1=76% 胜率 / +2.6% 均收益）")
        lines.append("  ❌ 避开：**长 hold**（T+5+ 显著走弱）")
    else:
        lines.append(f"🎯 **大盘 {score:.0f}/10 (熊市) — 跌破 MA60**")
        lines.append("  ✅ 优先：**市值 / 热榜 T+1 反弹**（短打 50%+ 胜率）")
        lines.append("  ❌ 避开：**任何 hold > T+1**（T+5/T+10 普遍负预期）")
    return lines


def _load_escalator() -> list[dict]:
    """Returns [{code, name, score, tier, close, slope_pct, r2, daily_amp}, ...]
    from escalator_latest.json (E0/E1/E2)."""
    d = _load(DATA / "escalator_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[evening_strategy] escalator_latest.json 数据已过期，跳过扶梯策略")
        return []
    picks = []
    for p in d.get("all_picks", []):
        if p.get("code"):
            picks.append({"code": p["code"], "name": p.get("name", ""),
                          "score": float(p.get("r2", 0)) * 100,
                          "tier": p.get("tier", ""),
                          "close": p.get("close"),
                          "slope_pct": p.get("slope_pct"),
                          "r2": p.get("r2"),
                          "daily_amp": p.get("daily_amp")})
    return picks


# ── Merge & tag ───────────────────────────────────────────────────────────────

def _merge(
    main: list[dict],
    small: list[dict],
    gc: list[dict],
    chip: list[dict],
    marketcap: list[dict],
    hot: list[dict],
    sideways: list[dict],
    escalator: list[dict],
) -> dict[str, dict]:
    """
    Returns registry: {code: {name, tags: list[str], details: {tag: pick_dict}}}.
    Tag order: 主 小 叉 筹 市 热 横 扶
    """
    registry: dict[str, dict] = {}

    def _add(tag: str, picks: list[dict]) -> None:
        for p in picks:
            code = p["code"]
            if code not in registry:
                registry[code] = {"name": p.get("name", ""), "tags": [], "details": {}}
            if tag not in registry[code]["tags"]:
                registry[code]["tags"].append(tag)
            registry[code]["details"][tag] = p

    _add("主", main)
    _add("小", small)
    _add("叉", gc)
    _add("筹", chip)
    _add("市", marketcap)
    _add("热", hot)
    _add("横", sideways)
    _add("扶", escalator)

    return registry


# ── 今日精选（基于回测组合优先级 + T+N hold 标注）────────────────────────────────

# 优先级越小越靠前；hold = 推荐持有周期；expected = 回测期望（仅展示）
# 命中多个规则时取 priority 最小的（最强信号）
# entry 字段：入场时点建议（基于 _entry_timing.py backtest 结论）
#   开盘抢   = D close 入场实测最强但当前 22:00 架构来不及，必须 D+1 open 抢，涨停就放弃
#   开盘买   = D+1 open 入场仍强（chip+gc[G0] D close 多赚 7pp avg，但 open 也能拿主体 alpha）
#   任意时点 = D+1 open / close 都行（escalator E0 / chip+gc[G2] 长 hold 不敏感）
#   尾盘谨慎 = D+1 close 入场会少赚一半 avg（chip[C1]+gc[G0] 类型）
_PICKS_RULES: list[dict] = [
    # 3-way + 小盘：n=12 / T+10 = 100% / +10.7% — 最强信号
    {"need": {"筹", "叉", "市"}, "chip": None, "gc": None, "mv_max": 50,
     "hold": "T+10", "expected": "75-100% / +10%", "entry": "任意时点", "priority": 0},
    # gc[G1]+市 TOP20：T+1 = 100% / +2.62% — 短线最稳（D+1 close 入场退化到 57%）
    {"need": {"叉", "市"}, "gc": "G1", "mv_max": 20,
     "hold": "T+1", "expected": "100% / +2.62%", "entry": "开盘抢", "priority": 1},
    # gc[G2]+市 TOP20：T+1 = 90.5% / +2.56% — 短线次选
    {"need": {"叉", "市"}, "gc": "G2", "mv_max": 20,
     "hold": "T+1", "expected": "90% / +2.56%", "entry": "开盘抢", "priority": 2},
    # chip[C1]+gc[G0]：T+10 = 75% / +17.70% — 中线王炸（D+1 close 少赚 7pp avg）
    {"need": {"筹", "叉"}, "chip": "C1", "gc": "G0",
     "hold": "T+10", "expected": "75% / +17.70%", "entry": "开盘买", "priority": 3},
    # chip[C1]+escalator[E0]：T+10 = 100% / +11.49%（n=13）— 漏补
    {"need": {"筹", "扶"}, "chip": "C1", "escalator": "E0",
     "hold": "T+10", "expected": "100% / +11.49% (n=13)", "entry": "开盘买", "priority": 4},
    # gc[G2]+市 TOP21-50：T+10 = 100% / +6.50%
    {"need": {"叉", "市"}, "gc": "G2", "mv_max": 50, "mv_min": 21,
     "hold": "T+10", "expected": "100% / +6.50%", "entry": "开盘买", "priority": 5},
    # chip[C0]+市 TOP20：T+5 = 83.3% / +3.95% (T+10=66.7%)
    {"need": {"筹", "市"}, "chip": "C0", "mv_max": 20,
     "hold": "T+5", "expected": "83% / +3.95%", "entry": "开盘买", "priority": 6},
    # chip[C1]+市 TOP20：T+10 = 100% / +17.95%（小样本）
    {"need": {"筹", "市"}, "chip": "C1", "mv_max": 20,
     "hold": "T+10", "expected": "100% / +17.95% (n小)", "entry": "开盘买", "priority": 7},
    # chip[C2]+市 TOP20：T+10 = 100% / +7.13%（n=10）— 漏补
    {"need": {"筹", "市"}, "chip": "C2", "mv_max": 20,
     "hold": "T+10", "expected": "100% / +7.13% (n=10)", "entry": "开盘买", "priority": 8},
    # 3-way mv51-100：T+10 = 75% / +6.03%（n=14）— 漏补
    {"need": {"筹", "叉", "市"}, "chip": None, "gc": None, "mv_max": 100, "mv_min": 51,
     "hold": "T+10", "expected": "75% / +6.03%", "entry": "任意时点", "priority": 9},
    # chip[C0/C1]+gc[G2]：T+10 = 73-74% / +9-11% — 中线大样本（三种入场差不多）
    {"need": {"筹", "叉"}, "chip": "C0", "gc": "G2",
     "hold": "T+10", "expected": "73% / +9.85%", "entry": "任意时点", "priority": 10},
    {"need": {"筹", "叉"}, "chip": "C1", "gc": "G2",
     "hold": "T+10", "expected": "74% / +11.30%", "entry": "任意时点", "priority": 11},
    # escalator[E0] 单：T+10 = 69% / +8.25%（三种入场都 88-91% win）
    {"need": {"扶"}, "alone": True, "escalator": "E0",
     "hold": "T+10", "expected": "69% / +8.25%", "entry": "任意时点", "priority": 12},
    # ── Regime-conditional fallback（具体组合都不命中时启用）──
    {"need": {"筹", "市"}, "mv_max": 20, "regime_max": 4,
     "hold": "T+1", "expected": "80% (regime caution)", "entry": "开盘抢", "priority": 20},
    {"need": {"筹", "市"}, "chip_in": {"C0", "C1"}, "mv_max": 20, "regime_min": 7,
     "hold": "T+20", "expected": "80% / +18% (regime bull, 重 hold)", "entry": "任意时点", "priority": 21},
]


def _classify_pick(code: str, entry: dict, regime_score: float | None = None) -> dict | None:
    """匹配第一个适用规则。返回 {"hold", "expected", "priority", "rule_desc"} 或 None。

    regime_score: 当前 CSI300 regime（1-9）；某些 fallback 规则要求 regime 区间。
    """
    tags = set(entry["tags"])
    det  = entry["details"]
    chip_tier = det.get("筹", {}).get("tier", "")
    gc_tier   = det.get("叉", {}).get("tier", "")
    esc_tier  = det.get("扶", {}).get("tier", "")
    mv_rank   = det.get("市", {}).get("mv_rank")
    try:
        mv_rank = int(mv_rank) if mv_rank is not None else None
    except (TypeError, ValueError):
        mv_rank = None

    for rule in _PICKS_RULES:
        if not rule["need"].issubset(tags):
            continue
        if rule.get("alone") and tags != rule["need"]:
            continue
        if rule.get("chip") and chip_tier != rule["chip"]:
            continue
        if rule.get("chip_in") and chip_tier not in rule["chip_in"]:
            continue
        if rule.get("gc") and gc_tier != rule["gc"]:
            continue
        if rule.get("escalator") and esc_tier != rule["escalator"]:
            continue
        if rule.get("mv_max"):
            if mv_rank is None or mv_rank > rule["mv_max"]:
                continue
            if rule.get("mv_min") and mv_rank < rule["mv_min"]:
                continue
        # regime conditional gates
        if rule.get("regime_max") is not None:
            if regime_score is None or regime_score > rule["regime_max"]:
                continue
        if rule.get("regime_min") is not None:
            if regime_score is None or regime_score < rule["regime_min"]:
                continue
        # 命中
        return {
            "hold": rule["hold"],
            "expected": rule["expected"],
            "entry": rule.get("entry", "开盘买"),
            "priority": rule["priority"],
            "rule_tags": "+".join(sorted(rule["need"])),
        }
    return None


def _build_today_picks(registry: dict[str, dict], max_picks: int = 15,
                        regime_score: float | None = None) -> list[tuple[str, dict, dict]]:
    """返回 [(code, entry, classification), ...] 按 priority + mv_rank 升序，最多 max_picks 个"""
    classified: list[tuple[str, dict, dict]] = []
    for code, entry in registry.items():
        c = _classify_pick(code, entry, regime_score=regime_score)
        if c is not None:
            classified.append((code, entry, c))
    # priority 升序 → mv_rank 升序（小盘优先）→ code
    classified.sort(key=lambda x: (
        x[2]["priority"],
        x[1]["details"].get("市", {}).get("mv_rank") or 9999,
        x[0],
    ))
    return classified[:max_picks]


def _fmt_today_pick(code: str, entry: dict, cls: dict) -> str:
    """⭐ T+10  000123 ABC ¥10.50 ┃ 75%/+9.85% ┃ 筹C0+叉G2"""
    name = entry["name"]
    det  = entry["details"]
    # price
    price = None
    for tag in ("市", "主", "小"):
        p = det.get(tag, {}).get("price")
        if p:
            price = float(p)
            break
    if price is None:
        for tag in ("叉", "筹", "热", "横", "扶"):
            close = det.get(tag, {}).get("close")
            if close:
                price = float(close)
                break
    price_str = f" ¥{price:.2f}" if price else ""
    # tier annotations (compact)
    pieces = []
    if "筹" in det:
        t = det["筹"].get("tier", "")
        if t:
            pieces.append(f"筹{t}")
    if "叉" in det:
        t = det["叉"].get("tier", "")
        if t:
            pieces.append(f"叉{t}")
    if "扶" in det:
        t = det["扶"].get("tier", "")
        if t:
            pieces.append(f"扶{t}")
    if "市" in det:
        rk = det["市"].get("mv_rank")
        if rk:
            pieces.append(f"市#{rk}")
    tier_str = "+".join(pieces) if pieces else cls["rule_tags"]
    # entry 标签前缀：开盘抢 → 🚨 强提示 / 开盘买 → 📈 / 任意时点 → ⚪
    entry = cls.get("entry", "开盘买")
    entry_icon = {"开盘抢": "🚨", "开盘买": "📈", "任意时点": "⚪", "尾盘谨慎": "⚠️"}.get(entry, "")
    return (f"**[{cls['hold']}]** {entry_icon}{entry} ┃ {code} {name}{price_str} ┃ "
            f"_{cls['expected']}_ ┃ {tier_str}")


# ── Format ────────────────────────────────────────────────────────────────────

def _tag_str(tags: list[str]) -> str:
    return "`" + ",".join(tags) + "`"


def _fmt_pick(code: str, entry: dict) -> str:
    name = entry["name"]
    tags = _tag_str(entry["tags"])
    details = entry["details"]

    # Price fallback chain: 市/主/小 用 price 字段，叉/筹/热/横/扶 用 close
    price = None
    for tag in ("市", "主", "小"):
        p = details.get(tag, {}).get("price")
        if p:
            price = float(p)
            break
    if price is None:
        for tag in ("叉", "筹", "热", "横", "扶"):
            close = details.get(tag, {}).get("close")
            if close:
                price = float(close)
                break

    price_str = f"  ¥{price:.2f}" if price else ""

    # Score annotations
    annotations = []
    if "主" in details:
        s = details["主"].get("score", 0)
        if s:
            annotations.append(f"主{s:.0f}")
    if "小" in details:
        s = details["小"].get("score", 0)
        if s:
            annotations.append(f"小{s:.0f}")
    if "叉" in details:
        t = details["叉"].get("tier", "")
        s = details["叉"].get("score", 0)
        annotations.append(f"叉{t}({s}叉)")
    if "筹" in details:
        t = details["筹"].get("tier", "")
        s = details["筹"].get("score", 0)
        annotations.append(f"筹{t}({s:.0f}%)")
    if "市" in details:
        mv = details["市"].get("marketcap_yi")
        if mv:
            annotations.append(f"市值{mv:.1f}亿")
    if "热" in details:
        r = details["热"].get("rank")
        if r:
            annotations.append(f"热#{r}")
    if "横" in details:
        t = details["横"].get("tier", "")
        rp = details["横"].get("range_pct")
        amt = details["横"].get("avg_amt_5d_yi")
        ind = details["横"].get("industry", "")
        if t and rp is not None:
            amt_s = f"/{amt:.1f}亿" if amt else ""
            ind_s = f"·{ind}" if ind else ""
            spec = _SIDEWAYS_TIER_SHORT.get(t, t)
            annotations.append(f"{spec}(振{rp:.1f}%{amt_s}{ind_s})")
    if "扶" in details:
        t = details["扶"].get("tier", "")
        sp = details["扶"].get("slope_pct")
        r2 = details["扶"].get("r2")
        if t and sp is not None and r2 is not None:
            # 档位 → 窗口天数（跟 escalator_scan._TIER_SPEC 同步）；slope 整数；R² 保留两位
            window = {"E0": 20, "E1": 10, "E2": 5}.get(t, 0)
            annotations.append(f"扶{window}d/{sp:.0f}%/R²{r2:.2f}")

    ann_str = "  " + "  ".join(annotations) if annotations else ""
    return f"**{code} {name}** {tags}{price_str}{ann_str}"


MAX_SINGLE = 20  # per-strategy section cap (single-strategy stocks only)
MAX_MULTI  = 50  # 多策略共振块 cap — 防止爆量日（如横盘 2000+）让 push body 超出 PushPlus 限制

# sideways tier 短标签（用于 _fmt_pick 注释）：HX严/HS宽 + 窗口×阈值
_SIDEWAYS_TIER_SHORT = {
    "HX0": "30d5%严", "HS0": "30d5%宽",
    "HX1": "20d4%严", "HS1": "20d4%宽",
    "HX2": "10d3%严", "HS2": "10d3%宽",
    "HX3": "5d2%严",  "HS3": "5d2%宽",
}

_TIER_ORDER = {
    "G0": 0, "G1": 1, "G2": 2,
    "C0": 0, "C1": 1, "C2": 2,
    "HX0": 0, "HS0": 1, "HX1": 2, "HS1": 3, "HX2": 4, "HS2": 5, "HX3": 6, "HS3": 7,
    "E0": 0, "E1": 1, "E2": 2,
}


def _build_message(
    registry: dict[str, dict],
    tech_only: bool = False,
    source_status: dict[str, dict] | None = None,
    regime: dict | None = None,
) -> tuple[str, str]:
    label = "[多策略·晚间·科技]" if tech_only else "[多策略·晚间]"
    if not registry:
        return label, "今日八路策略均无信号"

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    title = f"{label} | {now_str}"

    # Sort by tag count desc, then by tag combination alphabetically
    sorted_codes = sorted(
        registry.keys(),
        key=lambda c: (-len(registry[c]["tags"]), ",".join(registry[c]["tags"]), c),
    )

    # Group: multi-strategy (≥2 tags) vs single-strategy
    multi = [c for c in sorted_codes if len(registry[c]["tags"]) >= 2]
    single = [c for c in sorted_codes if len(registry[c]["tags"]) == 1]

    parts: list[str] = [f"*{now_str}*"]

    # Regime-conditional strategy recommendations（基于 2026-05-22 80 天回测结果）
    if regime is not None:
        rec_lines = _regime_recommendation(regime.get("score"))
        if rec_lines:
            parts.extend(rec_lines)

    # 🎯 今日精选 —— 按 hold 周期分组，每组内按 alpha priority 排序
    today_picks = _build_today_picks(registry, max_picks=15,
                                       regime_score=(regime.get("score") if regime else None))
    if today_picks:
        # group by hold horizon
        from collections import defaultdict as _dd
        by_hold: _dd[str, list] = _dd(list)
        for code, entry, cls in today_picks:
            by_hold[cls["hold"]].append((code, entry, cls))
        # 显示顺序：T+1 → T+3 → T+5 → T+10 → T+20
        _hold_order = ["T+1", "T+3", "T+5", "T+10", "T+20"]
        _hold_desc = {
            "T+1":  "次日卖出 — 短打反弹",
            "T+3":  "持 3 个交易日",
            "T+5":  "持 5 个交易日 — 一周内",
            "T+10": "持 10 个交易日 — 两周内",
            "T+20": "持 20 个交易日 — 一个月",
        }
        parts.append(f"<br>🎯 **今日精选** (共 {len(today_picks)} 只，按 hold 周期分组)")
        parts.append("> 🚨开盘抢=必须 9:30 买/涨停就放弃｜📈开盘买=优先开盘｜⚪任意时点=尾盘也行")
        for hold in _hold_order:
            picks_in_hold = by_hold.get(hold, [])
            if not picks_in_hold:
                continue
            parts.append(f"<br>📍 **{hold} hold** ({len(picks_in_hold)} 只) — _{_hold_desc[hold]}_")
            for code, entry, cls in picks_in_hold:
                parts.append("  " + _fmt_today_pick(code, entry, cls))

    # Freshness banner — explicitly flag missing/stale sources at the top
    if source_status:
        bad = [(tag, info) for tag, info in source_status.items() if info["status"] != "fresh"]
        if bad:
            n_total = len(source_status)
            n_bad   = len(bad)
            detail_strs = []
            for tag, info in bad:
                if info["status"] == "stale":
                    detail_strs.append(f"{tag}({info.get('age_days', '?')}个交易日前)")
                else:
                    detail_strs.append(f"{tag}缺")
            parts.append(f"⚠️ **源 {n_total - n_bad}/{n_total} 正常**，缺失/过期: {' '.join(detail_strs)}")

    # ── Multi-strategy resonance block (cap to MAX_MULTI) ─────────────────────
    # sorted_codes 已按 -len(tags) 降序，所以 multi 天然按命中策略数从多到少。
    if multi:
        shown_multi = multi[:MAX_MULTI]
        omitted_multi = len(multi) - len(shown_multi)
        parts.append(f"**【多策略共振】{len(multi)}只**")
        for code in shown_multi:
            parts.append(_fmt_pick(code, registry[code]))
        if omitted_multi:
            parts.append(f"_...还有{omitted_multi}只_")

    # 单策略板块已移除（用户反馈太长，回测显示单策略 alpha 弱于共振）
    # 想看 raw picks 可以查 data/<strategy>_latest.json，但日常下单不依赖

    parts.append("<br>> 仅供参考，不构成投资建议")
    body = "<br>".join(parts)

    return title, body


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--push",    action="store_true", help="推送微信")
    parser.add_argument("--dry-run", action="store_true", help="打印不推送")
    parser.add_argument("--tech-only", action="store_true",
                        help="仅展示科技 TMT（默认覆盖全行业）")
    parser.add_argument("--no-liquidity-filter", action="store_true",
                        help="关闭流动性下限过滤（默认 5 日均成交额 ≥ 0.5 亿）")
    parser.add_argument("--no-blacklist-filter", action="store_true",
                        help="关闭行业黑名单过滤（默认剔除酒/金融/地产/消费/农业/交运）")
    args = parser.parse_args()

    # Source freshness check first — surfaces missing/stale sources as banner
    source_status = _check_sources(max_days=1)
    _bad = {t: i for t, i in source_status.items() if i["status"] != "fresh"}
    if _bad:
        _detail = {t: (i["status"] if i["status"] == "missing" else f"stale-{i.get('age_days', '?')}td") for t, i in _bad.items()}
        print(f"[evening_strategy] 源状态: {len(source_status) - len(_bad)}/{len(source_status)} 正常，问题源: {_detail}")
    else:
        print(f"[evening_strategy] 源状态: {len(source_status)}/{len(source_status)} 全部正常")

    main_picks, small_picks = _load_main()
    gc_picks       = _load_gc()
    chip_picks     = _load_chip()
    mc_picks       = _load_marketcap()
    hot_picks      = _load_hot()
    sideways_picks = _load_sideways()
    escalator_picks = _load_escalator()

    def _counts() -> tuple:
        return (len(main_picks), len(small_picks), len(gc_picks), len(chip_picks),
                len(mc_picks), len(hot_picks), len(sideways_picks), len(escalator_picks))

    def _print_filter(label: str, before: tuple, after: tuple) -> None:
        print(f"[evening_strategy] {label}: "
              f"主{before[0]}→{after[0]} 小{before[1]}→{after[1]} "
              f"叉{before[2]}→{after[2]} 筹{before[3]}→{after[3]} "
              f"市{before[4]}→{after[4]} 热{before[5]}→{after[5]} "
              f"横{before[6]}→{after[6]} 扶{before[7]}→{after[7]}")

    # 0) 默认：行业黑名单过滤（剔除酒/金融/地产/消费/农业/交运）
    if not args.no_blacklist_filter:
        ind_map = _load_industry_map()
        if ind_map:
            before = _counts()
            main_picks      = _filter_blacklist(main_picks, ind_map)
            small_picks     = _filter_blacklist(small_picks, ind_map)
            gc_picks        = _filter_blacklist(gc_picks, ind_map)
            chip_picks      = _filter_blacklist(chip_picks, ind_map)
            mc_picks        = _filter_blacklist(mc_picks, ind_map)
            hot_picks       = _filter_blacklist(hot_picks, ind_map)
            sideways_picks  = _filter_blacklist(sideways_picks, ind_map)
            escalator_picks = _filter_blacklist(escalator_picks, ind_map)
            _print_filter("黑名单过滤", before, _counts())

    # 1) 可选：tech-only 行业过滤（默认 off）
    if args.tech_only:
        ind_map = _load_industry_map()
        if ind_map:
            before = _counts()
            main_picks      = _filter_tech(main_picks, ind_map)
            small_picks     = _filter_tech(small_picks, ind_map)
            gc_picks        = _filter_tech(gc_picks, ind_map)
            chip_picks      = _filter_tech(chip_picks, ind_map)
            mc_picks        = _filter_tech(mc_picks, ind_map)
            hot_picks       = _filter_tech(hot_picks, ind_map)
            sideways_picks  = _filter_tech(sideways_picks, ind_map)
            escalator_picks = _filter_tech(escalator_picks, ind_map)
            _print_filter("科技过滤", before, _counts())

    # 2) 默认：流动性下限过滤（amt_5d_yi ≥ 0.5 亿）
    if not args.no_liquidity_filter:
        amt_cache: dict[str, float | None] = {}
        before = _counts()
        main_picks      = _filter_liquid(main_picks,      _LIQUIDITY_MIN_AMT_YI, amt_cache)
        small_picks     = _filter_liquid(small_picks,     _LIQUIDITY_MIN_AMT_YI, amt_cache)
        gc_picks        = _filter_liquid(gc_picks,        _LIQUIDITY_MIN_AMT_YI, amt_cache)
        chip_picks      = _filter_liquid(chip_picks,      _LIQUIDITY_MIN_AMT_YI, amt_cache)
        mc_picks        = _filter_liquid(mc_picks,        _LIQUIDITY_MIN_AMT_YI, amt_cache)
        hot_picks       = _filter_liquid(hot_picks,       _LIQUIDITY_MIN_AMT_YI, amt_cache)
        sideways_picks  = _filter_liquid(sideways_picks,  _LIQUIDITY_MIN_AMT_YI, amt_cache)
        escalator_picks = _filter_liquid(escalator_picks, _LIQUIDITY_MIN_AMT_YI, amt_cache)
        _print_filter("流动性过滤", before, _counts())

    cnt = _counts()
    total = sum(cnt)
    print(f"[evening_strategy] 主{cnt[0]} 小{cnt[1]} 叉{cnt[2]} 筹{cnt[3]} "
          f"市{cnt[4]} 热{cnt[5]} 横{cnt[6]} 扶{cnt[7]}  共{total}只信号")

    if total == 0:
        print("[evening_strategy] 无信号，退出")
        sys.exit(0)

    registry = _merge(main_picks, small_picks, gc_picks, chip_picks,
                       mc_picks, hot_picks, sideways_picks, escalator_picks)
    unique = len(registry)
    multi_count = sum(1 for e in registry.values() if len(e["tags"]) >= 2)
    print(f"[evening_strategy] 去重后 {unique} 只（多策略共振 {multi_count} 只）")

    regime_info = _load_regime()
    if regime_info:
        print(f"[evening_strategy] regime score={regime_info['score']:.1f} ({regime_info.get('signal','')})")

    title, body = _build_message(registry, tech_only=args.tech_only,
                                  source_status=source_status, regime=regime_info)

    print(f"\n{'='*40}")
    print(f"{title}")
    print(body.replace("<br>", "\n"))
    print(f"{'='*40}\n")

    # 落盘当日精选 picks log（无论 dry_run / push 都落盘，便于跟踪验证）
    _save_picks_log(registry, regime_info)

    if args.dry_run:
        return

    if args.push:
        try:
            push_wechat(title, body)
            print("[evening_strategy] 微信推送完成")
        except Exception as e:
            print(f"[evening_strategy] 推送失败: {e}")
            sys.exit(1)


def _save_picks_log(registry: dict, regime_info: dict | None) -> None:
    """把今日精选落盘到 data/evening_picks_log_<date>.json，供 evening_perf_track 复盘"""
    regime_score = regime_info.get("score") if regime_info else None
    today_picks = _build_today_picks(registry, max_picks=15, regime_score=regime_score)
    if not today_picks:
        return
    out: list[dict] = []
    for code, entry, cls in today_picks:
        det = entry["details"]
        # 取入场价（优先 市 价格，否则 主/小/筹/叉/扶 等）
        price = None
        for tag in ("市", "主", "小"):
            p = det.get(tag, {}).get("price")
            if p:
                price = float(p); break
        if price is None:
            for tag in ("叉", "筹", "热", "横", "扶"):
                c = det.get(tag, {}).get("close")
                if c:
                    price = float(c); break
        out.append({
            "code":     code,
            "name":     entry.get("name", ""),
            "price":    price,
            "hold":     cls["hold"],
            "expected": cls["expected"],
            "entry":    cls.get("entry", "开盘买"),
            "priority": cls["priority"],
            "rule_tags": cls["rule_tags"],
        })
    today_str = datetime.now().strftime("%Y%m%d")
    payload = {
        "date":         today_str,
        "timestamp":    datetime.now().isoformat(),
        "regime_score": regime_score,
        "regime_signal": regime_info.get("signal", "") if regime_info else "",
        "picks":        out,
    }
    path = DATA / f"evening_picks_log_{today_str}.json"
    tmp  = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    print(f"[evening_strategy] 精选 log 已落盘: {path.name} ({len(out)} 只)")


if __name__ == "__main__":
    main()
