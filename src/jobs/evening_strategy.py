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


def _is_fresh(date_str: str, fmt: str = "%Y%m%d", max_days: int = 1) -> bool:
    """昨天或今天的数据都算新鲜。"""
    if not date_str:
        return False
    try:
        d = datetime.strptime(date_str[:10].replace("-", "")[:8], "%Y%m%d")
        return (datetime.now() - d).days <= max_days
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
    """
    today = datetime.now().date()
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
        age = (today - file_date).days
        status = "fresh" if age <= max_days else "stale"
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
    """Returns [{code, name, price, marketcap_yi}, ...] from marketcap_latest.json."""
    d = _load(DATA / "marketcap_latest.json")
    if not d:
        return []
    if not _is_fresh(d.get("date", ""), "%Y%m%d"):
        print("[evening_strategy] marketcap_latest.json 数据已过期，跳过市值策略")
        return []
    return [
        {"code": p["code"], "name": p.get("name", ""), "score": 0,
         "price": p.get("price"), "marketcap_yi": p.get("marketcap_yi")}
        for p in d.get("picks", []) if p.get("code")
    ]


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
            annotations.append(f"扶{t}({sp:+.1f}%/R²{r2:.2f})")

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

    # Freshness banner — explicitly flag missing/stale sources at the top
    if source_status:
        bad = [(tag, info) for tag, info in source_status.items() if info["status"] != "fresh"]
        if bad:
            n_total = len(source_status)
            n_bad   = len(bad)
            detail_strs = []
            for tag, info in bad:
                if info["status"] == "stale":
                    detail_strs.append(f"{tag}({info.get('age_days', '?')}日前)")
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

    # ── Per-strategy sections (capped at MAX_SINGLE each) ────────────────────
    tag_order = ["主", "小", "叉", "筹", "市", "热", "横", "扶"]
    tag_label = {"主": "主策略", "小": "小盘策略", "叉": "金叉", "筹": "筹码",
                 "市": "低市值", "热": "热榜", "横": "横盘", "扶": "扶梯"}

    for tag in tag_order:
        tag_codes = [c for c in single if registry[c]["tags"] == [tag]]
        if not tag_codes:
            in_multi_with_tag = [c for c in multi if tag in registry[c]["tags"]]
            if not in_multi_with_tag:
                continue
            else:
                continue  # already shown in multi block
        if tag in ("叉", "筹", "横", "扶"):
            tag_codes.sort(key=lambda c, _t=tag: (
                _TIER_ORDER.get(registry[c]["details"][_t].get("tier", ""), 99),
                -registry[c]["details"][_t].get("score", 0),
            ))
        elif tag in ("主", "小"):
            tag_codes.sort(key=lambda c, _t=tag: -registry[c]["details"][_t].get("score", 0))
        elif tag == "市":
            tag_codes.sort(key=lambda c: registry[c]["details"]["市"].get("marketcap_yi") or float("inf"))
        elif tag == "热":
            tag_codes.sort(key=lambda c: registry[c]["details"]["热"].get("rank") or 9999)
        shown = tag_codes[:MAX_SINGLE]
        omitted = len(tag_codes) - len(shown)
        parts.append(f"<br>**【{tag_label[tag]}】{len(tag_codes)}只**")
        for code in shown:
            parts.append(_fmt_pick(code, registry[code]))
        if omitted:
            parts.append(f"_...还有{omitted}只_")

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
        _detail = {t: (i["status"] if i["status"] == "missing" else f"stale-{i.get('age_days', '?')}d") for t, i in _bad.items()}
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

    title, body = _build_message(registry, tech_only=args.tech_only, source_status=source_status)

    print(f"\n{'='*40}")
    print(f"{title}")
    print(body.replace("<br>", "\n"))
    print(f"{'='*40}\n")

    if args.dry_run:
        return

    if args.push:
        try:
            push_wechat(title, body)
            print("[evening_strategy] 微信推送完成")
        except Exception as e:
            print(f"[evening_strategy] 推送失败: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
