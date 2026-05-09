#!/usr/bin/env python3
"""
src/report/reporter.py — 小红书文案生成器

用法（从仓库根目录运行）:
    python src/report/reporter.py morning  [--query "低估值高成长"] [--top 5] [--style 1|2|3|all|auto]
    python src/report/reporter.py midday   [--style ...]
    python src/report/reporter.py evening  [--style ...] [--no-tomorrow]
    python src/report/reporter.py milestone [--force]   # Day 7/14/30 周期性复盘
    python src/report/reporter.py status
    python src/report/reporter.py history
"""

import argparse
import json
import random
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPORT_DIR  = Path(__file__).parent
REPO_ROOT   = REPORT_DIR.parent.parent
SCRIPTS_DIR = REPO_ROOT / "src"
sys.path.insert(0, str(SCRIPTS_DIR))

LATEST_PICKS_FILE    = REPO_ROOT / "data" / "latest_picks.json"
_LATEST_PICKS_MAX_AGE_MIN = 90

# ---------------------------------------------------------------------------
# Record management  (delegated to records.py)
# ---------------------------------------------------------------------------
from records import (  # noqa: E402
    RECORDS_DIR, POSTS_DIR, META_FILE, MILESTONE_DAYS,
    ensure_dirs, load_meta, save_meta, today_record_file,
    load_today, load_yesterday, save_today, get_day_number,
    load_recent_history, save_post_file,
)
from report.utils import load_json as _load_json_safe  # noqa: E402

# ---------------------------------------------------------------------------
# Templates  (delegated to templates.py)
# ---------------------------------------------------------------------------
from templates import (  # noqa: E402
    ACCOUNT_TAGLINE,
    MORNING_CTA, EVENING_CTA, MILESTONE_CTA, NIGHT_CTA,
    NIGHT_TITLES, NIGHT_OPENERS, NIGHT_CLOSERS,
    MORNING_TITLES, MORNING_OPENERS, MORNING_CLOSERS,
    MIDDAY_TITLES_NORMAL, MIDDAY_TITLES_DRAMATIC,
    EVENING_TITLES, EVENING_OPENERS, EVENING_REFLECTIONS,
)


from common import push_wechat as _send_wechat_notify, get_spot_em  # noqa: E402


# ---------------------------------------------------------------------------
# Screener integration
# ---------------------------------------------------------------------------

def _load_latest_picks(top_n: int) -> Optional[dict]:
    """Return monitor's latest_picks.json if it exists and is fresh enough."""
    if not LATEST_PICKS_FILE.exists():
        return None
    try:
        data = json.loads(LATEST_PICKS_FILE.read_text(encoding="utf-8"))
        ts = datetime.fromisoformat(data.get("timestamp", "1970-01-01"))
        age_min = (datetime.now() - ts).total_seconds() / 60
        if age_min > _LATEST_PICKS_MAX_AGE_MIN:
            print(f"[~] latest_picks.json 已过期（{age_min:.0f}min），重新筛选...")
            return None
        picks = data.get("results", [])[:top_n]
        print(f"[+] 复用 monitor 扫描结果（{age_min:.0f}min 前，{len(picks)} 只）")
        return {"results": picks, "regime": data.get("regime", "NORMAL")}
    except Exception as e:
        print(f"[~] 读取 latest_picks.json 失败: {e}，重新筛选...")
        return None


def run_screener(query: str, top_n: int = 5) -> Optional[dict]:
    cached = _load_latest_picks(top_n)
    if cached is not None:
        return cached
    try:
        from screener import screen_stocks
        return screen_stocks(query=query, top_n=top_n)
    except Exception as e:
        print(f"[!] screener 失败: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Price fetching
# ---------------------------------------------------------------------------

def fetch_current_prices(codes: list[str]) -> dict[str, dict]:
    try:
        df = get_spot_em()
        result = {}
        for code in codes:
            row = df[df["代码"] == code]
            if row.empty:
                continue
            r = row.iloc[0]
            result[code] = {
                "code":       code,
                "name":       str(r.get("名称", "")),
                "price":      float(r.get("最新价", 0) or 0),
                "change_pct": float(r.get("涨跌幅", 0) or 0),
                "open":       float(r.get("今开", 0) or 0),
                "high":       float(r.get("最高", 0) or 0),
                "low":        float(r.get("最低", 0) or 0),
                "prev_close": float(r.get("昨收", 0) or 0),
            }
        return result
    except Exception as e:
        print(f"[!] 获取行情失败: {e}", file=sys.stderr)
        return {}


def fetch_benchmark_change() -> Optional[float]:
    try:
        import akshare as ak
        df = ak.stock_zh_index_spot_sina()
        row = df[df["代码"].str.contains("000300", na=False)]
        if not row.empty:
            return float(row.iloc[0].get("涨跌幅", 0) or 0)
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------

def compute_verdict(pick_changes: list[float], benchmark: Optional[float]) -> str:
    if not pick_changes:
        return "数据不足"
    avg  = sum(pick_changes) / len(pick_changes)
    ref  = benchmark if benchmark is not None else 0.0
    spread = avg - ref
    if spread >= 1.0:
        return "对了"
    elif spread >= -0.5:
        return "偏了"
    else:
        return "错了"


def verdict_emoji(verdict: str) -> str:
    return {"对了": "✅", "偏了": "🤔", "错了": "❌", "数据不足": "❓"}.get(verdict, "")


# ---------------------------------------------------------------------------
# Streak analysis
# ---------------------------------------------------------------------------

def compute_streak(records: list[dict]) -> dict:
    verdicts = []
    for r in reversed(records):
        v = r.get("evening", {}).get("verdict")
        if v and v != "数据不足":
            verdicts.append(v)
        if len(verdicts) >= 10:
            break
    if not verdicts:
        return {"type": None, "count": 0}
    streak_type = verdicts[0]
    count = sum(1 for v in verdicts if v == streak_type)
    # only consecutive
    count = 0
    for v in verdicts:
        if v == streak_type:
            count += 1
        else:
            break
    return {"type": streak_type, "count": count}


def streak_title(streak: dict, slot: str) -> Optional[str]:
    """Return a streak-driven title override when streak >= 3. None otherwise."""
    t, n = streak["type"], streak["count"]
    if n < 3 or t is None:
        return None
    if slot == "morning":
        if t == "对了":
            return random.choice([
                f"模型连续{n}天判断正确，今天会打破吗",
                f"已经连对{n}次了，我有点不安",
                f"连续{n}天，模型还没翻车",
                f"第{n + 1}天，还是先按模型走",
            ])
        elif t == "错了":
            return random.choice([
                f"已经连续{n}次判断偏差了",
                f"模型连续{n}天翻车，我开始重新审视",
                f"连续{n}次错，今天看看能不能扳回来",
            ])
        elif t == "偏了" and n >= 3:
            return f"连续{n}次模糊判断，今天应该有个答案了"
    elif slot == "evening":
        if t == "对了":
            return random.choice([
                f"连续{n}天对了，但我不打算庆祝",
                f"又对了，这是第{n}次",
                f"已经{n}连对，说实话有点出乎意料",
            ])
        elif t == "错了":
            return random.choice([
                f"第{n}次，模型还是错了",
                f"连续{n}次翻车，我需要重新想想",
                f"已经{n}次偏差了，开始怀疑",
            ])
    return None


def streak_narrative(streak: dict, slot: str) -> str:
    t, n = streak["type"], streak["count"]
    if n == 0 or t is None:
        return ""
    if slot == "morning":
        if t == "对了" and n >= 3:
            return f"（模型最近连续{n}次判断正确，今天继续测试。）"
        elif t == "对了" and n == 2:
            return "（昨天也对了，今天继续记。）"
        elif t == "错了" and n >= 2:
            return f"（最近连续{n}次偏差，今天看看能不能扳回来。）"
        elif t == "偏了" and n >= 2:
            return "（最近连续几次判断比较模糊，今天看看是否清晰一点。）"
    elif slot == "evening":
        if t == "对了" and n >= 3:
            return f"\n连续{n}天判断正确了。\n但我不打算提前庆祝，数据不够多。"
        elif t == "对了" and n == 2:
            return "\n连续两天对了。先继续记录。"
        elif t == "错了" and n >= 3:
            return f"\n已经连续{n}次偏差了。\n我开始有点怀疑这个模型了。"
        elif t == "错了" and n == 2:
            return "\n连续两次判断偏了。\n有点说不过去，继续观察。"
    return ""


# ---------------------------------------------------------------------------
# Beat-benchmark title (evening only)
# ---------------------------------------------------------------------------

def beat_benchmark_title(avg_change: Optional[float], benchmark: Optional[float]) -> Optional[str]:
    """Return a special title when picks significantly beat or lag the benchmark."""
    if avg_change is None or benchmark is None:
        return None
    alpha = avg_change - benchmark
    if alpha >= 3.0:
        return random.choice([
            f"今天模型跑赢大盘{alpha:.1f}%，有点东西",
            f"大盘{benchmark:+.1f}%，这批方向{avg_change:+.1f}%",
            f"这次跑赢{alpha:.1f}%，不像是运气",
        ])
    elif alpha >= 2.0:
        return random.choice([
            f"今天跑赢沪深300超过2%",
            f"大盘{benchmark:+.1f}%，这批picks {avg_change:+.1f}%",
        ])
    elif alpha <= -3.0:
        return random.choice([
            f"今天大幅跑输大盘{abs(alpha):.1f}%",
            f"大盘{benchmark:+.1f}%，这批方向{avg_change:+.1f}%，差距有点大",
        ])
    return None


# ---------------------------------------------------------------------------
# Smart style suggestion
# ---------------------------------------------------------------------------

def suggest_style(streak: dict, alpha: Optional[float] = None) -> int:
    """
    Recommend a style based on context:
    - High streak (对了) + significant alpha → style 3 (反差: "又对了")
    - Losing streak → style 3 (反差: "又打脸了")
    - Notable data story (alpha) → style 2 (data)
    - Default → style 1
    """
    t, n = streak["type"], streak["count"]
    if n >= 3:
        return 3
    if alpha is not None and abs(alpha) >= 2.0:
        return 2
    return 1


# ---------------------------------------------------------------------------
# Signal highlight extraction
# ---------------------------------------------------------------------------

def extract_signal_hook(picks: list[dict]) -> str:
    if not picks:
        return ""

    def _name(p: dict) -> str:
        return p.get("name", p.get("code", "?"))

    high_vol = sorted(picks, key=lambda p: float(p.get("volume_ratio", 0) or 0), reverse=True)
    high_mom = sorted(picks, key=lambda p: float(p.get("return_3m", 0) or 0), reverse=True)
    low_pe   = [p for p in picks if 0 < float(p.get("pe_ttm", 0) or 0) < 15]
    high_div = sorted(picks, key=lambda p: float(p.get("div_yield", 0) or 0), reverse=True)

    hints = []
    top_vol = high_vol[0] if high_vol else None
    if top_vol and float(top_vol.get("volume_ratio", 0) or 0) > 2.5:
        vr = float(top_vol["volume_ratio"])
        hints.append(f"里面有只{_name(top_vol)}，今天量比达到{vr:.1f}倍")

    top_mom = high_mom[0] if high_mom else None
    if top_mom and float(top_mom.get("return_3m", 0) or 0) > 20:
        ret = float(top_mom["return_3m"])
        hints.append(f"{_name(top_mom)}近3个月涨了{ret:.0f}%")

    if low_pe:
        pe = float(low_pe[0].get("pe_ttm", 0))
        hints.append(f"{_name(low_pe[0])}的PE只有{pe:.0f}，处于低位")

    top_div = high_div[0] if high_div else None
    if top_div and float(top_div.get("div_yield", 0) or 0) > 3.0:
        dy = float(top_div["div_yield"])
        hints.append(f"{_name(top_div)}股息率{dy:.1f}%")

    if not hints:
        avg_score = sum(float(p.get("score", 0) or 0) for p in picks) / len(picks)
        return f"这批综合评分平均{avg_score:.0f}分，算是今天的前排。"

    return hints[0] + "，有点意思。"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# XHS compliance — stock name obfuscation
# ---------------------------------------------------------------------------

# Pinyin initials for common 2-char A-share name prefixes


# ---------------------------------------------------------------------------
# Safe hashtags (XHS compliant — avoids direct financial terms)
# ---------------------------------------------------------------------------

def _safe_hashtags(slot: str = "morning") -> str:
    base = "#量化记录 #每日复盘 #数据实验 #模型测试 #记录帖"
    if slot == "milestone":
        return base + " #阶段复盘"
    return base


# ---------------------------------------------------------------------------
# Account tagline and series declaration
# ---------------------------------------------------------------------------

def _series_declaration(day: int) -> str:
    """Inject series premise for Day 1-3 so new readers understand the context."""
    if day == 1:
        return (
            "\n\n我打算连续跑30天，"
            "每天记录一个量化模型的判断和实际结果，"
            "\n看看这个东西到底有没有用。"
            "\n不做主观干预，只记录。"
        )
    elif day == 2:
        return "\n\n这是这个实验的第2天。计划连续跑30天，每天记录模型判断 vs 实际结果。"
    elif day == 3:
        return "\n\n第3天，还有27天，继续执行。"
    return ""


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_picks_morning(picks: list[dict], show_score: bool = False) -> str:
    lines = []
    for i, p in enumerate(picks, 1):
        name   = p.get("name", p.get("code", "?"))
        score  = float(p.get("score", 0) or 0)
        change = float(p.get("change_pct", 0) or 0)
        arrow  = "↑" if change > 0 else ("↓" if change < 0 else "—")
        if show_score:
            lines.append(f"  {i}. {name}（{score:.0f}分）{arrow}")
        else:
            lines.append(f"  {i}. {name} {arrow}")
    return "\n".join(lines)


def format_picks_evening(morning_picks: list[dict], prices: dict[str, dict]) -> str:
    lines = []
    for p in morning_picks:
        code = p.get("code", "")
        name = p.get("name", code)
        q = prices.get(code)
        if q:
            chg   = q["change_pct"]
            arrow = "📈" if chg > 1 else ("📉" if chg < -1 else "➡️")
            lines.append(f"  {name}  {arrow}  {chg:+.2f}%")
        else:
            lines.append(f"  {name}  — 数据获取失败")
    return "\n".join(lines)


def pick_names_short(picks: list[dict], n: int = 3) -> str:
    names = [p.get("name", p.get("code", "?")) for p in picks[:n]]
    return "、".join(names)


def format_tomorrow_teaser(tomorrow_picks: list[dict]) -> str:
    if not tomorrow_picks:
        return ""
    return "\n明天的方向已经跑出来了。\n明早更新。"


# ---------------------------------------------------------------------------
# Suspense hooks
# ---------------------------------------------------------------------------

def _suspense_hook(day: int) -> str:
    """End-of-post hook: milestone countdown or generic 'to be continued'."""
    next_ms = min((m for m in MILESTONE_DAYS if m > day), default=None)
    if next_ms:
        gap = next_ms - day
        if gap == 1:
            return f"\n明天是第{next_ms}天，我会发一次完整复盘。"
        elif gap <= 3:
            return f"\n再过{gap}天就是第{next_ms}天了，到时候发一次完整复盘。"
    return random.choice(["\n继续执行。", "\n明天见。", "\n继续记录。", ""])


# ---------------------------------------------------------------------------
# Title deduplication — avoids repeating the same title within a window
# ---------------------------------------------------------------------------

def _pick_fresh_title(pool: list[str], window: int = 21) -> str:
    """
    Pick a title from pool that hasn't been used in the last `window` posts.
    Falls back to least-recently-used if every option has been seen recently.
    Updates recent_titles in meta.json so the window persists across sessions.
    """
    meta = load_meta()
    recent: list[str] = meta.get("recent_titles", [])
    recent_set = set(recent[-window:])

    fresh = [t for t in pool if t not in recent_set]
    if not fresh:
        # All used recently — pick the one used least recently
        for t in reversed(recent):
            if t in pool:
                fresh = [t]
                break
        if not fresh:
            fresh = pool

    chosen = random.choice(fresh)
    recent.append(chosen)
    meta["recent_titles"] = recent[-window:]
    save_meta(meta)
    return chosen


# ---------------------------------------------------------------------------
# Human skeptic angle (for style 3 morning)
# ---------------------------------------------------------------------------

def _human_skeptic_angle(picks: list[dict]) -> str:
    """Auto-generate a human-intuition skeptic line based on picks characteristics."""
    if not picks:
        return "这个方向我不太确定"

    avg_mom = sum(float(p.get("return_3m", 0) or 0) for p in picks) / len(picks)
    pe_vals = [float(p.get("pe_ttm", 0) or 0) for p in picks if float(p.get("pe_ttm", 0) or 0) > 0]
    avg_pe  = sum(pe_vals) / len(pe_vals) if pe_vals else 0
    avg_div = sum(float(p.get("div_yield", 0) or 0) for p in picks) / len(picks)

    if avg_mom > 15:
        return random.choice([
            "涨了这么多了，我的直觉是已经过热",
            "近期涨幅不小，我自己不敢追",
            "动量这么强，我会担心追高",
        ])
    elif 0 < avg_pe < 12:
        return random.choice([
            "估值是低，但低估值往往有它的原因",
            "PE低不代表要涨，我自己不太看这个信号",
            "这种低估值标的，我一般会等一个催化剂",
        ])
    elif avg_div > 3:
        return random.choice([
            "靠股息驱动的逻辑，在这个市场我不太确定",
            "高股息在当下行情，我觉得吸引力有限",
        ])
    else:
        return random.choice([
            "这批标的从直觉来看，我不太会选",
            "说实话我自己不会这么选",
            "这个方向和我的判断有明显偏差",
        ])


# ---------------------------------------------------------------------------
# Morning post generator
# ---------------------------------------------------------------------------

def generate_morning_post(
    picks: list[dict],
    day: int,
    query: str,
    regime: str,
    streak: dict,
    style: int = 1,
    night_context: str = "",
) -> str:
    # --- Empty picks: no strong signal today ---
    if not picks:
        hashtags    = _safe_hashtags("morning")
        series_decl = _series_declaration(day)
        suspense    = _suspense_hook(day)
        tagline_line = f"\n\n{ACCOUNT_TAGLINE}" if day <= 3 else ""
        cta          = random.choice(MORNING_CTA)
        _no_signal_opener = random.choice([
            "今天模型跑了一遍，没有找到符合条件的方向。",
            "今天筛下来，没有达到门槛的标的。",
            "今天跑完，信号不够强，没有输出。",
        ])
        _no_signal_closer = random.choice([
            "不强行选，今天记录一个「无信号」。\n\n等有信号的时候再跟进。",
            "宁可不动，不乱动。\n\n今天结论：等待。",
            "没有达标的方向，就不记。\n\n空白也是数据的一部分。",
        ])
        text = f"""今天模型没有给出方向

Day {day}，连续记录实验。{series_decl}

{_no_signal_opener}

{_no_signal_closer}{suspense}{tagline_line}

{cta}

{hashtags}"""
        return text.strip()

    title = streak_title(streak, "morning") or _pick_fresh_title(MORNING_TITLES[style])
    cta   = random.choice(MORNING_CTA)

    picks_block  = format_picks_morning(picks, show_score=(style == 2))
    signal_hook  = extract_signal_hook(picks)
    streak_note  = streak_narrative(streak, "morning")
    suspense     = _suspense_hook(day)
    series_decl  = _series_declaration(day)
    hashtags     = _safe_hashtags("morning")
    # For Day 1-3 append account tagline so new readers know what this is
    tagline_line = f"\n\n{ACCOUNT_TAGLINE}" if day <= 3 else ""

    regime_note = ""
    if regime and regime not in ("NORMAL", ""):
        regime_map = {
            "BEAR":         "（当前市场：熊市模式，模型已降低仓位权重）",
            "CAUTION":      "（当前市场：谨慎信号）",
            "BULL":         "（当前市场：偏多信号）",
            "EXTREME_BULL": "（当前市场：极度偏多，注意过热风险）",
        }
        regime_note = f"\n{regime_map.get(regime, '')}"

    signal_line = f"\n{signal_hook}" if signal_hook else ""
    streak_line = f"\n{streak_note}" if streak_note else ""
    night_line  = f"\n（{night_context}）" if night_context else ""

    # Style 3: human vs model battle format
    if style == 3:
        skeptic = _human_skeptic_angle(picks)
        text = f"""{title}

Day {day}，连续记录实验。{series_decl}{night_line}

今天我的直觉：{skeptic}。
模型的判断：

{picks_block}
{signal_line}{regime_note}

两者出现了分歧。
照旧，不干预，按模型执行。
晚上看谁对。{suspense}{tagline_line}

{cta}

{hashtags}"""
    else:
        opener = random.choice(MORNING_OPENERS[style])
        closer = random.choice(MORNING_CLOSERS[style])
        text = f"""{title}

Day {day}，连续记录实验。{series_decl}{night_line}

{opener}{streak_line}

这次出来的结果是：

{picks_block}
{signal_line}{regime_note}

{closer}{suspense}{tagline_line}

{cta}

{hashtags}"""

    return text.strip()


# ---------------------------------------------------------------------------
# Night post generator (22:00 — pre-select tomorrow's candidates)
# ---------------------------------------------------------------------------

def generate_night_post(
    picks: list[dict],
    day: int,
    query: str,
    style: int = 1,
) -> str:
    hashtags = _safe_hashtags("morning")
    if not picks:
        return f"""今晚没有跑出符合条件的方向

Day {day}，连续记录实验。

今晚模型跑了一遍，没有达到门槛的标的输出。

记录一个「无信号」。

明天继续。

{hashtags}""".strip()

    title   = _pick_fresh_title(NIGHT_TITLES)
    opener  = random.choice(NIGHT_OPENERS)
    closer  = random.choice(NIGHT_CLOSERS)
    cta     = random.choice(NIGHT_CTA)
    picks_block = format_picks_morning(picks, show_score=(style == 2))

    text = f"""{title}

Day {day}，连续记录实验。

{opener}

这批是今晚的结果：

{picks_block}

{closer}

{cta}

{hashtags}"""
    return text.strip()


def generate_midday_post(
    morning_picks: list[dict],
    prices: dict[str, dict],
    day: int,
    style: int = 1,
) -> str:
    picks_block = (
        format_picks_evening(morning_picks, prices)
        if prices
        else format_picks_morning(morning_picks)
    )
    changes = [
        prices[p["code"]]["change_pct"]
        for p in morning_picks
        if p.get("code") in prices
    ]

    # Classify movers: 涨停 (≥9.9%), strong (≥5%), dramatic (≥3%), down (≤-3%)
    limit_up  = [p for p in morning_picks if p.get("code") in prices
                 and prices[p["code"]]["change_pct"] >= 9.9]
    strong_up = [p for p in morning_picks if p.get("code") in prices
                 and 5.0 <= prices[p["code"]]["change_pct"] < 9.9]
    dramatic  = [p for p in morning_picks if p.get("code") in prices
                 and abs(prices[p["code"]]["change_pct"]) > 3.0]

    if dramatic or limit_up:
        title = random.choice(MIDDAY_TITLES_DRAMATIC)
        mover = (limit_up or strong_up or dramatic)[0]
        chg   = prices[mover["code"]]["change_pct"]
        name  = mover.get("name", mover.get("code", "?"))
        if chg >= 9.9:
            drama_note = random.choice([
                f"{name}中场封板了，模型这次方向对了。",
                f"{name}直接封板，中场情况不错。",
                f"{name}涨停，今天这个方向跑出来了。",
            ])
        elif chg >= 5.0:
            drama_note = f"{name}中场涨幅{chg:.1f}%，方向基本对了。"
        else:
            arrow = "大涨" if chg > 0 else "大跌"
            drama_note = (
                f"{name}中场{arrow}{abs(chg):.1f}%，"
                f"{'比早上预期的方向强' if chg > 0 else '和早上的方向反了'}。"
            )
        body = f"{drama_note}\n\n其余的：\n\n{picks_block}"
    else:
        title = random.choice(MIDDAY_TITLES_NORMAL)
        avg = sum(changes) / len(changes) if changes else None
        if avg is None:
            mid_note = "数据还在加载中。"
        elif avg > 1.5:
            mid_note = "中场来看，方向还不错。"
        elif avg > 0:
            mid_note = "中场略有起色，还要看收盘。"
        elif avg > -1.5:
            mid_note = "中场表现平平，继续观察。"
        else:
            mid_note = "中场表现偏弱，等收盘确认。"
        body = f"{picks_block}\n\n目前来看：{mid_note}"

    text = f"""{title}

早上模型给出的方向：

{body}

不做干预，等晚盘收官。

Day {day}"""

    return text.strip()


def generate_evening_post(
    morning_picks: list[dict],
    prices: dict[str, dict],
    benchmark: Optional[float],
    verdict: str,
    day: int,
    streak: dict,
    tomorrow_picks: list[dict],
    style: int = 1,
) -> str:
    avg_change = None
    changes = [prices[c]["change_pct"] for c in [p.get("code", "") for p in morning_picks] if c in prices]
    if changes:
        avg_change = sum(changes) / len(changes)

    # Title priority: streak override > beat-benchmark > normal pool
    alpha = (avg_change - benchmark) if (avg_change is not None and benchmark is not None) else None
    title = (
        streak_title(streak, "evening")
        or beat_benchmark_title(avg_change, benchmark)
        or _pick_fresh_title(EVENING_TITLES.get(verdict, EVENING_TITLES["偏了"])[style])
    )

    names_short    = pick_names_short(morning_picks)
    picks_block    = format_picks_evening(morning_picks, prices) if prices else "（行情数据获取失败）"
    benchmark_note = f"\n（沪深300今天 {benchmark:+.2f}%）" if benchmark is not None else ""
    streak_note    = streak_narrative(streak, "evening")
    cta            = random.choice(EVENING_CTA)

    # Highlight 涨停 or strong movers (understated — just factual)
    highlight_note = ""
    if prices:
        limit_up = [p for p in morning_picks if p.get("code") in prices
                    and prices[p["code"]]["change_pct"] >= 9.9]
        strong   = [p for p in morning_picks if p.get("code") in prices
                    and 5.0 <= prices[p["code"]]["change_pct"] < 9.9]
        if limit_up:
            names_lu = "、".join(
                p.get("name", p.get("code", "?"))
                for p in limit_up
            )
            highlight_note = random.choice([
                f"\n{names_lu}今天封板了，这个结果还不错。",
                f"\n{names_lu}涨停，方向对了。",
                f"\n其中{names_lu}封板，今天这批跑出来了。",
            ])
        elif strong:
            names_s = strong[0].get("name", strong[0].get("code", "?"))
            chg_s = prices[strong[0]["code"]]["change_pct"]
            highlight_note = f"\n{names_s}今天涨了{chg_s:.1f}%，表现不错。"

    # Verdict-aware opener (replaces the fixed "更新一下今天的记录")
    opener_pool = EVENING_OPENERS.get(verdict, EVENING_OPENERS["偏了"])
    ev_opener   = random.choice(opener_pool)

    # End hook: prefer tomorrow teaser; fall back to milestone countdown
    end_hook = format_tomorrow_teaser(tomorrow_picks) or _suspense_hook(day)

    verdict_line = {
        "对了":    "👉 模型这次对了 ✅",
        "偏了":    "👉 这个算对了还是没对？你们来判断 🤔",
        "错了":    "👉 这次模型判断错了 ❌",
        "数据不足": "👉 数据不足，暂时无法判断",
    }.get(verdict, "")

    if style == 1:
        reflection = random.choice(EVENING_REFLECTIONS[1])
    elif style == 2:
        bm_line = f"\n沪深300今天：{benchmark:+.2f}%" if benchmark is not None else ""
        base = random.choice(EVENING_REFLECTIONS[2])
        reflection = f"{base}{bm_line}"
    else:
        pool = EVENING_REFLECTIONS[3].get(verdict, EVENING_REFLECTIONS[3]["偏了"])
        reflection = random.choice(pool)

    hashtags = _safe_hashtags("morning")

    text = f"""{title}

Day {day}，来更新今天的结果。

{ev_opener}

早上模型给出的结果是：{names_short}

今天的表现是：

{picks_block}{benchmark_note}{highlight_note}

目前来看：
{verdict_line}

{reflection}{streak_note}

{end_hook}

{cta}

{hashtags}"""

    return text.strip()


# ---------------------------------------------------------------------------
# Milestone post (Day 7 / 14 / 30 周期复盘)
# ---------------------------------------------------------------------------

def compute_milestone_stats(records: list[dict]) -> dict:
    """Aggregate stats across all records that have evening data."""
    days_with_data = [r for r in records if r.get("evening", {}).get("verdict")]
    verdicts = [r["evening"]["verdict"] for r in days_with_data]
    avgs     = [r["evening"].get("avg_change_pct") for r in days_with_data if r["evening"].get("avg_change_pct") is not None]
    bms      = [r["evening"].get("benchmark_change_pct") for r in days_with_data if r["evening"].get("benchmark_change_pct") is not None]

    # Best and worst day
    best = worst = None
    for r in days_with_data:
        avg = r["evening"].get("avg_change_pct")
        if avg is None:
            continue
        if best is None or avg > best["avg"]:
            picks = r.get("morning", {}).get("picks", [])
            best  = {"day": r.get("day"), "avg": avg, "picks": pick_names_short(picks, 2)}
        if worst is None or avg < worst["avg"]:
            picks = r.get("morning", {}).get("picks", [])
            worst = {"day": r.get("day"), "avg": avg, "picks": pick_names_short(picks, 2)}

    cumulative_model = sum(avgs) if avgs else 0.0
    cumulative_bm    = sum(bms) if bms else 0.0

    return {
        "total":       len(days_with_data),
        "wins":        verdicts.count("对了"),
        "losses":      verdicts.count("错了"),
        "neutral":     verdicts.count("偏了"),
        "win_rate":    verdicts.count("对了") / len(verdicts) * 100 if verdicts else 0,
        "best":        best,
        "worst":       worst,
        "cum_model":   cumulative_model,
        "cum_bm":      cumulative_bm,
        "cum_alpha":   cumulative_model - cumulative_bm,
    }


def _milestone_observation(stats: dict) -> str:
    wr  = stats["win_rate"]
    alpha = stats["cum_alpha"]
    if wr >= 65 and alpha > 0:
        return "胜率和超额都在正区间，但样本还不够多，不能下结论。"
    elif wr >= 60:
        return "胜率勉强过半，模型整体方向偏对，但分散度很大。"
    elif wr <= 35:
        return "胜率偏低。要么模型有问题，要么市场最近不适合这套逻辑。"
    elif alpha > 2:
        return "胜率一般，但超额收益是正的，说明方向错了但幅度控制还行。"
    elif alpha < -2:
        return "胜率和超额都不好看，这个阶段模型表现很普通。"
    else:
        return "目前没有明显规律。继续跑，等样本多了再看。"


def generate_milestone_post(day: int, stats: dict) -> str:
    observation = _milestone_observation(stats)
    cta      = random.choice(MILESTONE_CTA)
    hashtags = _safe_hashtags("milestone")

    best_line  = f"跑得最好的一次：Day {stats['best']['day']}，{stats['best']['picks']}（均涨{stats['best']['avg']:+.1f}%）" if stats.get("best") else ""
    worst_line = f"跑得最差的一次：Day {stats['worst']['day']}，{stats['worst']['picks']}（均涨{stats['worst']['avg']:+.1f}%）" if stats.get("worst") else ""

    alpha_str = f"{stats['cum_alpha']:+.1f}%"
    bm_str    = f"{stats['cum_bm']:+.1f}%"
    model_str = f"{stats['cum_model']:+.1f}%"

    # Every other milestone, generate a "meta" post instead of pure data
    use_meta = (day // 7) % 2 == 0 if day >= 14 else False

    if use_meta:
        text = f"""Day {day}｜说几句实话

到今天，这个实验已经跑了{stats['total']}天了。

有人问我，为什么要每天记录这个。

说实话，我自己也不确定这个模型有没有用。
我做这件事，就是想搞清楚这个问题。

目前的结果：

胜率{stats['win_rate']:.0f}%，
模型方向累计{model_str}，
大盘同期{bm_str}，
超额{alpha_str}。

不算好，也不算差。
还不够多，没法下结论。

我不想在数据够之前就说这个模型有用或者没用。
所以我还在跑。

{observation}

继续执行，不做干预。

{ACCOUNT_TAGLINE}

{cta}

{hashtags}"""
    else:
        text = f"""Day {day}｜跑了{stats['total']}天，来做个复盘

先把数字摆出来：

✅ 判断正确：{stats['wins']}次
❌ 判断错误：{stats['losses']}次
🤔 判断模糊：{stats['neutral']}次

胜率：{stats['win_rate']:.0f}%

{best_line}
{worst_line}

累计表现（简单求和，非复利）：
模型方向：{model_str}
沪深300：{bm_str}
超额：{alpha_str}

我的观察：
{observation}

继续执行，不做干预。
等下一个节点再复盘一次。

{ACCOUNT_TAGLINE}

{cta}

{hashtags}"""

    return text.strip()


def run_milestone(args):
    ensure_dirs()
    records = load_recent_history(90)
    record  = load_today()
    day     = record.get("day") or get_day_number()

    if day not in MILESTONE_DAYS and not args.force:
        print(f"[!] 今天是 Day {day}，不是里程碑节点（{sorted(MILESTONE_DAYS)}）。")
        print("    加 --force 可以强制生成。")
        return

    stats = compute_milestone_stats(records)
    if stats["total"] == 0:
        print("[!] 暂无足够的历史数据来生成里程碑复盘。")
        return

    post  = generate_milestone_post(day, stats)
    saved = save_post_file(post, "milestone", day)
    print(f"{'='*52}")
    print(f"🏁 里程碑复盘帖  Day {day}  （已保存 → {saved.relative_to(REPO_ROOT)}）")
    print(f"{'='*52}")
    print(post)
    print()


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def _resolve_styles(style_arg: str, streak: dict, alpha: Optional[float] = None) -> list[int]:
    if style_arg == "all":
        return [1, 2, 3]
    if style_arg == "auto":
        return [suggest_style(streak, alpha)]
    return [int(style_arg)]


def _load_alert_config() -> dict:
    return _load_json_safe(REPO_ROOT / "alert_config.json")


def _load_holdings() -> list:
    """Load holdings.json (root level). Returns list of holding dicts with shares > 0."""
    raw = _load_json_safe(REPO_ROOT / "holdings.json")
    if isinstance(raw, list):
        return [h for h in raw if h.get("shares", 0) > 0]
    return []


def _parse_watchlist(cfg: dict) -> tuple[list[str], dict[str, str]]:
    """
    Extract (codes, name_map) from watchlist in either format:
      new: [{"code": "002361", "name": "神剑股份"}, ...]
      old: ["SZ002361", ...] + watchlist_names dict
    """
    raw = cfg.get("watchlist", [])
    if not raw:
        return [], {}
    if isinstance(raw[0], dict):
        codes    = [e["code"] for e in raw]
        name_map = {e["code"]: e.get("name", e["code"]) for e in raw}
    else:
        codes    = [c[-6:] if len(c) > 6 else c for c in raw]
        name_map = cfg.get("watchlist_names", {})
    return codes, name_map


def run_morning(args):
    ensure_dirs()
    record = load_today()
    if "morning" in record:
        print("[~] 今日早盘文案已生成，跳过（dedup guard）。")
        return

    day  = get_day_number()
    meta = load_meta()
    query = args.query or meta.get("last_query") or "综合"

    # Load yesterday's night picks for confirmation context
    yesterday = load_yesterday()
    night_preview_picks = yesterday.get("night_preview", {}).get("picks", []) if yesterday else []
    night_codes = {p.get("code") for p in night_preview_picks if p.get("code")}

    print(f"[+] 正在运行模型筛选（竞价后确认，query='{query}', top={args.top}）...")
    screener_output = run_screener(query, args.top)

    if screener_output is None:
        print("[!] screener 未返回结果，生成占位文案。")
        picks, regime = [], "NORMAL"
    else:
        picks  = screener_output.get("results", [])[:args.top]
        regime = screener_output.get("regime", "NORMAL") or "NORMAL"
        meta["last_query"] = query
        save_meta(meta)

    # Compute night-pick confirmation stats for the post
    confirmed_codes = {p.get("code") for p in picks if p.get("code") in night_codes}
    if night_codes and confirmed_codes:
        print(f"[+] 昨晚 night picks 确认率: {len(confirmed_codes)}/{len(night_codes)}")

    record = load_today()
    record.update({
        "date": str(date.today()),
        "day":  day,
        "morning": {
            "timestamp":      datetime.now().strftime("%H:%M:%S"),
            "query":          query,
            "regime":         regime,
            "picks":          picks,
            "night_confirmed": len(confirmed_codes),
            "night_total":     len(night_codes),
        },
    })
    save_today(record)
    print(f"[+] 记录已保存 → xhs/records/{date.today()}.json  (Day {day})\n")

    if day in MILESTONE_DAYS:
        print(f"[!] 今天是 Day {day}，里程碑节点！建议运行: python xhs/reporter.py milestone\n")

    history = load_recent_history(14)
    streak  = compute_streak(history)
    styles  = _resolve_styles(args.style, streak)

    # Build a brief night-confirmation note to inject into morning post
    if night_codes and confirmed_codes:
        n_conf = len(confirmed_codes)
        n_total = len(night_codes)
        if n_conf == n_total:
            night_context = f"昨晚预判的{n_total}个方向，今早竞价后全部确认。"
        else:
            night_context = f"昨晚{n_total}个方向，今早竞价后确认了{n_conf}个。"
    elif night_codes and not confirmed_codes:
        night_context = "昨晚的预判方向今早竞价后有变化，更新如下。"
    else:
        night_context = ""

    first_post = None
    for s in styles:
        post  = generate_morning_post(picks, day, query, regime, streak, style=s,
                                      night_context=night_context)
        saved = save_post_file(post, "morning", s)
        print(f"{'='*52}")
        print(f"🌅 早盘文案 — 风格{s}  （已保存 → {saved.relative_to(REPO_ROOT)}）")
        print(f"{'='*52}")
        print(post)
        print()
        if first_post is None:
            first_post = post
    if first_post:
        _send_wechat_notify(f"📊 Day {day} 早盘文案｜09:30–09:40 发布", first_post)


def cmd_night(args):
    ensure_dirs()
    record = load_today()
    if "night_preview" in record:
        print("[~] 今日晚间文案已生成，跳过（dedup guard）。")
        return

    day = record.get("day") or get_day_number()

    # Read preview picks written by monitor.py's 18:00 scan
    preview_path = REPO_ROOT / "data" / "preview_picks.json"
    preview = _load_json_safe(preview_path)
    picks = preview.get("picks", [])
    regime = preview.get("regime", "unknown")
    scan_date = preview.get("date", "")
    scan_time = preview.get("time", "")

    if not picks:
        print("[!] data/preview_picks.json 暂无数据，生成占位文案。")

    # Build content
    lines = []
    for i, p in enumerate(picks[:8], 1):
        code = p.get("code", "")
        name = p.get("name", code)
        buy  = p.get("buy_score", 0) or 0
        sell = p.get("sell_score", 0) or 0
        bullish = p.get("bullish", [])
        tag  = f"（{bullish[0]}）" if bullish else ""
        lines.append(f"  {i}. {name}  买:{buy:.0f} 卖:{sell:.0f}{tag}")

    picks_block = "\n".join(lines) if lines else "  （暂无候选标的）"

    regime_label = {
        "BULL": "强势", "EXTREME_BULL": "极强势",
        "BEAR": "弱势", "CAUTION": "谨慎",
    }.get(regime, "中性")

    post = f"""今晚的预选方向出来了

Day {day}，连续记录实验。

18:00 跑了一遍全量因子筛选（市场状态：{regime_label}）。

明日关注候选（买入分 / 卖出分）：

{picks_block}

这是模型分数，不是买入建议。
明天开盘看实际走势，记录结果。

#量化选股 #A股 #选股实验 #每日复盘 #打板"""

    post = post.strip()
    saved = save_post_file(post, "night", 1)
    print(f"{'='*52}")
    print(f"🌙 晚间预告文案  (Day {day}, 数据时间 {scan_date} {scan_time})")
    print(f"  已保存 → {saved.relative_to(REPO_ROOT)}")
    print(f"{'='*52}")
    print(post)
    print()

    record.setdefault("date", str(date.today()))
    record.setdefault("day",  day)
    record["night_preview"] = {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "picks":     picks,
        "regime":    regime,
    }
    save_today(record)

    _send_wechat_notify(f"🌙 Day {day} 明日预告｜18:00", post)


# ---------------------------------------------------------------------------
# Shared formatting helpers for midday / evening
# ---------------------------------------------------------------------------

def _fmt_picks_section(title: str, picks: list[dict], prices: dict[str, dict]) -> list[str]:
    """Format a generic picks section with current price change."""
    if not picks:
        return []
    lines = [f"**{title}**"]
    rows = sorted(
        [{"code": p["code"], "name": p.get("name", p["code"]),
          "chg": (prices.get(p["code"]) or {}).get("change_pct")}
         for p in picks],
        key=lambda r: r["chg"] if r["chg"] is not None else -999,
        reverse=True,
    )
    for r in rows:
        chg = r["chg"]
        if chg is None:
            lines.append(f"{r['code']} {r['name']} —  ")
        else:
            icon = "📈" if chg > 0 else ("📉" if chg < 0 else "➡️")
            lines.append(f"{icon} {r['code']} {r['name']} **{chg:+.2f}%**  ")
    lines.append("")
    return lines


def _fmt_chip_section(chip_data: dict, prices: dict[str, dict], slot: str = "midday") -> list[str]:
    """Format chip strategy section using calc_pick_stats."""
    from report.utils import calc_pick_stats
    picks = chip_data.get("all_picks", [])
    if not picks:
        return []
    s = calc_pick_stats(picks, prices)
    fallback_s = "（交集）" if chip_data.get("filter") == "CAH∩CAD∩CADM" else "（CAD）"
    lines = []
    if s["results"]:
        lines.append(f"**【筹码策略 {s['n_total']}只{fallback_s}】"
                     f"  胜率 {s['win_rate']:.0f}%  均 {s['avg_ret']:+.2f}%**  ")
        if slot == "midday":
            lines.append("涨幅前五：  ")
            for i, r in enumerate(s["top5"], 1):
                lines.append(f"{i}. {r['code']} {r['name']} **{r['change_pct']:+.2f}%**  ")
            if s["watch_up"] or s["watch_dn"]:
                lines.append("下午关注：  ")
                for r in s["watch_up"][:3]:
                    lines.append(f"📈 {r['code']} {r['name']} {r['change_pct']:+.2f}%  ")
                for r in s["watch_dn"][:2]:
                    lines.append(f"📉 {r['code']} {r['name']} {r['change_pct']:+.2f}%  ")
        else:  # evening
            lines.append("收益前五：  ")
            for i, r in enumerate(s["top5"], 1):
                price_s = f" ¥{r['price']:.2f}" if r.get("price") else ""
                lines.append(f"{i}. {r['code']} {r['name']}{price_s} **{r['change_pct']:+.2f}%**  ")
            if s.get("nan_stocks"):
                codes_s = " ".join(f"{r['code']}{r['name']}" for r in s["nan_stocks"])
                lines.append(f"⚠️ 行情缺失：{codes_s}  ")
    else:
        lines.append(f"**【筹码策略 {len(picks)}只{fallback_s}】**  （行情暂不可用）  ")
    lines.append("")
    return lines


def _fmt_etf_section(etf_picks: list[dict], prices: dict[str, dict], slot: str = "midday") -> list[str]:
    """Format ETF section matching chip style: 【ETF策略 N只】胜率X% 均Y%"""
    from report.utils import calc_pick_stats
    if not etf_picks:
        return []
    s = calc_pick_stats(etf_picks, prices)
    lines = []
    if s["results"]:
        lines.append(f"**【ETF策略 {s['n_total']}只】"
                     f"  胜率 {s['win_rate']:.0f}%  均 {s['avg_ret']:+.2f}%**<br>")
        if slot == "midday":
            lines.append("涨幅前五：  ")
            for i, r in enumerate(s["top5"], 1):
                lines.append(f"{i}. {r['code']} {r['name']} **{r['change_pct']:+.2f}%**  ")
            if s["watch_up"] or s["watch_dn"]:
                lines.append("下午关注：  ")
                for r in s["watch_up"][:3]:
                    lines.append(f"📈 {r['code']} {r['name']} {r['change_pct']:+.2f}%  ")
                for r in s["watch_dn"][:2]:
                    lines.append(f"📉 {r['code']} {r['name']} {r['change_pct']:+.2f}%  ")
        else:
            lines.append("收益前五：  ")
            for i, r in enumerate(s["top5"], 1):
                price_s = f" ¥{r['price']:.2f}" if r.get("price") else ""
                lines.append(f"{i}. {r['code']} {r['name']}{price_s} **{r['change_pct']:+.2f}%**  ")
    else:
        lines.append(f"**【ETF策略 {len(etf_picks)}只】**  （行情暂不可用）  ")
    lines.append("")
    return lines


def _fmt_gc_section(gc_data: dict, prices: dict[str, dict]) -> list[str]:
    """Format golden cross section (cascade from G0 down until >= 30 stocks)."""
    from report.utils import calc_pick_stats
    if not gc_data:
        return []
    tiers = gc_data.get("tiers", {})
    _LABELS = {"G0": "7信号", "G1": "6信号", "G2": "5信号", "G3": "4信号"}
    keep: list[str] = []
    total = 0
    for t in ("G0", "G1", "G2", "G3"):
        cnt = len(tiers.get(t, []))
        if cnt == 0:
            continue
        keep.append(t)
        total += cnt
        if total >= 30:
            break
    if total == 0:
        return []
    all_gc_picks = [dict(p, tier=t) for t in keep for p in tiers.get(t, [])]
    overall = calc_pick_stats(all_gc_picks, prices)
    overall_s = (f"  胜率{overall['win_rate']:.0f}%  均{overall['avg_ret']:+.2f}%"
                 if overall["results"] else "")
    gc_date = gc_data.get("date", "")
    date_s  = f" {gc_date[4:6]}/{gc_date[6:]}" if len(gc_date) == 8 else ""
    out = ["", f"**【金叉共振{date_s} {total}只】{overall_s}**  "]
    for t in keep:
        label = _LABELS.get(t, t)
        picks = tiers.get(t, [])
        if not picks:
            continue
        ts = calc_pick_stats([dict(p, tier=t) for p in picks], prices)
        stat_s = (f"  胜率{ts['win_rate']:.0f}%  均{ts['avg_ret']:+.2f}%"
                  if ts["results"] else "")
        out.append(f"**{t} {label}（{len(picks)}只）{stat_s}**  ")
        for p in picks[:3]:
            pr = prices.get(p["code"])
            pct_s = f" **{pr['change_pct']:+.2f}%**" if pr else ""
            out.append(f"{p['code']} {p['name']}{pct_s}  ")
        if len(picks) > 3:
            out.append(f"  ……共{len(picks)}只  ")
        out.append("")
    return out


def _load_all_strategy_data() -> tuple[list, list, list[dict], dict, dict]:
    """Load all strategy data sources. Returns (main, smallcap, etf_picks, chip, gc)."""
    from chip.daily_scan import load_chip_results
    from strategies.golden_cross_scan import load_gc_results

    picks_data  = _load_latest_picks(top_n=10)
    main_picks  = picks_data.get("results", []) if picks_data else []
    small_picks = picks_data.get("smallcap", []) if picks_data else []

    cfg = _load_alert_config()

    # 优先用 etf_strategy 的评分结果（已按 buy_score 降序），无则退回 config 列表顺序
    etf_picks: list[dict] = []
    etf_scan_path = REPO_ROOT / "data" / "etf_scan_latest.json"
    if etf_scan_path.exists():
        try:
            etf_scan = json.loads(etf_scan_path.read_text(encoding="utf-8"))
            etf_picks = [
                {"code": s["code"], "name": s.get("name", s["code"]),
                 "buy_score": s.get("buy_score", 0)}
                for s in etf_scan.get("scores", [])
            ]
        except Exception:
            pass
    if not etf_picks:
        raw_etf = cfg.get("etf_watchlist", [])
        for e in (raw_etf or []):
            if isinstance(e, dict):
                etf_picks.append({"code": e["code"], "name": e.get("name", e["code"])})
            else:
                code = e[-6:] if len(e) > 6 else e
                etf_picks.append({"code": code, "name": code})

    chip_data = load_chip_results()
    gc_data   = load_gc_results()

    return main_picks, small_picks, etf_picks, chip_data, gc_data


# ---------------------------------------------------------------------------
# Midday and Evening commands (all-strategy unified)
# ---------------------------------------------------------------------------

def run_midday(args):
    ensure_dirs()
    record = load_today()
    if "midday" in record:
        print("[~] 今日午盘文案已生成，跳过（dedup guard）。")
        return

    day = record.get("day") or get_day_number()
    print("[midday] 加载策略数据...")
    main_picks, small_picks, etf_picks, chip_data, gc_data = _load_all_strategy_data()

    chip_picks   = chip_data.get("all_picks", [])
    gc_codes     = [p["code"] for t in ("G0", "G1") for p in gc_data.get("tiers", {}).get(t, [])]
    etf_shown    = etf_picks[:30]
    all_codes    = list(dict.fromkeys(
        [p["code"] for p in main_picks + small_picks]
        + [p["code"] for p in etf_shown]
        + [p["code"] for p in chip_picks] + gc_codes
    ))
    print(f"[midday] 批量拉取行情 {len(all_codes)} 只...")
    prices = fetch_current_prices(all_codes) if all_codes else {}

    lines: list[str] = [
        f"☀️ 午间快报 | 第{day}天 | {datetime.now().strftime('%H:%M')}", ""
    ]
    lines += _fmt_picks_section(f"主策略精选 {len(main_picks)}只", main_picks, prices)
    lines += _fmt_picks_section(f"小盘策略 {len(small_picks)}只",  small_picks, prices)
    if etf_shown:
        lines += _fmt_etf_section(etf_shown, prices, slot="midday")
    lines += _fmt_chip_section(chip_data, prices, slot="midday")
    lines += _fmt_gc_section(gc_data, prices)
    lines += ["⚠️ 仅供参考，不构成投资建议", "#量化记录 #A股 #选股", ""]
    post = "\n".join(lines)

    record.setdefault("date", str(date.today()))
    record.setdefault("day",  day)
    record["midday"] = {"timestamp": datetime.now().strftime("%H:%M:%S")}
    save_today(record)

    saved = save_post_file(post, "midday", "auto")
    print(f"{'='*52}")
    print(f"☀️  午间快报  （已保存 → {saved.relative_to(REPO_ROOT)}）")
    print(f"{'='*52}")
    print(post)
    _send_wechat_notify(f"☀️ Day {day} 午间快报｜11:35", post)


def run_evening(args):
    ensure_dirs()
    record = load_today()
    if "evening" in record:
        print("[~] 今日收盘文案已生成，跳过（dedup guard）。")
        return

    day = record.get("day") or get_day_number()
    print("[evening] 加载策略数据...")
    main_picks, small_picks, etf_picks, chip_data, gc_data = _load_all_strategy_data()

    chip_picks = chip_data.get("all_picks", [])
    gc_codes   = [p["code"] for t in ("G0", "G1") for p in gc_data.get("tiers", {}).get(t, [])]
    etf_shown  = etf_picks[:30]
    all_codes  = list(dict.fromkeys(
        [p["code"] for p in main_picks + small_picks]
        + [p["code"] for p in etf_shown]
        + [p["code"] for p in chip_picks] + gc_codes
    ))
    print(f"[evening] 批量拉取收盘行情 {len(all_codes)} 只...")
    prices    = fetch_current_prices(all_codes) if all_codes else {}
    benchmark = fetch_benchmark_change()

    from report.utils import calc_pick_stats
    main_stats  = calc_pick_stats(main_picks,  prices)
    small_stats = calc_pick_stats(small_picks, prices)

    lines: list[str] = [
        f"📊 收盘总结 | 第{day}天 | {date.today().strftime('%m/%d')}", ""
    ]

    if benchmark is not None:
        lines.append(f"沪深300 **{benchmark:+.1f}%**  ")
        lines.append("")

    if main_stats["results"]:
        lines.append(f"**主策略  胜率{main_stats['win_rate']:.0f}%  均{main_stats['avg_ret']:+.2f}%**  ")
        for r in main_stats["results"]:
            icon = "📈" if r["change_pct"] > 0 else "📉"
            lines.append(f"{icon} {r['code']} {r['name']} **{r['change_pct']:+.2f}%**  ")
        lines.append("")

    if small_stats["results"]:
        lines.append(f"**小盘策略  胜率{small_stats['win_rate']:.0f}%  均{small_stats['avg_ret']:+.2f}%**  ")
        for r in small_stats["results"]:
            icon = "📈" if r["change_pct"] > 0 else "📉"
            lines.append(f"{icon} {r['code']} {r['name']} **{r['change_pct']:+.2f}%**  ")
        lines.append("")

    if etf_shown:
        lines += _fmt_etf_section(etf_shown, prices, slot="evening")

    lines += _fmt_chip_section(chip_data, prices, slot="evening")
    lines += _fmt_gc_section(gc_data, prices)
    lines += ["⚠️ 仅供参考，不构成投资建议", "#量化记录 #A股 #收盘", ""]
    post = "\n".join(lines)

    all_chg = ([r["change_pct"] for r in main_stats["results"]]
               + [r["change_pct"] for r in small_stats["results"]])
    avg_chg = round(sum(all_chg) / len(all_chg), 2) if all_chg else None
    verdict = compute_verdict(all_chg, benchmark)

    record.setdefault("date", str(date.today()))
    record.setdefault("day",  day)
    record["evening"] = {
        "timestamp":            datetime.now().strftime("%H:%M:%S"),
        "avg_change_pct":       avg_chg,
        "benchmark_change_pct": benchmark,
        "verdict":              verdict,
    }
    save_today(record)

    saved = save_post_file(post, "evening", "auto")
    print(f"{'='*52}")
    print(f"📊 收盘总结  （已保存 → {saved.relative_to(REPO_ROOT)}）")
    print(f"{'='*52}")
    print(post)
    _send_wechat_notify(
        f"📊 Day {day} 收盘总结｜{verdict_emoji(verdict)} {verdict}｜15:05",
        post,
    )


def run_status(args):
    ensure_dirs()
    record = load_today()
    if not record:
        print(f"今天（{date.today()}）暂无记录。")
        return

    print(f"📅 {record.get('date')}  Day {record.get('day', '?')}")

    if "morning" in record:
        m     = record["morning"]
        picks = m.get("picks", [])
        names = ", ".join(p.get("name", p.get("code", "?")) for p in picks)
        print(f"\n🌅 早盘  {m.get('timestamp', '')}  regime={m.get('regime', '')}")
        print(f"   query : {m.get('query', '')}")
        print(f"   picks : {names}")

    if "evening" in record:
        e       = record["evening"]
        verdict = e.get("verdict", "—")
        avg     = e.get("avg_change_pct")
        bm      = e.get("benchmark_change_pct")
        print(f"\n🌙 晚盘  {e.get('timestamp', '')}")
        print(f"   verdict : {verdict} {verdict_emoji(verdict)}")
        avg_str = f"{avg:+.2f}%" if avg is not None else "N/A"
        bm_str  = f"  (沪深300: {bm:+.2f}%)" if bm is not None else ""
        print(f"   avg chg : {avg_str}{bm_str}")
    else:
        print("\n🌙 晚盘记录暂无（运行 python xhs/reporter.py evening）")

    if "tomorrow_preview" in record:
        t     = record["tomorrow_preview"]
        names = ", ".join(p.get("name", "?") for p in t.get("picks", []))
        print(f"\n🔮 明日预告 : {names}")


def run_history(args):
    ensure_dirs()
    records = load_recent_history(14)
    if not records:
        print("暂无历史记录。")
        return

    streak = compute_streak(records)
    if streak["count"] > 1 and streak["type"]:
        print(f"当前连续「{streak['type']}」{streak['count']}次\n")

    verdicts = [r.get("evening", {}).get("verdict") for r in records]
    verdicts = [v for v in verdicts if v and v != "数据不足"]
    if verdicts:
        win_rate = verdicts.count("对了") / len(verdicts) * 100
        print(
            f"近{len(verdicts)}天  "
            f"对了 {verdicts.count('对了')} / 偏了 {verdicts.count('偏了')} / 错了 {verdicts.count('错了')}  "
            f"胜率 {win_rate:.0f}%\n"
        )

    print(f"{'Day':>4}  {'日期':12}  {'结果':4}  {'均涨跌':>8}  {'沪深300':>8}  picks")
    print("-" * 70)
    for r in records:
        day     = r.get("day", "?")
        d       = r.get("date", "?")
        e       = r.get("evening", {})
        verdict = e.get("verdict", "—")
        avg     = e.get("avg_change_pct")
        bm      = e.get("benchmark_change_pct")
        picks   = r.get("morning", {}).get("picks", [])
        names   = "、".join(p.get("name", "?") for p in picks[:3])
        emoji   = verdict_emoji(verdict) if verdict != "—" else " "
        avg_str = f"{avg:+.2f}%" if avg is not None else "   —  "
        bm_str  = f"{bm:+.2f}%"  if bm  is not None else "   —  "
        print(f"  {day:>3}  {d}  {emoji}{verdict:<3}  {avg_str:>8}  {bm_str:>8}  {names}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="小红书文案生成器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python xhs/reporter.py morning --query "低估值高成长" --top 5
  python xhs/reporter.py morning --style auto     # 根据当前 streak 自动选风格
  python xhs/reporter.py evening --style 1
  python xhs/reporter.py evening --no-tomorrow    # 跳过明日筛选（省2分钟）
  python xhs/reporter.py milestone                # Day 7/14/30 复盘帖
  python xhs/reporter.py milestone --force        # 任意时间强制生成复盘
  python xhs/reporter.py history
""",
    )
    sub = parser.add_subparsers(dest="cmd")

    p_morning = sub.add_parser("morning", help="生成早盘文案（竞价后，09:30 运行）")
    p_morning.add_argument("--query",  default="", help="筛选条件（留空则复用上次）")
    p_morning.add_argument("--top",    type=int, default=5, help="展示前N支股票（默认5）")
    p_morning.add_argument("--style",  default="all", help="文风 1/2/3/all/auto（默认all）")

    p_midday = sub.add_parser("midday", help="生成午盘中场更新文案")
    p_midday.add_argument("--style", default="all")

    p_night = sub.add_parser("night", help="生成晚间预告文案（22:00，筛明日候选方向）")
    p_night.add_argument("--query", default="", help="筛选条件（留空则复用上次）")
    p_night.add_argument("--top",   type=int, default=5, help="展示前N支股票（默认5）")
    p_night.add_argument("--style", default="auto", help="文风 1/2/3/all/auto（默认auto）")

    p_evening = sub.add_parser("evening", help="生成晚盘复盘+明日预告文案")
    p_evening.add_argument("--style", default="all")
    p_evening.add_argument("--no-tomorrow", action="store_true",
                           help="跳过明日预告筛选（省约2分钟）")

    p_milestone = sub.add_parser("milestone", help="生成里程碑复盘帖（Day 7/14/30）")
    p_milestone.add_argument("--force", action="store_true", help="忽略日期限制强制生成")

    sub.add_parser("status",  help="查看今日记录")
    sub.add_parser("history", help="查看近14天胜负统计")

    args = parser.parse_args()
    dispatch = {
        "morning":   run_morning,
        "midday":    run_midday,
        "evening":   run_evening,
        "night":     cmd_night,
        "milestone": run_milestone,
        "status":    run_status,
        "history":   run_history,
    }
    fn = dispatch.get(args.cmd)
    if fn:
        fn(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
