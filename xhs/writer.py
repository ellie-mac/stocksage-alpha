#!/usr/bin/env python3
"""
xhs/writer.py — 小红书文案生成器

用法（从仓库根目录运行）:
    python xhs/writer.py morning  [--query "低估值高成长"] [--top 5] [--style 1|2|3|all|auto]
    python xhs/writer.py midday   [--style ...]
    python xhs/writer.py evening  [--style ...] [--no-tomorrow]
    python xhs/writer.py milestone [--force]   # Day 7/14/30 周期性复盘
    python xhs/writer.py status
    python xhs/writer.py history
"""

import argparse
import json
import random
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
XHS_DIR    = Path(__file__).parent
REPO_ROOT  = XHS_DIR.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
RECORDS_DIR = XHS_DIR / "records"
POSTS_DIR   = RECORDS_DIR / "posts"
META_FILE   = RECORDS_DIR / "meta.json"

MILESTONE_DAYS = {7, 14, 21, 30, 60, 90}


# ---------------------------------------------------------------------------
# Record management
# ---------------------------------------------------------------------------

def ensure_dirs():
    RECORDS_DIR.mkdir(exist_ok=True)
    POSTS_DIR.mkdir(exist_ok=True)


def load_meta() -> dict:
    if META_FILE.exists():
        return json.loads(META_FILE.read_text(encoding="utf-8"))
    return {
        "start_date":       str(date.today()),
        "day_count":        0,
        "last_record_date": None,
        "last_query":       "综合",
    }


def save_meta(meta: dict):
    META_FILE.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def today_record_file() -> Path:
    return RECORDS_DIR / f"{date.today()}.json"


def load_today() -> dict:
    f = today_record_file()
    return json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}


def load_yesterday() -> dict:
    from datetime import timedelta
    f = RECORDS_DIR / f"{date.today() - timedelta(days=1)}.json"
    return json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}


def save_today(record: dict):
    today_record_file().write_text(
        json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def get_day_number() -> int:
    meta = load_meta()
    today = str(date.today())
    if meta.get("last_record_date") != today:
        meta["day_count"] = meta.get("day_count", 0) + 1
        meta["last_record_date"] = today
        save_meta(meta)
    return meta["day_count"]


def load_recent_history(n: int = 90) -> list[dict]:
    records = []
    for p in sorted(RECORDS_DIR.glob("????-??-??.json"), reverse=True)[:n]:
        try:
            records.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            pass
    return list(reversed(records))


def save_post_file(content: str, slot: str, style: int | str) -> Path:
    fname = POSTS_DIR / f"{date.today()}_{slot}_s{style}.txt"
    fname.write_text(content, encoding="utf-8-sig")
    return fname


def _send_wechat_notify(title: str, content: str):
    """Push a WeChat message via Server酱·Turbo. Silently skips if config missing."""
    try:
        config_file = XHS_DIR / "config.json"
        if not config_file.exists():
            return
        cfg = json.loads(config_file.read_text(encoding="utf-8"))
        sendkey = cfg.get("sendkey", "").strip()
        if not sendkey:
            return
        import urllib.request
        import urllib.parse
        url  = f"https://sctapi.ftqq.com/{sendkey}.send"
        data = urllib.parse.urlencode({"title": title, "desp": content}).encode()
        req  = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
        if result.get("code") == 0:
            print(f"[+] 微信通知已发送 ✅")
        else:
            print(f"[!] 微信通知返回异常: {result}", file=sys.stderr)
    except Exception as e:
        print(f"[!] 微信通知发送失败: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Screener integration
# ---------------------------------------------------------------------------

def run_screener(query: str, top_n: int = 5) -> Optional[dict]:
    cmd = [sys.executable, str(SCRIPTS_DIR / "screener.py"), query, f"--top={top_n}"]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, encoding="utf-8",
            timeout=120, cwd=str(SCRIPTS_DIR),
        )
        if result.returncode != 0:
            print(f"[!] screener.py 报错:\n{result.stderr[:500]}", file=sys.stderr)
            return None
        return json.loads(result.stdout)
    except subprocess.TimeoutExpired:
        print("[!] screener.py 超时（>120s）", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"[!] 解析 screener 输出失败: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[!] 调用 screener.py 失败: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Price fetching
# ---------------------------------------------------------------------------

def fetch_current_prices(codes: list[str]) -> dict[str, dict]:
    try:
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
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
        return obfuscate_stock_name(p.get("name", p.get("code", "?")))

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
_PINYIN_PREFIX: dict[str, str] = {
    "中国": "zg", "中远": "zy", "中信": "zx", "中铁": "zt", "中建": "zj",
    "中石": "zs", "中海": "zh", "中航": "zh", "中粮": "zl", "中联": "zl",
    "中电": "zd", "中交": "zj", "中冶": "zy", "中核": "zh", "中广": "zg",
    "中煤": "zm", "中化": "zh", "中船": "zc", "中科": "zk", "中钢": "zg",
    "贵州": "gz", "上海": "sh", "北京": "bj", "深圳": "sz", "广州": "gz",
    "招商": "zs", "工商": "gs", "建设": "js", "农业": "ny", "交通": "jt",
    "民生": "ms", "平安": "pa", "格力": "gl", "万科": "wk", "万达": "wd",
    "华夏": "hx", "华润": "hr", "华能": "hn", "华电": "hd", "华泰": "ht",
    "华侨": "hq", "华友": "hy", "华西": "hx", "华工": "hg", "华海": "hh",
    "比亚": "by", "宁德": "nd", "隆基": "lj", "海尔": "he", "海信": "hx",
    "海螺": "hl", "海康": "hk", "海天": "ht", "海通": "ht", "海澜": "hl",
    "东方": "df", "东风": "df", "东吴": "dw", "东阿": "da", "东鹏": "dp",
    "南方": "nf", "南京": "nj", "南钢": "ng", "南山": "ns", "南威": "nw",
    "北方": "bf", "北新": "bx", "北汽": "bq", "北控": "bk",
    "国泰": "gt", "国电": "gd", "国投": "gt", "国轩": "gx", "国际": "gj",
    "国联": "gl", "国睿": "gr", "国瓷": "gc", "国金": "gj", "国药": "gy",
    "兴业": "xy", "浦发": "pf", "光大": "gd", "招银": "zy", "招金": "zj",
    "长城": "cc", "长安": "ca", "长江": "cj", "长电": "cd", "长春": "cc",
    "三一": "sy", "三安": "sa", "三花": "sh", "三诺": "sn", "三峡": "sx",
    "徐工": "xg", "福耀": "fy", "汇川": "hc", "汇丰": "hf",
    "恒瑞": "hr", "恒大": "hd", "恒力": "hl", "恒生": "hs",
    "碧桂": "bg", "新希": "xx", "新华": "xh", "新能": "xn",
    "天齐": "tq", "天山": "ts", "天合": "th", "天津": "tj",
    "科大": "kd", "科华": "kh", "科锐": "kr", "科创": "kc",
    "立讯": "lx", "迈瑞": "mr", "药明": "ym", "爱尔": "ae",
    "宁波": "nb", "苏州": "sz", "无锡": "wx", "杭州": "hz",
}


def obfuscate_stock_name(name: str) -> str:
    """
    Obfuscate stock name per XHS content policy.
    Replaces first 2 chars with pinyin initials when known; otherwise drops first char.
    2-char names are left unchanged (already abbreviated enough).
    """
    if not name or len(name) <= 2:
        return name
    for prefix, pinyin in _PINYIN_PREFIX.items():
        if name.startswith(prefix):
            return pinyin + name[len(prefix):]
    # Fallback: drop first character
    return name[1:]


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

ACCOUNT_TAGLINE = "用量化模型每天筛选标的，只记录结果，不做主观干预。"


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
        name   = obfuscate_stock_name(p.get("name", p.get("code", "?")))
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
        name = obfuscate_stock_name(p.get("name", code))
        q = prices.get(code)
        if q:
            chg   = q["change_pct"]
            arrow = "📈" if chg > 1 else ("📉" if chg < -1 else "➡️")
            lines.append(f"  {name}  {arrow}  {chg:+.2f}%")
        else:
            lines.append(f"  {name}  — 数据获取失败")
    return "\n".join(lines)


def pick_names_short(picks: list[dict], n: int = 3) -> str:
    names = [obfuscate_stock_name(p.get("name", p.get("code", "?"))) for p in picks[:n]]
    return "、".join(names)


def format_tomorrow_teaser(tomorrow_picks: list[dict]) -> str:
    if not tomorrow_picks:
        return ""
    return "\n明天的方向已经跑出来了。\n明早更新。"


# ---------------------------------------------------------------------------
# CTA (Call to Action) — drives comments
# ---------------------------------------------------------------------------

MORNING_CTA = [
    "\n你觉得今天会对吗？评论区猜一下。",
    "\n猜猜今天会对还是翻车？",
    "\n今天这个方向你觉得对的可能性多大？",
    "\n评论区先猜结果，晚上揭晓。",
    "\n今天这批方向，你觉得会怎么走？",
    "\n你来判断一下，今天会不会对？",
]

EVENING_CTA = [
    "\n你们觉得这个结果正常吗？",
    "\n如果是你，还会继续跑这个模型吗？",
    "\n你觉得这个模型有没有用？",
    "\n有没有做过类似的事，结果怎么样？",
    "\n这种长期记录的方式，你们觉得有意义吗？",
    "\n留言说说你的看法？",
]

MILESTONE_CTA = [
    "\n你觉得这个模型值得继续跑下去吗？",
    "\n有没有人也在做类似的实验？",
    "\n你觉得再跑一个月会怎样？",
]

NIGHT_CTA = [
    "\n你觉得这批明天会怎么走？",
    "\n明早竞价后来看确认结果。",
    "\n先记着，明早看开盘再说。",
    "\n明天开盘后来更新。",
]

NIGHT_TITLES = [
    "今晚模型跑完了，记一下明天的方向",
    "睡前把明天可能的方向先记下来",
    "晚上10点，今晚的输出出来了",
    "今晚跑了一遍，明早竞价后再确认",
    "今晚的筛选结果，先留个记录",
    "今晚出来的方向，明天关注一下",
]

NIGHT_OPENERS = [
    "今晚跑了一遍模型，把明天可能关注的方向先记下来。",
    "收盘后的数据稳了，今晚跑出来了一批。",
    "今晚的模型输出，先记录下来。",
    "睡前把筛选结果跑出来，明早竞价后再核对一遍。",
    "今晚跑完了，这是结果。",
]

NIGHT_CLOSERS = [
    "明早9点半竞价结束后会来确认，\n看看开盘后信号有没有变化。",
    "这是今晚收盘数据的输出，\n竞价可能改变状态，明早更新。",
    "先记着，明早看开盘再做判断。\n\n明天见。",
    "竞价结束后来确认，\n有变化会及时更新。",
]


# ---------------------------------------------------------------------------
# Morning post templates
# ---------------------------------------------------------------------------

MORNING_TITLES = {
    1: [
        "今天模型给了一个我没想到的结果",
        "今天模型又选了一批，有点出乎意料",
        "今天早上跑完模型，有个方向我没想到",
        "今天这个结果，我看了两遍",
        "早上跑完，有个标的我没想到会出现",
        "今天的方向，说实话我有点意外",
    ],
    2: [
        "今天模型的判断，结果有点意思",
        "早盘数据跑出来了，记录一下",
        "今天模型的综合评分，跑完有些发现",
        "数据出来了，今天这批有个信号值得看",
        "今天跑了一遍，评分分布有点特别",
        "今天的筛选结果，有几个数字比较异常",
    ],
    3: [
        "这个方向，我原本是不会看的",
        "模型今天选的方向，和我直觉有点偏",
        "今天的结果让我犹豫了一下",
        "说实话，今天这批我自己不会这么选",
        "今天模型和我的判断出现了分歧",
        "这次我和模型的意见不太一样",
    ],
}

MORNING_OPENERS = {
    1: [
        "今天的结果出来，我盯着看了两秒没反应。",
        "今天这批方向，有一个我完全没预料到。",
        "说实话，今天的结果让我有点意外。",
        "跑出来之后，我看了一遍，又看了一遍。",
        "今天出来的方向，我第一反应是：这个？",
        "今天结果出来，有点东西。",
    ],
    2: [
        "今天数据出来了，有几个数字比较异常。",
        "今天跑了一遍，评分分布和往常不太一样。",
        "今天的筛选结果，有个信号值得关注。",
        "数据出来了，先把最关键的摆出来。",
        "今天的模型输出，有一处让我多看了两眼。",
        "数字出来了，有个地方有点意思。",
    ],
    3: [
        "今天模型和我的判断出现了明显分歧。",
        "说实话，今天这批我自己不会这么选。",
        "今天这个方向，从我的直觉来看基本是反的。",
        "我和模型今天站在了不同方向。",
    ],
}

MORNING_CLOSERS = {
    1: [
        "我现在不会去改它，\n就按模型原样记录下来。\n\n这个账号会持续记录每一次结果，\n包括它判断错的时候。\n\n想看看长期执行下来，\n这个东西到底有没有用。",
        "不做干预，只记录。\n\n晚上会来更新结果。",
        "继续按计划执行，\n不主观干预。\n\n结果晚上见。",
        "这个方向对不对，今晚见分晓。\n\n先按模型执行，不做人工干预。",
        "模型给什么我记什么，\n不加过滤，不做判断。\n\n晚上复盘。",
    ],
    2: [
        "今天晚盘会来复盘实际表现。\n\n目前不做任何主观干预，\n只记录模型原始输出。",
        "后续会持续跟踪，\n把真实结果跑出来看。",
        "今天的表现，晚上来更新。",
        "数据说话，晚上见真章。",
        "不加主观判断，\n晚盘结果直接贴出来。",
    ],
    3: [
        "但我不打算去改它。\n\n就按模型的逻辑走，\n看看最后谁对。",
        "我还是会按模型执行，\n不做人为干预。\n\n晚上看结果。",
        "暂时先记下来，\n等收盘看看是不是模型更准。",
        "我决定先压住自己的直觉，\n完全按模型执行。\n\n晚盘见。",
        "就让模型和我的直觉正面竞争一次，\n晚上公布结果。",
    ],
}


# ---------------------------------------------------------------------------
# Evening openers (verdict-aware)
# ---------------------------------------------------------------------------

EVENING_OPENERS = {
    "对了": [
        "早上问了大家今天会不会对，结果出来了——答对了。",
        "早上让你们猜，现在揭晓。今天是好消息。",
        "今天收盘，来更新结果。模型这次没翻车。",
        "早上的预测来对答案了。今天对了。",
    ],
    "偏了": [
        "今天的结果出来了，有意思——说对了有道理，说没对也有道理。",
        "早上问了你们的预测，今天这个结果有点难说，大家来帮我判断。",
        "收盘了。今天比较有争议，这个算对还是没对？",
        "早上让你们猜，今天这个结果我自己也没想好怎么定性。",
    ],
    "错了": [
        "早上问了大家的预测，结果出来了——翻车了。",
        "今天结果出来，来对答案。猜到会翻车的同学可以开心一下。",
        "收盘了，如实汇报。今天没对。",
        "早上的预测来揭晓了，今天这次翻车了。",
    ],
    "数据不足": [
        "今天数据有点问题，先记录一下现有的。",
    ],
}


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


# ---------------------------------------------------------------------------
# Midday post templates
# ---------------------------------------------------------------------------

MIDDAY_TITLES_NORMAL = [
    "中场来看一眼，模型今天的方向",
    "午盘中途更新一下",
    "中场：记录一下目前的状态",
    "中场数据出来了，顺手记一下",
]

MIDDAY_TITLES_DRAMATIC = [
    "中场意外：有只走势和预期完全反了",
    "中场出了点意思，需要记一下",
    "有只今天中场走了个极端",
    "中场有异动，先记录一下",
]


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
        name  = obfuscate_stock_name(mover.get("name", mover.get("code", "?")))
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


# ---------------------------------------------------------------------------
# Evening post templates
# ---------------------------------------------------------------------------

EVENING_TITLES = {
    "对了": {
        1: [
            "早上的那个结果，今天表现出来了",
            "今天模型这次判断对了",
            "早盘预测，今天有了答案",
            "今天验证了，模型这次没有打脸",
        ],
        2: [
            "今天复盘：模型跑赢了",
            "数据出来了，今天模型的判断",
            "今天的结果，比预期要好",
            "今天的数字说话了",
        ],
        3: [
            "模型今天又对了",
            "这次模型没有打脸",
            "今天还真让模型猜对了",
            "又对了，但我不敢掉以轻心",
        ],
    },
    "偏了": {
        1: [
            "今天的结果，有点出乎意料",
            "早上的那个方向，今天有些偏差",
            "模型今天表现平平",
            "今天这个结果，说对不对，说错不错",
        ],
        2: [
            "今天复盘：结果参差不齐",
            "今天的数据，不算对也不算错",
            "今天出现了一点分歧",
            "今天的结果比较难定性",
        ],
        3: [
            "今天有点偏，但也没全错",
            "这次模型有点犹豫",
            "今天的结果让我有点拿不准",
            "今天模型给了一个模糊的答案",
        ],
    },
    "错了": {
        1: [
            "早上那个方向，今天打脸了",
            "今天结果和预期反了",
            "模型今天判断有偏差",
            "今天模型错了，如实记录",
        ],
        2: [
            "今天复盘：结果不太好看",
            "今天数据出来，比较难看",
            "今天模型的判断，确实偏了",
            "今天这个数字，不好看",
        ],
        3: [
            "模型今天打脸了",
            "这次真的有点离谱",
            "今天，我开始有点怀疑这个模型了",
            "又错了，开始认真想这个问题了",
        ],
    },
    "数据不足": {
        1: ["今天数据更新延迟，先记录一下"],
        2: ["今天数据暂时缺失"],
        3: ["今天的数据还在确认中"],
    },
}

EVENING_REFLECTIONS = {
    1: [
        "我还是按之前的方式：\n不做干预，只记录。\n\n其实我更关心的是，\n连续执行一段时间之后，\n这个模型到底有没有稳定性。\n\n后面会继续每天更新，\n把完整结果跑出来看看。",
        "继续执行，不做干预。\n\n一次的对错不说明什么，\n要跑够数量才能看出规律。",
        "先把结果记下来。\n\n不在乎单次的对错，\n在乎的是长期的稳定性。",
    ],
    2: [
        "持续跟踪中，不做主观干预。",
        "数据如实记录，不加主观滤镜。",
        "继续跑，继续记。",
    ],
    3: {
        "错了": [
            "我开始有点怀疑这个模型了。\n\n但还不到改的时候，\n先把数据跑够再说。",
            "这次有点说不过去。\n但先继续执行，\n等样本多了再下结论。",
        ],
        "对了": [
            "但一次对了不代表什么，\n继续跑，继续记。",
            "对了，但我不打算因此就信任它，\n继续看。",
        ],
        "偏了": [
            "不知道该说对还是错，\n先继续执行。",
            "今天这个结果很模糊，\n说不清楚，继续记录。",
        ],
        "数据不足": [
            "数据有点问题，明天继续。",
        ],
    },
}


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
                obfuscate_stock_name(p.get("name", p.get("code", "?")))
                for p in limit_up
            )
            highlight_note = random.choice([
                f"\n{names_lu}今天封板了，这个结果还不错。",
                f"\n{names_lu}涨停，方向对了。",
                f"\n其中{names_lu}封板，今天这批跑出来了。",
            ])
        elif strong:
            names_s = obfuscate_stock_name(
                strong[0].get("name", strong[0].get("code", "?"))
            )
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


def cmd_milestone(args):
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


def cmd_morning(args):
    ensure_dirs()
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
        print(f"[!] 今天是 Day {day}，里程碑节点！建议运行: python xhs/writer.py milestone\n")

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
    day  = get_day_number()
    meta = load_meta()
    query = args.query or meta.get("last_query") or "综合"

    print(f"[+] 晚间筛选明日候选方向（query='{query}', top={args.top}）...")
    screener_output = run_screener(query, args.top)

    if screener_output is None:
        print("[!] screener 未返回结果，生成占位文案。")
        picks = []
    else:
        picks = screener_output.get("results", [])[:args.top]
        meta["last_query"] = query
        save_meta(meta)

    record = load_today()
    record.setdefault("date", str(date.today()))
    record.setdefault("day",  day)
    record["night_preview"] = {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "query":     query,
        "picks":     picks,
    }
    save_today(record)
    print(f"[+] 晚间预告已保存 → xhs/records/{date.today()}.json  (Day {day})\n")

    history = load_recent_history(14)
    streak  = compute_streak(history)
    styles  = _resolve_styles(args.style, streak)

    first_post = None
    for s in styles:
        post  = generate_night_post(picks, day, query, style=s)
        saved = save_post_file(post, "night", s)
        print(f"{'='*52}")
        print(f"🌙 晚间文案 — 风格{s}  （已保存 → {saved.relative_to(REPO_ROOT)}）")
        print(f"{'='*52}")
        print(post)
        print()
        if first_post is None:
            first_post = post
    if first_post:
        _send_wechat_notify(f"🌙 Day {day} 晚间文案｜22:00 发布", first_post)


def cmd_midday(args):
    ensure_dirs()
    record = load_today()
    if "morning" not in record:
        print("[!] 今天还没有早盘记录，请先运行: python xhs/writer.py morning")
        return

    day           = record.get("day", get_day_number())
    morning_picks = record["morning"].get("picks", [])
    codes         = [p["code"] for p in morning_picks if "code" in p]

    print("[+] 正在获取最新行情...")
    prices = fetch_current_prices(codes) if codes else {}

    history = load_recent_history(14)
    streak  = compute_streak(history)
    styles  = _resolve_styles(args.style, streak)

    first_post = None
    for s in styles:
        post  = generate_midday_post(morning_picks, prices, day, style=s)
        saved = save_post_file(post, "midday", s)
        print(f"{'='*52}")
        print(f"☀️  午盘文案 — 风格{s}  （已保存 → {saved.relative_to(REPO_ROOT)}）")
        print(f"{'='*52}")
        print(post)
        print()
        if first_post is None:
            first_post = post
    if first_post:
        _send_wechat_notify(f"☀️ Day {day} 午盘文案｜11:50–12:00 发布", first_post)


def cmd_evening(args):
    ensure_dirs()
    record = load_today()
    if "morning" not in record:
        print("[!] 今天还没有早盘记录，请先运行: python xhs/writer.py morning")
        return

    day           = record.get("day", get_day_number())
    morning_picks = record["morning"].get("picks", [])
    query         = record["morning"].get("query", "综合")
    codes         = [p["code"] for p in morning_picks if "code" in p]

    print("[+] 正在获取收盘数据...")
    prices    = fetch_current_prices(codes) if codes else {}
    benchmark = fetch_benchmark_change()

    changes = [prices[c]["change_pct"] for c in codes if c in prices]
    verdict = compute_verdict(changes, benchmark)
    avg_change = round(sum(changes) / len(changes), 2) if changes else None
    alpha = (avg_change - benchmark) if (avg_change is not None and benchmark is not None) else None

    tomorrow_picks = []
    if not args.no_tomorrow:
        print("[+] 正在筛选明日预告方向...")
        tomorrow_output = run_screener(query, top_n=3)
        tomorrow_picks  = tomorrow_output.get("results", [])[:3] if tomorrow_output else []

    record["evening"] = {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "picks_performance": [
            {
                "code":       p.get("code"),
                "name":       p.get("name"),
                "change_pct": prices.get(p.get("code", ""), {}).get("change_pct"),
            }
            for p in morning_picks
        ],
        "benchmark_change_pct": benchmark,
        "verdict":       verdict,
        "avg_change_pct": avg_change,
    }
    if tomorrow_picks:
        record["tomorrow_preview"] = {
            "picks":     tomorrow_picks,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        }
    save_today(record)
    print(f"[+] 晚盘记录已保存。verdict={verdict} {verdict_emoji(verdict)}\n")

    history = load_recent_history(14)
    streak  = compute_streak(history)
    styles  = _resolve_styles(args.style, streak, alpha)

    first_post = None
    for s in styles:
        post  = generate_evening_post(
            morning_picks, prices, benchmark, verdict,
            day, streak, tomorrow_picks, style=s,
        )
        saved = save_post_file(post, "evening", s)
        print(f"{'='*52}")
        print(f"🌙 晚盘文案 — 风格{s}  （已保存 → {saved.relative_to(REPO_ROOT)}）")
        print(f"{'='*52}")
        print(post)
        print()
        if first_post is None:
            first_post = post
    if first_post:
        _send_wechat_notify(
            f"🌙 Day {day} 晚盘文案｜{verdict_emoji(verdict)} {verdict}｜15:35–15:45 发布",
            first_post,
        )


def cmd_status(args):
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
        for pp in e.get("picks_performance", []):
            chg     = pp.get("change_pct")
            chg_str = f"{chg:+.2f}%" if chg is not None else "N/A"
            print(f"            {pp.get('name', '?')}: {chg_str}")
    else:
        print("\n🌙 晚盘记录暂无（运行 python xhs/writer.py evening）")

    if "tomorrow_preview" in record:
        t     = record["tomorrow_preview"]
        names = ", ".join(p.get("name", "?") for p in t.get("picks", []))
        print(f"\n🔮 明日预告 : {names}")


def cmd_history(args):
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
  python xhs/writer.py morning --query "低估值高成长" --top 5
  python xhs/writer.py morning --style auto     # 根据当前 streak 自动选风格
  python xhs/writer.py evening --style 1
  python xhs/writer.py evening --no-tomorrow    # 跳过明日筛选（省2分钟）
  python xhs/writer.py milestone                # Day 7/14/30 复盘帖
  python xhs/writer.py milestone --force        # 任意时间强制生成复盘
  python xhs/writer.py history
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
        "morning":   cmd_morning,
        "midday":    cmd_midday,
        "evening":   cmd_evening,
        "night":     cmd_night,
        "milestone": cmd_milestone,
        "status":    cmd_status,
        "history":   cmd_history,
    }
    fn = dispatch.get(args.cmd)
    if fn:
        fn(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
