"""
concept_guard.py — 半导体材料概念逻辑证伪监控
==============================================
每日早盘前(8:50)和午盘前(12:50)运行，搜索17个概念的最新新闻，
检测逻辑证伪信号，若触发则推送飞书告警。

证伪信号：
  1. 日本宣布钨替代方案或恢复WF6产能
  2. 中国放松钨/锗出口管制
  3. 台积电/三星宣布转用中国WF6（利好兑现）
  4. NF3/WF6价格大幅下跌
  5. 关键票业绩暴雷（Q2验证失败）
  6. 覆铜板/电子布大幅降价
  7. 半导体材料板块系统性利空

用法：
  python -X utf8 src/jobs/concept_guard.py
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from notify.notify import push_feishu_card

# ── 监控关键词 ────────────────────────────────────────────────────────────────

ALERT_SIGNALS = {
    "WF6断供终结": [
        "日本 WF6 恢复",
        "日本 六氟化钨 复产",
        "关东电化 恢复生产",
        "中央硝子 复产",
        "钨 出口管制 放松",
        "钨 出口 解禁",
        "WF6 价格 下跌",
        "六氟化钨 降价",
    ],
    "NF3利空": [
        "NF3 价格下跌",
        "三氟化氮 降价",
        "NF3 产能过剩",
    ],
    "覆铜板/电子布降价": [
        "覆铜板 降价",
        "CCL 降价",
        "电子布 降价",
        "玻纤布 价格下跌",
    ],
    "半导体材料系统利空": [
        "半导体材料 利空",
        "台积电 转用 中国",
        "三星 WF6 国产替代完成",
        "半导体 去库存",
        "晶圆厂 砍单",
    ],
    "关键票暴雷": [
        "昊华科技 业绩下滑",
        "中巨芯 亏损扩大",
        "雅克科技 业绩不及预期",
        "菲利华 订单下滑",
        "江丰电子 业绩暴雷",
    ],
}

# 正面信号（加仓信号，提醒关注）
POSITIVE_SIGNALS = {
    "涨价催化": [
        "WF6 涨价",
        "六氟化钨 涨价",
        "NF3 涨价",
        "三氟化氮 涨价",
        "覆铜板 涨价",
        "电子特气 涨价",
        "靶材 涨价",
    ],
    "新订单/认证": [
        "昊华科技 订单",
        "中巨芯 认证",
        "菲利华 英伟达",
        "江丰电子 客户",
        "安集科技 认证",
    ],
}


def _search_news(keywords: list[str]) -> list[dict]:
    """Search recent news via akshare or simple web scraping. Returns list of hits."""
    hits = []
    try:
        import akshare as ak
        for kw in keywords:
            try:
                df = ak.stock_news_em(symbol=kw)
                if df is not None and not df.empty:
                    # Only keep today's news
                    today = datetime.now().strftime("%Y-%m-%d")
                    recent = df[df["发布时间"].str.startswith(today)] if "发布时间" in df.columns else df.head(3)
                    for _, row in recent.iterrows():
                        hits.append({
                            "keyword": kw,
                            "title": row.get("新闻标题", row.get("title", "")),
                            "time": row.get("发布时间", ""),
                            "url": row.get("新闻链接", row.get("url", "")),
                        })
            except Exception:
                pass
            time.sleep(0.5)
    except ImportError:
        # Fallback: use eastmoney news search
        import urllib.request
        for kw in keywords[:5]:
            try:
                url = f"https://search-api-web.eastmoney.com/search/jsonp?cb=x&param=%7B%22uid%22%3A%22%22%2C%22keyword%22%3A%22{kw}%22%2C%22type%22%3A%5B%22cmsArticleWebOld%22%5D%2C%22client%22%3A%22web%22%2C%22clientType%22%3A%22web%22%2C%22clientVersion%22%3A%22curr%22%2C%22param%22%3A%7B%22cmsArticleWebOld%22%3A%7B%22searchScope%22%3A%22default%22%2C%22sort%22%3A%22default%22%2C%22pageIndex%22%3A1%2C%22pageSize%22%3A3%2C%22preTag%22%3A%22%22%2C%22postTag%22%3A%22%22%7D%7D%7D"
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=5) as r:
                    text = r.read().decode("utf-8")
                    # parse jsonp
                    json_str = text[text.index("(") + 1 : text.rindex(")")]
                    data = json.loads(json_str)
                    articles = data.get("result", {}).get("cmsArticleWebOld", {}).get("list", [])
                    for art in articles[:2]:
                        hits.append({
                            "keyword": kw,
                            "title": art.get("title", "").replace("<em>", "").replace("</em>", ""),
                            "time": art.get("date", ""),
                            "url": art.get("url", ""),
                        })
            except Exception:
                pass
            time.sleep(0.3)
    return hits


def run_guard():
    """Main guard logic: search for alert signals, push if found."""
    now = datetime.now()
    period = "早盘前" if now.hour < 12 else "午盘前"
    print(f"[concept_guard] {now.strftime('%Y-%m-%d %H:%M')} {period}扫描开始", flush=True)

    alerts = []
    positives = []

    # Check negative signals
    for category, keywords in ALERT_SIGNALS.items():
        hits = _search_news(keywords)
        if hits:
            alerts.append((category, hits))

    # Check positive signals
    for category, keywords in POSITIVE_SIGNALS.items():
        hits = _search_news(keywords)
        if hits:
            positives.append((category, hits))

    # Build message
    if alerts:
        lines = [f"⚠️ {period}逻辑证伪监控 — 发现{len(alerts)}类告警信号！", ""]
        for cat, hits in alerts:
            lines.append(f"🔴 【{cat}】")
            for h in hits[:3]:
                lines.append(f"  · {h['title'][:50]}  ({h['time']})")
            lines.append("")
        lines.append("⚡ 建议动作：检查相关持仓，考虑减仓")
        push_feishu_card(f"🚨 逻辑证伪告警 — {period}", lines)
        print(f"[concept_guard] ⚠️ 发现告警信号 {len(alerts)} 类，已推送飞书", flush=True)
    elif positives:
        lines = [f"✅ {period}概念监控 — 发现{len(positives)}类正面信号", ""]
        for cat, hits in positives:
            lines.append(f"🟢 【{cat}】")
            for h in hits[:3]:
                lines.append(f"  · {h['title'][:50]}  ({h['time']})")
            lines.append("")
        push_feishu_card(f"📈 正面催化信号 — {period}", lines)
        print(f"[concept_guard] ✅ 发现正面信号 {len(positives)} 类，已推送飞书", flush=True)
    else:
        print(f"[concept_guard] ✅ 无异常信号，逻辑未被证伪", flush=True)
        # 不推飞书，避免噪音


if __name__ == "__main__":
    run_guard()
