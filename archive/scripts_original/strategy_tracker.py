"""
Daily strategy pool recorder and forward-return tracker.

Records each day's top picks from each strategy with entry prices.
On subsequent runs, computes equal-weighted forward returns.

Storage: data/strategy_perf.json (rolling 30 days)
"""

import json
import os
from datetime import datetime

PERF_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "strategy_perf.json")
_MAX_DAYS = 30

_LABELS: dict[str, str] = {
    "lowvol":   "低波动多因子",
    "smallcap": "小市值",
}


def _load() -> list[dict]:
    try:
        with open(PERF_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save(records: list[dict]) -> None:
    try:
        with open(PERF_FILE, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def record_pool(date: str, strategy: str, stocks: list[dict]) -> None:
    """Record today's pool for a given strategy.

    Args:
        date:     'YYYY-MM-DD'
        strategy: 'lowvol' | 'smallcap'
        stocks:   list of dicts with at least {code, price};
                  name and buy_score are stored if present.
    """
    records = _load()
    entry = next((r for r in records if r["date"] == date), None)
    if entry is None:
        entry = {"date": date, "strategies": {}}
        records.append(entry)
    entry["strategies"][strategy] = [
        {
            "code":      s["code"],
            "name":      s.get("name", s["code"]),
            "price":     round(float(s["price"]), 4),
            "buy_score": s.get("buy_score"),
        }
        for s in stocks
        if s.get("price") and float(s["price"]) > 0
    ]
    records = sorted(records, key=lambda r: r["date"])[-_MAX_DAYS:]
    _save(records)


def compute_returns(price_map: dict[str, float]) -> dict[str, list[dict]]:
    """Compute cumulative forward returns for all past pools.

    Args:
        price_map: {code: current_price}

    Returns:
        {strategy: [{date, return_pct, n_stocks}]} sorted newest-first.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    result: dict[str, list[dict]] = {}
    for rec in _load():
        if rec["date"] >= today:
            continue
        for strat, stocks in rec.get("strategies", {}).items():
            rets = []
            for s in stocks:
                ep = s.get("price")
                cp = price_map.get(s["code"])
                if ep and ep > 0 and cp and cp > 0:
                    rets.append((cp - ep) / ep * 100)
            if not rets:
                continue
            result.setdefault(strat, []).append({
                "date":       rec["date"],
                "return_pct": round(sum(rets) / len(rets), 2),
                "n_stocks":   len(rets),
            })
    for strat in result:
        result[strat].sort(key=lambda x: x["date"], reverse=True)
    return result


def format_perf_section(price_map: dict[str, float]) -> str:
    """Return a markdown section comparing strategy performance.

    Returns empty string if no historical data exists yet.
    """
    perf = compute_returns(price_map)
    if not perf:
        return ""
    today = datetime.now().strftime("%Y-%m-%d")
    lines = ["## 策略表现对比\n"]
    for strat in ("lowvol", "smallcap"):
        entries = perf.get(strat)
        if not entries:
            continue
        label = _LABELS.get(strat, strat)
        lines.append(f"**{label}**")
        for e in entries[:5]:
            days = (
                datetime.strptime(today, "%Y-%m-%d")
                - datetime.strptime(e["date"], "%Y-%m-%d")
            ).days
            sign = "+" if e["return_pct"] >= 0 else ""
            lines.append(
                f"  - {days}日前 ({e['date']}): "
                f"{sign}{e['return_pct']:.1f}% ({e['n_stocks']}只)"
            )
        lines.append("")
    return "\n".join(lines)
