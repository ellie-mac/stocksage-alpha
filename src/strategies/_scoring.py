"""扫描公共骨架 — 把 ThreadPool 评分 + bear regime 提分 + 标准过滤抽出来。

main/small/watchlist scan() 都重复了：
  1. weights → ThreadPoolExecutor(score_one_buy) → sort
  2. bear regime 把 buy_trig × 1.25/1.15
  3. 同质的过滤循环（skip error/held/limit-up + sell_guard + top_n）

这里只放骨架；具体过滤规则（bear_sell_cap、small 的 _sc_signal）留在各策略里。
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from typing import Optional

from factors import weights_from_config_dict
from report.utils import score_one_buy as _score_one_buy


def score_universe(
    codes: list[str],
    weights_dict: dict,
    *,
    max_workers: int = 8,
) -> list[dict]:
    """对 codes 并发打分，按 buy_score 降序返回所有评分（不过滤）。

    weights_dict 由调用方按 regime / 策略类型选定（REGIME_WEIGHTS[rk]、
    REGIME_WEIGHTS_SMALLCAP[rk]、FACTOR_WEIGHTS_ETF）。
    """
    if not codes:
        return []
    fw = weights_from_config_dict(weights_dict)
    _score = partial(_score_one_buy, weights=fw)
    scored: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_score, c): c for c in codes}
        for fut in as_completed(futs):
            scored.append(fut.result())
    scored.sort(key=lambda x: -x.get("buy_score", 0))
    return scored


def adjust_buy_trig(buy_trig: float, regime_score: float) -> float:
    """Bear regime 把买入阈值提高：≤2 → ×1.25，≤4 → ×1.15。"""
    if regime_score <= 2:
        return round(buy_trig * 1.25, 1)
    if regime_score <= 4:
        return round(buy_trig * 1.15, 1)
    return buy_trig


def filter_buys(
    scored: list[dict],
    *,
    buy_trig: float,
    sell_guard: float,
    top_n: int,
    held_codes: Optional[set] = None,
    bear_sell_cap: Optional[float] = None,
    limit_pct: float = 9.5,
) -> list[dict]:
    """从已排序 scored 里挑 top_n 个买入候选。

    scored 必须按 buy_score 降序——遇到 score < buy_trig 就 break。
    held_codes / 涨停 / sell_score >= sell_guard / bear_sell_cap 一律剔除。
    """
    held = held_codes or set()
    out: list[dict] = []
    for s in scored:
        if s.get("error") or s["code"] in held:
            continue
        if s["buy_score"] < buy_trig:
            break
        if s["sell_score"] >= sell_guard:
            continue
        if bear_sell_cap is not None and s["sell_score"] >= bear_sell_cap:
            continue
        if (s.get("change_pct") or 0) >= limit_pct:
            continue
        out.append(s)
        if len(out) >= top_n:
            break
    return out
