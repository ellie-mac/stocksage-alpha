from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
from ._utils import _extract, _extract_two, _neutral, _get_price_position


def score_social_heat(
    social_dict: Optional[dict],
    price_df: Optional[pd.DataFrame] = None,
    financial_df: Optional[pd.DataFrame] = None,
    best_concept_ret: Optional[float] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    East Money hot stock ranking as a sentiment proxy (max 10).
    CONTRARIAN: very high ranking (extreme retail attention) = sell signal.
    Moderate attention = positive signal (institutional interest growing).

      rank_pct top 1%   -> sell 8 (extreme retail frenzy)
      rank_pct top 5%   -> sell 5, buy 4
      rank_pct top 20%  -> buy 7 (healthy attention)
      rank_pct > 50%    -> buy 3 (low attention)
      no data           -> neutral

    Context cross with 52w price position:
      Extreme heat (top 5%) + high position (> 0.7) -> hype at the peak, amplify contrarian sell (+2)
      Extreme heat (top 5%) + low position (< 0.3)  -> retail FOMO on a beaten-down stock;
        A-share short squeezes from lows are real → soften contrarian sell (-2)

    Fundamental quality cross (requires financial_df): genuine discovery vs speculative frenzy
      High heat (top 5%) + ROE >= 15% -> institutional discovery of quality, soften contrarian sell (-1.5), buy +1
      High heat (top 5%) + ROE <  5%  -> speculative retail frenzy on weak business, amplify sell (+2)
      Moderate heat (top 20%) + ROE >= 15% -> quality company gaining deserved attention, buy +1

    Volume cross (requires price_df): real money vs pure social noise
      High heat (top 20%) + volume surge (v5/v20 >= 2x) -> retail FOMO confirmed, amplify contrarian sell +1.5
      High heat (top 20%) + volume low (v5/v20 < 0.7x)  -> buzz fading quickly, sell +1
      Low heat (> 50%) + volume breakout (>= 2x)        -> institutional buying quietly, buy +1.5

    Concept momentum alignment cross (requires best_concept_ret):
      High heat (top 20%) + hot concept (>= +8%) -> 双重FOMO信号，炒作顶部 -> sell +1.5
      High heat (top 20%) + concept cold (< 0%)  -> 热度没有板块支撑，快速消退 -> sell +1
      Low heat (> 50%) + hot concept (>= +8%)    -> 机构在推概念，散户未感知，早期机会 -> buy +1.5

    Market regime cross (requires market_regime_score):
      Extreme heat (top 5%) + bear market (regime <= 3) -> 熊市炒作无持续性，更强卖出 -> sell +1.5
      Extreme heat (top 5%) + bull market (regime >= 7) -> 牛市热度有基础，略减弱卖出 -> sell -0.5
    """
    if social_dict is None or "rank_pct" not in social_dict:
        return _neutral(10)

    rank_pct = float(social_dict["rank_pct"])  # lower = more popular (top ranked)

    # Buy: moderate attention is good, extreme attention is bad
    if rank_pct <= 1:
        # Top 1%: extreme retail heat = contrarian sell
        score = 2.0
        sell_score = 8.0
        signal = "extreme retail heat (contrarian sell)"
    elif rank_pct <= 5:
        score = 4.0
        sell_score = 5.0
        signal = "very high attention"
    elif rank_pct <= 20:
        score = 7.0
        sell_score = 1.0
        signal = "healthy attention"
    elif rank_pct <= 50:
        score = 5.0
        sell_score = 0.0
        signal = "moderate attention"
    else:
        score = 3.0
        sell_score = 0.0
        signal = "low attention"

    # --- Context cross: retail heat × 52w price position ---
    position = _get_price_position(price_df)
    if position is not None and rank_pct <= 5:
        # Only applies when retail heat is high (top 5%)
        if position > 0.7:
            # Extreme attention at price highs: classic "last buyer" scenario — amplify contrarian
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + " (at highs — peak frenzy, strong contrarian sell)"
        elif position < 0.3:
            # Extreme attention on a beaten-down stock: retail FOMO from lows
            # A-share low-position squeezes are real; soften the contrarian sell
            sell_score = max(0.0, sell_score - 2.0)
            signal = signal + " (at lows — retail FOMO from low base, soften contrarian)"

    # --- Fundamental quality cross: genuine interest vs speculative frenzy ---
    roe = None
    if financial_df is not None and not financial_df.empty:
        for key in ["净资产收益率(%)", "加权净资产收益率(%)", "ROE(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    roe = float(vals.iloc[0])
                break

    if roe is not None:
        if rank_pct <= 5:
            # High heat: quality makes the difference between discovery and frenzy
            if roe >= 15:
                # Institutions accumulating a quality company: contrarian signal overstated
                sell_score = max(0.0, sell_score - 1.5)
                score = min(10.0, score + 1.0)
                signal = signal + f" + high ROE {roe:.0f}% (institutional discovery, not frenzy)"
            elif roe < 5:
                # Pure speculative retail pile-in on a weak business: classic pump pattern
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + f" + low ROE {roe:.0f}% (speculative frenzy, amplify contrarian)"
        elif rank_pct <= 20 and roe >= 15:
            # Moderate heat + quality: deserved attention growing, genuine buy signal
            score = min(10.0, score + 1.0)
            signal = signal + f" + high ROE {roe:.0f}% (quality company gaining attention)"

    # --- Volume cross: distinguish real money interest from pure social noise ---
    if price_df is not None and "volume" in price_df.columns and len(price_df) >= 25:
        try:
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(vol) >= 25:
                v5  = float(vol.tail(5).mean())
                v20 = float(vol.tail(25).head(20).mean())
                if v20 > 0:
                    heat_vol_ratio = v5 / v20
                    if rank_pct <= 20 and heat_vol_ratio >= 2.0:
                        # Trending socially + volume surge: retail FOMO is confirmed, amplify contrarian sell
                        sell_score = min(10.0, sell_score + 1.5)
                        signal = signal + f" + volume surge ×{heat_vol_ratio:.1f} (retail FOMO confirmed — amplify contrarian)"
                    elif rank_pct <= 20 and heat_vol_ratio < 0.7:
                        # Social buzz but volume drying up: attention without trading = fading quickly
                        sell_score = min(10.0, sell_score + 1.0)
                        signal = signal + f" + volume low ×{heat_vol_ratio:.1f} (social buzz without trading — fading)"
                    elif rank_pct > 50 and heat_vol_ratio >= 2.0:
                        # Low social heat but volume surging: institutional buying quietly without retail noise
                        score = min(10.0, score + 1.5)
                        signal = signal + f" + volume breakout ×{heat_vol_ratio:.1f} (institutional buying quietly)"
        except Exception:
            pass

    # --- Concept momentum alignment cross ---
    if best_concept_ret is not None:
        if rank_pct <= 20 and best_concept_ret >= 8:
            # Social heat + hot concept: double FOMO signal, classic A-share pump top
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + f" + hot concept +{best_concept_ret:.1f}% (双重FOMO — 炒作顶部风险)"
        elif rank_pct <= 20 and best_concept_ret < 0:
            # Social heat but concept is falling: buzz without sector backing, will fade quickly
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + f" + concept {best_concept_ret:.1f}% (热度无板块支撑 — 快速消退)"
        elif rank_pct > 50 and best_concept_ret >= 8:
            # Low social heat + hot concept: institutional rotation without retail noise = early
            score = min(10.0, score + 1.5)
            signal = signal + f" + hot concept +{best_concept_ret:.1f}% (低热度+板块热 — 机构推动早期)"

    # --- Market regime cross: social heat contrarian signal is stronger in bear markets ---
    if market_regime_score is not None and rank_pct <= 5:
        if market_regime_score <= 3:
            # Bear market extreme heat: speculative pop has no macro tailwind, fades faster
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (bear market — 熊市热炒无持续性，更强反向信号)"
        elif market_regime_score >= 7:
            # Bull market extreme heat: rising tide softens the contrarian sell
            sell_score = max(0.0, sell_score - 0.5)
            signal = signal + " (bull market — 牛市热度有基础，略减弱卖出)"

    # --- Industry excess cross: hot stock in hot sector vs isolated retail hype ---
    industry_signal_s = None
    if industry_ret_1m is not None and market_ret_1m is not None:
        excess_s = industry_ret_1m - market_ret_1m
        if rank_pct <= 20:
            if excess_s >= 3.0:
                # High social heat + strong sector: institutional and retail converging — bubble risk higher
                sell_score = min(10.0, sell_score + 1.0)
                industry_signal_s = f"社交热+行业强(超额{excess_s:+.1f}%) — 机构散户共振炒作，泡沫风险更高"
            elif excess_s <= -3.0:
                # High social heat + weak sector: isolated retail hype with no sector support, fades faster
                sell_score = min(10.0, sell_score + 1.5)
                industry_signal_s = f"社交热+行业弱(超额{excess_s:+.1f}%) — 散户孤立炒作无行业支撑，热度更快消退"
        elif rank_pct > 50 and excess_s >= 3.0:
            # Low heat + hot sector: stock being overlooked while sector rallies — catch-up potential
            score = min(10.0, score + 1.0)
            industry_signal_s = f"低热度+行业强(超额{excess_s:+.1f}%) — 行业热但此股被忽视，补涨机会"

    # --- Earnings revision cross: analyst view vs retail sentiment divergence ---
    revision_signal_sh = None
    if revision_df is not None and not revision_df.empty:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_sh   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_sh = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_sh  = up_sh - down_sh
                if rank_pct <= 20 and net_sh <= -1:
                    # High social heat + analyst downgrade: retail pump while institutions exit
                    sell_score = min(10.0, sell_score + 2.0)
                    revision_signal_sh = f"社交热+分析师下调({net_sh:+d}家) — 散户炒作机构撤退，最强卖出信号"
                elif rank_pct <= 20 and net_sh >= 2:
                    # High social heat + analyst upgrade: rare dual confirmation
                    sell_score = max(0.0, sell_score - 1.0)
                    revision_signal_sh = f"社交热+分析师上调({net_sh:+d}家) — 基本面情绪共振，热度有业绩支撑"
        except Exception:
            pass

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "rank":            social_dict.get("rank"),
            "rank_pct":        round(rank_pct, 1),
            "position_52w":    round(position, 3) if position is not None else None,
            "roe_pct":         round(roe, 1) if roe is not None else None,
            "best_concept_ret": round(best_concept_ret, 2) if best_concept_ret is not None else None,
            "market_regime_score": market_regime_score,
            "industry_excess_pct": round(industry_ret_1m - market_ret_1m, 1) if (industry_ret_1m is not None and market_ret_1m is not None) else None,
            "industry_signal":  industry_signal_s,
            "revision_signal":  revision_signal_sh,
            "signal":           signal,
            "sell_score":       round(sell_score, 1),
        },
    }




def score_concept_momentum(
    concept_data: Optional[list],
    price_df: Optional[pd.DataFrame] = None,
    market_regime_score: Optional[float] = None,
    financial_df: Optional[pd.DataFrame] = None,
    industry_excess: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Concept/theme board momentum score (max 10).

    Uses the stock's top-5 concept boards by 1-month return magnitude.
    The best concept (highest return) drives the buy signal; the worst
    (most negative return) drives the sell signal.

    Cross-rule: compare the stock's own 1m return against its hottest concept.
      - Stock lags concept by ≥15%  → buy +2 (catch-up candidate)
      - Stock leads concept by ≥20% → sell +2 (dragon-head fade risk)

    Market regime cross: concept rallies behave differently across market environments.
      Hot concept (best_ret ≥ +10%) + bear market (regime ≤ 3) → buy -2, sell +1.5
        (熊市题材炒作持续性极差，快进快出的游资行为为主)
      Hot concept + bull market (regime ≥ 7) → buy +1
        (牛市板块共振具有延续性，跟进性价比更高)

    ROE quality cross: distinguishes fundamentals-backed thematic rally from pure speculation.
      Hot concept (best_ret ≥ +8%) + ROE >= 15% → buy +1.5 (quality company in hot sector)
      Hot concept + ROE < 5%                    → sell +2  (speculative play, no earnings support)

    Volume confirmation cross (requires price_df):
      Hot concept (best_ret >= 8%) + volume breakout (vol_5d/vol_20d >= 1.5) → buy +1.5 (real participation)
      Hot concept + volume contraction (vol_5d/vol_20d < 0.7) → buy -1, sell +1 (hype without money)

    Industry × concept dual momentum cross (requires industry_excess):
      Hot concept + industry outperforming (excess >= 3%) → buy +1.5 (dual catalyst — sector + concept)
      Hot concept + industry underperforming (excess <= -3%) → sell +0.5 (isolated play, less reliable)

    52w price position cross (requires price_df):
      Hot concept (best_ret >= +8%) + low position (< 0.3) -> concept rally + low base = buy +1.5
      Hot concept (best_ret >= +8%) + high position (> 0.7) -> overextended concept rally, sell +1
    """
    if not concept_data:
        return _neutral(10)

    best  = max(concept_data, key=lambda x: x["ret_1m"])
    worst = min(concept_data, key=lambda x: x["ret_1m"])
    best_ret  = best["ret_1m"]
    worst_ret = worst["ret_1m"]

    # Buy signal: driven by best concept performance
    if best_ret >= 15:
        score = 9.0
        signal = f"hot concept 【{best['name']}】 +{best_ret:.1f}%"
    elif best_ret >= 8:
        score = 7.0
        signal = f"strong concept 【{best['name']}】 +{best_ret:.1f}%"
    elif best_ret >= 3:
        score = 5.5
        signal = f"rising concept 【{best['name']}】 +{best_ret:.1f}%"
    elif best_ret >= 0:
        score = 4.0
        signal = f"flat concept 【{best['name']}】 {best_ret:.1f}%"
    else:
        score = 2.0
        signal = f"all concepts falling, best: 【{best['name']}】 {best_ret:.1f}%"

    # Sell signal: driven by worst concept performance
    if worst_ret <= -15:
        sell_score = 8.0
        sell_signal = f"concept collapse 【{worst['name']}】 {worst_ret:.1f}%"
    elif worst_ret <= -8:
        sell_score = 5.0
        sell_signal = f"concept weakness 【{worst['name']}】 {worst_ret:.1f}%"
    elif worst_ret <= -3:
        sell_score = 3.0
        sell_signal = f"concept softening 【{worst['name']}】 {worst_ret:.1f}%"
    else:
        sell_score = 1.0
        sell_signal = "no significant concept sell pressure"

    # Cross-rule: stock return vs. best concept return
    stock_ret_1m = None
    if price_df is not None and "close" in price_df.columns:
        close = price_df["close"].dropna()
        if len(close) >= 20:
            stock_ret_1m = float((close.iloc[-1] / close.iloc[-20] - 1) * 100)

    if stock_ret_1m is not None and best_ret >= 8:
        lag = best_ret - stock_ret_1m
        if lag >= 15:
            # Stock massively lags its hot concept — catch-up opportunity
            score = min(10.0, score + 2.0)
            signal = signal + f" (stock lags concept by {lag:.1f}% — catch-up potential)"
        elif stock_ret_1m - best_ret >= 20:
            # Stock has massively outrun its concept board — dragon-head fade risk
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + f" (stock leads concept by {stock_ret_1m - best_ret:.1f}% — dragon-head fade risk)"

    # --- Market regime cross: concept rally sustainability ---
    if market_regime_score is not None and best_ret >= 10:
        if market_regime_score <= 3:
            # Bear market concept pump: game played by short-term traders, 3-5 day window max
            score      = max(0.0, score - 2.0)
            sell_score = min(10.0, sell_score + 1.5)
            signal = signal + " (bear market — concept pump likely short-lived)"
        elif market_regime_score >= 7:
            # Bull market concept rally: institutional participation, more follow-through
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — concept rally more sustainable)"

    # --- ROE quality cross: is this a real thematic rally or pure speculation? ---
    roe_concept = None
    if financial_df is not None and not financial_df.empty:
        for key in ["净资产收益率(%)", "加权净资产收益率(%)", "ROE(%)"]:
            if key in financial_df.columns:
                vals = pd.to_numeric(financial_df[key], errors="coerce").dropna()
                if not vals.empty:
                    roe_concept = float(vals.iloc[0])
                break

    if roe_concept is not None and best_ret >= 8:
        if roe_concept >= 15:
            # Hot sector + quality business: theme rally backed by real earnings power
            score = min(10.0, score + 1.5)
            signal = signal + f" + ROE {roe_concept:.0f}% (fundamentals-backed theme — sustainable)"
        elif roe_concept < 5:
            # Hot sector + near-zero earnings: pure speculative play with no fundamental anchor
            sell_score = min(10.0, sell_score + 2.0)
            signal = signal + f" + ROE {roe_concept:.0f}% (speculative theme — no earnings support)"

    # --- Volume confirmation cross: is the concept rally backed by real trading activity? ---
    if price_df is not None and "volume" in price_df.columns and len(price_df) >= 25 and best_ret >= 8:
        try:
            vol = pd.to_numeric(price_df["volume"], errors="coerce").dropna()
            if len(vol) >= 25:
                v5  = float(vol.tail(5).mean())
                v20 = float(vol.tail(25).head(20).mean())
                if v20 > 0:
                    concept_vol_ratio = v5 / v20
                    if concept_vol_ratio >= 1.5:
                        # Hot concept + volume expansion: real money participating
                        score = min(10.0, score + 1.5)
                        signal = signal + f" + volume ×{concept_vol_ratio:.1f} (real participation confirmed)"
                    elif concept_vol_ratio < 0.7:
                        # Hot concept but volume shrinking: social/news hype without trading follow-through
                        score = max(0.0, score - 1.0)
                        sell_score = min(10.0, sell_score + 1.0)
                        signal = signal + f" + volume ×{concept_vol_ratio:.1f} (hype without real money)"
        except Exception:
            pass

    # --- Industry × concept dual momentum cross ---
    if industry_excess is not None and best_ret >= 8:
        if industry_excess >= 3:
            # Both concept board AND industry sector are hot: dual catalyst, mutual amplification
            score = min(10.0, score + 1.5)
            signal = signal + f" + industry also outperforming ({industry_excess:+.1f}%) — dual momentum"
        elif industry_excess <= -3:
            # Hot concept but the underlying industry is weak: isolated play without sector support
            sell_score = min(10.0, sell_score + 0.5)
            signal = signal + f" + industry underperforming ({industry_excess:+.1f}%) — isolated concept play"

    # --- 52w price position cross: concept rally at a low base = best setup ---
    position_cm = _get_price_position(price_df)
    if position_cm is not None and best_ret >= 8:
        if position_cm < 0.3:
            # Hot concept + stock at 52w low: concept rally has a fresh base to run
            score = min(10.0, score + 1.5)
            signal = signal + f" (low position {position_cm:.2f} — 热概念+低位，上涨空间大)"
        elif position_cm > 0.7:
            # Hot concept but stock already at highs: late-entry risk, overextended
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + f" (high position {position_cm:.2f} — 热概念+高位，注意回调风险)"

    # --- Earnings revision cross: fundamental support separates real from speculative theme plays ---
    revision_signal_cm = None
    if revision_df is not None and not revision_df.empty and best_ret >= 8:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_cm   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_cm = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_cm  = up_cm - down_cm
                if net_cm >= 2:
                    # Hot concept + analyst upgrades: fundamental backing, not pure speculation
                    score = min(10.0, score + 1.5)
                    revision_signal_cm = f"热概念+分析师上调({net_cm:+d}家) — 有业绩支撑的概念行情，可持续性强"
                elif net_cm <= -2:
                    # Hot concept + analyst downgrades: pure theme bubble, no earnings support
                    sell_score = min(10.0, sell_score + 2.0)
                    revision_signal_cm = f"热概念+分析师下调({net_cm:+d}家) — 纯主题泡沫无业绩支撑，下调确认见顶"
        except Exception:
            pass

    return {
        "score":      round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "best_concept":        best["name"],
            "best_ret_1m":         round(best_ret, 2),
            "worst_concept":       worst["name"],
            "worst_ret_1m":        round(worst_ret, 2),
            "concepts_count":      len(concept_data),
            "stock_ret_1m":        round(stock_ret_1m, 2) if stock_ret_1m is not None else None,
            "market_regime_score": market_regime_score,
            "industry_excess_pct": round(industry_excess, 2) if industry_excess is not None else None,
            "position_52w":        round(position_cm, 3) if position_cm is not None else None,
            "roe_pct":             round(roe_concept, 1) if roe_concept is not None else None,
            "revision_signal":     revision_signal_cm,
            "signal":              signal,
            "sell_signal":         sell_signal,
            "sell_score":          round(sell_score, 1),
        },
    }


