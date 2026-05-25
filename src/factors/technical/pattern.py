from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
from .._utils import _neutral, _get_price_position


def score_limit_open_rate(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """涨停开板率因子 — limit-up break (开板) rate over recent trading days.

    Detects when a stock touched the limit-up price intraday but failed to
    close there, i.e., the limit was "broken" (开板).  High open-board rate =
    heavy supply at the limit price = distribution, bearish.

    Detection (per day, using OHLC):
      prev_limit = prev_close × 1.099  (≈ +10% limit, with tolerance)
      touched    = high  ≥ prev_limit  (price reached limit intraday)
      broke      = touched AND close < prev_limit  (couldn't hold limit by close)

    Lookback windows:
      - 20-day window (medium-term signal)
      - 5-day recency window (recent sell pressure amplifier)

    Score (MAX = 10):
      touched == 0  → neutral (no limit events, factor not applicable)
      break_rate == 0  → 8 (all limits held, solid momentum)
      break_rate 0–30% → 6–8 (mostly held)
      break_rate 30–60% → 3–6 (mixed / weakening)
      break_rate > 60%  → 0–3 (heavy distribution)

    Sell score: mirrors break_rate; amplified if recent (5d) break rate is high.
    """
    MAX = 10
    if price_df is None or len(price_df) < 6:
        return _neutral(MAX)

    required_cols = {"close", "open", "high", "low"}
    if not required_cols.issubset(price_df.columns):
        return _neutral(MAX)

    try:
        df = price_df.copy()
        for col in ("close", "open", "high", "low"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close", "high"]).tail(25).reset_index(drop=True)
        if len(df) < 5:
            return _neutral(MAX)

        prev_close = df["close"].shift(1)
        limit_price = prev_close * 1.099  # ≈ +10% limit threshold

        touched = (df["high"] >= limit_price) & (prev_close.notna())
        broke   = touched & (df["close"] < limit_price)

        # 20-day window
        window20 = slice(max(0, len(df) - 20), len(df))
        n_touched20 = int(touched.iloc[window20].sum())
        n_broke20   = int(broke.iloc[window20].sum())

        # 5-day recency window
        window5 = slice(max(0, len(df) - 5), len(df))
        n_touched5 = int(touched.iloc[window5].sum())
        n_broke5   = int(broke.iloc[window5].sum())

        if n_touched20 == 0:
            # No limit-up events — factor not applicable, return neutral
            return _neutral(MAX)

        break_rate20 = n_broke20 / n_touched20  # 0.0–1.0
        break_rate5  = (n_broke5 / n_touched5) if n_touched5 > 0 else break_rate20

        # Score: low break rate = good (momentum intact); high = bad (distribution)
        score = float(np.clip((1.0 - break_rate20) * 10.0, 0.0, 10.0))

        # Sell score: break rate + recency amplifier
        base_sell = float(np.clip(break_rate20 * 10.0, 0.0, 10.0))
        recency_boost = 0.0
        if n_touched5 > 0 and break_rate5 > break_rate20 + 0.2:
            # Recent break rate significantly worse than 20d average
            recency_boost = min(2.0, (break_rate5 - break_rate20) * 5.0)
        sell_score = float(np.clip(base_sell + recency_boost, 0.0, 10.0))

        # Signal text
        if break_rate20 == 0.0:
            signal = f"all {n_touched20} limit(s) held — solid momentum, no distribution"
        elif break_rate20 < 0.3:
            signal = (
                f"low break rate {break_rate20:.0%} ({n_broke20}/{n_touched20}) "
                f"— mostly holding, minor supply"
            )
        elif break_rate20 < 0.6:
            signal = (
                f"moderate break rate {break_rate20:.0%} ({n_broke20}/{n_touched20}) "
                f"— supply pressure building"
            )
        else:
            signal = (
                f"high break rate {break_rate20:.0%} ({n_broke20}/{n_touched20}) "
                f"— distribution at limit, bearish"
            )

        if recency_boost > 0:
            signal += f" | recent 5d rate {break_rate5:.0%} — worsening"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":         signal,
                "n_touched_20d":  n_touched20,
                "n_broke_20d":    n_broke20,
                "break_rate_20d": round(break_rate20, 3),
                "n_touched_5d":   n_touched5,
                "n_broke_5d":     n_broke5,
                "break_rate_5d":  round(break_rate5, 3),
                "recency_boost":  round(recency_boost, 2),
                "sell_score":     round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_upper_shadow_reversal(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """上涨中长上影线因子 — bearish reversal signal in uptrend.

    Detects shooting-star / gravestone-doji candles that appear during a
    recent uptrend.  The pattern signals distribution: price tested higher
    intraday but sellers pushed it back down — supply is heavy at these levels.

    Upper-shadow candle criteria (per candle):
      - upper_shadow  ≥ 50% of total range   (明显上引线)
      - upper_shadow  ≥ 2× real body          (引线远超实体)
      - lower_shadow  ≤ 20% of total range    (无明显下引线, 排除十字星)
      - real body     ≥ 3% of total range     (非无量空心)
      - total_range   > 0.3% of close price   (排除无波动日)

    Uptrend context (required to generate a signal):
      - 10-day return > +3%  OR  close > MA10

    Strength bonuses (each +1 quality point, up to 4):
      - Bear body: close < open  (阴线上影 更危险)
      - Very long shadow: upper_shadow ≥ 3× body
      - Recent (within last 3 days)
      - Strong prior uptrend: 10d return > +8%  (上涨越猛, 反转越危险)

    Score (MAX = 10):
      No pattern in 10d OR no uptrend context  → neutral (4.0)
      Pattern present, quality 0–4             → sell_score 5–9
      Multiple patterns in 10d                 → sell_score +1 (capped 10)

    This is primarily a SELL signal.  Buy score is inverted (absence of
    upper shadows in uptrend = clean trend = slight buy signal).
    """
    MAX = 10
    if price_df is None or len(price_df) < 15:
        return _neutral(MAX)

    required = {"close", "open", "high", "low"}
    if not required.issubset(price_df.columns):
        return _neutral(MAX)

    try:
        df = price_df.copy().tail(25)
        for col in ("close", "open", "high", "low"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close", "open", "high", "low"]).reset_index(drop=True)
        if len(df) < 10:
            return _neutral(MAX)

        close_arr = df["close"].values

        # ── Uptrend check (using last 10 candles before the window) ────
        if len(close_arr) >= 10:
            ret_10d = (close_arr[-1] - close_arr[-10]) / max(close_arr[-10], 1e-8) * 100
        else:
            ret_10d = 0.0
        ma10 = float(pd.Series(close_arr).rolling(10).mean().iloc[-1]) if len(close_arr) >= 10 else close_arr[-1]
        in_uptrend = ret_10d > 3.0 or close_arr[-1] > ma10

        # ── Scan last 10 candles for upper-shadow pattern ──────────────
        window = df.tail(10).reset_index(drop=True)
        shadow_days: list[dict] = []

        for i in range(len(window)):
            row  = window.iloc[i]
            o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])

            body  = abs(c - o)
            upper = h - max(o, c)
            lower = min(o, c) - l
            rng   = h - l

            if rng < 1e-8 or rng < 0.003 * c:
                continue

            body_r  = body  / rng
            upper_r = upper / rng
            lower_r = lower / rng

            if (
                upper_r >= 0.50
                and upper >= 2.0 * max(body, 1e-8)
                and lower_r <= 0.20
                and body_r >= 0.03
            ):
                quality = 0
                notes   = []

                if c < o:
                    quality += 1
                    notes.append("阴线上影")
                if upper >= 3.0 * max(body, 1e-8):
                    quality += 1
                    notes.append(f"超长上影({upper_r:.0%})")
                if i >= len(window) - 3:
                    quality += 1
                    notes.append("近期出现")
                if ret_10d > 8.0:
                    quality += 1
                    notes.append(f"强势上涨后({ret_10d:+.1f}%)")

                shadow_days.append({
                    "day_index": i,
                    "quality":   quality,
                    "upper_r":   upper_r,
                    "notes":     notes,
                })

        # ── Scoring ────────────────────────────────────────────────────
        if not shadow_days or not in_uptrend:
            # No pattern or not in uptrend → no sell signal
            # Clean uptrend without upper shadows is slightly bullish
            if in_uptrend and not shadow_days:
                score      = 6.0  # clean uptrend, no distribution candles
                sell_score = 0.0
                signal     = f"clean uptrend ({ret_10d:+.1f}% 10d) — no upper-shadow distribution"
            else:
                score      = float(MAX) * 0.4   # neutral
                sell_score = float(MAX) * 0.2
                signal     = "no upper-shadow pattern in uptrend — neutral"
            return {
                "score":      round(score, 1),
                "sell_score": round(sell_score, 1),
                "max":        MAX,
                "details": {
                    "signal":        signal,
                    "in_uptrend":    in_uptrend,
                    "ret_10d":       round(ret_10d, 2),
                    "shadow_count":  0,
                    "sell_score":    round(sell_score, 1),
                },
            }

        best    = max(shadow_days, key=lambda x: x["quality"])
        quality = min(best["quality"], 4)

        sell_score = float(np.clip(5.0 + quality, 0.0, 9.0))
        if len(shadow_days) >= 2:
            sell_score = min(sell_score + 1.0, float(MAX))

        # Buy score inversely reflects distribution risk
        score = float(np.clip(float(MAX) - sell_score, 0.0, float(MAX)))

        notes_str  = " + ".join(best["notes"]) if best["notes"] else "basic upper shadow"
        multi_note = f" ({len(shadow_days)} patterns in 10d)" if len(shadow_days) >= 2 else ""
        signal = (
            f"上涨中长上影线{multi_note}: {notes_str}, "
            f"shadow={best['upper_r']:.0%}, trend={ret_10d:+.1f}%"
        )

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":        signal,
                "in_uptrend":    in_uptrend,
                "ret_10d":       round(ret_10d, 2),
                "shadow_count":  len(shadow_days),
                "best_quality":  quality,
                "best_upper_r":  round(best["upper_r"], 3),
                "best_notes":    best["notes"],
                "sell_score":    round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_limit_hits(
    price_df: Optional[pd.DataFrame],
    financial_df: Optional[pd.DataFrame] = None,
    social_dict: Optional[dict] = None,
    best_concept_ret: Optional[float] = None,
    market_regime_score: Optional[float] = None,
    industry_ret_1m: Optional[float] = None,
    market_ret_1m: Optional[float] = None,
    revision_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Limit-up / limit-down frequency in last 20 trading days (max 10).
    A-share limit = ±10% (±9.9% threshold used to be safe).

    Crossed with 52w position:
      Net limit-ups + low position  -> momentum breakout from base (strong buy)
      Net limit-ups + high position -> overheated at top (sell warning)
      Net limit-downs + low position -> panic capitulation (potential reversal)
      Net limit-downs + high position -> distribution selling (strong sell)

    Fundamental quality cross: hot money vs genuine momentum (requires financial_df)
      Net limit-ups (>= 2) + ROE >= 12% -> earnings-backed acceleration, buy +1.5
      Net limit-ups (>= 2) + ROE < 5%   -> pure hot money/speculation, sell +2

    Social heat cross: A-share pump detection (requires social_dict)
      Net limit-ups (>= 2) + extreme heat (rank_pct <= 5%)  -> 炒作顶部三件套, sell +2
      Net limit-ups (>= 2) + low heat (rank_pct > 50%)      -> institutional-driven, buy +1.5
      Net limit-downs (>= 2) + extreme heat at lows         -> retail panic = contrarian bottom, sell -1

    Concept momentum cross (requires best_concept_ret):
      Net limit-ups (>= 2) + hot concept (>= +8%)  -> 板块联动连板，持续性强 -> buy +1.5
      Net limit-ups (>= 2) + concept cold (< 0%)   -> 孤立炒作，无板块支撑 -> sell +0.5

    Market regime cross (requires market_regime_score):
      Net limit-ups (>= 1) + bull market (regime >= 7) -> 牛市连板延续性强 -> buy +1.5
      Net limit-ups (>= 1) + bear market (regime <= 3) -> 熊市连板难以持续 -> buy -1, sell +1

    Industry excess return cross (requires industry_ret_1m and market_ret_1m):
      Net limit-ups (>= 1) + industry outperforming (excess >= +3%) -> buy +1 (行业顺风中连板，有逻辑支撑，可持续性强)
      Net limit-ups (>= 1) + industry weak (excess <= -3%) -> sell +0.5 (独立于行业的纯题材炒作，回撤风险高)
    """
    from .._utils import _extract
    if price_df is None or len(price_df) < 5 or "change_pct" not in price_df.columns:
        return _neutral(10)

    try:
        chg = pd.to_numeric(price_df["change_pct"], errors="coerce").dropna().tail(20)
        up_count   = int((chg >= 9.9).sum())
        down_count = int((chg <= -9.9).sum())
        net = up_count - down_count
    except Exception:
        return _neutral(10)

    # Base score from net limit-up count
    if net >= 3:
        score = 9.0
        signal = "frequent limit-ups (strong momentum)"
    elif net >= 1:
        score = 7.0
        signal = "net limit-ups"
    elif net == 0 and up_count == 0 and down_count == 0:
        score = 5.0  # no limit events = neutral
        signal = "no limit events"
    elif net == 0:
        score = 4.0
        signal = "balanced limit events"
    elif net == -1:
        score = 3.0
        signal = "slight net limit-downs"
    else:
        score = 1.0
        signal = "frequent limit-downs"

    # Base sell score
    if net <= -3:
        sell_score = 9.0
    elif net <= -1:
        sell_score = 6.0
    elif net == 0 and down_count >= 2:
        sell_score = 4.0
    else:
        sell_score = 0.0

    # Context cross with 52w position
    position = _get_price_position(price_df)
    if position is not None:
        if net >= 2:
            if position < 0.3:
                # Limit-up breakout from base = genuine momentum
                score = min(10.0, score + 1.0)
                signal = "limit-up breakout from base (strong buy)"
            elif position > 0.7:
                # Frequent limit-ups at top = overheated retail frenzy
                sell_score = min(10.0, sell_score + 3.0)
                signal = "overheated at high position (limit-up frenzy)"
        elif net <= -2:
            if position < 0.3:
                # Limit-downs at bottom = panic selling = potential reversal
                score = min(10.0, score + 3.0)  # contrarian: panic = buy
                sell_score = max(0.0, sell_score - 2.0)
                signal = "panic selling at lows (potential reversal)"
            elif position > 0.7:
                # Limit-downs from high = institutional distribution
                sell_score = min(10.0, sell_score + 2.0)
                signal = "distribution selling from highs (strong sell)"

    # --- Fundamental quality cross: genuine momentum vs hot money ---
    if financial_df is not None and net >= 2:
        roe = _extract(financial_df, ["净资产收益率(%)", "加权净资产收益率(%)", "ROE(%)"])
        if roe is not None:
            if roe >= 12:
                # Limit-ups backed by solid earnings: sustainable acceleration
                score = min(10.0, score + 1.5)
                signal = signal + " + strong ROE (genuine momentum)"
            elif roe < 5:
                # Limit-ups with near-zero returns: purely speculative hot money
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + " + weak ROE (hot money, no fundamentals)"

    # --- Social heat cross: A-share pump detection ---
    if social_dict is not None and "rank_pct" in social_dict:
        rank_pct = float(social_dict["rank_pct"])
        if net >= 2:
            if rank_pct <= 5:
                # Consecutive limit-ups + trending on social media: the classic A-share pump trio
                # (游资 + retail FOMO + hot-search = top signal)
                sell_score = min(10.0, sell_score + 2.0)
                signal = signal + " + extreme social heat (pump pattern — 炒作顶部)"
            elif rank_pct > 50:
                # Limit-ups with no social buzz: institutions quietly driving price, more sustainable
                score = min(10.0, score + 1.5)
                signal = signal + " + low social heat (institutional-driven, sustainable)"
        elif net <= -2 and rank_pct <= 10 and position is not None and position < 0.3:
            # Panic limit-downs at lows with extreme social heat: retail capitulation = bottom
            sell_score = max(0.0, sell_score - 1.0)
            signal = signal + " + extreme heat at lows (retail panic = contrarian bottom)"

    # --- Concept momentum cross: 连板 with or without sector support ---
    if best_concept_ret is not None and net >= 2:
        if best_concept_ret >= 8:
            # 连板 + hot concept board: 板块联动, strong persistence (full sector rotation)
            score = min(10.0, score + 1.5)
            signal = signal + f" + concept +{best_concept_ret:.1f}% (板块联动连板 — 持续性强)"
        elif best_concept_ret < 0:
            # 连板 but concept board falling: isolated speculation, quick fade
            sell_score = min(10.0, sell_score + 0.5)
            signal = signal + f" + concept {best_concept_ret:.1f}% (孤立炒作 — 无板块支撑)"

    # --- Market regime cross: limit-up persistence is regime-dependent ---
    if market_regime_score is not None and net >= 1:
        if market_regime_score >= 7:
            # Bull market: consecutive limit-ups have follow-through as institutional momentum persists
            score = min(10.0, score + 1.5)
            signal = signal + " (bull market — 牛市连板延续性强)"
        elif market_regime_score <= 3:
            # Bear market: limit-up rallies are short-lived, frequently reverse
            score = max(0.0, score - 1.0)
            sell_score = min(10.0, sell_score + 1.0)
            signal = signal + " (bear market — 熊市连板难持续，警惕反转)"

    # --- Industry excess return cross: sector context determines 连板 sustainability ---
    industry_signal = None
    if industry_ret_1m is not None and market_ret_1m is not None and net >= 1:
        excess = industry_ret_1m - market_ret_1m
        if excess >= 3.0:
            # Limit-ups with sector tailwind: fundamental logic supports continuation
            score = min(10.0, score + 1.0)
            industry_signal = f"连板+行业强(超额{excess:.1f}%) — 行业逻辑支撑，可持续性强"
        elif excess <= -3.0:
            # Limit-ups against a weak sector: pure speculative play, high reversal risk
            sell_score = min(10.0, sell_score + 0.5)
            industry_signal = f"连板+行业弱(超额{excess:.1f}%) — 纯题材炒作，脱离行业，回撤风险高"

    # --- Earnings revision cross: news-driven events need fundamental validation ---
    revision_signal_lh = None
    if revision_df is not None and not revision_df.empty:
        try:
            rating_cols = [c for c in revision_df.columns
                           if any(k in c for k in ["评级", "rating", "建议", "recommendation"])]
            if rating_cols:
                col_str = revision_df[rating_cols[0]].astype(str).str.lower()
                up_lh   = int(col_str.str.contains("上调|upgrade|buy|strong buy").sum())
                down_lh = int(col_str.str.contains("下调|downgrade|sell|reduce").sum())
                net_lh  = up_lh - down_lh
                if net >= 2 and net_lh >= 2:
                    # Multiple limit-ups + analyst upgrades: fundamental news driving the event
                    score = min(10.0, score + 2.0)
                    revision_signal_lh = f"连板+分析师上调({net_lh:+d}家) — 基本面新闻驱动，非游资炒作，可持续性强"
                elif net <= -2 and net_lh <= -2:
                    # Multiple limit-downs + analyst downgrades: don't catch the knife
                    sell_score = min(10.0, sell_score + 2.0)
                    revision_signal_lh = f"连跌停+分析师下调({net_lh:+d}家) — 业绩暴雷确认，不要接刀"
        except Exception:
            pass

    return {
        "score": round(min(10.0, score), 1),
        "sell_score": round(min(10.0, sell_score), 1),
        "max": 10,
        "details": {
            "limit_up_count_20d":   up_count,
            "limit_down_count_20d": down_count,
            "net_limit_up":         net,
            "position_52w":         round(position, 3) if position is not None else None,
            "best_concept_ret":     round(best_concept_ret, 2) if best_concept_ret is not None else None,
            "market_regime_score":  market_regime_score,
            "industry_signal":      industry_signal,
            "revision_signal":      revision_signal_lh,
            "signal":               signal,
            "sell_score":           round(min(10.0, sell_score), 1),
        },
    }


def score_gap_frequency(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """跳空频率 — fraction of significant overnight gaps in past 20 days (inverted).

    Measures how often the stock gaps significantly at open vs prior close.
    A significant gap is defined as |open - prev_close| / prev_close > 0.5%.

    High gap frequency = news-driven, unpredictable, high tail risk.
    Low gap frequency = steady, predictable price action (institutional flow).

    Distinct from ATR (which includes intraday range) — gaps capture
    *overnight* risk specifically. Stocks that frequently gap are harder
    to hold and tend to underperform on risk-adjusted basis.

    Score is *inverted*: high gap frequency → low score.
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)

    open_col = None
    for c in ["open", "开盘", "open_price"]:
        if c in price_df.columns:
            open_col = c
            break
    if open_col is None or "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        opn   = pd.to_numeric(price_df[open_col], errors="coerce")

        df = pd.DataFrame({"close": close, "open": opn}).dropna().tail(21)
        if len(df) < 10:
            return _neutral(MAX)

        prev_close = df["close"].shift(1).dropna()
        curr_open  = df["open"].iloc[1:].reset_index(drop=True)
        prev_close = prev_close.reset_index(drop=True)

        gap_ratio = ((curr_open - prev_close) / prev_close).abs()
        gap_freq  = float((gap_ratio > 0.005).mean())  # >0.5% = significant gap

        # Inverted score: score = (1 - gap_freq) * 10, clipped [0, 10]
        # gap_freq=0→10, gap_freq=0.5→5, gap_freq=1.0→0
        score      = float(np.clip((1.0 - gap_freq) * 10.0, 0.0, 10.0))
        sell_score = float(np.clip(gap_freq * 10.0, 0.0, 10.0))

        pct = gap_freq * 100
        if gap_freq <= 0.1:
            signal = f"very low gap frequency ({pct:.0f}%) — stable, predictable"
        elif gap_freq <= 0.25:
            signal = f"low gap frequency ({pct:.0f}%) — mostly steady"
        elif gap_freq <= 0.5:
            signal = f"moderate gaps ({pct:.0f}%) — some news sensitivity"
        elif gap_freq <= 0.7:
            signal = f"high gap frequency ({pct:.0f}%) — news-driven, hard to hold"
        else:
            signal = f"very high gap frequency ({pct:.0f}%) — extreme tail risk"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":    signal,
                "gap_freq":  round(gap_freq, 3),
                "gap_pct":   round(pct, 1),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_price_efficiency(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """价格效率因子 (Kaufman效率比率) — directional efficiency of price movement.

    Kaufman Efficiency Ratio (ER) = |net_price_change| / sum(|daily_changes|)
    over a rolling 20-day window.

    ER = 1.0: price moved perfectly directionally (straight line).
    ER -> 0:  price is fully random / choppy (path cancels out).

    High ER: clean trending move (institutional accumulation).
    Low ER: noisy / whipsawing (retail-dominated or indecisive).

    Score: ER in [0, 1] -> [0, 10].
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)
    if "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce").dropna().tail(21)
        if len(close) < 10:
            return _neutral(MAX)

        daily_changes = close.diff().dropna().abs()
        net_change    = abs(float(close.iloc[-1] - close.iloc[0]))
        total_path    = float(daily_changes.sum())

        if total_path < 1e-8:
            return _neutral(MAX)

        er = net_change / total_path  # Kaufman ER in [0, 1]

        score      = float(np.clip(er * 10.0, 0.0, 10.0))
        sell_score = float(np.clip((1.0 - er) * 10.0, 0.0, 10.0))

        if er >= 0.7:
            signal = f"very efficient trend (ER={er:.2f}) — clean directional move"
        elif er >= 0.5:
            signal = f"efficient (ER={er:.2f}) — mostly directional"
        elif er >= 0.3:
            signal = f"moderate efficiency (ER={er:.2f}) — some noise"
        elif er >= 0.15:
            signal = f"low efficiency (ER={er:.2f}) — choppy/sideways"
        else:
            signal = f"very low efficiency (ER={er:.2f}) — random / indecisive"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":     signal,
                "er":         round(er, 3),
                "net_change": round(net_change, 3),
                "total_path": round(total_path, 3),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_hammer_bottom(
    price_df: Optional[pd.DataFrame],
) -> dict:
    return {"score": 0, "sell_score": 0, "max": 10, "details": {}}


def score_intraday_vs_overnight(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """日内vs隔夜收益分拆因子 — institutional (intraday) vs retail (overnight) signal.

    Decomposes total return into:
      - Intraday:   (close - open) / open       -> institutional activity proxy
      - Overnight:  (open - prev_close) / prev_close -> retail/news reaction proxy

    Net signal = avg_intraday - avg_overnight over 20 days.

    Positive net: institutions buying intraday while retail gaps fade -> bullish.
    Negative net: retail gaps up then institutions distribute -> bearish distribution.

    Score: net in [-1.5%, +1.5%] -> [0, 10]; center 0% -> 5.
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)

    open_col = None
    for c in ["open", "开盘", "open_price"]:
        if c in price_df.columns:
            open_col = c
            break
    if open_col is None or "close" not in price_df.columns:
        return _neutral(MAX)

    try:
        close = pd.to_numeric(price_df["close"], errors="coerce")
        opn   = pd.to_numeric(price_df[open_col], errors="coerce")

        df = pd.DataFrame({"close": close, "open": opn}).dropna().tail(21)
        if len(df) < 10:
            return _neutral(MAX)

        prev_close = df["close"].shift(1).dropna()
        curr_open  = df["open"].iloc[1:].reset_index(drop=True)
        curr_close = df["close"].iloc[1:].reset_index(drop=True)
        prev_close = prev_close.reset_index(drop=True)

        intraday_ret  = (curr_close - curr_open) / curr_open.replace(0, np.nan)
        overnight_ret = (curr_open - prev_close) / prev_close.replace(0, np.nan)

        avg_intraday  = float(intraday_ret.dropna().mean()) * 100   # pct
        avg_overnight = float(overnight_ret.dropna().mean()) * 100  # pct
        net           = avg_intraday - avg_overnight  # pct

        # Map net in [-1.5%, +1.5%] -> [0, 10]
        score      = float(np.clip((net + 1.5) / 3.0 * 10.0, 0.0, 10.0))
        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        if net >= 0.5:
            signal = f"institutional accumulation (net={net:+.2f}%): intraday buying > overnight gap"
        elif net >= 0.1:
            signal = f"mild institutional bias (net={net:+.2f}%)"
        elif net >= -0.1:
            signal = f"balanced intraday/overnight (net={net:+.2f}%) — neutral"
        elif net >= -0.5:
            signal = f"mild distribution signal (net={net:+.2f}%)"
        else:
            signal = f"distribution pattern (net={net:+.2f}%): retail gaps, institutions sell"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":        signal,
                "avg_intraday":  round(avg_intraday, 3),
                "avg_overnight": round(avg_overnight, 3),
                "net":           round(net, 3),
                "sell_score":    round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)
