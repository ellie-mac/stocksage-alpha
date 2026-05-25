from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
from .._utils import _neutral


def score_turnover_percentile(
    price_df: Optional[pd.DataFrame],
    market_regime_score: Optional[float] = None,
) -> dict:
    """
    Turnover rate vs 90-day rolling average (max 10).
    Rewards moderate elevated turnover (accumulation zone).
      ratio 1.5–3.0x -> 8-10 pts (sweet spot)
      ratio 1.0–1.5x ->  5-8 pts
      ratio < 0.8x   ->  2 pts (cold)
      ratio >= 4.0x  ->  5 pts (climax caution)

    Market regime cross (requires market_regime_score):
      Bull market (regime >= 7) + high turnover + price up -> broad participation confirmed -> buy +1
      Bear market (regime <= 3) + high turnover + price down -> panic / distribution amplified -> sell +1.5

    52w position cross (uses price_df):
      High turnover (>= 1.5x) + low position (< 0.3) + price up -> buy +1.5 (低位放量=底部确认承接)
      High turnover (>= 1.5x) + high position (> 0.7) + price down -> sell +1.5 (高位放量下跌=顶部分发确认)
      Low turnover (< 0.8x) + high position (> 0.7) + price up -> sell +1 (高位缩量上涨=上涨乏力)
    """
    if price_df is None or len(price_df) < 10 or "turnover" not in price_df.columns:
        return _neutral(10)

    turnover = pd.to_numeric(price_df["turnover"], errors="coerce").dropna()
    if len(turnover) < 10:
        return _neutral(10)

    current_5d = float(turnover.tail(5).mean())
    avg_90d = float(turnover.tail(90).mean()) if len(turnover) >= 20 else float(turnover.mean())

    if avg_90d <= 0:
        return _neutral(10)

    ratio = current_5d / avg_90d

    # Get today's price direction
    last = price_df.iloc[-1]
    today_chg = float(last.get("change_pct", 0) or 0) if "change_pct" in price_df.columns else 0.0

    # --- Buy score: cross turnover ratio with price direction ---
    if ratio >= 4.0:
        if today_chg <= -2.0:
            score = 3.0   # high turnover + big drop = distribution, not a buy
            signal = "climax selloff (distribution)"
        else:
            score = 5.0   # keep climax caution for neutral/up days
            signal = "climax volume (caution)"
    elif ratio >= 3.0:
        if today_chg >= 1.0:
            score = 10.0
            signal = "strong accumulation confirmed"
        elif today_chg <= -2.0:
            score = 4.0
            signal = "high turnover selloff (caution)"
        else:
            score = 8.0
            signal = "strong accumulation"
    elif ratio >= 1.5:
        if today_chg >= 0.5:
            score = 8.0 + (ratio - 1.5) / 1.5 * 2.0
            score = min(10.0, score)
            signal = "active (price up)"
        elif today_chg <= -2.0:
            score = 4.0
            signal = "active volume but declining"
        else:
            score = 8.0 + (ratio - 1.5) / 1.5 * 2.0
            score = min(10.0, score)
            signal = "active"
    elif ratio >= 1.0:
        score = 5.0 + (ratio - 1.0) / 0.5 * 3.0
        signal = "slightly above average"
    elif ratio >= 0.8:
        if today_chg <= -0.5:
            score = 5.5   # 缩量下跌 — mild positive
            signal = "low volume decline (selling exhausted)"
        else:
            score = 5.0
            signal = "normal"
    else:
        if today_chg <= -0.5:
            score = 4.5   # very low volume decline — possible bottom
            signal = "very low volume decline (possible bottoming)"
        elif today_chg >= 0.5:
            score = 1.5   # 缩量上涨 — weak
            signal = "low volume rally (weak)"
        else:
            score = ratio / 0.8 * 2.0
            signal = "cold"

    # --- Sell score: cross turnover ratio with price direction ---
    if ratio >= 3.0 and today_chg <= -2.0:
        sell_score = 9.0   # high turnover + big drop = distribution
    elif ratio >= 1.5 and today_chg <= -2.0:
        sell_score = 6.0
    elif ratio < 0.8 and today_chg >= 1.0:
        sell_score = 5.0   # 缩量上涨
    elif ratio >= 4.0:
        sell_score = 7.0   # climax even without price context
    elif ratio >= 3.0:
        sell_score = 4.0
    else:
        sell_score = 0.0

    # --- 1m return cross: quiet accumulation vs active distribution ---
    ret_1m = None
    if len(price_df) >= 20 and "close" in price_df.columns:
        closes = pd.to_numeric(price_df["close"], errors="coerce").dropna()
        if len(closes) >= 20:
            ret_1m = float((closes.iloc[-1] - closes.iloc[-20]) / closes.iloc[-20] * 100)

    if ret_1m is not None:
        if ratio < 0.7 and 3.0 <= ret_1m <= 15.0:
            # Low volume + gradual price rise: retail exhaustion done, slow accumulation underway
            score = min(10.0, score + 1.5)
            signal = signal + " (quiet accumulation: price rising on shrinking volume)"
        elif ratio >= 2.0 and ret_1m <= -5.0:
            # High volume + sustained 1m decline: active distribution by holders
            sell_score = min(9.0, sell_score + 1.5)
            signal = signal + " (active distribution: sustained decline with elevated volume)"

    # --- Market regime cross: high turnover meaning depends on market environment ---
    if market_regime_score is not None:
        if market_regime_score >= 7 and ratio >= 1.5 and today_chg >= 0.5:
            # Bull market + high turnover + price up: broad participation, trend continuation
            score = min(10.0, score + 1.0)
            signal = signal + " (bull market — broad participation confirmed)"
        elif market_regime_score <= 3 and ratio >= 1.5:
            # Bear market + elevated turnover: more likely distribution than accumulation
            sell_score = min(9.0, sell_score + 1.0)
            signal = signal + " (bear market — high turnover likely distribution)"

    # --- 52w position cross: price level determines what high/low turnover means ---
    position_signal = None
    if len(price_df) >= 252 and "close" in price_df.columns:
        try:
            window = price_df["close"].tail(260)
            hi = float(window.max()); lo = float(window.min()); cur = float(window.iloc[-1])
            if hi > lo:
                pos = (cur - lo) / (hi - lo)
                if ratio >= 1.5 and pos < 0.3 and today_chg >= 0.5:
                    # High volume + low price + rising: institutional bottom accumulation confirmed
                    score = min(10.0, score + 1.5)
                    position_signal = f"高换手+低位({pos:.2f})+上涨 — 底部放量承接，买入确认"
                elif ratio >= 1.5 and pos > 0.7 and today_chg <= -1.0:
                    # High volume + high price + falling: top distribution confirmed
                    sell_score = min(9.0, sell_score + 1.5)
                    position_signal = f"高换手+高位({pos:.2f})+下跌 — 顶部分发，卖出确认"
                elif ratio < 0.8 and pos > 0.7 and today_chg >= 0.5:
                    # Low volume + high price + rising: unsustainable, no one is buying
                    sell_score = min(9.0, sell_score + 1.0)
                    position_signal = f"低换手+高位({pos:.2f})+上涨 — 高位缩量，上涨乏力"
        except Exception:
            pass

    return {
        "score": round(score, 1),
        "sell_score": round(sell_score, 1),
        "max": 10,
        "details": {
            "current_turnover_5d": round(current_5d, 2),
            "avg_90d_turnover": round(avg_90d, 2),
            "ratio": round(ratio, 2),
            "ret_1m": round(ret_1m, 1) if ret_1m is not None else None,
            "market_regime_score": market_regime_score,
            "position_signal": position_signal,
            "signal": signal,
            "sell_score": round(sell_score, 1),
        },
    }


def score_turnover_acceleration(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """换手率加速度 — whether market attention/accumulation is accelerating.

    Logic:
      acceleration = avg_turnover(last 5d) / avg_turnover(last 20d)
      A rising ratio means participation is increasing relative to the recent
      baseline, which in A-shares often signals institutional accumulation or
      fresh retail interest.

      Combined with 5-day price direction for confirmation:
        High acceleration + price rising  → strong bullish (放量上涨)
        High acceleration + price falling → strong bearish (放量下跌 / distribution)
        Low acceleration  (turnover drying up) → slightly bearish

    Distinct from score_volume_breakout (absolute volume vs 20d MA) and
    score_turnover_percentile (level vs 90d history):
    this factor captures the *rate of change* in turnover rate (换手率 %).
    """
    MAX = 10
    if price_df is None or len(price_df) < 22:
        return _neutral(MAX)

    turn_col = next((c for c in ["turnover", "turnover_rate"] if c in price_df.columns), None)
    if turn_col is None:
        return _neutral(MAX)

    try:
        turnover = pd.to_numeric(price_df[turn_col], errors="coerce").dropna()
        close    = pd.to_numeric(price_df["close"],   errors="coerce").dropna()
        if len(turnover) < 22 or len(close) < 6:
            return _neutral(MAX)

        avg_5d  = float(turnover.tail(5).mean())
        avg_20d = float(turnover.tail(20).mean())
        if avg_20d < 1e-10:
            return _neutral(MAX)

        accel = avg_5d / avg_20d           # >1 = accelerating, <1 = decelerating

        # 5-day price direction: +1 rising, -1 falling
        ret_5d = float((close.iloc[-1] / close.iloc[-6] - 1) * 100) if len(close) >= 6 else 0.0
        price_dir = 1 if ret_5d > 0.5 else (-1 if ret_5d < -0.5 else 0)

        # Base score from acceleration (neutral=5 at accel=1)
        base = float(np.clip(5.0 + (accel - 1.0) * 4.0, 1.0, 9.0))

        # Direction modifier: amplify signal if acceleration + direction agree
        if price_dir == 1 and accel >= 1.3:
            score = float(np.clip(base + 1.5, 0.0, 10.0))
            signal = f"放量上涨 accel={accel:.2f}x ret5d={ret_5d:+.1f}%"
        elif price_dir == -1 and accel >= 1.3:
            score = float(np.clip(base - 2.0, 0.0, 10.0))
            signal = f"放量下跌(分发) accel={accel:.2f}x ret5d={ret_5d:+.1f}%"
        elif accel < 0.7:
            score = float(np.clip(base - 0.5, 0.0, 10.0))
            signal = f"缩量 accel={accel:.2f}x"
        else:
            score = base
            signal = f"换手正常 accel={accel:.2f}x"

        sell_score = float(np.clip(10.0 - score, 0.0, 10.0))

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":       signal,
                "accel_5d_20d": round(accel, 3),
                "ret_5d_pct":   round(ret_5d, 2),
                "sell_score":   round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_obv_trend(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """OBV趋势因子 — On Balance Volume slope as directional accumulation signal.

    OBV[t] = OBV[t-1] + vol if close rises, - vol if close falls.
    Linear regression slope of OBV over last 20 days, normalised by avg volume.

    Positive slope = net accumulation = bullish.
    Distinct from volume_breakout (spike) and main_inflow (order-size based):
    captures *directional* volume persistence without large-order classification.

    Scoring:
      slope_norm ≥ +0.15 → score 8–10  (strong accumulation)
      slope_norm ~ 0     → score 5
      slope_norm ≤ -0.15 → score 0–2   (strong distribution)
    """
    MAX = 10
    if price_df is None or len(price_df) < 25:
        return _neutral(MAX)

    if not {"close", "volume"}.issubset(price_df.columns):
        return _neutral(MAX)

    try:
        close  = pd.to_numeric(price_df["close"],  errors="coerce").ffill()
        volume = pd.to_numeric(price_df["volume"], errors="coerce").fillna(0)

        if len(close) < 25:
            return _neutral(MAX)

        direction = np.sign(close.diff().fillna(0))
        obv       = (direction * volume).cumsum()

        N         = 20
        obv_slice = obv.tail(N).values
        vol_avg   = float(volume.tail(N).mean())
        if vol_avg < 1:
            return _neutral(MAX)

        x       = np.arange(N, dtype=float)
        x_c     = x - x.mean()
        slope   = float(np.dot(x_c, obv_slice) / np.dot(x_c, x_c))
        slope_norm = slope / vol_avg

        score      = float(np.clip(5.0 + slope_norm * 33.0, 0.0, 10.0))
        sell_score = float(np.clip(5.0 - slope_norm * 33.0, 0.0, 10.0))

        if slope_norm >= 0.15:
            signal = f"OBV强势积累 slope={slope_norm:+.3f}"
        elif slope_norm >= 0.03:
            signal = f"OBV温和积累 slope={slope_norm:+.3f}"
        elif slope_norm >= -0.03:
            signal = f"OBV中性 slope={slope_norm:+.3f}"
        elif slope_norm >= -0.15:
            signal = f"OBV温和分发 slope={slope_norm:+.3f}"
        else:
            signal = f"OBV强势分发 slope={slope_norm:+.3f}"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":      signal,
                "slope_norm":  round(slope_norm, 4),
                "vol_avg_20d": round(vol_avg, 0),
                "sell_score":  round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_volume_expansion(
    price_df: Optional[pd.DataFrame],
) -> dict:
    """成交量扩张因子 — volume trend as accumulation/distribution signal.

    Ratio of recent 10-day average volume to 60-day average volume.
    Rising volume alongside price = institutional accumulation (bullish).
    Shrinking volume = distribution or loss of interest.

    Unlike `volume` (absolute turnover level), this captures directional change.
    Especially powerful in bull markets combined with price momentum.
    """
    MAX = 10
    if price_df is None or len(price_df) < 65:
        return _neutral(MAX)

    vol_col = None
    for c in ["volume", "成交量", "vol", "turnover", "换手率", "turnover_rate"]:
        if c in price_df.columns:
            vol_col = c
            break
    if vol_col is None:
        return _neutral(MAX)

    try:
        vol = pd.to_numeric(price_df[vol_col], errors="coerce").dropna()
        if len(vol) < 65:
            return _neutral(MAX)

        avg_10 = float(vol.tail(10).mean())
        avg_60 = float(vol.tail(60).mean())
        if avg_60 <= 0:
            return _neutral(MAX)

        ratio = avg_10 / avg_60

        # score = (ratio - 1.0) * 6.67 + 5, clipped [0, 10]
        # ratio=0.25→0, ratio=1.0→5, ratio=1.75→10
        score      = float(np.clip((ratio - 1.0) * 6.67 + 5.0, 0.0, 10.0))
        sell_score = float(np.clip((1.0 - ratio) * 6.67 + 5.0, 0.0, 10.0))

        if ratio >= 1.8:
            signal = f"volume surging ({ratio:.2f}×) — strong accumulation"
        elif ratio >= 1.3:
            signal = f"volume expanding ({ratio:.2f}×) — buying interest growing"
        elif ratio >= 0.8:
            signal = f"volume flat ({ratio:.2f}×) — neutral"
        elif ratio >= 0.5:
            signal = f"volume contracting ({ratio:.2f}×) — interest fading"
        else:
            signal = f"volume drying up ({ratio:.2f}×) — distribution"

        return {
            "score":      round(score, 1),
            "sell_score": round(sell_score, 1),
            "max":        MAX,
            "details": {
                "signal":    signal,
                "vol_ratio": round(ratio, 3),
                "avg_10d":   round(avg_10, 0),
                "avg_60d":   round(avg_60, 0),
                "sell_score": round(sell_score, 1),
            },
        }

    except Exception:
        return _neutral(MAX)


def score_price_volume_corr(
    price_df: Optional[pd.DataFrame],
) -> dict:
    return {"score": 0, "sell_score": 0, "max": 10, "details": {}}
