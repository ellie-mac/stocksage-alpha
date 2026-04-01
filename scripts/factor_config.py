"""
Factor configuration — IC-based weights, excluded factor registry,
and regime-adaptive weight sets.

Based on rolling 6-period IC analysis (20d forward, Group A, 50 stocks, 2026-04-01).
Re-run factor_analysis.py --rolling 6 periodically to refresh.

To re-activate an excluded factor: move it from EXCLUDED_FACTORS back to FACTOR_WEIGHTS.

Regime logic (CSI 300 MA signal):
  NORMAL  — price > MA20  : full IC-optimised weights, 100% exposure
  CAUTION — price < MA20  : shift to defensive, 60% exposure
  CRISIS  — price < MA60  : defensive anchors only, 30% exposure
"""

# ---------------------------------------------------------------------------
# NORMAL regime — full IC-calibrated weights (price > MA20).
# Used as the default / fallback.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS: dict[str, float] = {
    # ── Tier 1: IC ≥ 0.10, ICIR ≥ 0.55 (full 2× weight) ────────────
    "low_volatility":      2.0,   # IC=+0.234, ICIR=0.560
    "idiosyncratic_vol":   2.0,   # IC=+0.229, ICIR=0.578 — residual vol; A股彩票效应反转
    "cash_flow_quality":   2.0,   # IC=+0.164, ICIR=0.894 — earnings backed by cash
    "momentum_concavity":  2.0,   # IC=+0.135, ICIR=0.566 — momentum acceleration
    "divergence":          2.0,   # IC=+0.130, ICIR=0.810 — multi-indicator confluence
    "price_inertia":       2.0,   # IC=+0.107, ICIR=0.787 — most stable momentum signal
    "asset_growth":        2.0,   # IC=+0.109, ICIR=0.585
    "atr_normalized":      2.0,   # IC=+0.249, ICIR=0.802 — low ATR = low realised risk; co-linear w/ low_vol but additive
    "gap_frequency":       2.0,   # IC=+0.250, ICIR=0.717 — low overnight gap frequency = stable, predictable; inverted in score

    # ── Tier 2: IC ≥ 0.05 (1× weight) ───────────────────────────────
    "volume":              1.0,   # IC=+0.090, ICIR=0.432
    "piotroski":           1.0,   # IC=+0.067, ICIR=1.006 — highest ICIR of all
    "ma60_deviation":      1.0,   # IC=+0.098, ICIR=0.668 — mean-reversion: stocks below MA60 outperform
    "nearness_to_high":    0.5,   # IC=+0.106, ICIR=0.378 — proximity to 20d high, breakout momentum
    "main_inflow":         0.5,   # IC=+0.060, ICIR=0.239 — institutional flow, low ICIR
    "bb_squeeze":          0.5,   # IC=+0.064, ICIR=0.399 — volatility squeeze signal
    "roe_trend":           0.5,   # IC=+0.053, ICIR=0.362 — ROE direction

    # ── Tier 3: Weak-positive ─────────────────────────────────────────
    "quality":             0.5,   # IC=+0.025, ICIR=0.232

    # ── Inverted (IC < 0, contrarian) ────────────────────────────────
    "growth":                -0.5,   # IC=-0.073, ICIR=-0.721 — A-share growth trap
    "limit_hits":            -0.5,   # IC=-0.060, ICIR=-0.362 — post-limit-up reversal
    "obv_trend":             -1.0,   # IC=-0.115, ICIR=-0.479 — OBV积累=零售追涨=反转信号
    "medium_term_momentum":  -1.0,   # IC=-0.108, ICIR=-0.352 — 中期动量在A股均值回归
    "amihud_illiquidity":    -0.5,   # IC=-0.062, ICIR=-0.275 — 非流动性溢价短期无效
    "price_volume_corr":     -0.5,   # IC=-0.066, ICIR=-0.624 — 量价配合=散户追涨=反转信号 (inverted)
    "intraday_vs_overnight": -0.5,   # IC=-0.103, ICIR=-0.461 — A股日内追涨=散户=反转; 非隔夜跳空=弱势 (inverted)
}

# Alias so code can refer to it by regime name
FACTOR_WEIGHTS_NORMAL = FACTOR_WEIGHTS

# ---------------------------------------------------------------------------
# BULL regime — prior-20d CSI 300 return > +3.5% (strong rally / recovery).
#
# In A-share bull markets, high-beta growth and momentum stocks lead.
# Defensive factors (low_volatility, piotroski) actively hurt — they select
# stocks that lag speculative rallies. Key changes vs NORMAL:
#   - low_volatility drastically reduced (defensive laggards in rallies)
#   - growth inverted signal removed (growth stocks LEAD in bull markets)
#   - limit_hits inversion removed (limit-up stocks continue in bull)
#   - volume and price_inertia upweighted (momentum + liquidity work in rallies)
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_BULL: dict[str, float] = {
    # ── Momentum core (maximally upweighted in bull) ───────────────────
    "price_inertia":      3.0,   # trend-following is king in bull markets
    "momentum_concavity": 3.0,   # accelerating momentum = the strongest bull signal
    "nearness_to_high":   2.5,   # stocks near 20d high lead bull breakouts
    # volume_expansion excluded (IC=+0.021, noise)
    "volume":             2.0,   # high-turnover stocks attract most bull flows
    "divergence":         1.5,   # multi-indicator confluence still useful
    "bb_squeeze":         1.5,   # volatility breakouts thrive in bull
    "main_inflow":        1.5,   # institutional flow drives bull market leads
    # ── Growth / quality (moderate) ────────────────────────────────────
    "asset_growth":       2.0,   # balance-sheet growth rewarded in risk-on
    "cash_flow_quality":  1.0,   # quality screen avoids blow-ups even in bull
    "roe_trend":          0.5,
    "quality":            0.5,
    "piotroski":          0.5,
    # ── Low-vol cluster: near-zero — actively hurts in rallies ─────────
    "low_volatility":     0.1,
    "idiosyncratic_vol":  0.1,
    "atr_normalized":     0.1,
    "gap_frequency":      0.1,   # reduced — high-gap speculative names lead in bull
    # ── No inversions in bull — prior losers and high-vol names lead ───
    # price_volume_corr not inverted in bull — volume-confirmed moves can continue
    # growth, limit_hits, medium_term_momentum, obv_trend NOT inverted
    # ma60_deviation omitted — extended stocks keep running in bull
}

# ---------------------------------------------------------------------------
# CAUTION regime — price < MA20 (short-term weakness / minor correction).
# Strategy: shift weight toward stable defensive factors; reduce growth-sensitive
# factors that tend to underperform when market momentum turns negative.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_CAUTION: dict[str, float] = {
    # ── Defensive anchors (upweighted) ────────────────────────────────
    "low_volatility":     3.0,   # primary screen in sell-offs
    "idiosyncratic_vol":  2.5,   # low residual vol = avoids speculative bombs
    "atr_normalized":     2.5,   # low realised range = avoids volatile names in corrections
    "gap_frequency":      2.5,   # low gap = stable stocks that don't blow up in corrections
    "price_volume_corr":  -1.0,  # volume-confirmed moves reverse harder in corrections
    "cash_flow_quality":  2.0,   # earnings quality critical in corrections
    "piotroski":          2.0,   # financial health matters more in downturns
    "quality":            2.0,   # profitable companies hold up better
    "ma60_deviation":     1.5,   # stocks near/below MA60 have less downside risk in corrections

    # ── Moderate (kept but trimmed) ───────────────────────────────────
    "roe_trend":          1.0,   # improving ROE = resilient business
    "price_inertia":      1.0,   # inertia less reliable when trend breaks
    "asset_growth":       0.5,

    # ── Volume / momentum reduced in caution ──────────────────────────
    "volume":             0.5,
    "nearness_to_high":   0.2,   # momentum signal, minimal in corrections

    # ── Inverted (kept / amplified in caution) ────────────────────────
    "limit_hits":              -0.5,
    "medium_term_momentum":    -1.5,   # mean-reversion especially strong in corrections
    "obv_trend":               -1.0,   # OBV-chased stocks fall harder in downturns
    # momentum_concavity dropped — unreliable when trend is breaking
    # growth dropped — too noisy in weak market
}

# ---------------------------------------------------------------------------
# CRISIS regime — price < MA60 (black-swan / major structural downturn).
# Strategy: only use capital-preservation factors; accept fewer picks but
# higher conviction on the most defensive names. Still hold 30% (don't go
# fully to cash — avoids missing sharp V-shaped recoveries).
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_CRISIS: dict[str, float] = {
    "low_volatility":    4.0,   # dominant — low-beta stocks survive crashes
    "idiosyncratic_vol": 3.0,   # low residual vol = avoids speculative collapse
    "atr_normalized":    3.0,   # low realised range = capital preservation in crash
    "gap_frequency":     3.0,   # low gap = stocks that don't implode overnight in crisis
    "cash_flow_quality": 2.0,   # cash-backed earnings = survival in crisis
    "piotroski":         2.0,   # balance-sheet strength: avoid distress risk
    "quality":           2.0,   # earnings stability
    "ma60_deviation":    1.5,   # stocks near/below MA60 have better risk-reward in crashes
    # All other factors dropped — too noisy in crash environment
}

# ---------------------------------------------------------------------------
# Per-regime exposure multipliers and labels.
# ---------------------------------------------------------------------------
REGIME_LABELS = ("NORMAL", "CAUTION", "CRISIS")

REGIME_EXPOSURE: dict[str, float] = {
    "NORMAL":       1.0,   # full exposure
    "CAUTION":      0.7,   # prior-20d < -3%  (mild decline)
    "CRISIS":       0.4,   # prior-20d < -6%  (severe decline)
    "BULL":         0.8,   # prior-20d > +2.5% (moderate-to-strong rally)
    "EXTREME_BULL": 0.55,  # prior-20d > +6%  (extreme/parabolic rally — hard to catch)
    # Trend-filter overlay (applied on top of return-based regime)
    "BEAR":         0.15,  # CSI 300 < MA60 (structural downtrend) — near-cash, avoid drawdown
}

REGIME_WEIGHTS: dict[str, dict] = {
    "NORMAL":       FACTOR_WEIGHTS_NORMAL,
    "CAUTION":      FACTOR_WEIGHTS_CAUTION,
    "CRISIS":       FACTOR_WEIGHTS_CRISIS,
    "BULL":         FACTOR_WEIGHTS_BULL,
    "EXTREME_BULL": FACTOR_WEIGHTS_BULL,   # same bull weights, just less exposure
    "BEAR":         FACTOR_WEIGHTS_CRISIS, # BEAR uses crisis weights: max defensive
}

# ---------------------------------------------------------------------------
# Fixed return-based thresholds for regime classification.
# Applied to CSI 300 prior-20d return at each cross-section.
# Simpler and more robust than MA thresholds for exposure control.
# ---------------------------------------------------------------------------
REGIME_CAUTION_THRESHOLD       = -3.0   # prior-20d < this  -> CAUTION
REGIME_CRISIS_THRESHOLD        = -6.0   # prior-20d < this  -> CRISIS
REGIME_BULL_THRESHOLD          = +3.5   # prior-20d > this  -> BULL (tested optimal)
REGIME_EXTREME_BULL_THRESHOLD  = +6.0   # prior-20d > this  -> EXTREME_BULL

# ---------------------------------------------------------------------------
# MA periods for regime detection (applied to CSI 300 close).
# ---------------------------------------------------------------------------
REGIME_MA_SHORT = 20    # short MA — price below this → CAUTION
REGIME_MA_LONG  = 60    # long  MA — price below this → CRISIS

# ---------------------------------------------------------------------------
# Excluded factors — reasons documented; functions still in factors*.py.
# ---------------------------------------------------------------------------
EXCLUDED_FACTORS: dict[str, str] = {
    # Noise: |IC| < 0.02
    "northbound":          "noise: IC=+0.001 — fund flow signal absent",
    "turnover_percentile": "noise: IC=+0.005",
    "ma_alignment":        "noise: IC=+0.010",
    "reversal":            "noise: IC=+0.012",
    "position_52w":        "noise: IC=-0.025",

    # Data unavailable (East Money blocked — real-time PE/PB missing)
    "value":               "no data: EM quote blocked, PE/PB=0 -> NaN score",
    "div_yield":           "no data: requires real-time quote",
    "volume_ratio":        "no data: requires real-time quote",
    "short_interest":      "no data: margin data often missing",
    "accruals":            "insufficient signal: IC near 0",

    # Weak or directionally unstable
    "momentum":            "weak negative: IC=-0.048, mean-reversion dominates in A-share",
    "rsi_signal":          "noise: IC~0 across periods",
    "macd_signal":         "noise: IC~0 across periods",
    "chip_distribution":   "weak negative: IC=-0.042",
    "bollinger_position":      "wrong direction: IC=-0.045 — A-share momentum not mean-reversion; near-upper-band stocks continue rising",
    "turnover_acceleration":   "noise: IC≈0 — turnover rate change uncorrelated with forward returns",
    "gross_margin_trend":      "no data: 毛利率 not in financial indicators API (balance-sheet item)",
    "ar_quality":              "no data: 应收账款 not in financial indicators API (balance-sheet item)",
    "size_factor":             "no data: circ_cap unreliable in IC analysis (always returns current value)",
    "turnover_acceleration":   "noise: IC≈0 — turnover rate change uncorrelated with forward returns",
    "market_beta":             "noise: IC=+0.016, ICIR=0.049 — beta alone adds no signal beyond low_volatility/atr_normalized",
    "volume_expansion":           "noise: IC=+0.021, ICIR=0.095 — 10d/60d volume ratio uncorrelated with forward returns",
    "trend_linearity":            "noise: IC=-0.062, ICIR=-0.210 — R² trend fit weakly negative but ICIR too low to be reliable",
    "upday_ratio":                "noise: IC=-0.029, ICIR=-0.155 — up-day consistency uncorrelated with forward returns",
    "max_return":                 "redundant: IC=+0.216 ICIR=0.947 strong individually but collinear with low_volatility/atr_normalized/idiosyncratic_vol — over-tilts portfolio to low-vol, hurts win rate in up markets",
    "return_skewness":            "redundant: IC=+0.105 ICIR=0.872 strong individually but collinear with low-vol cluster — same exclusion rationale as max_return",
    "market_relative_strength":   "noise: IC=+0.0006, ICIR=0.003 — 20d relative return vs CSI300 adds nothing beyond price_inertia",
    "price_efficiency":           "weak: IC=+0.034, ICIR=0.249 — Kaufman ER insufficient signal for A-shares",
}
