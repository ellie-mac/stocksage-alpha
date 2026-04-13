"""
Factor configuration — IC-based weights, excluded factor registry,
and regime-adaptive weight sets.

Based on rolling 6-period IC analysis (20d forward, Group AB, 152 stocks, 2026-04-13).
6 periods: primarily down market (mean fwd ret -5.46% to -0.12%), 1-2 up periods.
Re-run factor_analysis.py --rolling 6 periodically to refresh.

To re-activate an excluded factor: move it from EXCLUDED_FACTORS back to FACTOR_WEIGHTS.

Regime logic (CSI 300 prior-20d return signal):
  NORMAL       — default              : full IC-optimised weights, 100% exposure
  CAUTION      — prior-20d < -3%      : shift to defensive, 70% exposure
  CRISIS       — prior-20d < -6%      : defensive anchors only, 40% exposure
  BULL         — prior-20d > +3.5%    : growth/momentum tilt, 80% exposure
  EXTREME_BULL — prior-20d > +6%      : bull weights, 70% exposure
  BEAR         — CSI300 < MA60        : crisis weights, 15% exposure
"""

# ---------------------------------------------------------------------------
# NORMAL regime — full IC-calibrated weights (2026-04-13 backtest).
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS: dict[str, float] = {
    # ── Tier 1: ICIR ≥ 0.50 AND IC ≥ 0.10 (2× weight) ───────────────────
    "div_yield":           2.0,   # IC=+0.105, ICIR=3.36 — promoted from 1.5; outstanding consistency
    "return_skewness":     1.5,   # IC=+0.137, ICIR=1.84 — demoted from 2.0; ICIR dropped 4.87→1.84
    "ma60_deviation":      2.0,   # IC=+0.143, ICIR=1.22 — ICIR improved 0.98→1.22
    "idiosyncratic_vol":   2.0,   # IC=+0.176, ICIR=0.54
    "low_volatility":      2.0,   # IC=+0.168, ICIR=0.52
    "gap_frequency":       2.0,   # IC=+0.142, ICIR=0.51 — low overnight gap = stable stock
    "atr_normalized":      2.0,   # IC=+0.154, ICIR=0.55 — low realised range
    "max_return":          2.0,   # IC=+0.152, ICIR=0.62 — promoted from 1.5; ICIR improved

    # ── Tier 1.5: ICIR ≥ 0.65 (1.5× weight) ──────────────────────────────
    "asset_growth":        1.5,   # IC=+0.108, ICIR=0.64 — demoted from 2.0
    "divergence":          1.5,   # IC=+0.095, ICIR=0.77 — demoted from 2.0; ICIR dropped 1.11→0.77
    "cash_flow_quality":   1.5,   # IC=+0.075, ICIR=0.65
    "amihud_illiquidity":  1.5,   # IC=+0.080, ICIR=1.50 — N=2 only, treat with caution

    # ── Tier 2: ICIR ≥ 0.50 (1× weight) ──────────────────────────────────
    "main_inflow":         1.0,   # IC=+0.090, ICIR=0.70
    "northbound":          1.0,   # IC=+0.074, ICIR=0.71
    "chip_distribution":   1.0,   # IC=+0.051, ICIR=0.69 — promoted from 0.2; ICIR crossed threshold
    "price_efficiency":    0.5,   # IC=+0.052, ICIR=0.60 — re-activated from EXCLUDED; ICIR 0.475→0.597

    # ── Tier 3: Weak-positive, ICIR marginal (0.5× weight) ────────────────
    "turnover_percentile": 0.5,   # IC=+0.032, ICIR=0.28 — demoted from 1.0; ICIR dropped 0.57→0.28
    "roe_trend":           0.5,   # IC=+0.053, ICIR=0.53
    "price_inertia":       0.5,   # IC=+0.037, ICIR=0.52 — ICIR improved 0.42→0.52

    # ── Inverted (IC < 0; higher score = worse forward return) ────────────
    "limit_hits":            -2.0,  # IC=-0.152, ICIR=-1.64 — strongest inverted signal
    "institutional_visits":  -1.5,  # IC=-0.078, ICIR=-0.84 — Group B; 机构调研=出货信号
    "medium_term_momentum":  -1.0,  # IC=-0.120, ICIR=-0.52 — 中期动量均值回归
    "momentum":              -1.0,  # IC=-0.077, ICIR=-0.53 — strengthened from -0.5; ICIR -0.47→-0.53
    "limit_open_rate":       -1.0,  # IC=-0.077, ICIR=-0.54 — 高开板率=派发中
    "quality":               -1.0,  # IC=-0.069, ICIR=-0.50 — high-quality = already priced in
    "volume":                -0.5,  # IC=-0.055, ICIR=-0.55 — direction REVERSED from +1.0; 量大=追高=反转
    "price_volume_corr":     -0.5,  # IC=-0.024, ICIR=-0.46 — 量价配合=散户追涨
    "hammer_bottom":         -0.5,  # IC=-0.037, ICIR=-0.34 — demoted from -1.0; ICIR weakened -0.75→-0.34
}

# Alias so code can refer to it by regime name
FACTOR_WEIGHTS_NORMAL = FACTOR_WEIGHTS

# ---------------------------------------------------------------------------
# BULL regime — prior-20d CSI 300 return > +3.5% (strong rally / recovery).
#
# In A-share bull markets, high-beta growth and momentum stocks lead.
# Low-vol/defensive factors tend to lag speculative rallies.
# Key changes vs NORMAL:
#   - low_volatility cluster drastically reduced
#   - growth/momentum inversions removed
#   - volume and price_inertia upweighted
#   - institutional_visits inversion kept (institutions distribute at tops in bull)
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_BULL: dict[str, float] = {
    # ── Momentum core (maximally upweighted in bull) ───────────────────────
    "price_inertia":       3.0,   # trend-following is king in bull markets
    "volume":              1.5,   # bull markets driven by liquidity; moderate positive (reversed in neutral)
    "divergence":          1.5,   # multi-indicator confluence still useful
    "main_inflow":         1.5,   # institutional flow drives bull market leads
    "turnover_percentile": 1.0,   # active stocks attract bull flows
    "chip_distribution":   1.0,   # momentum confirmation
    # ── Growth / quality (moderate) ────────────────────────────────────────
    "asset_growth":        2.0,   # balance-sheet growth rewarded in risk-on
    "cash_flow_quality":   1.0,   # quality screen avoids blow-ups even in bull
    "div_yield":           0.5,   # yield matters less in bull but still a quality screen
    # ── Low-vol cluster: near-zero — keep minimal momentum-trap filter ──────
    "low_volatility":      0.1,
    "idiosyncratic_vol":   0.1,
    "atr_normalized":      0.1,
    "gap_frequency":       0.1,
    # ── Inverted signals (kept in bull) ──────────────────────────────────
    "limit_hits":            -1.0,  # even in bull, post-limit reversal matters
    "institutional_visits":  -1.0,  # distribution signal holds in bull tops
    "medium_term_momentum":  -0.5,  # weaker inversion in bull (momentum works)
    "momentum":              -0.5,  # short-term mean-reversion, weaker in bull
}

# ---------------------------------------------------------------------------
# CAUTION regime — prior-20d < -3% (short-term weakness / minor correction).
# Strategy: shift weight toward stable defensive factors.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_CAUTION: dict[str, float] = {
    # ── Defensive anchors (upweighted) ────────────────────────────────────
    "low_volatility":      3.0,   # primary screen in sell-offs
    "idiosyncratic_vol":   2.5,   # low residual vol = avoids speculative bombs
    "atr_normalized":      2.5,   # low realised range = avoids volatile names
    "gap_frequency":       2.5,   # low gap = stocks that don't blow up
    "cash_flow_quality":   2.0,   # earnings quality critical in corrections
    "return_skewness":     2.0,   # ICIR=1.84; still very stable signal across regimes
    "ma60_deviation":      2.0,   # IC=+0.143, ICIR=1.22; proximity to MA60
    "divergence":          1.5,   # IC=+0.095, ICIR=0.77; robust across regimes
    "amihud_illiquidity":  1.5,   # ICIR=3.17; liquidity premium stable in corrections

    # ── Moderate (kept but trimmed) ───────────────────────────────────────
    "roe_trend":           1.0,   # improving ROE = resilient business
    "price_inertia":       1.0,   # inertia less reliable when trend breaks
    "asset_growth":        0.5,
    "div_yield":           1.0,   # yield support matters more in down markets

    # ── Volume inverted in corrections (confirmed by IC reversal) ────────────
    "volume":              -0.5,  # reversed in neutral; doubly negative in corrections

    # ── Inverted (kept / amplified in caution) ────────────────────────────
    "limit_hits":              -2.0,  # ICIR=-1.92; strongest signal in down markets
    "institutional_visits":    -1.5,  # distribution signal amplified in corrections
    "quality":                 -1.0,  # confirmed inverted; high quality = already priced in
    "medium_term_momentum":    -1.5,  # mean-reversion especially strong in corrections
    "hammer_bottom":           -1.0,  # weak rebounds fail more in corrections
    "limit_open_rate":         -1.0,  # distribution signal
    "price_volume_corr":       -1.0,  # volume-confirmed moves reverse harder in corrections
}

# ---------------------------------------------------------------------------
# CRISIS regime — prior-20d < -6% (black-swan / major structural downturn).
# Strategy: capital-preservation only; accept fewer picks but highest conviction
# on most defensive names. Hold 40% — avoids missing V-shaped recoveries.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_CRISIS: dict[str, float] = {
    "low_volatility":      4.0,   # dominant — low-beta stocks survive crashes
    "idiosyncratic_vol":   3.0,   # low residual vol = avoids speculative collapse
    "atr_normalized":      3.0,   # low realised range = capital preservation in crash
    "gap_frequency":       3.0,   # low gap = stocks that don't implode overnight
    "cash_flow_quality":   2.0,   # cash-backed earnings = survival in crisis
    "return_skewness":     2.0,   # ICIR=1.84; stable signal; keep in crash
    "ma60_deviation":      1.5,   # stocks near/below MA60 have better risk-reward
    "amihud_illiquidity":  1.0,   # illiquidity premium exists even in crises
    # ── Strong inverted signals survive even in crash ──────────────────────
    "limit_hits":          -2.0,  # ICIR=-1.92; stocks with limit history crash hardest
    "institutional_visits": -1.0, # distribution signal
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
    "EXTREME_BULL": 0.70,  # prior-20d > +6%  (extreme rally — raised 0.55->0.70 on 2026-04-03: P8 lost -8% alpha at 55% when bench+10.9%; small-sample caution but structural)
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
    # ── Noise / signal collapsed (2026-04-13 backtest) ───────────────────
    "nearness_to_high":    "signal collapsed: IC=+0.021, ICIR=0.099 (was ICIR=0.48); 2026-04-13",
    "upday_ratio":         "degraded: IC=+0.054, ICIR=0.435 (was ICIR=0.55); dropped below threshold 2026-04-13",
    "turnover_acceleration": "degraded: IC=+0.030, ICIR=0.311 (was ICIR=0.52); dropped below threshold 2026-04-13",
    "bb_squeeze":          "degraded: IC=+0.028, ICIR=0.216 (was IC=+0.152, ICIR=0.987); signal collapsed",
    "piotroski":           "noise: IC=-0.023, ICIR=-0.194; not strong enough to include or invert",
    "obv_trend":           "noise: IC=-0.006, ICIR=-0.043; no signal",
    "intraday_vs_overnight": "noise: IC=-0.054, ICIR=-0.361; below -0.50 threshold",
    "bollinger_position":  "noise: IC=+0.018, ICIR=0.111; signal too weak",
    "growth":              "noise: IC=-0.012, ICIR=-0.094; insufficient signal",
    "market_relative_strength": "noise: IC=-0.002, ICIR=-0.012; no signal",
    "momentum_concavity":  "noise: IC=+0.029, ICIR=0.207; never recovered",
    "volume_expansion":    "noise: IC=+0.005, ICIR=0.030; no signal",
    "market_beta":         "noise: IC=+0.063, ICIR=0.452; below 0.50 threshold",
    "trend_linearity":     "noise: IC=-0.037, ICIR=-0.186",
    "reversal":            "noise: IC=+0.020, ICIR=0.113; subsumed by price_inertia",
    "position_52w":        "noise: IC=-0.033, ICIR=-0.415; below -0.50 inversion threshold",
    "upper_shadow_reversal": "noise: IC=-0.005, ICIR=-0.054; no signal",
    "concept_momentum":    "noise: IC=-0.016, ICIR=-0.247; Group B, no stable signal",
    "lhb":                 "noise: IC=+0.008, ICIR=0.085; Group B, no signal",

    # ── Data unavailable ──────────────────────────────────────────────────
    "value":               "no data: EM quote blocked, PE/PB=0 -> NaN score",
    "volume_ratio":        "no data: requires real-time quote",
    "short_interest":      "no data: margin data often missing",
    "accruals":            "no data: insufficient signal",
    "gross_margin_trend":  "no data: 毛利率 not in financial indicators API",
    "ar_quality":          "no data: 应收账款 not in financial indicators API",
    "size_factor":         "no data: circ_cap unreliable in IC analysis (always current value)",
    "sector_sympathy":     "no data: returns None in backtest (requires real-time industry classification)",
    "overhead_resistance": "no data: returns None in backtest (cyq_df often unavailable)",
    "rsi_signal":          "no data / noise: IC~0 across all periods",
    "macd_signal":         "no data / noise: IC~0 across all periods",

    # ── Group B: no data / insufficient ──────────────────────────────────
    "shareholder_change":  "Group B: no data in backtest (quarterly; insufficient period coverage)",
    "lockup_pressure":     "Group B: no data in backtest",
    "insider":             "Group B: no data in backtest",
    "northbound_actual":   "Group B: no data in backtest (陆股通 API unreliable)",
    "social_heat":         "Group B: no data in backtest (API rate limited)",
    "market_regime":       "Group B: macro regime score used in regime filter, not as stock factor",
    "earnings_revision":   "Group B: no data in backtest",
    "industry_momentum":   "Group B: no data in backtest",
}
