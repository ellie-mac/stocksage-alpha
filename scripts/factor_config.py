"""
Factor configuration — IC-based weights, excluded factor registry,
and regime-adaptive weight sets.

Based on rolling 6-period IC analysis (20d forward, Group AB, 152 stocks, 2026-04-14 rerun).
6 periods: mixed market including sharp Apr-2026 correction and partial rebound.
Data sources stabilised vs prior run: BaoStock PE/PB + Tushare daily(qfq) now reliable.
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
# NORMAL regime — full IC-calibrated weights (2026-04-14 rerun).
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS: dict[str, float] = {
    # ── Tier 1: outstanding consistency ───────────────────────────────────
    "div_yield":             2.0,   # IC=+0.1092, ICIR=5.661 — best signal in universe

    # ── Tier 1b: strong signal, ICIR ≥ 0.8 ───────────────────────────────
    "return_skewness":       1.5,   # IC=+0.1055, ICIR=0.860 ↑ (was 1.0, ICIR 0.55→0.86)
    "upper_shadow_reversal": 1.5,   # IC=+0.0584, ICIR=1.217 — consistent reversal entry signal

    # ── Tier 2: ICIR ≥ 0.50, |IC| ≥ 0.05 ────────────────────────────────
    "max_return":            1.0,   # IC=+0.0935, ICIR=0.509 RE-ACTIVATED (data gap confirmed)
    "ma60_deviation":        1.0,   # IC=+0.0915, ICIR=0.980 — proximity to MA60
    "upday_ratio":           1.0,   # IC=+0.0814, ICIR=0.867
    "volume":                1.0,   # IC=+0.0592, ICIR=0.540 DIRECTION FLIPPED (was -0.5)
    "roe_trend":             0.5,   # IC=+0.0571, ICIR=0.637
    "cash_flow_quality":     0.5,   # IC=+0.0646, ICIR=0.495
    "divergence":            0.5,   # IC=+0.0488, ICIR=0.797 (ICIR improved)
    "reversal":              0.5,   # IC=+0.0511, ICIR=0.481 RE-ACTIVATED (was noise; data gap)
    "obv_trend":             0.3,   # IC=+0.0301, ICIR=0.361 — weak but consistent

    # ── Inverted strong ───────────────────────────────────────────────────
    "limit_hits":            -1.5,  # IC=-0.1348, ICIR=-1.044 ↑ (was -0.588)
    "intraday_vs_overnight": -1.5,  # IC=-0.1278, ICIR=-1.157

    # ── Inverted moderate ─────────────────────────────────────────────────
    "institutional_visits":  -1.0,  # IC=-0.0737, ICIR=-0.750 (reduced from -1.5)
    "volume_expansion":      -0.5,  # IC=-0.0666, ICIR=-0.577 RE-ACTIVATED as inverted
    "northbound":            -0.5,  # IC=-0.0549, ICIR=-0.872 RE-ACTIVATED as inverted
    "quality":               -0.5,  # IC=-0.0537, ICIR=-0.451
    "limit_open_rate":       -0.5,  # IC=-0.0455, ICIR=-0.807 (ICIR improved)
    "medium_term_momentum":  -0.5,  # IC=-0.0322, ICIR=-0.264 (reduced from -1.0; weakened)
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
    # ── Momentum / volume core ────────────────────────────────────────────
    "max_return":            1.5,   # bull: recent outperformers keep outperforming
    "upday_ratio":           1.5,   # up-day momentum amplified in bull
    "volume":                1.5,   # confirmed positive; bull driven by expanding volume
    "divergence":            1.0,   # multi-indicator confluence
    "obv_trend":             1.0,   # volume trend positive in bull
    "reversal":              0.5,   # mild oversold entry timing
    # ── Quality anchor ───────────────────────────────────────────────────
    "cash_flow_quality":     1.0,   # avoids blow-ups even in bull
    "div_yield":             0.5,   # yield matters less in bull but still a quality screen
    "return_skewness":       0.5,   # outlier/blow-up screen
    "upper_shadow_reversal": 0.5,   # entry timing signal
    # ── Inverted signals ─────────────────────────────────────────────────
    "limit_hits":            -1.0,  # post-limit reversal still matters
    "institutional_visits":  -1.0,  # distribution signal holds at bull tops
    "intraday_vs_overnight": -1.0,  # strong inverted (ICIR=-1.16); keep in all regimes
    "volume_expansion":      -0.5,  # expansion = late-stage chasing; mild signal in bull
    "northbound":            -0.5,  # northbound outflows = distribution; keep mild
    "medium_term_momentum":  -0.5,  # weaker inversion in bull
}

# ---------------------------------------------------------------------------
# CAUTION regime — prior-20d < -3% (short-term weakness / minor correction).
# Strategy: shift weight toward stable defensive factors.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_CAUTION: dict[str, float] = {
    # ── Defensive anchors (upweighted) ────────────────────────────────────
    "div_yield":             3.0,   # IC=+0.1092, ICIR=5.661; yield = strongest anchor in corrections
    "return_skewness":       2.5,   # IC=+0.1055, ICIR=0.860; positive skew = fewer blow-ups
    "ma60_deviation":        2.0,   # IC=+0.0915, ICIR=0.980; near-MA60 = mean-reversion anchor
    "cash_flow_quality":     2.0,   # IC=+0.0646, ICIR=0.495; earnings quality critical
    "upper_shadow_reversal": 1.5,   # IC=+0.0584, ICIR=1.217; oversold reversal amplified
    "upday_ratio":           1.5,   # IC=+0.0814, ICIR=0.867; resilient stocks survive corrections
    "max_return":            1.0,   # IC=+0.0935, ICIR=0.509; recent strength = relative resilience
    "divergence":            1.0,   # multi-signal confirmation
    "reversal":              0.5,   # IC=+0.0511, ICIR=0.481; oversold bounce signal
    # ── Moderate ─────────────────────────────────────────────────────────
    "roe_trend":             1.0,   # improving ROE = resilient business
    "volume":                0.5,   # confirmed positive; rising volume on resilient stocks
    # ── Inverted (amplified in caution) ───────────────────────────────────
    "intraday_vs_overnight": -2.0,  # IC=-0.1278, ICIR=-1.157; gap-down = weak hands
    "institutional_visits":  -2.0,  # distribution signal amplified in corrections
    "limit_hits":            -2.0,  # post-limit reversal stronger in corrections
    "volume_expansion":      -1.0,  # IC=-0.0666, ICIR=-0.577; expansion = distribution/panic
    "northbound":            -1.0,  # IC=-0.0549, ICIR=-0.872; outflows = continued weakness
    "medium_term_momentum":  -1.5,  # mean-reversion especially strong in corrections
    "quality":               -0.5,  # already priced in
    "limit_open_rate":       -0.5,  # distribution signal
}

# ---------------------------------------------------------------------------
# CRISIS regime — prior-20d < -6% (black-swan / major structural downturn).
# Strategy: capital-preservation only; accept fewer picks but highest conviction
# on most defensive names. Hold 40% — avoids missing V-shaped recoveries.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_CRISIS: dict[str, float] = {
    # ── Survival anchors — consistent positive IC ─────────────────────────
    "div_yield":             4.0,   # IC=+0.1092, ICIR=5.661; yield = capital preservation anchor
    "return_skewness":       3.0,   # IC=+0.1055, ICIR=0.860; positive skew = fewer catastrophic drops
    "upper_shadow_reversal": 2.0,   # IC=+0.0584, ICIR=1.217; oversold reversal; useful in V-bounces
    "upday_ratio":           2.0,   # IC=+0.0814, ICIR=0.867; stocks that hold up = survivors
    "ma60_deviation":        1.5,   # IC=+0.0915, ICIR=0.980; near-MA60 = mean-reversion support
    "cash_flow_quality":     1.5,   # cash-backed earnings = survival in crisis
    # ── Strong inverted signals survive even in crash ─────────────────────
    "intraday_vs_overnight": -2.0,  # IC=-0.1278, ICIR=-1.157; gap-down stocks crash hardest
    "institutional_visits":  -2.0,  # distribution signal amplified in crisis
    "limit_hits":            -2.0,  # stocks with limit history collapse hardest
    "volume_expansion":      -1.5,  # IC=-0.0666, ICIR=-0.577; panic volume = continued selling
    "northbound":            -1.0,  # IC=-0.0549, ICIR=-0.872; outflows confirm downtrend
    "medium_term_momentum":  -1.0,  # fallen momentum leaders fail to bounce
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
# Small-cap strategy configuration.
# Weights start identical to NORMAL — re-calibrate after running
# factor_analysis.py on a small-cap-only universe (市值 < max_cap_yi亿).
# ---------------------------------------------------------------------------
SMALLCAP_CONFIG: dict = {
    "max_cap_yi":   50,   # 市值上限（亿元），可在 alert_config.json 中覆盖
    "prefilter_n":  60,   # 从过滤后的小市值股里按换手率取前N只参与深度评分
    "top_n":         8,   # 最终输出只数
}

# Written as an explicit literal (NOT derived from FACTOR_WEIGHTS_NORMAL) so that
# updates to the main strategy weights never silently bleed into the small-cap strategy.
# Re-calibrate by running:
#   python factor_analysis.py --rolling 6 --step 20 --group AB \
#       --universe smallcap_universe.json --out factor_ic_smallcap.json
# Calibrated 2026-04-15 from factor_ic_smallcap.json (200 stocks, 6 periods x 20d)
FACTOR_WEIGHTS_SMALLCAP: dict[str, float] = {
    # ── Tier 1: ICIR ≥ 1.0 (positive) ───────────────────────────────────
    "piotroski":             1.5,   # IC=+0.0746, ICIR=1.367
    "amihud_illiquidity":    1.5,   # IC=+0.1590, ICIR=1.333 (illiquid small-cap premium)
    "ma60_deviation":        1.5,   # IC=+0.1254, ICIR=1.292
    "return_skewness":       1.5,   # IC=+0.1442, ICIR=1.137
    "nearness_to_high":      1.0,   # IC=+0.0962, ICIR=1.125
    "max_return":            1.0,   # IC=+0.1726, ICIR=1.084
    "reversal":              1.0,   # IC=+0.0892, ICIR=1.063
    "atr_normalized":        1.0,   # IC=+0.1738, ICIR=0.918
    "gap_frequency":         1.0,   # IC=+0.1436, ICIR=0.892
    "low_volatility":        1.0,   # IC=+0.1686, ICIR=0.779
    "idiosyncratic_vol":     1.0,   # IC=+0.1674, ICIR=0.758
    # ── Tier 2: ICIR 0.4–0.75 (positive) ────────────────────────────────
    "market_beta":           0.5,   # IC=+0.0538, ICIR=0.605
    "chip_distribution":     0.5,   # IC=+0.0358, ICIR=0.589
    "main_inflow":           0.5,   # IC=+0.0407, ICIR=0.465
    "northbound":            0.5,   # IC=+0.0349, ICIR=0.438
    "divergence":            0.5,   # IC=+0.0428, ICIR=0.424
    "upper_shadow_reversal": 0.5,   # IC=+0.0258, ICIR=0.423
    # ── Inverted: ICIR ≤ -0.5 ────────────────────────────────────────────
    "limit_open_rate":       -2.0,  # IC=-0.1082, ICIR=-3.551 (strongest signal)
    "limit_hits":            -2.0,  # IC=-0.1219, ICIR=-1.775
    "momentum":              -1.0,  # IC=-0.1293, ICIR=-1.050 (mean-reversion dominant)
    "medium_term_momentum":  -1.0,  # IC=-0.0910, ICIR=-0.922
    "volume_expansion":      -1.0,  # IC=-0.1140, ICIR=-0.874
    "market_relative_strength": -1.0,  # IC=-0.0765, ICIR=-0.862
    "price_volume_corr":     -1.0,  # IC=-0.0617, ICIR=-0.857
    "intraday_vs_overnight": -1.0,  # IC=-0.1024, ICIR=-0.837
    "ma_alignment":          -1.0,  # IC=-0.0679, ICIR=-0.833
    "price_inertia":         -0.5,  # IC=-0.0460, ICIR=-0.676
}

FACTOR_WEIGHTS_SMALLCAP_CAUTION: dict[str, float] = {
    # Defensive: boost quality + illiquidity premium, tighten momentum penalties
    "amihud_illiquidity":    2.0,
    "ma60_deviation":        2.0,
    "return_skewness":       2.0,
    "piotroski":             2.0,
    "low_volatility":        1.5,
    "nearness_to_high":      1.0,
    "reversal":              1.0,
    "upper_shadow_reversal": 1.0,
    "chip_distribution":     0.5,
    "divergence":            0.5,
    "limit_open_rate":       -2.0,
    "limit_hits":            -2.0,
    "medium_term_momentum":  -1.5,
    "momentum":              -1.5,
    "intraday_vs_overnight": -1.5,
    "volume_expansion":      -1.0,
    "ma_alignment":          -1.0,
    "market_relative_strength": -1.0,
}

FACTOR_WEIGHTS_SMALLCAP_CRISIS: dict[str, float] = {
    # Max defensive: only highest-conviction factors
    "amihud_illiquidity":    2.5,
    "piotroski":             2.0,
    "low_volatility":        2.0,
    "ma60_deviation":        1.5,
    "return_skewness":       1.5,
    "limit_open_rate":       -2.0,
    "limit_hits":            -2.0,
    "momentum":              -2.0,
    "medium_term_momentum":  -1.5,
    "intraday_vs_overnight": -1.5,
    "volume_expansion":      -1.0,
}

FACTOR_WEIGHTS_SMALLCAP_BULL: dict[str, float] = {
    # Aggressive: lean into momentum-adjacent signals, keep key negatives
    "max_return":            2.0,
    "atr_normalized":        1.5,
    "gap_frequency":         1.5,
    "main_inflow":           1.5,
    "idiosyncratic_vol":     1.0,
    "market_beta":           1.0,
    "return_skewness":       1.0,
    "chip_distribution":     1.0,
    "divergence":            1.0,
    "northbound":            0.5,
    "upper_shadow_reversal": 0.5,
    "limit_open_rate":       -2.0,
    "limit_hits":            -1.5,
    "intraday_vs_overnight": -1.0,
    "medium_term_momentum":  -0.5,
}

REGIME_WEIGHTS_SMALLCAP: dict[str, dict] = {
    "NORMAL":       FACTOR_WEIGHTS_SMALLCAP,
    "CAUTION":      FACTOR_WEIGHTS_SMALLCAP_CAUTION,
    "CRISIS":       FACTOR_WEIGHTS_SMALLCAP_CRISIS,
    "BULL":         FACTOR_WEIGHTS_SMALLCAP_BULL,
    "EXTREME_BULL": FACTOR_WEIGHTS_SMALLCAP_BULL,
    "BEAR":         FACTOR_WEIGHTS_SMALLCAP_CRISIS,
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
    # ── Signal collapsed / noise (2026-04-14 rerun) ───────────────────────
    "idiosyncratic_vol":    "weak: IC=+0.059, ICIR=0.262; borderline, below ICIR threshold",
    "gap_frequency":        "weak: IC=+0.026, ICIR=0.131; noise territory",
    "atr_normalized":       "weak: IC=+0.052, ICIR=0.262; borderline, below ICIR threshold",
    "asset_growth":         "noise: IC=+0.009, ICIR=0.067",
    "amihud_illiquidity":   "noise: IC=-0.001, ICIR=-0.004; no signal across periods",
    "price_inertia":        "weak: IC=+0.044, ICIR=0.342; borderline positive but subsumed by reversal",
    "turnover_percentile":  "noise: IC=-0.002, ICIR=-0.033; no signal",
    "price_efficiency":     "weak: IC=+0.021, ICIR=0.168; below threshold",
    "momentum":             "noise: IC=+0.013, ICIR=0.097; unstable direction across periods",
    "price_volume_corr":    "noise: IC=-0.029, ICIR=-0.306; below -0.50 threshold",
    "hammer_bottom":        "noise: IC=+0.018, ICIR=0.350; positive but weak IC",
    "low_volatility":       "weak: IC=+0.066, ICIR=0.311; below ICIR threshold; subsumed by return_skewness",
    "nearness_to_high":     "weak: IC=+0.043, ICIR=0.352; borderline; subsumed by ma60_deviation",
    "turnover_acceleration": "noise: IC=+0.030, ICIR=0.280; unstable (N=4 periods)",
    "bb_squeeze":           "noise: IC=+0.017, ICIR=0.112",
    "piotroski":            "noise: IC=+0.004, ICIR=0.044",
    "bollinger_position":   "inverted-noise: IC=-0.039, ICIR=-0.234; below -0.50 threshold",
    "growth":               "noise: IC=+0.009, ICIR=0.080",
    "market_relative_strength": "inverted-noise: IC=-0.047, ICIR=-0.396; below -0.50 threshold",
    "momentum_concavity":   "noise: IC=+0.029, ICIR=0.133",
    "trend_linearity":      "noise: IC=-0.017, ICIR=-0.162",
    "position_52w":         "noise: IC=+0.022, ICIR=0.159",
    "concept_momentum":     "noise: IC=+0.016, ICIR=0.221; Group B",
    "lhb":                  "no data: N=0 periods; Group B",
    "ma_alignment":         "noise: IC=+0.001, ICIR=0.008",
    # ── Previously active, now degraded to noise ──────────────────────────
    "market_beta":          "degraded: IC=+0.020, ICIR=0.226 (was IC=+0.082, ICIR=0.761); removed 2026-04-14 rerun",
    "main_inflow":          "reversed: IC=-0.025, ICIR=-0.402 (was IC=+0.040, ICIR=0.631); removed 2026-04-14 rerun",
    "chip_distribution":    "reversed: IC=-0.022, ICIR=-0.220 (was IC=+0.045, ICIR=0.604); removed 2026-04-14 rerun",
    "overhead_resistance":  "no data: N=0 periods in rerun (was IC=-0.050 in 2026-04-14); removed 2026-04-14 rerun",

    # ── Data unavailable ──────────────────────────────────────────────────
    "value":               "no data: EM quote blocked, PE/PB=0 -> NaN score",
    "volume_ratio":        "no data: requires real-time quote",
    "short_interest":      "no data: margin data often missing",
    "accruals":            "no data: insufficient signal",
    "gross_margin_trend":  "no data: 毛利率 not in financial indicators API",
    "ar_quality":          "no data: 应收账款 not in financial indicators API",
    "size_factor":         "no data: circ_cap unreliable in IC analysis (always current value)",
    "sector_sympathy":     "no data: returns None in backtest (requires real-time industry classification)",
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
