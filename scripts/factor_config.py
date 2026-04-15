"""
Factor configuration — IC-based weights, excluded factor registry,
and regime-adaptive weight sets.

Based on rolling 6-period IC analysis (20d forward, Group A, 705 stocks CSI300+CSI500, 2026-04-15).
6 periods: mixed market including sharp Apr-2026 correction and partial rebound.
Data sources: BaoStock PE/PB + Tushare daily(qfq).
Re-run: python factor_analysis.py --rolling 6 --step 20 --group A --universe main_universe.json

To re-activate an excluded factor: move it from EXCLUDED_FACTORS back to FACTOR_WEIGHTS.

Regime logic (CSI 300 prior-20d return signal):
  NORMAL       — default              : IC-optimised weights, 85% exposure (was 100%; capped 2026-04-15)
  CAUTION      — prior-20d < -3%      : shift to defensive, 70% exposure
  CRISIS       — prior-20d < -6%      : defensive anchors only, 40% exposure
  BULL         — prior-20d > +2.5%    : growth/momentum tilt, 80% exposure (threshold lowered 3.5->2.5 on 2026-04-15)
  EXTREME_BULL — prior-20d > +6%      : bull weights, 70% exposure
  BEAR         — CSI300 < MA60        : crisis weights, 15% exposure
"""

# ---------------------------------------------------------------------------
# NORMAL regime — full IC-calibrated weights (2026-04-15, 705-stock run).
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS: dict[str, float] = {
    # ── Tier 1: ICIR ≥ 1.0 ────────────────────────────────────────────────
    "overhead_resistance":   1.5,   # IC=+0.0836, ICIR=2.056 (was N/A in 150-stock run; CSI300/500 have data)
    "return_skewness":       1.5,   # IC=+0.0673, ICIR=1.021

    # ── Tier 2: ICIR 0.5–1.0 ─────────────────────────────────────────────
    "div_yield":             1.0,   # IC=+0.0281, ICIR=0.959 (was 2.0; ICIR normalised with larger sample)
    "position_52w":          1.0,   # IC=+0.1154, ICIR=0.934 (new: nearness to 52w high is positive)
    "growth":                1.0,   # IC=+0.0545, ICIR=0.900 (new activation)
    "main_inflow":           0.5,   # IC=+0.0546, ICIR=0.568
    "volume":                0.5,   # IC=+0.0195, ICIR=0.562
    "medium_term_momentum":  0.5,   # IC=+0.0567, ICIR=0.541 (FLIPPED: was -0.5; large-cap momentum works)
    "market_beta":           0.5,   # IC=+0.0451, ICIR=0.483
    "momentum":              0.5,   # IC=+0.0772, ICIR=0.435
    "hammer_bottom":         0.5,   # IC=+0.0693, ICIR=0.612

    # ── Inverted ──────────────────────────────────────────────────────────
    "limit_hits":            -1.5,  # IC=-0.0831, ICIR=-0.925 (consistent across all runs)
    "chip_distribution":     -1.0,  # IC=-0.0232, ICIR=-0.750 (FLIPPED: was +1.0; large-cap reversal)
    "volume_expansion":      -0.5,  # IC=-0.0738, ICIR=-0.590
    "price_inertia":         -0.5,  # IC=-0.0483, ICIR=-0.459 (new negative)
    "limit_open_rate":       -0.5,  # IC=-0.0261, ICIR=-0.426
    "cash_flow_quality":     -0.5,  # IC=-0.0201, ICIR=-0.422 (FLIPPED: was +0.5)
    "intraday_vs_overnight": -0.5,  # IC=-0.0571, ICIR=-0.371 (reduced from -1.5; weaker with 705 stocks)
    "gap_frequency":         -0.5,  # IC=-0.0810, ICIR=-0.348 (FLIPPED: was +1.0 in small sample)
}

# Alias so code can refer to it by regime name
FACTOR_WEIGHTS_NORMAL = FACTOR_WEIGHTS

# ---------------------------------------------------------------------------
# BULL regime — prior-20d CSI 300 return > +2.5% (confirmed rally).
#
# Root cause of P07 -18.68% L/S spread (2026-04-15 backtest analysis):
#   - NORMAL weights penalise limit_hits=-1.5: 连板股 scored last, but they
#     are the bull market leaders in A-shares. This single signal inverted the
#     entire model cross-section during the Oct-Dec 2025 rally.
#   - volume_expansion=-0.5 further penalised expanding-volume breakout stocks.
#   - chip_distribution=-1.0 penalised momentum-driven stocks with heavy chips.
#
# Key changes vs NORMAL:
#   - limit_hits  NEUTRALISED (0): stop scoring 连板股 as "bad stocks"
#   - volume_expansion FLIPPED (+1): expanding volume = bull confirmation
#   - chip_distribution NEUTRALISED (0): momentum > distribution in bull
#   - return_skewness / div_yield ZEROED: defensive anchors lag rallies
#   - momentum / medium_term_momentum RAISED to 2.0
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_BULL: dict[str, float] = {
    # ── Momentum / growth core (amplified) ───────────────────────────────
    "momentum":              2.0,   # raised 1.5→2.0: core bull signal
    "medium_term_momentum":  2.0,   # raised 1.5→2.0
    "growth":                1.5,   # quality growth leads in bull
    "market_beta":           1.0,   # high-beta outperforms in bull
    "main_inflow":           1.0,   # capital inflows confirm trend
    "volume":                1.0,   # volume confirms bull
    "position_52w":          1.0,   # breakout stocks lead
    # ── Volume/momentum signals FLIPPED for bull ─────────────────────────
    "volume_expansion":      1.0,   # FLIPPED −0.5→+1.0: expansion = continuation in bull
    # ── Technical screen (mild) ──────────────────────────────────────────
    "overhead_resistance":   0.5,   # mild; less dominant when market pushes through resistance
    # ── Neutralised: these caused the P07 model inversion ────────────────
    "limit_hits":            0.0,   # NEUTRALISED −1.0→0: stop penalising 连板股
    "chip_distribution":     0.0,   # NEUTRALISED −1.0→0: momentum > distribution in bull
    "return_skewness":       0.0,   # NEUTRALISED +0.5→0: in bull you want outlier up-moves
    "div_yield":             0.0,   # NEUTRALISED +0.5→0: yield stocks lag rallies
    # ── Keep: overnight gaps still risky even in bull ─────────────────────
    "intraday_vs_overnight": -0.5,
}

# ---------------------------------------------------------------------------
# EXTREME_BULL regime — prior-20d > +6% (parabolic / 连板行情 phase).
#
# Previously shared BULL weights. Now separated: at >+6% prior return the
# market is in a 连板 frenzy. 连板 stocks (high limit_hits) become the
# explicit leaders; max momentum exposure; defensive factors zeroed.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_EXTREME_BULL: dict[str, float] = {
    # ── Max momentum: parabolic phase ────────────────────────────────────
    "momentum":              2.5,
    "medium_term_momentum":  2.5,
    "market_beta":           1.5,   # highest-beta leads in extreme bull
    "main_inflow":           1.5,   # persistent inflows = continuation
    "growth":                1.0,
    "volume":                1.0,
    "volume_expansion":      1.5,   # extreme bull = persistent volume surge
    "position_52w":          0.5,   # everything breaks out; less discriminating
    # ── 连板股 are THE leaders in A-share extreme bull ─────────────────────
    "limit_hits":            1.0,   # POSITIVE: reward 连板 leaders explicitly
    # ── All defensive screens removed ────────────────────────────────────
    "overhead_resistance":   0.0,   # market pushes through all resistance
    "intraday_vs_overnight": -0.5,  # keep: even parabolic tops have gap risks
}

# ---------------------------------------------------------------------------
# CAUTION regime — prior-20d < -3% (short-term weakness / minor correction).
# Strategy: shift weight toward stable defensive factors.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_CAUTION: dict[str, float] = {
    # ── Defensive anchors (upweighted) ────────────────────────────────────
    "overhead_resistance":   2.5,   # ICIR=2.056; technical resistance = strong screen in corrections
    "return_skewness":       2.0,   # ICIR=1.021; positive skew = fewer blow-ups
    "div_yield":             2.0,   # ICIR=0.959; yield = capital preservation
    "position_52w":          1.5,   # ICIR=0.934; relative strength = resilient stocks
    "growth":                1.0,   # ICIR=0.900; quality growth survives corrections
    "main_inflow":           0.5,   # ICIR=0.568; capital staying in = resilience
    # ── Inverted (amplified in caution) ───────────────────────────────────
    "limit_hits":            -2.0,  # ICIR=-0.925; post-limit reversal stronger in corrections
    "chip_distribution":     -1.5,  # ICIR=-0.750; distribution amplified in weakness
    "volume_expansion":      -1.0,  # ICIR=-0.590; panic volume = continued selling
    "price_inertia":         -1.0,  # ICIR=-0.459; downward inertia accelerates in corrections
    "intraday_vs_overnight": -1.0,  # ICIR=-0.371; gap-down = weak hands
    "limit_open_rate":       -1.0,  # distribution signal amplified
    "cash_flow_quality":     -0.5,  # ICIR=-0.422; keep mild
    "gap_frequency":         -0.5,  # ICIR=-0.348; gap stocks underperform in corrections
}

# ---------------------------------------------------------------------------
# CRISIS regime — prior-20d < -6% (black-swan / major structural downturn).
# Strategy: capital-preservation only; accept fewer picks but highest conviction
# on most defensive names. Hold 40% — avoids missing V-shaped recoveries.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_CRISIS: dict[str, float] = {
    # ── Survival anchors only ─────────────────────────────────────────────
    "overhead_resistance":   3.0,   # ICIR=2.056; strongest screen — avoid overhead supply
    "return_skewness":       2.5,   # ICIR=1.021; positive skew = fewer catastrophic drops
    "div_yield":             2.0,   # ICIR=0.959; yield = capital preservation anchor
    "position_52w":          2.0,   # ICIR=0.934; relative strength = survivors in crash
    "growth":                1.5,   # ICIR=0.900; quality growth holds up in crisis
    # ── Strong inverted signals survive even in crash ─────────────────────
    "limit_hits":            -2.0,  # ICIR=-0.925; stocks with limit history collapse hardest
    "chip_distribution":     -2.0,  # ICIR=-0.750; heavy distribution = continued collapse
    "volume_expansion":      -1.5,  # ICIR=-0.590; panic volume = continued selling
    "price_inertia":         -1.5,  # ICIR=-0.459; downward inertia dominates in crisis
    "intraday_vs_overnight": -1.0,  # ICIR=-0.371; gap-down stocks crash hardest
    "limit_open_rate":       -1.0,  # distribution/trapped longs signal
    # All other factors dropped — too noisy in crash environment
}

# ---------------------------------------------------------------------------
# Per-regime exposure multipliers and labels.
# ---------------------------------------------------------------------------
REGIME_LABELS = ("NORMAL", "CAUTION", "CRISIS")

REGIME_EXPOSURE: dict[str, float] = {
    "NORMAL":       0.85,  # capped exposure — regime detection lags; 0.85 limits damage during undetected bull runs (was 1.0)
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
    "EXTREME_BULL": FACTOR_WEIGHTS_EXTREME_BULL,  # separate: 连板行情 weights (was reusing BULL)
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
REGIME_BULL_THRESHOLD          = +2.5   # prior-20d > this  -> BULL (lowered 3.5->2.5 on 2026-04-15: catches rally one period earlier; A-share moves skip the +3.5–6% zone)
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
