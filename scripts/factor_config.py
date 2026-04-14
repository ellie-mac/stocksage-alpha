"""
Factor configuration — IC-based weights, excluded factor registry,
and regime-adaptive weight sets.

Based on rolling 6-period IC analysis (20d forward, Group AB, 151 stocks, 2026-04-14).
6 periods: mixed market including sharp Apr-2026 correction and partial rebound.
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
# NORMAL regime — full IC-calibrated weights (2026-04-14 backtest).
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS: dict[str, float] = {
    # ── Tier 1: ICIR ≥ 1.0 (2× weight) ───────────────────────────────────
    "div_yield":             2.0,   # IC=+0.093, ICIR=2.236 — outstanding consistency
    "upper_shadow_reversal": 1.5,   # IC=+0.081, ICIR=1.550 — re-activated (was excluded; signal reversed)
    "upday_ratio":           1.0,   # IC=+0.083, ICIR=1.010 — re-activated (ICIR recovered 0.44→1.01)

    # ── Tier 2: ICIR ≥ 0.50 AND |IC| ≥ 0.05 (1× weight) ─────────────────
    "return_skewness":       1.0,   # IC=+0.092, ICIR=0.547 — demoted from 1.5; ICIR dropped 1.84→0.55
    "market_beta":           1.0,   # IC=+0.082, ICIR=0.761 — re-activated (ICIR crossed threshold 0.45→0.76)
    "ma60_deviation":        1.0,   # IC=+0.072, ICIR=0.701 — demoted from 2.0; IC dropped 0.143→0.072
    "main_inflow":           1.0,   # IC=+0.040, ICIR=0.631 — held; consistent institutional flow signal
    "chip_distribution":     0.5,   # IC=+0.045, ICIR=0.604 — demoted from 1.0; IC slightly weaker
    "obv_trend":             0.5,   # IC=+0.031, ICIR=0.512 — re-activated (ICIR recovered -0.04→0.51)

    # ── Tier 3: Weak-positive, moderate ICIR (0.5× weight) ───────────────
    "cash_flow_quality":     0.5,   # IC=+0.070, ICIR=0.474 — demoted from 1.5; ICIR dropped 0.65→0.47
    "roe_trend":             0.5,   # IC=+0.051, ICIR=0.387 — held
    "divergence":            0.5,   # IC=+0.033, ICIR=0.444 — demoted from 1.5; ICIR dropped 0.77→0.44

    # ── Inverted (IC < 0; higher score = worse forward return) ────────────
    "intraday_vs_overnight": -1.5,  # IC=-0.102, ICIR=-1.282 — re-activated (ICIR strengthened -0.36→-1.28)
    "institutional_visits":  -1.5,  # IC=-0.084, ICIR=-0.730 — held; 机构调研=出货信号
    "limit_hits":            -1.5,  # IC=-0.102, ICIR=-0.588 — reduced from -2.0; ICIR weakened -1.64→-0.59
    "medium_term_momentum":  -1.0,  # IC=-0.061, ICIR=-0.666 — held; 中期动量均值回归
    "overhead_resistance":   -1.0,  # IC=-0.050, ICIR=-0.794 — re-activated (was no-data; now available)
    "quality":               -0.5,  # IC=-0.047, ICIR=-0.352 — reduced from -1.0; ICIR weakened -0.50→-0.35
    "volume":                -0.5,  # IC=-0.040, ICIR=-0.321 — held; 量大=追高=反转
    "limit_open_rate":       -0.5,  # IC=-0.037, ICIR=-0.311 — reduced from -1.0; ICIR weakened -0.54→-0.31
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
    # ── Momentum / flow core (maximally upweighted in bull) ───────────────
    "main_inflow":           2.0,   # institutional flow drives bull market leads
    "chip_distribution":     1.5,   # momentum confirmation via chip structure
    "market_beta":           1.5,   # high-beta outperforms in bull; positive in normal too
    "upday_ratio":           1.0,   # up-day ratio momentum confirmation
    "volume":                1.0,   # bull markets driven by liquidity; positive in bull
    "divergence":            1.0,   # multi-indicator confluence still useful
    "obv_trend":             1.0,   # volume trend positive in bull
    # ── Quality anchor (moderate) ─────────────────────────────────────────
    "cash_flow_quality":     1.0,   # avoids blow-ups even in bull
    "div_yield":             0.5,   # yield matters less in bull but still a quality screen
    "return_skewness":       0.5,   # skewness as outlier screen
    "upper_shadow_reversal": 0.5,   # minor contrarian signal; useful as entry timing in bull
    # ── Inverted signals (kept in bull) ───────────────────────────────────
    "limit_hits":            -1.0,  # even in bull, post-limit reversal matters
    "institutional_visits":  -1.0,  # distribution signal holds at bull tops
    "intraday_vs_overnight": -1.0,  # strong inverted signal (ICIR=-1.28); keep in all regimes
    "medium_term_momentum":  -0.5,  # weaker inversion in bull (momentum partly works)
}

# ---------------------------------------------------------------------------
# CAUTION regime — prior-20d < -3% (short-term weakness / minor correction).
# Strategy: shift weight toward stable defensive factors.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_CAUTION: dict[str, float] = {
    # ── Defensive anchors (upweighted) ────────────────────────────────────
    "div_yield":             3.0,   # IC=0.093, ICIR=2.236; yield support strongest in corrections
    "return_skewness":       2.5,   # IC=0.092, ICIR=0.547; positive skew = fewer blow-ups
    "ma60_deviation":        2.0,   # IC=0.072, ICIR=0.701; proximity to MA60 = mean-reversion anchor
    "cash_flow_quality":     2.0,   # IC=0.070, ICIR=0.474; earnings quality critical in corrections
    "upper_shadow_reversal": 1.5,   # IC=0.081, ICIR=1.550; oversold reversal signal amplified
    "upday_ratio":           1.5,   # IC=0.083, ICIR=1.010; resilient stocks survive corrections
    "chip_distribution":     1.0,   # IC=0.045, ICIR=0.604; low overhang = less selling pressure
    "divergence":            1.0,   # IC=0.033, ICIR=0.444; multi-signal confirmation

    # ── Moderate (kept but trimmed) ───────────────────────────────────────
    "roe_trend":             1.0,   # improving ROE = resilient business
    "main_inflow":           0.5,   # flow matters less when market is falling broadly

    # ── Inverted (kept / amplified in caution) ────────────────────────────
    "intraday_vs_overnight": -2.0,  # IC=-0.102, ICIR=-1.282; gap-down stocks = weak hands
    "institutional_visits":  -2.0,  # distribution signal amplified in corrections
    "limit_hits":            -2.0,  # post-limit reversal stronger in corrections
    "medium_term_momentum":  -1.5,  # mean-reversion especially strong in corrections
    "overhead_resistance":   -1.5,  # IC=-0.050, ICIR=-0.794; overhead selling pressure heavy in corrections
    "quality":               -0.5,  # inverted confirmed; high quality = already priced in
    "limit_open_rate":       -0.5,  # distribution signal
    "volume":                -0.5,  # 量大=追高=反转; more negative in corrections
}

# ---------------------------------------------------------------------------
# CRISIS regime — prior-20d < -6% (black-swan / major structural downturn).
# Strategy: capital-preservation only; accept fewer picks but highest conviction
# on most defensive names. Hold 40% — avoids missing V-shaped recoveries.
# ---------------------------------------------------------------------------
FACTOR_WEIGHTS_CRISIS: dict[str, float] = {
    # ── Survival anchors — factors with consistent positive IC ────────────
    "div_yield":             4.0,   # IC=0.093, ICIR=2.236; yield = capital preservation anchor
    "return_skewness":       3.0,   # IC=0.092, ICIR=0.547; positive skew = fewer catastrophic drops
    "upper_shadow_reversal": 2.0,   # IC=0.081, ICIR=1.550; oversold reversal; useful in V-bounces
    "upday_ratio":           2.0,   # IC=0.083, ICIR=1.010; stocks that hold up = survivors
    "ma60_deviation":        1.5,   # IC=0.072, ICIR=0.701; near-MA60 = mean-reversion support
    "cash_flow_quality":     1.5,   # cash-backed earnings = survival in crisis
    # ── Strong inverted signals survive even in crash ──────────────────────
    "intraday_vs_overnight": -2.0,  # IC=-0.102, ICIR=-1.282; gap-down stocks crash hardest
    "institutional_visits":  -2.0,  # distribution signal amplified in crisis
    "limit_hits":            -2.0,  # stocks with limit history collapse hardest
    "overhead_resistance":   -1.5,  # IC=-0.050, ICIR=-0.794; heavy overhang = continued selling
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

FACTOR_WEIGHTS_SMALLCAP: dict[str, float] = dict(FACTOR_WEIGHTS_NORMAL)

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
    # ── Signal collapsed / degraded to noise (2026-04-14 backtest) ───────
    "idiosyncratic_vol":   "collapsed: IC=+0.045, ICIR=0.215 (was IC=+0.176, ICIR=0.54); 2026-04-14",
    "gap_frequency":       "collapsed: IC=+0.018, ICIR=0.107 (was IC=+0.142, ICIR=0.51); 2026-04-14",
    "atr_normalized":      "collapsed: IC=+0.036, ICIR=0.214 (was IC=+0.154, ICIR=0.55); 2026-04-14",
    "max_return":          "collapsed: IC=+0.046, ICIR=0.279 (was IC=+0.152, ICIR=0.62); 2026-04-14",
    "asset_growth":        "collapsed: IC=+0.018, ICIR=0.138 (was IC=+0.108, ICIR=0.64); noise territory",
    "amihud_illiquidity":  "insufficient periods: N=0 in new backtest; 2026-04-14",
    "northbound":          "reversed: IC=-0.001, ICIR=-0.031 (was IC=+0.074, ICIR=0.71); no signal",
    "price_inertia":       "reversed: IC=-0.012, ICIR=-0.094 (was IC=+0.037, ICIR=0.52); direction flipped",
    "turnover_percentile": "reversed: IC=-0.000, ICIR=-0.002; no signal; 2026-04-14",
    "price_efficiency":    "collapsed: IC=+0.009, ICIR=0.111 (was ICIR=0.60); 2026-04-14",
    "momentum":            "reversed: IC=+0.011, ICIR=0.132 (was IC=-0.077, ICIR=-0.53); direction flipped; 2026-04-14",
    "price_volume_corr":   "noise: IC=-0.009, ICIR=-0.097 (was -0.46); insufficient to invert; 2026-04-14",
    "hammer_bottom":       "noise: IC=-0.019, ICIR=-0.257; insufficient to invert; 2026-04-14",
    "low_volatility":      "collapsed: IC=+0.052, ICIR=0.266 (was IC=+0.168, ICIR=0.52); 2026-04-14",
    "nearness_to_high":    "noise: IC=+0.019, ICIR=0.114; 2026-04-14",
    "turnover_acceleration": "noise: IC=-0.008, ICIR=-0.043; 2026-04-14",
    "bb_squeeze":          "inverted-noise: IC=-0.037, ICIR=-0.290; below -0.50 threshold; 2026-04-14",
    "piotroski":           "noise: IC=-0.007, ICIR=-0.064; 2026-04-14",
    "bollinger_position":  "noise: IC=-0.010, ICIR=-0.063; 2026-04-14",
    "growth":              "noise: IC=-0.001, ICIR=-0.005; 2026-04-14",
    "market_relative_strength": "inverted-noise: IC=-0.040, ICIR=-0.315; below -0.50 threshold; 2026-04-14",
    "momentum_concavity":  "noise: IC=-0.024, ICIR=-0.090; 2026-04-14",
    "volume_expansion":    "inverted-noise: IC=-0.030, ICIR=-0.320; below -0.50 threshold; 2026-04-14",
    "trend_linearity":     "noise: IC=-0.012, ICIR=-0.144; 2026-04-14",
    "reversal":            "noise: IC=+0.031, ICIR=0.268; marginal, subsumed by price_inertia; 2026-04-14",
    "position_52w":        "noise: IC=+0.005, ICIR=0.078; 2026-04-14",
    "concept_momentum":    "weak: IC=+0.040, ICIR=0.421; below ICIR threshold; Group B; 2026-04-14",
    "lhb":                 "noise: IC=-0.014, ICIR=-0.138; Group B; 2026-04-14",
    "ma_alignment":        "noise: IC=+0.000, ICIR=0.002; 2026-04-14",

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
