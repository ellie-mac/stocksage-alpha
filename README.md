# stocksage-alpha

**A-share multi-factor alpha engine** — a 50-factor scoring system for systematic stock selection in Chinese equity markets.

## Overview

stocksage-alpha scores individual A-share stocks across 47 factors spanning fundamentals, technicals, sentiment, and market structure. Each factor produces independent **buy** and **sell** scores, enabling nuanced signal separation rather than a single net score.

The system is designed around A-share market microstructure: limit-up/limit-down mechanics, northbound (沪深港通) flow, Dragon & Tiger Board (龙虎榜) data, retail sentiment proxies, and concept/theme momentum.

## Factor Groups

| Group | Factors | Description |
|-------|---------|-------------|
| **Core** | 1–10 | Value, growth, quality, momentum, technicals |
| **Extended A** | 11–20 | Fund flow, chip distribution, shareholder structure |
| **Extended B** | 21–28 | Institutional behavior, analyst revisions, northbound |
| **Extended C** | 29–33 | Behavioral & market-context factors |
| **Extended A2** | 34–47 | IC-validated additions: low-vol family (idiosyncratic vol, ATR, MAX effect, return skewness), cash flow quality, momentum concavity, mean-reversion signals (medium-term momentum, OBV trend, MA60 deviation), divergence, BB squeeze, ROE trend, main inflow, Amihud illiquidity |
| **Extended A3** | 48–50 | Batch 8: intraday vs overnight return split (IC=−0.103, contrarian), market relative strength (noise, excluded), price efficiency/Kaufman ER (weak, excluded) |

See [FACTORS.md](FACTORS.md) for full documentation of all 47 factors including scoring logic, cross-rules, and key principles — in both Chinese and English.

## Key Features

- **Dual scoring**: every factor has independent `buy_score` and `sell_score`
- **Cross-rules (交叉规则)**: factor signals modulated by context (price position, volume, ROE, market regime, etc.)
- **Market regime weighting**: bull/bear market dynamically adjusts factor weights
- **A-share specific signals**: limit-hit patterns, LHB institutional flow, social heat (东方财富热榜), northbound contra-sector detection

## Structure

```
scripts/
├── factors.py           # Core factors (1–10)
├── factors_extended.py  # Extended factors (11–33)
├── factor_analysis.py   # Batch scoring engine
├── research.py          # Single-stock research report
├── screener.py          # Multi-stock screener
├── fetcher.py           # Data fetching layer
├── cache.py             # Request caching
└── industry.py          # Industry momentum utilities
```

## Usage

```python
# Single stock research report
python scripts/research.py 600519

# Batch factor scoring
python scripts/factor_analysis.py

# Screen stocks by factor scores
python scripts/screener.py
```

## Requirements

**Python 3.10+** required (uses `match` syntax and `list[type]` annotations).

```bash
pip install -r requirements.txt
```

Dependencies: `akshare>=1.14.0`, `pandas>=2.0.0`, `numpy>=1.24.0`, `ta>=0.11.0`

### First-run setup

```bash
# 1. Pre-warm financial data cache (~5000 stocks, ~1h, resumable)
python scripts/batch_financials.py

# 2. Pre-warm industry valuation comparisons (~90 API calls, cached 7 days)
python -c "from scripts.industry import build_industry_map; build_industry_map()"

# 3. Build screener universe (A-share sector/concept coverage, ~5–10 min)
python scripts/build_universe.py
```

Steps 1–3 are optional but significantly improve signal quality. After first run they refresh automatically:
- `batch_financials.py`: recommended daily at 02:00 via cron / Task Scheduler
- `build_universe.py`: auto-triggered by `monitor.py` every Monday pre-market

## Factor Documentation

Full factor specs: [FACTORS.md](FACTORS.md)

Covers scoring conditions, cross-rule logic, and the academic/empirical rationale behind each signal — including citations to Fama-French, Ang et al. (low-volatility anomaly), Piotroski F-score, and A-share specific research.
