# stocksage-alpha

**A-share multi-factor alpha engine** — a 45-factor scoring system for systematic stock selection in Chinese equity markets.

## Overview

stocksage-alpha scores individual A-share stocks across 45 factors spanning fundamentals, technicals, sentiment, and market structure. Each factor produces independent **buy** and **sell** scores, enabling nuanced signal separation rather than a single net score.

The system is designed around A-share market microstructure: limit-up/limit-down mechanics, northbound (沪深港通) flow, Dragon & Tiger Board (龙虎榜) data, retail sentiment proxies, and concept/theme momentum.

## Factor Groups

| Group | Factors | Description |
|-------|---------|-------------|
| **Core** | 1–10 | Value, growth, quality, momentum, technicals |
| **Extended A** | 11–20 | Fund flow, chip distribution, shareholder structure |
| **Extended B** | 21–28 | Institutional behavior, analyst revisions, northbound |
| **Extended C** | 29–33 | Behavioral & market-context factors |
| **Extended A2** | 34–45 | IC-validated additions: low-vol family (idiosyncratic vol, ATR), cash flow quality, momentum concavity, mean-reversion signals (medium-term momentum, OBV trend, MA60 deviation), divergence, BB squeeze, ROE trend, main inflow, Amihud illiquidity |

See [FACTORS.md](FACTORS.md) for full documentation of all 45 factors including scoring logic, cross-rules, and key principles — in both Chinese and English.

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

```
pip install -r requirements.txt
```

## Factor Documentation

Full factor specs: [FACTORS.md](FACTORS.md)

Covers scoring conditions, cross-rule logic, and the academic/empirical rationale behind each signal — including citations to Fama-French, Ang et al. (low-volatility anomaly), Piotroski F-score, and A-share specific research.
