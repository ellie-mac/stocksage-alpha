"""
统一数据契约：Signal / StrategyResult

用 dataclass 代替 Pydantic（无额外依赖）；字段命名与现有 signals_log.json 对齐。
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


@dataclass
class Signal:
    code: str
    name: str
    score: float                        # buy_score [0-100]
    sell_score: float = 0.0
    change_pct: float | None = None
    price: float | None = None
    industry: str | None = None
    market_cap_b: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    turnover_rate: float | None = None
    volume_ratio: float | None = None
    volume_million: float | None = None
    bullish: list[str] = field(default_factory=list)
    bearish: list[str] = field(default_factory=list)
    factor_scores: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Signal":
        keys = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in d.items() if k in keys})


@dataclass
class StrategyResult:
    strategy: str           # "main" | "small" | "etf" | "hot_scan" | "chip_scan" | ...
    date: str               # YYYY-MM-DD
    run_time: str           # YYYY-MM-DD HH:MM
    signals: list[Signal] = field(default_factory=list)
    regime_score: float | None = None
    regime_label: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "StrategyResult":
        signals = [Signal.from_dict(s) for s in d.get("signals", [])]
        return cls(
            strategy=d["strategy"],
            date=d["date"],
            run_time=d["run_time"],
            signals=signals,
            regime_score=d.get("regime_score"),
            regime_label=d.get("regime_label"),
            metadata=d.get("metadata", {}),
        )
