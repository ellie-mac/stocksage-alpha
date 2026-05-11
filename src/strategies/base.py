"""
BaseStrategy ABC + 懒加载策略注册表

每个适配器封装一个现有策略的 scan() 函数，输出统一 StrategyResult。
用法：
    from strategies.base import get_strategy, list_strategies
    print(list_strategies())           # ['main', 'small', 'etf', 'hot_scan']
    result = get_strategy("main").run(config)
"""
from __future__ import annotations

import json
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent   # src/strategies/
_ROOT = _HERE.parent.parent               # repo root
sys.path.insert(0, str(_HERE.parent))     # src/
sys.path.insert(0, str(_HERE))            # src/strategies/ — for dynamic imports of main_strategy etc.

from strategies.schemas import Signal, StrategyResult  # noqa: E402


# ── 公共工具 ──────────────────────────────────────────────────────────────────

def _load_config() -> dict:
    cfg = _ROOT / "alert_config.json"
    return json.loads(cfg.read_text(encoding="utf-8")) if cfg.exists() else {}


def _get_regime(fetcher_mod: Any) -> tuple[float, str]:
    try:
        from factors import score_market_regime
        mkt = score_market_regime(fetcher_mod.get_market_regime_data())
        if mkt:
            return mkt.get("score", 5.0), mkt.get("details", {}).get("signal", "unknown")
    except Exception:
        pass
    return 5.0, "unknown"


def _signal_from_dict(d: dict) -> Signal:
    return Signal(
        code=d.get("code", ""),
        name=d.get("name", d.get("code", "")),
        score=float(d.get("buy_score", d.get("score", 0))),
        sell_score=float(d.get("sell_score", 0)),
        change_pct=d.get("change_pct"),
        price=d.get("price"),
        industry=d.get("industry"),
        market_cap_b=d.get("market_cap_b"),
        pe_ttm=d.get("pe_ttm"),
        pb=d.get("pb"),
        turnover_rate=d.get("turnover_rate"),
        volume_ratio=d.get("volume_ratio"),
        volume_million=d.get("volume_million"),
        bullish=d.get("bullish", []),
        bearish=d.get("bearish", []),
        factor_scores=d.get("factor_scores", {}),
    )


# ── 抽象基类 ──────────────────────────────────────────────────────────────────

class BaseStrategy(ABC):
    name: str = ""

    def run(self, config: dict, *, dry_run: bool = False) -> StrategyResult:
        """执行策略扫描，返回统一 StrategyResult（不推送）。
        异常时返回 metadata["failed"]=True 的空结果，不向上抛出，
        防止夜跑中一个策略崩溃中断其余策略。
        """
        import traceback as _tb
        t0 = datetime.now()
        try:
            result = self._run(config, dry_run=dry_run)
            if not isinstance(result, StrategyResult):
                raise TypeError(
                    f"{type(self).__name__}._run() 必须返回 StrategyResult，"
                    f"实际返回 {type(result)}"
                )
            return result
        except Exception:
            return StrategyResult(
                strategy=self.name,
                date=t0.strftime("%Y-%m-%d"),
                run_time=t0.strftime("%Y-%m-%d %H:%M"),
                metadata={"failed": True, "error": _tb.format_exc()},
            )

    @abstractmethod
    def _run(self, config: dict, *, dry_run: bool = False) -> StrategyResult:
        ...

    def publish(self, result: StrategyResult, config: dict, *, dry_run: bool = False) -> None:
        """推送/写文件等副作用步骤（可选实现）。
        子类覆盖此方法时只处理推送，不做计算。
        nightly_scan 在 strategy.run() 成功后调用。
        """

    def _result(
        self,
        signals: list[Signal],
        regime_score: float | None = None,
        regime_label: str | None = None,
        **metadata: Any,
    ) -> StrategyResult:
        now = datetime.now()
        return StrategyResult(
            strategy=self.name,
            date=now.strftime("%Y-%m-%d"),
            run_time=now.strftime("%Y-%m-%d %H:%M"),
            signals=signals,
            regime_score=regime_score,
            regime_label=regime_label,
            metadata=metadata,
        )


# ── 主策略适配器 ──────────────────────────────────────────────────────────────

class MainStrategyAdapter(BaseStrategy):
    name = "main"

    def _run(self, config: dict, *, dry_run: bool = False) -> StrategyResult:
        import fetcher
        import main_strategy

        thresholds = config.get("thresholds", {})
        regime_score, regime_label = _get_regime(fetcher)

        uni_file = _ROOT / "data" / "universe_main.json"
        universe = (
            json.loads(uni_file.read_text(encoding="utf-8"))
            if uni_file.exists()
            else config.get("screener_universe", [])
        )

        buy_alerts, scored = main_strategy.scan(universe, thresholds, regime_score)
        signals = [_signal_from_dict(s) for s in buy_alerts]
        return self._result(
            signals, regime_score=regime_score, regime_label=regime_label,
            universe_size=len(universe), scored_count=len(scored),
            _raw_buys=buy_alerts, _raw_scored=scored,
        )

    def publish(self, result: "StrategyResult", config: dict, *, dry_run: bool = False) -> None:
        import main_strategy
        main_strategy._push_results(
            result.metadata.get("_raw_buys", []),
            result.metadata.get("_raw_scored", []),
            result.regime_score or 5.0,
            result.regime_label or "unknown",
            result.run_time,
            config,
            dry_run,
        )


# ── 小盘策略适配器 ────────────────────────────────────────────────────────────

class SmallStrategyAdapter(BaseStrategy):
    name = "small"

    def _run(self, config: dict, *, dry_run: bool = False) -> StrategyResult:
        import fetcher
        import small_strategy

        thresholds = config.get("thresholds", {})
        regime_score, regime_label = _get_regime(fetcher)

        picks = small_strategy.scan(config, thresholds, regime_score=regime_score)
        signals = [_signal_from_dict(s) for s in picks]
        return self._result(
            signals, regime_score=regime_score, regime_label=regime_label,
            _raw_candidates=picks,
        )

    def publish(self, result: "StrategyResult", config: dict, *, dry_run: bool = False) -> None:
        import small_strategy
        small_strategy._push_results(
            result.metadata.get("_raw_candidates", []),
            result.regime_score or 5.0,
            result.regime_label or "unknown",
            result.run_time,
            config,
            dry_run,
        )


# ── ETF策略适配器 ─────────────────────────────────────────────────────────────

class EtfStrategyAdapter(BaseStrategy):
    name = "etf"

    def _run(self, config: dict, *, dry_run: bool = False) -> StrategyResult:
        import fetcher
        import etf_strategy

        thresholds = config.get("thresholds", {})
        etf_list   = config.get("etf_watchlist", [])
        regime_score, regime_label = _get_regime(fetcher)

        buys, sells, all_scores = etf_strategy.scan(etf_list, thresholds, regime_score)
        signals = [_signal_from_dict(s) for s in buys]
        sell_signals = [_signal_from_dict(s) for s in sells]
        return self._result(
            signals, regime_score=regime_score, regime_label=regime_label,
            sell_signals=[s.to_dict() for s in sell_signals],
            etf_count=len(etf_list),
            _raw_buys=buys, _raw_sells=sells, _raw_all_scores=all_scores,
        )

    def publish(self, result: "StrategyResult", config: dict, *, dry_run: bool = False) -> None:
        import etf_strategy
        etf_strategy._push_results(
            result.metadata.get("_raw_buys", []),
            result.metadata.get("_raw_sells", []),
            result.metadata.get("_raw_all_scores", []),
            result.regime_score or 5.0,
            result.regime_label or "unknown",
            config,
            dry_run,
        )


# ── 热榜扫描适配器 ────────────────────────────────────────────────────────────

class HotScanAdapter(BaseStrategy):
    name = "hot_scan"

    def _run(self, config: dict, *, dry_run: bool = False) -> StrategyResult:
        import hot_scan as _hs

        result = _hs.run_hot_scan(top_pct=100.0, cah=True, push=False)
        picks = result.get("picks", [])
        signals = [
            Signal(
                code=p.get("code", ""),
                name=p.get("name", ""),
                score=float(p.get("composite_score", p.get("momentum", 0))),
                sell_score=0.0,
                bullish=p.get("tags", []),
            )
            for p in picks
        ]
        return self._result(
            signals,
            metadata={"total_hot": result.get("total", 0),
                      "fetch_time": result.get("fetch_time", "")},
        )


# ── 注册表 ────────────────────────────────────────────────────────────────────

_STRATEGY_CLASSES: dict[str, type[BaseStrategy]] = {
    "main":     MainStrategyAdapter,
    "small":    SmallStrategyAdapter,
    "etf":      EtfStrategyAdapter,
    "hot_scan": HotScanAdapter,
}


def list_strategies() -> list[str]:
    """返回所有已注册策略名称。"""
    return list(_STRATEGY_CLASSES)


def get_strategy(name: str) -> BaseStrategy:
    """按需实例化策略适配器（避免导入时级联失败）。"""
    if name not in _STRATEGY_CLASSES:
        raise ValueError(f"未知策略: {name!r}，可选: {list_strategies()}")
    return _STRATEGY_CLASSES[name]()


# backward-compat: 仍可用 STRATEGY_REGISTRY 做键名检查，但勿直接调 .run()
STRATEGY_REGISTRY = _STRATEGY_CLASSES
