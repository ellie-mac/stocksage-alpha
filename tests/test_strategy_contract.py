"""
策略契约测试

覆盖：
  - list_strategies() 返回非空列表
  - get_strategy(name) 返回 BaseStrategy 实例
  - BaseStrategy.run() 在 _run() 返回非 StrategyResult 时抛 TypeError
  - BaseStrategy.run() 在 _run() 抛异常时返回 StrategyResult(metadata.failed=True)
  - 失败的 StrategyResult 不含任何 signals
"""
from __future__ import annotations

import pytest
from strategies.base import (
    BaseStrategy, get_strategy, list_strategies, _STRATEGY_CLASSES
)
from strategies.schemas import StrategyResult


def test_list_strategies_nonempty():
    names = list_strategies()
    assert len(names) > 0
    assert "main" in names
    assert "etf" in names


def test_get_strategy_returns_instance():
    for name in list_strategies():
        strat = get_strategy(name)
        assert isinstance(strat, BaseStrategy)
        assert strat.name == name


def test_get_strategy_unknown_raises():
    with pytest.raises(ValueError, match="未知策略"):
        get_strategy("nonexistent_strategy_xyz")


def test_run_raises_on_wrong_return_type():
    """_run() 返回 dict 而非 StrategyResult 时，run() 应抛 TypeError（被 try/except 捕获后返回 failed）"""
    class BadAdapter(BaseStrategy):
        name = "bad"
        def _run(self, config, *, dry_run=False):
            return {"not": "a StrategyResult"}  # wrong type

    result = BadAdapter().run({})
    assert isinstance(result, StrategyResult)
    assert result.metadata.get("failed") is True
    assert len(result.signals) == 0


def test_run_catches_exception_returns_failed():
    """_run() 抛异常时，run() 不向上传播，返回 failed StrategyResult"""
    class CrashAdapter(BaseStrategy):
        name = "crash"
        def _run(self, config, *, dry_run=False):
            raise RuntimeError("simulated crash")

    result = CrashAdapter().run({})
    assert isinstance(result, StrategyResult)
    assert result.metadata.get("failed") is True
    assert "RuntimeError" in result.metadata.get("error", "")
    assert len(result.signals) == 0


def test_successful_run_returns_strategy_result():
    """正常 _run() 返回 StrategyResult 时，run() 透传"""
    from strategies.schemas import Signal

    class GoodAdapter(BaseStrategy):
        name = "good"
        def _run(self, config, *, dry_run=False):
            return self._result([])

    result = GoodAdapter().run({})
    assert isinstance(result, StrategyResult)
    assert not result.metadata.get("failed")
    assert result.strategy == "good"
