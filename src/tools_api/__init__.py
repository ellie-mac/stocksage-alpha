"""
工具层注册表 — 提供统一的工具调用接口，兼容 OpenAI function-calling 格式。

使用示例：
    from tools_api import TOOL_REGISTRY
    result = TOOL_REGISTRY["run_strategy_scan"](strategy_name="main")
    if result.success:
        print(result.data)
    else:
        print(result.error)

或直接导入：
    from tools_api.tools import run_strategy_scan
    result = run_strategy_scan(strategy_name="main")
"""
from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class ToolResult:
    """统一工具返回信封：调用方只需检查 success，无需解析各工具内部 error key。"""
    success: bool
    data: Any = None
    error: str | None = None

    def to_dict(self) -> dict:
        return {"success": self.success, "data": self.data, "error": self.error}


@dataclass
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]
    fn: Callable[..., Any]

    def to_openai_schema(self) -> dict:
        """返回 OpenAI function-calling 格式的 schema。"""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": {
                    "type": "object",
                    "properties": self.parameters,
                    "required": [k for k, v in self.parameters.items()
                                 if not v.get("optional", False)],
                },
            },
        }

    def __call__(self, **kwargs: Any) -> ToolResult:
        try:
            data = self.fn(**kwargs)
            return ToolResult(success=True, data=data)
        except Exception:
            return ToolResult(success=False, error=traceback.format_exc())


TOOL_REGISTRY: dict[str, ToolSpec] = {}


def tool(name: str, description: str, parameters: dict[str, Any]) -> Callable:
    """装饰器：注册函数为工具。"""
    def decorator(fn: Callable) -> Callable:
        TOOL_REGISTRY[name] = ToolSpec(
            name=name, description=description,
            parameters=parameters, fn=fn,
        )
        return fn
    return decorator


# 导入所有工具，触发注册（延迟 import 避免循环依赖）
def _register_tools() -> None:
    import importlib
    importlib.import_module("tools_api.tools")

_register_tools()
