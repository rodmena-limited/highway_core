# highway_core/tools/decorators.py
from typing import Any, Callable

TOOL_REGISTRY = {}


def tool(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to register a function as a Highway tool."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if name in TOOL_REGISTRY:
            raise ValueError(f"Duplicate tool name: {name}")
        TOOL_REGISTRY[name] = func
        return func

    return decorator

    return decorator
