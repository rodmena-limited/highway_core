# highway_core/tools/decorators.py
from typing import Any, Callable, Optional

TOOL_REGISTRY = {}


def tool(
    name: Optional[str] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to register a function as a Highway tool.
    If no name is provided, the function name is used.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # If no name was provided, use the function's module and name
        if name is None:
            # Get module name and function name to form the registry key
            module_name = func.__module__.split(".")[
                -1
            ]  # Get the last part of the module
            registry_name = f"{module_name}.{func.__name__}"
        else:
            registry_name = name

        if registry_name in TOOL_REGISTRY:
            raise ValueError(f"Duplicate tool name: {registry_name}")
        TOOL_REGISTRY[registry_name] = func
        return func

    return decorator
