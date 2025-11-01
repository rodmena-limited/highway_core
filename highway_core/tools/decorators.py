# highway_core/tools/decorators.py
TOOL_REGISTRY = {}


def tool(name: str):
    """Decorator to register a function as a Highway tool."""

    def decorator(func):
        if name in TOOL_REGISTRY:
            raise ValueError(f"Duplicate tool name: {name}")
        TOOL_REGISTRY[name] = func
        return func

    return decorator
