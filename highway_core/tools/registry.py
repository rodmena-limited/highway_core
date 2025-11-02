import logging
import pkgutil
import importlib
from typing import Any, Callable, Dict
from .decorators import TOOL_REGISTRY

logger = logging.getLogger(__name__)


class ToolRegistry:
    def __init__(self) -> None:
        # The registry is now just a reference to the one
        # populated by the @tool decorator.
        self.functions = TOOL_REGISTRY
        self._discover_tools()
        logger.info("ToolRegistry loaded with %s functions.", len(self.functions))

    def _discover_tools(self) -> None:
        """Dynamically imports all modules in 'highway_core.tools'."""
        import highway_core.tools

        # This iterates over all modules in the 'tools' package
        # and imports them, which triggers their @tool decorators.
        for _, name, _ in pkgutil.walk_packages(
            highway_core.tools.__path__, highway_core.tools.__name__ + "."
        ):
            importlib.import_module(name)

    def get_function(self, name: str) -> Callable[..., Any]:
        func = self.functions.get(name)
        if func is None:
            logger.error("Error: Tool '%s' not found in registry.", name)
            raise KeyError(f"Tool '{name}' not found.")
        return func
