import pkgutil
import importlib
from typing import Callable, Dict
from .decorators import TOOL_REGISTRY


class ToolRegistry:
    def __init__(self):
        # The registry is now just a reference to the one
        # populated by the @tool decorator.
        self.functions = TOOL_REGISTRY
        self._discover_tools()
        print(f"ToolRegistry loaded with {len(self.functions)} functions.")

    def _discover_tools(self):
        """Dynamically imports all modules in 'highway_core.tools'."""
        import highway_core.tools

        # This iterates over all modules in the 'tools' package
        # and imports them, which triggers their @tool decorators.
        for _, name, _ in pkgutil.walk_packages(
            highway_core.tools.__path__, highway_core.tools.__name__ + "."
        ):
            importlib.import_module(name)

    def get_function(self, name: str) -> Callable:
        func = self.functions.get(name)
        if func is None:
            print(f"Error: Tool '{name}' not found in registry.")
            raise KeyError(f"Tool '{name}' not found.")
        return func
