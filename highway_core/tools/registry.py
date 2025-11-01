from typing import Callable, Dict
from . import log
from . import memory


class ToolRegistry:
    def __init__(self):
        self.functions: Dict[str, Callable] = {}
        self._register_tools()
        print(f"ToolRegistry loaded with {len(self.functions)} functions.")

    def _register_tools(self):
        """Internal method to load all Tier 1 tools."""
        self.register_tool("tools.log.info", log.info)
        self.register_tool("tools.log.error", log.error)
        self.register_tool("tools.memory.set", memory.set_memory)

    def register_tool(self, name: str, func: Callable):
        self.functions[name] = func

    def get_function(self, name: str) -> Callable:
        func = self.functions.get(name)
        if func is None:
            print(f"Error: Tool '{name}' not found in registry.")
            raise KeyError(f"Tool '{name}' not found.")
        return func
