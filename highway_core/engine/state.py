import re
from copy import deepcopy
from typing import Any


class WorkflowState:
    """Manages all data for a single workflow run."""

    # Regex to find {{ variable.name }}
    TEMPLATE_REGEX = re.compile(r"\{\{([\s\w.-]+)\}\}")

    def __init__(self, initial_variables: dict):
        # Deepcopy to ensure isolation
        self._data = {
            "variables": deepcopy(initial_variables),
            "results": {},  # Stores task outputs, e.g., "mem_report": {...}
            "memory": {},  # For tools.memory.set
        }
        print("WorkflowState initialized.")

    def set_result(self, result_key: str, value: Any):
        """Saves the output of a task (from 'result_key')."""
        print(f"State: Setting result for key: {result_key}")
        self._data["results"][result_key] = value

    def _get_value(self, path: str) -> Any:
        """
        Retrieves a value from the state using dot-delimited path.
        e.g., "mem_report.key" -> self._data['results']['mem_report']['key']
        """
        path = path.strip()
        parts = path.split(".")

        # Determine the root context (e.g., 'variables' or 'results')
        root_key = parts[0]
        if root_key in self._data:
            current_val = self._data[root_key]
        else:
            # Fallback for keys that are not explicitly namespaced
            # e.g., 'mem_report' instead of 'results.mem_report'
            current_val = self._data["results"].get(root_key)
            if current_val is None:
                print(f"State: Warning - could not find root key: {root_key}")
                return None

        # Traverse the nested path
        for part in parts[1:]:
            if isinstance(current_val, dict):
                current_val = current_val.get(part)
            else:
                print(f"State: Error - Cannot access property '{part}' on non-dict.")
                return None
        return current_val

    def resolve_templating(self, input_data: Any) -> Any:
        """
        Recursively resolves templated strings like '{{mem_report.key}}'.
        """
        if isinstance(input_data, str):
            # Check if the *entire string* is a variable
            match = self.TEMPLATE_REGEX.fullmatch(input_data)
            if match:
                return self._get_value(match.group(1))

            # Otherwise, replace all occurrences within the string
            def replacer(m):
                val = self._get_value(m.group(1))
                return str(val) if val is not None else m.group(0)

            return self.TEMPLATE_REGEX.sub(replacer, input_data)

        if isinstance(input_data, list):
            return [self.resolve_templating(item) for item in input_data]

        if isinstance(input_data, dict):
            return {k: self.resolve_templating(v) for k, v in input_data.items()}

        # Return non-templatable types as-is (e.g., int, bool)
        return input_data
