# --- engine/state.py ---
# Purpose: Manages all workflow data, results, and resolves variables.
# Responsibilities:
# - Holds all workflow 'variables' (e.g., {{base_api_url}}).
# - Holds all task results (e.g., {{response_1}}).
# - Provides a 'resolve_templating' function for string interpolation.
# - Manages the volatile 'tools.memory' cache.

import re
from copy import deepcopy


class WorkflowState:
    def __init__(self, initial_variables: dict):
        self._data = {
            "variables": deepcopy(initial_variables),
            "results": {},
            "memory": {},
            "loop_context": {},  # For 'item' in ForEach
        }
        self.templating_regex = re.compile(r"\{\{([^}]+)\}\}")
        print("WorkflowState initialized.")

    def set_result(self, task_id: str, result: any):
        """Saves the output of a task."""
        self._data["results"][task_id] = result

    def get_result(self, task_id: str) -> any:
        """Gets the output of a previously run task."""
        return self._data["results"].get(task_id)

    def set_memory(self, key: str, value: any):
        """Saves a value to the volatile memory tool."""
        self._data["memory"][key] = value

    def _get_value(self, path: str) -> any:
        """
        Retrieves a value from the state using dot notation.
        e.g., 'response_1.data.userId'
        """
        parts = path.split(".")
        current_val = self._data

        for part in parts:
            if part == "item":  # Check for loop context first
                try:
                    current_val = self._data["loop_context"]
                    continue
                except KeyError:
                    pass  # Fallback to main context

            if isinstance(current_val, dict):
                current_val = current_val.get(part)
            else:
                return None  # Path is invalid
        return current_val

    def resolve_templating(self, input_data: any) -> any:
        """
        Recursively resolves templated strings like '{{response_1.data}}'.
        """
        if isinstance(input_data, str):
            # Check for exact match first
            if self.templating_regex.fullmatch(input_data):
                return self._get_value(input_data[2:-2].strip())

            # Else, replace all occurrences
            return self.templating_regex.sub(
                lambda m: str(self._get_value(m.group(1).strip())), input_data
            )

        if isinstance(input_data, list):
            return [self.resolve_templating(item) for item in input_data]

        if isinstance(input_data, dict):
            return {k: self.resolve_templating(v) for k, v in input_data.items()}

        return input_data
