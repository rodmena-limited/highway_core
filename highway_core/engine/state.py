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
            "results": {},  # Stores task outputs, e.g., "todo_1": {...}
            "memory": {},  # For tools.memory.set
            "loop_context": {},  # For foreach/while loop items
        }
        print("WorkflowState initialized.")

    def set_result(self, result_key: str, value: Any):
        """Saves the output of a task (from 'result_key')."""
        print(f"State: Setting result for key: {result_key}")
        self._data["results"][result_key] = value

    def set_variable(self, key: str, value: Any):
        """Sets a variable in the workflow state."""
        self._data["variables"][key] = value

    def get_variable(self, key: str):
        """Retrieves a variable from the workflow state."""
        return self._data["variables"].get(key)

    def get_result(self, result_key: str):
        """Retrieves the output of a task by result key."""
        return self._data["results"].get(result_key)

    def _get_value(self, path: str) -> Any:
        """
        Retrieves a value from the state using explicit paths:
        - variables.key -> self._data['variables']['key']
        - results.key -> self._data['results']['key']
        - memory.key -> self._data['memory']['key']
        - item -> self._data['loop_context']['item']
        """
        path = path.strip()

        if path.startswith("variables."):
            # Extract the variable key after "variables."
            var_key = path[len("variables.") :]
            parts = var_key.split(".")

            # Start with the variables dict
            current_val = self._data["variables"]

            # Traverse the nested path
            for part in parts:
                if isinstance(current_val, dict):
                    current_val = current_val.get(part)
                    if current_val is None:
                        print(f"State: Warning - could not find variable path: {path}")
                        return None
                else:
                    print(
                        f"State: Error - Cannot access property '{part}' on non-dict variable."
                    )
                    return None
            return current_val

        elif path.startswith("results."):
            # Extract the result key after "results."
            result_key = path[len("results.") :]
            parts = result_key.split(".")

            # Start with the results dict
            current_val = self._data["results"]

            # Get the first part which should be the result key
            current_val = current_val.get(parts[0])
            if current_val is None:
                print(f"State: Warning - could not find result key: {parts[0]}")
                return None

            # Traverse remaining nested path
            for part in parts[1:]:
                if isinstance(current_val, dict):
                    current_val = current_val.get(part)
                    if current_val is None:
                        print(
                            f"State: Warning - could not find nested result path: {path}"
                        )
                        return None
                else:
                    print(
                        f"State: Error - Cannot access property '{part}' on non-dict result."
                    )
                    return None
            return current_val

        elif path.startswith("memory."):
            # Extract the memory key after "memory."
            mem_key = path[len("memory.") :]
            parts = mem_key.split(".")

            # Start with the memory dict
            current_val = self._data["memory"]

            # Traverse the nested path
            for part in parts:
                if isinstance(current_val, dict):
                    current_val = current_val.get(part)
                    if current_val is None:
                        print(f"State: Warning - could not find memory path: {path}")
                        return None
                else:
                    print(
                        f"State: Error - Cannot access property '{part}' on non-dict memory."
                    )
                    return None
            return current_val

        elif path == "item":
            # Special case for loop context item
            return self._data["loop_context"].get("item")

        else:
            # For backward compatibility or other cases
            print(
                f"State: Warning - path must start with 'variables.', 'results.', 'memory.', or be 'item': {path}"
            )
            return None

    def resolve_templating(self, input_data: Any) -> Any:
        """
        Recursively resolves templated strings like '{{results.todo_1.status}}'.
        """
        if isinstance(input_data, str):
            # Check if the *entire string* is a variable
            match = self.TEMPLATE_REGEX.fullmatch(input_data)
            if match:
                val = self._get_value(match.group(1))
                return (
                    val if val is not None else input_data
                )  # Return original if not found

            # Otherwise, replace all occurrences within the string
            def replacer(m):
                val = self._get_value(m.group(1))
                return (
                    val if val is not None else m.group(0)
                )  # Keep original template if not found

            return self.TEMPLATE_REGEX.sub(replacer, input_data)

        if isinstance(input_data, list):
            return [self.resolve_templating(item) for item in input_data]

        if isinstance(input_data, dict):
            return {k: self.resolve_templating(v) for k, v in input_data.items()}

        # Return non-templatable types as-is (e.g., int, bool)
        return input_data
