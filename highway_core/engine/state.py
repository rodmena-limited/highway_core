import logging
import re
from copy import deepcopy
from typing import Any, Optional, Dict, Union, ClassVar
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class WorkflowState(BaseModel):
    """Manages all data for a single workflow run."""

    # Use Field to initialize the dicts
    variables: Dict[str, Any] = Field(default_factory=dict)
    results: Dict[str, Any] = Field(default_factory=dict)
    memory: Dict[str, Any] = Field(default_factory=dict)
    loop_context: Dict[str, Any] = Field(default_factory=dict)

    # Regex to find {{ variable.name }}
    # Use ClassVar so it's not part of the serialized model
    TEMPLATE_REGEX: ClassVar[re.Pattern] = re.compile(r"\{\{([\s\w.-]+)\}\}")

    @classmethod
    def create_initial(cls, initial_variables: Dict[str, Any]):
        """Factory method to create an initial workflow state."""
        return cls(
            variables=deepcopy(initial_variables),
            results={},
            memory={},
            loop_context={},
        )

    def __init__(self, initial_variables: Dict[str, Any] = None, **data):
        if initial_variables is not None:
            # Called directly with initial_variables, initialize properly
            super().__init__(
                variables=deepcopy(initial_variables),
                results={},
                memory={},
                loop_context={},
                **{
                    k: v
                    for k, v in data.items()
                    if k not in ["variables", "results", "memory", "loop_context"]
                },
            )
        else:
            # Called by Pydantic deserialization - allow it to set values from data
            super().__init__(**data)
        logger.info("WorkflowState initialized.")

    def set_result(self, result_key: str, value: Any):
        """Saves the output of a task (from 'result_key')."""
        logger.info("State: Setting result for key: %s", result_key)
        self.results[result_key] = value

    def set_variable(self, key: str, value: Any):
        """Sets a variable in the workflow state."""
        self.variables[key] = value

    def get_variable(self, key: str) -> Any:
        """Retrieves a variable from the workflow state."""
        return self.variables.get(key)

    def get_result(self, result_key: str) -> Any:
        """Retrieves the output of a task by result key."""
        return self.results.get(result_key)

    def _get_value(self, path: str) -> Any:
        """
        Retrieves a value from the state using explicit paths:
        - variables.key -> self.variables['key']
        - results.key -> self.results['key']
        - memory.key -> self.memory['key']
        - item -> self.loop_context['item']
        """
        path = path.strip()

        if path.startswith("variables."):
            # Extract the variable key after "variables."
            var_key = path[len("variables.") :]
            parts = var_key.split(".")

            # Start with the variables dict
            current_val: Optional[Dict[str, Any]] = self.variables

            # Traverse the nested path
            for part in parts:
                if isinstance(current_val, dict):
                    current_val = current_val.get(part)
                    if current_val is None:
                        logger.warning(
                            "State: Warning - could not find variable path: %s", path
                        )
                        return None
                else:
                    logger.error(
                        "State: Error - Cannot access property '%s' on non-dict variable.",
                        part,
                    )
                    return None
            return current_val

        elif path.startswith("results."):
            # Extract the result key after "results."
            result_key = path[len("results.") :]
            parts = result_key.split(".")

            # Start with the results dict
            results_dict: Dict[str, Any] = self.results

            # Get the first part which should be the result key
            current_val = results_dict.get(parts[0])
            if current_val is None:
                logger.warning(
                    "State: Warning - could not find result key: %s", parts[0]
                )
                return None

            # Traverse remaining nested path
            for part in parts[1:]:
                if isinstance(current_val, dict):
                    current_val = current_val.get(part)
                    if current_val is None:
                        logger.warning(
                            "State: Warning - could not find nested result path: %s",
                            path,
                        )
                        return None
                else:
                    logger.error(
                        "State: Error - Cannot access property '%s' on non-dict result.",
                        part,
                    )
                    return None
            return current_val

        elif path.startswith("memory."):
            # Extract the memory key after "memory."
            mem_key = path[len("memory.") :]
            parts = mem_key.split(".")

            # Start with the memory dict
            memory_dict: Dict[str, Any] = self.memory

            # Traverse the nested path
            current_val = memory_dict
            for part in parts:
                if isinstance(current_val, dict):
                    current_val = current_val.get(part)
                    if current_val is None:
                        logger.warning(
                            "State: Warning - could not find memory path: %s", path
                        )
                        return None
                else:
                    logger.error(
                        "State: Error - Cannot access property '%s' on non-dict memory.",
                        part,
                    )
                    return None
            return current_val

        elif path == "item":
            # Special case for loop context item
            return self.loop_context.get("item")

        else:
            # For backward compatibility or other cases
            logger.warning(
                "State: Warning - path must start with 'variables.', 'results.', 'memory.', or be 'item': %s",
                path,
            )

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
                    str(val) if val is not None else m.group(0)
                )  # Keep original template if not found, otherwise convert to string

            return self.TEMPLATE_REGEX.sub(replacer, input_data)

        if isinstance(input_data, list):
            return [self.resolve_templating(item) for item in input_data]

        if isinstance(input_data, dict):
            return {k: self.resolve_templating(v) for k, v in input_data.items()}

        # Return non-templatable types as-is (e.g., int, bool)
        return input_data
