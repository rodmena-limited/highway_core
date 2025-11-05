import logging
import re
from copy import deepcopy
from typing import Any, ClassVar, Dict, Optional, Union

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
    TEMPLATE_REGEX: ClassVar[re.Pattern[str]] = re.compile(r"\{\{([\s\w.-]+)\}\}")

    @classmethod
    def create_initial(
        cls, initial_variables: Optional[Dict[str, Any]] = None
    ) -> "WorkflowState":
        """Factory method to create an initial workflow state."""
        return cls(
            variables=deepcopy(initial_variables or {}),
            results={},
            memory={},
            loop_context={},
        )

    def __init__(
        self, initial_variables: Optional[Dict[str, Any]] = None, **data: Any
    ) -> None:
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

    def set_result(self, result_key: str, value: Any) -> None:
        """Saves the output of a task (from 'result_key')."""
        logger.info("State: Setting result for key: %s", result_key)
        self.results[result_key] = value

    def set_variable(self, key: str, value: Any) -> None:
        """Sets a variable in the workflow state."""
        self.variables[key] = value

    def get_variable(self, key: str) -> Any:
        """Retrieves a variable from the workflow state."""
        return self.variables.get(key)

    def get_result(self, result_key: str) -> Any:
        """Retrieves the output of a task by result key."""
        return self.results.get(result_key)

    def get_value_from_path(self, path: str) -> Any:
        """
        Retrieves a value from the state using explicit paths:
        - variables.key -> self.variables['key']
        - results.key -> self.results['key']
        - memory.key -> self.memory['key']
        - item -> self.loop_context['item']
        - key.property -> self.results['key']['property'] (for backward compatibility)
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
                            "State: Warning - could not find variable path: %s",
                            path,
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
                            "State: Warning - could not find memory path: %s",
                            path,
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
            # Try to find the value in results with nested properties
            if "." in path:
                parts = path.split(".")
                # Try as results.key.property
                result_key = parts[0]
                if result_key in self.results:
                    current_val = self.results[result_key]
                    # Traverse remaining nested path
                    for part in parts[1:]:
                        if isinstance(current_val, dict):
                            current_val = current_val.get(part)
                            if current_val is None:
                                logger.warning(
                                    "State: Warning - could not find nested path: %s",
                                    path,
                                )
                                return None
                        else:
                            logger.error(
                                "State: Error - Cannot access property '%s' on non-dict value.",
                                part,
                            )
                            return None
                    return current_val
            
            # If not found, try as a simple result key
            if path in self.results:
                return self.results[path]
            
            # If not found, try as a variable
            if path in self.variables:
                return self.variables[path]
            
            # For backward compatibility, also try without the warning
            # This allows the condition handler to try different prefixes
            return None

    def resolve_templating(self, input_data: Any) -> Any:
        """
        Recursively resolves templated strings like '{{results.todo_1.status}}'.
        """
        if isinstance(input_data, str):
            # Check if the *entire string* is a variable
            match = self.TEMPLATE_REGEX.fullmatch(input_data)
            if match:
                val = self.get_value_from_path(match.group(1))
                return (
                    val if val is not None else input_data
                )  # Return original if not found

            # Otherwise, replace all occurrences within the string
            def replacer(m: re.Match) -> str:  # type: ignore
                val = self.get_value_from_path(m.group(1))
                if val is not None:
                    str_val = str(val)
                    # If this looks like it's used in a Python context, use repr() for safe string literal
                    # This approach is too heuristic-based, let's handle this at a higher level
                    return str_val  # Return the raw string value
                else:
                    # Keep original template if not found
                    return m.group(0)

            return self.TEMPLATE_REGEX.sub(replacer, input_data)

        if isinstance(input_data, list):
            return [self.resolve_templating(item) for item in input_data]

        if isinstance(input_data, dict):
            return {k: self.resolve_templating(v) for k, v in input_data.items()}

        # Return non-templatable types as-is (e.g., int, bool)
        return input_data
