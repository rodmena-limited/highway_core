# highway_core/engine/models.py
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import (  # Import model_validator
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from typing_extensions import Annotated

# Define the "kind" for each operator
OperatorType = Literal["task", "condition", "parallel", "foreach", "while", "wait"]


# --- Base Operator Model ---
class BaseOperatorModel(BaseModel):
    """The base model all operators share."""

    task_id: str
    operator_type: OperatorType
    dependencies: List[str] = Field(default_factory=list)
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)


# --- Specific Operator Models ---
class TaskOperatorModel(BaseOperatorModel):
    operator_type: Literal["task"]
    result_key: Optional[str] = None

    # --- NEW: Runtime selection ---
    # Defaults to "python" for backward compatibility
    runtime: str = Field(default="python")

    # --- Python runtime fields ---
    function: Optional[str] = None  # <-- Make Optional
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)

    # --- Docker runtime fields ---
    image: Optional[str] = None
    command: Optional[List[str]] = None
    # We can add env_vars, volumes, etc. here later

    # Add a validation rule
    @model_validator(mode="before")
    def validate_runtime_fields(cls, values):
        runtime = values.get("runtime", "python")
        if runtime == "python":
            if not values.get("function"):
                raise ValueError(
                    "Tasks with 'python' runtime must have a 'function' field."
                )
        elif runtime == "docker":
            if not values.get("image"):
                raise ValueError(
                    "Tasks with 'docker' runtime must have an 'image' field."
                )
        else:
            raise ValueError(f"Unsupported runtime: {runtime}")
        return values


class ConditionOperatorModel(BaseOperatorModel):
    operator_type: Literal["condition"]
    if_true: str
    if_false: Optional[str] = None
    condition: str


class ParallelOperatorModel(BaseOperatorModel):
    operator_type: Literal["parallel"]
    branches: Dict[str, List[str]]


class WaitOperatorModel(BaseOperatorModel):
    operator_type: Literal["wait"]
    wait_for: Any


class ForEachOperatorModel(BaseOperatorModel):
    operator_type: Literal["foreach"]
    items: str
    # This is the key change: the loop body is defined inline
    loop_body: List["AnyOperatorModel"]


class WhileOperatorModel(BaseOperatorModel):
    operator_type: Literal["while"]
    condition: str
    # This is the key change: the loop body is defined inline
    loop_body: List["AnyOperatorModel"]


# --- The Discriminated Union ---
AnyOperatorModel = Annotated[
    Union[
        TaskOperatorModel,
        ConditionOperatorModel,
        ParallelOperatorModel,
        WaitOperatorModel,
        ForEachOperatorModel,
        WhileOperatorModel,
    ],
    Field(discriminator="operator_type"),
]

# Manually update forward-references for the recursive models
ForEachOperatorModel.model_rebuild()
WhileOperatorModel.model_rebuild()


class WorkflowModel(BaseModel):
    """Parses the root YAML file."""

    name: str
    start_task: str
    variables: Dict[str, Any] = Field(default_factory=dict)
    mode: str = Field(default="LOCAL")  # Add mode field with default value

    # This is now a flat map of ONLY the top-level tasks
    tasks: Dict[str, AnyOperatorModel]
