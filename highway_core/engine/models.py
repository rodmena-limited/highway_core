from pydantic import BaseModel, Field, ConfigDict
from typing import Any, List, Dict, Optional, Literal, Union
from typing_extensions import Annotated

# Define the "kind" for each operator
OperatorType = Literal["task", "condition", "parallel", "foreach", "while", "wait"]


# --- Base Operator Model ---
class BaseOperatorModel(BaseModel):
    """The base model all operators share."""

    task_id: str
    operator_type: OperatorType
    dependencies: List[str] = Field(default_factory=list)
    model_config = ConfigDict(extra="allow")


# --- Specific Operator Models ---
class TaskOperatorModel(BaseOperatorModel):
    operator_type: Literal["task"]
    function: str
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    result_key: Optional[str] = None


class ConditionOperatorModel(BaseOperatorModel):
    operator_type: Literal["condition"]
    condition: str
    if_true: str
    if_false: str


class ParallelOperatorModel(BaseOperatorModel):
    operator_type: Literal["parallel"]
    branches: Dict[str, List[str]]  # Just parsing for now


class WaitOperatorModel(BaseOperatorModel):
    operator_type: Literal["wait"]
    wait_for: Any  # Can be str, int (for timedelta), etc.


class ForEachOperatorModel(BaseOperatorModel):
    operator_type: Literal["foreach"]
    items: str
    loop_body: List[Any]  # Just parsing for now


class WhileOperatorModel(BaseOperatorModel):
    operator_type: Literal["while"]
    condition: str
    loop_body: List[Any]  # Just parsing for now


# --- The Discriminated Union ---
# This allows Pydantic to automatically parse the correct model
# based on the 'operator_type' field.
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


class WorkflowModel(BaseModel):
    """Parses the root YAML file."""

    name: str
    start_task: str
    variables: Dict[str, Any] = Field(default_factory=dict)
    tasks: Dict[str, AnyOperatorModel]
