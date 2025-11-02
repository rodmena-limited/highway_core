# highway_core/engine/common.py
from pydantic import BaseModel, Field, ConfigDict
from typing import Any, List, Dict, Optional, Literal, Union
from typing_extensions import Annotated

OperatorType = Literal["task", "condition", "parallel", "foreach", "while", "wait"]


class BaseOperatorModel(BaseModel):
    task_id: str
    operator_type: OperatorType
    dependencies: List[str] = Field(default_factory=list)
    model_config = ConfigDict(extra="allow")


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
    if_false: Optional[str] = None


class ParallelOperatorModel(BaseOperatorModel):
    operator_type: Literal["parallel"]
    branches: Dict[str, List[str]]


class WaitOperatorModel(BaseOperatorModel):
    operator_type: Literal["wait"]
    wait_for: Any


class ForEachOperatorModel(BaseOperatorModel):
    operator_type: Literal["foreach"]
    items: str
    loop_body: List[str]  # <--- This is the inline list of models


class WhileOperatorModel(BaseOperatorModel):
    operator_type: Literal["while"]
    condition: str
    loop_body: List[str]  # <--- This is the inline list of models


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
