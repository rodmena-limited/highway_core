from pydantic import BaseModel, Field
from pydantic.config import ConfigDict
from typing import Any, List, Dict, Optional


class TaskOperatorModel(BaseModel):
    """Parses a single task from the YAML 'tasks' dict."""

    task_id: str
    operator_type: str  # For Tier 1, this will always be 'task'
    function: str
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    dependencies: List[str] = Field(default_factory=list)
    result_key: Optional[str] = None
    # Add other fields as needed (e.g., retry_policy)

    model_config = ConfigDict(extra="allow")  # Allow other operator keys for now


class WorkflowModel(BaseModel):
    """Parses the root YAML file."""

    name: str
    start_task: str
    variables: Dict[str, Any] = Field(default_factory=dict)
    tasks: Dict[str, TaskOperatorModel]
