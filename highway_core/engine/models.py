from pydantic import BaseModel, Field, ConfigDict
from typing import Any, List, Dict
from .common import AnyOperatorModel  # <--- Import from common


class WorkflowModel(BaseModel):
    """Parses the root YAML file."""

    name: str
    start_task: str
    variables: Dict[str, Any] = Field(default_factory=dict)

    # This is the key: a flat dictionary of *all* tasks
    # *including* loop children. This is the "Task Definition" map.
    tasks: Dict[str, AnyOperatorModel]
