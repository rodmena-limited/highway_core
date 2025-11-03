import json
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm.decl_api import DeclarativeMeta

Base = declarative_base()  # type: ignore


class Workflow(Base):  # type: ignore
    """
    Workflow model for tracking workflow execution instances
    Matches the original SQL schema + extensions added in code
    """

    __tablename__ = "workflows"

    workflow_id = Column(String, primary_key=True)
    workflow_name = Column(String(255), nullable=False)  # Match original schema
    start_time = Column(DateTime, default=datetime.utcnow)  # Match original schema
    end_time = Column(DateTime)  # Match original schema
    status = Column(String(50), default="running")  # running, completed, failed
    error_message = Column(Text)  # Match original schema
    variables_json = Column(Text)  # Store initial workflow variables
    # Additional fields added in the codebase
    start_task = Column(
        String(255)
    )  # Added to maintain compatibility with existing code
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )  # Added for compatibility

    # Relationship
    tasks = relationship(
        "Task", back_populates="workflow", cascade="all, delete-orphan"
    )
    memory_entries = relationship(
        "WorkflowMemory"
    )  # Removed back_populates to avoid join issues

    @property
    def variables(self):
        if self.variables_json:
            return json.loads(self.variables_json)
        return {}

    @variables.setter
    def variables(self, value):
        self.variables_json = json.dumps(value)

    @property
    def name(self):
        """Alias for workflow_name to maintain compatibility with existing code"""
        return self.workflow_name

    @name.setter
    def name(self, value):
        self.workflow_name = value


class Task(Base):  # type: ignore
    """
    Task model for tracking individual task executions
    Matches the original SQL schema (with updated_at added after schema creation in the original code)
    """

    __tablename__ = "tasks"

    # Composite primary key as defined in original schema
    task_id = Column(String, primary_key=True)
    workflow_id = Column(String, ForeignKey("workflows.workflow_id"), primary_key=True)
    operator_type = Column(String(255), nullable=False)
    runtime = Column(String(50), default="python")
    function = Column(String(255))
    image = Column(String(255))
    command_json = Column(Text)
    args_json = Column(Text)
    kwargs_json = Column(Text)
    result_key = Column(String(255))
    dependencies_json = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )  # Added after schema creation
    status = Column(
        String(50), default="pending"
    )  # pending, executing, completed, failed
    error_message = Column(Text)
    result_value_json = Column(Text)

    # Relationship
    workflow = relationship("Workflow", back_populates="tasks")
    # Removed relationship to avoid join condition issues since there's no proper FK
    dependencies = relationship(
        "TaskDependency",
        primaryjoin="Task.task_id == TaskDependency.task_id",
        foreign_keys="TaskDependency.task_id",
        cascade="all, delete-orphan",
    )
    dependents = relationship(
        "TaskDependency",
        primaryjoin="Task.task_id == TaskDependency.depends_on_task_id",
        foreign_keys="TaskDependency.depends_on_task_id",
        cascade="all, delete-orphan",
    )

    @property
    def command(self):
        if self.command_json:
            return json.loads(self.command_json)
        return None

    @command.setter
    def command(self, value):
        if value is None or (isinstance(value, list) and not value):
            self.command_json = None
        else:
            self.command_json = json.dumps(value)

    @property
    def args(self):
        if self.args_json:
            return json.loads(self.args_json)
        return None

    @args.setter
    def args(self, value):
        if value is None or (isinstance(value, list) and not value):
            self.args_json = None
        else:
            self.args_json = json.dumps(value)

    @property
    def kwargs(self):
        if self.kwargs_json:
            return json.loads(self.kwargs_json)
        return None

    @kwargs.setter
    def kwargs(self, value):
        if value is None or (isinstance(value, dict) and not value):
            self.kwargs_json = None
        else:
            self.kwargs_json = json.dumps(value)

    @property
    def dependencies_list(self):
        if self.dependencies_json:
            return json.loads(self.dependencies_json)
        return []

    @dependencies_list.setter
    def dependencies_list(self, value):
        if value is None or (isinstance(value, list) and not value):
            self.dependencies_json = None
        else:
            self.dependencies_json = json.dumps(value)

    @property
    def result_value(self):
        if self.result_value_json:
            return json.loads(self.result_value_json)
        return None

    @result_value.setter
    def result_value(self, value):
        if value is None:
            self.result_value_json = None
        else:
            self.result_value_json = json.dumps(value)


class TaskExecution(Base):  # type: ignore
    """
    Task execution model for tracking individual task execution attempts
    NOTE: This table was not in the original schema, so I'll keep it as is
    """

    __tablename__ = "task_executions"

    execution_id = Column(String, primary_key=True)
    task_id = Column(
        String, nullable=False
    )  # FK relationship will be handled by the back_populates
    workflow_id = Column(String, nullable=False)
    executor_runtime = Column(String(255))
    execution_args_json = Column(Text)
    execution_kwargs_json = Column(Text)
    result_json = Column(Text)
    error_message = Column(Text)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_ms = Column(Integer)
    status = Column(
        String(50), default="pending"
    )  # pending, running, completed, failed
    created_at = Column(DateTime, default=datetime.utcnow)

    # Composite foreign key constraint handled via application logic
    # Foreign key to task (workflow_id, task_id)

    # Removed relationship to avoid join condition issues since there's no proper FK

    @property
    def execution_args(self):
        if self.execution_args_json:
            return json.loads(self.execution_args_json)
        return None

    @execution_args.setter
    def execution_args(self, value):
        self.execution_args_json = json.dumps(value)

    @property
    def execution_kwargs(self):
        if self.execution_kwargs_json:
            return json.loads(self.execution_kwargs_json)
        return None

    @execution_kwargs.setter
    def execution_kwargs(self, value):
        self.execution_kwargs_json = json.dumps(value)

    @property
    def result(self):
        if self.result_json:
            return json.loads(self.result_json)
        return None

    @result.setter
    def result(self, value):
        self.result_json = json.dumps(value)


class WorkflowResult(Base):  # type: ignore
    """
    Workflow result model for storing task results
    Matches the original SQL schema - with FK to tasks table (workflow_id, task_id)
    """

    __tablename__ = "workflow_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    workflow_id = Column(String, nullable=False)  # Part of FK to tasks table
    task_id = Column(String, nullable=False)  # Part of FK to tasks table
    result_key = Column(String(255), nullable=False)
    result_value_json = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    @property
    def result_value(self):
        if self.result_value_json:
            return json.loads(self.result_value_json)
        return None

    @result_value.setter
    def result_value(self, value):
        self.result_value_json = json.dumps(value)


class WorkflowMemory(Base):  # type: ignore
    """
    Workflow memory model for storing workflow state variables
    NOTE: This table was not in the original schema, so I'll keep it as is
    """

    __tablename__ = "workflow_memory"

    id = Column(Integer, primary_key=True, autoincrement=True)
    workflow_id = Column(
        String, ForeignKey("workflows.workflow_id"), nullable=False
    )  # Add proper FK
    memory_key = Column(String(255), nullable=False)
    memory_value_json = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship removed to avoid join condition issues

    @property
    def memory_value(self):
        if self.memory_value_json:
            return json.loads(self.memory_value_json)
        return None

    @memory_value.setter
    def memory_value(self, value):
        self.memory_value_json = json.dumps(value)


class TaskDependency(Base):  # type: ignore
    """
    Task dependency model for tracking task dependencies
    NOTE: This table was not in the original schema, so I'll keep it as is
    """

    __tablename__ = "task_dependencies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, nullable=False)
    depends_on_task_id = Column(String, nullable=False)
    workflow_id = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Create indexes for better performance
    __table_args__ = (
        Index("idx_task_dependencies_task", "task_id"),
        Index("idx_task_dependencies_depends_on", "depends_on_task_id"),
        Index("idx_task_dependencies_workflow", "workflow_id"),
    )


# Create indexes that were in the original schema
Index("idx_workflows_status", Workflow.status)
Index("idx_tasks_workflow_id", Task.workflow_id)
Index("idx_tasks_workflow_status", Task.workflow_id, Task.status)
Index("idx_tasks_status", Task.status)
Index("idx_workflow_results_workflow", WorkflowResult.workflow_id)
