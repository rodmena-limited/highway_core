import json
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm.decl_api import DeclarativeMeta


def utc_now():
    """Function to return timezone-aware current datetime."""
    return datetime.now(timezone.utc)


Base = declarative_base()  # type: ignore


class Workflow(Base):  # type: ignore
    """
    Workflow model for tracking workflow execution instances
    Matches the original SQL schema + extensions added in code
    """

    __tablename__ = "workflows"

    workflow_id = Column(String, primary_key=True)
    workflow_name = Column(String(255), nullable=False)  # Match original schema
    start_time = Column(DateTime, default=utc_now)  # Match original schema
    end_time = Column(DateTime)  # Match original schema
    status = Column(String(50), default="running")  # running, completed, failed
    error_message = Column(Text)  # Match original schema
    variables_json = Column(Text)  # Store initial workflow variables
    # Additional fields added in the codebase
    start_task = Column(
        String(255)
    )  # Added to maintain compatibility with existing code
    updated_at = Column(
        DateTime, default=utc_now, onupdate=utc_now
    )  # Added for compatibility
    # Enterprise fields
    priority = Column(String(20), default="normal")  # low, normal, high, critical
    tags_json = Column(Text)  # JSON string for tags
    parent_workflow_id = Column(String, ForeignKey("workflows.workflow_id"))  # For nested workflows
    timeout_seconds = Column(Integer, default=3600)  # Workflow timeout

    # Relationship
    tasks = relationship(
        "Task", back_populates="workflow", cascade="all, delete-orphan"
    )
    memory_entries = relationship(
        "WorkflowMemory"
    )  # Removed back_populates to avoid join issues
    child_workflows = relationship("Workflow", remote_side="Workflow.workflow_id")

    @property
    def tags(self):
        if self.tags_json:
            return json.loads(self.tags_json)
        return []

    @tags.setter
    def tags(self, value):
        self.tags_json = json.dumps(value)

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
    created_at = Column(DateTime, default=utc_now)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    updated_at = Column(
        DateTime, default=utc_now, onupdate=utc_now
    )  # Added after schema creation
    status = Column(
        String(50), default="pending"
    )  # pending, executing, completed, failed
    error_message = Column(Text)
    result_value_json = Column(Text)

    # Enterprise fields
    priority = Column(String(20), default="normal")  # low, normal, high, critical
    tags_json = Column(Text)  # JSON string for tags
    timeout_seconds = Column(Integer, default=300)  # Task timeout
    max_retries = Column(Integer, default=3)  # Max retries for this task
    retry_delay_seconds = Column(Integer, default=5)  # Delay between retries
    result_type = Column(String(50), default="unknown")  # success, partial, failed

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
    def tags(self):
        if self.tags_json:
            return json.loads(self.tags_json)
        return []

    @tags.setter
    def tags(self, value):
        self.tags_json = json.dumps(value)

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

    # Explicit table arguments with unique constraint
    __table_args__ = (
        # Unique constraint on the composite key (task_id, workflow_id)
        UniqueConstraint("task_id", "workflow_id", name="uix_task_workflow_pair"),
        # Additional indexes for performance
        Index("idx_tasks_workflow_id", "workflow_id"),
        Index("idx_tasks_status", "status"),
        Index("idx_tasks_workflow_status", "workflow_id", "status"),
    )


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
    created_at = Column(DateTime, default=utc_now)

    # Composite foreign key constraint handled via application logic
    # Foreign key to task (workflow_id, task_id)

    # Ensure unique execution_id but also consider task executions per workflow
    __table_args__ = (UniqueConstraint("execution_id", name="uix_task_execution_id"),)

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
    created_at = Column(DateTime, default=utc_now)
    # Enterprise fields
    result_type = Column(String(50), default="success")  # success, partial, failed
    error_details_json = Column(Text)  # JSON string for detailed error information

    # Ensure unique result keys per task/workflow combination
    __table_args__ = (
        UniqueConstraint(
            "workflow_id",
            "task_id",
            "result_key",
            name="uix_workflow_task_result_unique",
        ),
    )

    @property
    def error_details(self):
        if self.error_details_json:
            return json.loads(self.error_details_json)
        return {}

    @error_details.setter
    def error_details(self, value):
        self.error_details_json = json.dumps(value)

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
    created_at = Column(DateTime, default=utc_now)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)

    # Ensure unique memory keys per workflow
    __table_args__ = (
        UniqueConstraint(
            "workflow_id", "memory_key", name="uix_workflow_memory_key_unique"
        ),
    )

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
    created_at = Column(DateTime, default=utc_now)

    # Create explicit unique and index constraints
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "depends_on_task_id",
            "workflow_id",
            name="uix_task_dependency_unique",
        ),
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


class Webhook(Base):  # type: ignore
    """
    Webhook model for tracking webhook events and their execution status
    """
    __tablename__ = "webhooks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, nullable=True)  # Can be null for system-wide webhooks
    execution_id = Column(String, nullable=True)  # Can be null for system-wide webhooks
    workflow_id = Column(String, nullable=True)  # Can be null for system-wide webhooks, no FK constraint to allow historical references
    url = Column(String, nullable=False)  # Webhook endpoint URL
    method = Column(String(10), default="POST")  # HTTP method (GET, POST, PUT, PATCH, DELETE)
    headers_json = Column(Text)  # JSON string for additional headers
    payload_json = Column(Text)  # JSON payload to send
    webhook_type = Column(String(50), nullable=False)  # on_start, on_complete, on_fail, on_retry, on_custom
    status = Column(String(50), default="pending")  # pending, sending, sent, failed, retry_scheduled, cancelled
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    next_retry_at = Column(DateTime)  # When to retry next
    response_status = Column(Integer)  # HTTP response status code
    response_headers_json = Column(Text)  # JSON string for response headers
    response_body = Column(Text)  # Response body from webhook
    rate_limit_requests = Column(Integer, default=10)  # Max requests per time window
    rate_limit_window = Column(Integer, default=60)  # Time window in seconds
    created_at = Column(DateTime, default=utc_now)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)

    # Ensure unique combination of task_id, execution_id, workflow_id, and webhook_type
    __table_args__ = (
        UniqueConstraint(
            "execution_id", "task_id", "workflow_id", "webhook_type",
            name="uix_webhook_task_execution_type"
        ),
        Index("idx_webhooks_workflow", "workflow_id"),
        Index("idx_webhooks_status", "status"),
        Index("idx_webhooks_next_retry", "next_retry_at"),
        Index("idx_webhooks_type", "webhook_type"),
    )

    @property
    def headers(self):
        if self.headers_json:
            return json.loads(self.headers_json)
        return {}

    @headers.setter
    def headers(self, value):
        self.headers_json = json.dumps(value)

    @property
    def payload(self):
        if self.payload_json:
            return json.loads(self.payload_json)
        return {}

    @payload.setter
    def payload(self, value):
        self.payload_json = json.dumps(value)

    @property
    def response_headers(self):
        if self.response_headers_json:
            return json.loads(self.response_headers_json)
        return {}

    @response_headers.setter
    def response_headers(self, value):
        self.response_headers_json = json.dumps(value)


class AdminTask(Base):  # type: ignore
    """
    AdminTask model for managing administrative operations like cleanup, cancellation, etc.
    """
    __tablename__ = "admin_tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_type = Column(String(100), nullable=False)  # cleanup_old_workflows, cancel_workflow, etc.
    target_id = Column(String, nullable=False)  # workflow_id or task_id
    status = Column(String(50), default="pending")  # pending, running, completed, failed
    parameters_json = Column(Text)  # JSON string for configurable parameters
    created_at = Column(DateTime, default=utc_now)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    error_message = Column(Text)

    # Indexes for performance
    __table_args__ = (
        Index("idx_admin_tasks_type", "task_type"),
        Index("idx_admin_tasks_status", "status"),
        Index("idx_admin_tasks_target", "target_id"),
    )

    @property
    def parameters(self):
        if self.parameters_json:
            return json.loads(self.parameters_json)
        return {}

    @parameters.setter
    def parameters(self, value):
        self.parameters_json = json.dumps(value)


class WorkflowTemplate(Base):  # type: ignore
    """
    WorkflowTemplate model for reusable workflow definitions
    """
    __tablename__ = "workflow_templates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    workflow_definition_json = Column(Text, nullable=False)  # JSON string of workflow definition
    created_by = Column(String(255))
    created_at = Column(DateTime, default=utc_now)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)

    # Indexes
    __table_args__ = (
        Index("idx_workflow_templates_name", "name"),
        Index("idx_workflow_templates_created_by", "created_by"),
    )

    @property
    def workflow_definition(self):
        if self.workflow_definition_json:
            return json.loads(self.workflow_definition_json)
        return {}

    @workflow_definition.setter
    def workflow_definition(self, value):
        self.workflow_definition_json = json.dumps(value)
