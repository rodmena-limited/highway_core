# --- engine/orchestrator.py ---
# Purpose: The central controller that runs the workflow step-by-step.
# Responsibilities:
# - Manages the task execution queue.
# - Resolves dependencies and determines the next runnable tasks.
# - Invokes the correct OperatorHandler for each task.
# - Manages the workflow's overall execution state.

from highway_dsl import Workflow, BaseOperator, OperatorType
from .state import WorkflowState
from persistence.manager import PersistenceManager
from .operator_handlers import (
    task_handler,
    condition_handler,
    parallel_handler,
    foreach_handler,
    while_handler,
    wait_handler,
)


class Orchestrator:
    def __init__(
        self, workflow: Workflow, state: WorkflowState, persistence: PersistenceManager
    ):
        self.workflow = workflow
        self.state = state
        self.persistence = persistence
        self.task_queue = [self.workflow.start_task]
        self.completed_tasks = set()

        self.handler_map = {
            OperatorType.TASK: task_handler,
            OperatorType.CONDITION: condition_handler,
            OperatorType.PARALLEL: parallel_handler,
            OperatorType.FOREACH: foreach_handler,
            OperatorType.WHILE: while_handler,
            OperatorType.WAIT: wait_handler,
            # Add other handlers as they are created
        }
        print("Orchestrator initialized.")

    def run(self):
        """
        Starts the workflow execution loop.
        """
        while self.task_queue:
            current_task_id = self.task_queue.pop(0)

            if current_task_id in self.completed_tasks:
                continue  # Skip if already done (e.g., multiple deps)

            task = self.workflow.tasks.get(current_task_id)
            if not task:
                raise ValueError(f"Task not found: {current_task_id}")

            # Check if dependencies are met
            if not self._dependencies_met(task.dependencies):
                # Put it back in the queue and try later
                self.task_queue.append(current_task_id)
                continue

            print(
                f"Orchestrator: Executing task {task.task_id} (Type: {task.operator_type})"
            )

            # Find the correct handler
            handler = self.handler_map.get(task.operator_type)
            if not handler:
                raise NotImplementedError(
                    f"No handler for operator type: {task.operator_type}"
                )

            # Execute and get next task(s)
            next_tasks = handler.execute(task, self.state)

            self.completed_tasks.add(current_task_id)

            if next_tasks:
                self.task_queue.extend(next_tasks)

            # Persist state after each step
            # self.persistence.save_workflow_state(self.state)

    def _dependencies_met(self, dependencies: list[str]) -> bool:
        """
        Checks if all dependencies for a task have been completed.
        """
        return all(dep_id in self.completed_tasks for dep_id in dependencies)
