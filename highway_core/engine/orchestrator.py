from collections import deque
from highway_core.engine.models import WorkflowModel, ConditionOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.engine.operator_handlers import task_handler, condition_handler
from highway_core.tools.bulkhead import BulkheadManager


class Orchestrator:
    def __init__(
        self, workflow: WorkflowModel, state: WorkflowState, registry: ToolRegistry
    ):
        self.workflow = workflow
        self.state = state
        self.registry = registry

        # Use a deque for an efficient LILO queue
        self.task_queue = deque([self.workflow.start_task])
        self.completed_tasks = set()

        # Initialize handler map
        # The handlers now return the list of next task IDs
        self.handler_map = {
            "task": self._execute_task,
            "condition": self._execute_condition,
            # Additional handlers will be added as implemented
        }

        # Create a single bulkhead manager for all task operations
        self.bulkhead_manager = BulkheadManager()

        print("Orchestrator initialized.")

    def _dependencies_met(self, dependencies: list[str]) -> bool:
        """Checks if all dependencies for a task are in the completed set."""
        return all(dep_id in self.completed_tasks for dep_id in dependencies)

    def _find_next_runnable_tasks(self) -> list[str]:
        """
        Finds all tasks whose dependencies are now met.
        This is a simple but non-performant O(n^2) check.
        It is fine for Tier 1.
        """
        runnable = []
        for task_id, task in self.workflow.tasks.items():
            if task_id not in self.completed_tasks and task_id not in self.task_queue:
                if self._dependencies_met(task.dependencies):
                    runnable.append(task_id)
        return runnable

    def _execute_task(self, task, state, registry):
        """Wrapper to execute task handler and return empty list of next tasks"""
        task_handler.execute(task, state, registry, self.bulkhead_manager)
        return []

    def _execute_condition(self, task, state, registry):
        """Wrapper to execute condition handler and return next task based on condition result"""
        # The condition handler will return the appropriate task based on condition
        # and also mark the non-taken path as conceptually completed
        return condition_handler.execute(task, state, self, registry)

    def run(self):
        """
        Runs the workflow execution loop.
        """
        print(f"Orchestrator: Starting workflow '{self.workflow.name}'")

        try:
            while self.task_queue:
                task_id = self.task_queue.popleft()

                task_model = self.workflow.tasks.get(task_id)
                if not task_model:
                    print(
                        f"Orchestrator: Error - Task ID '{task_id}' not found in workflow tasks."
                    )
                    continue

                # 1. Check dependencies
                if not self._dependencies_met(task_model.dependencies):
                    # This shouldn't happen with our current logic, but as a safeguard
                    self.task_queue.append(task_id)  # Put it back
                    continue

                # 2. Execute the appropriate handler based on operator type
                try:
                    handler_func = self.handler_map.get(task_model.operator_type)
                    if not handler_func:
                        print(
                            f"Orchestrator: Error - No handler for {task_model.operator_type}"
                        )
                        continue

                    # Handlers now return the list of next task IDs
                    next_task_ids = handler_func(task_model, self.state, self.registry)

                    # Mark task as completed
                    self.completed_tasks.add(task_id)
                    print(f"Orchestrator: Task {task_id} completed.")

                    # Add the specific next tasks from condition evaluation
                    for next_id in next_task_ids:
                        if next_id not in self.task_queue:
                            self.task_queue.append(next_id)

                    # Also add any tasks that are now unblocked
                    self.task_queue.extend(self._find_next_runnable_tasks())

                except Exception as e:
                    print(f"Orchestrator: FATAL ERROR executing task '{task_id}': {e}")
                    # In a real engine, this would trigger failure handling
                    break  # Stop the workflow
        finally:
            # Ensure bulkhead manager is properly shut down
            self.bulkhead_manager.shutdown_all()

        print(f"Orchestrator: Workflow '{self.workflow.name}' finished.")
