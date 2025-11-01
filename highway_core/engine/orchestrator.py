from collections import deque
from highway_core.engine.models import WorkflowModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.engine.operator_handlers import task_handler


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

    def run(self):
        """
        Runs the workflow execution loop.
        """
        print(f"Orchestrator: Starting workflow '{self.workflow.name}'")

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

            # 2. Execute the task (only TaskHandler for Tier 1)
            try:
                if task_model.operator_type == "task":
                    task_handler.execute(task_model, self.state, self.registry)
                else:
                    print(
                        f"Orchestrator: Warning - Skipping operator type '{task_model.operator_type}'."
                    )

                # 3. Mark as complete
                self.completed_tasks.add(task_id)
                print(f"Orchestrator: Task {task_id} completed.")

                # 4. Find and queue the next tasks
                next_tasks = self._find_next_runnable_tasks()
                for next_task_id in next_tasks:
                    if next_task_id not in self.task_queue:
                        self.task_queue.append(next_task_id)

            except Exception as e:
                print(f"Orchestrator: FATAL ERROR executing task '{task_id}': {e}")
                # In a real engine, this would trigger failure handling
                break  # Stop the workflow

        print(f"Orchestrator: Workflow '{self.workflow.name}' finished.")
