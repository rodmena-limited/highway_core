from collections import deque
import graphlib
from typing import Set
from highway_core.engine.models import WorkflowModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.engine.operator_handlers import (
    task_handler,
    condition_handler,
    parallel_handler,
    wait_handler,
)
from highway_core.tools.bulkhead import BulkheadManager


class Orchestrator:
    def __init__(
        self, workflow: WorkflowModel, state: WorkflowState, registry: ToolRegistry
    ):
        self.workflow = workflow
        self.state = state
        self.registry = registry

        # Build the dependency graph for TopologicalSorter
        graph = {}
        for task_id, task in workflow.tasks.items():
            graph[task_id] = set(task.dependencies)

        self.sorter = graphlib.TopologicalSorter(graph)
        self.completed_tasks: Set[str] = set()

        # Initialize handler map
        # The handlers now return the list of next task IDs
        self.handler_map = {
            "task": self._execute_task,
            "condition": self._execute_condition,
            "parallel": self._execute_parallel,
            "wait": self._execute_wait,
            # Additional handlers will be added as implemented
        }

        # Create a single bulkhead manager for all task operations
        self.bulkhead_manager = BulkheadManager()

        print("Orchestrator initialized.")

    def _dependencies_met(self, dependencies: list[str]) -> bool:
        """Checks if all dependencies for a task are in the completed set."""
        return all(dep_id in self.completed_tasks for dep_id in dependencies)

    def _execute_task(self, task, state, registry):
        """Wrapper to execute task handler"""
        task_handler.execute(task, state, registry, self.bulkhead_manager)

    def _execute_condition(self, task, state, registry):
        """Wrapper to execute condition handler and update workflow state"""
        # The condition handler will process the condition and mark appropriate tasks
        condition_handler.execute(task, state, self, registry)

    def _execute_parallel(self, task, state, registry):
        """Wrapper to execute parallel handler"""
        parallel_handler.execute(task, state, registry, self.bulkhead_manager)

    def _execute_wait(self, task, state, registry):
        """Wrapper to execute wait handler"""
        wait_handler.execute(task, state, registry, self.bulkhead_manager)

    def run(self):
        """
        Runs the workflow execution loop using TopologicalSorter.
        """
        print(f"Orchestrator: Starting workflow '{self.workflow.name}'")

        # Prepare the sorter
        self.sorter.prepare()

        try:
            while self.sorter.is_active():
                # Get all tasks that are ready to run (dependencies satisfied)
                runnable_tasks = self.sorter.get_ready()

                # Execute these tasks in parallel (for now, just sequentially)
                for task_id in runnable_tasks:
                    task_model = self.workflow.tasks.get(task_id)
                    if not task_model:
                        print(
                            f"Orchestrator: Error - Task ID '{task_id}' not found in workflow tasks."
                        )
                        continue

                    # Execute the appropriate handler based on operator type
                    try:
                        handler_func = self.handler_map.get(task_model.operator_type)
                        if not handler_func:
                            print(
                                f"Orchestrator: Error - No handler for {task_model.operator_type}"
                            )
                            # Mark task as completed in the sorter to not block downstream tasks
                            self.sorter.done(task_id)
                            self.completed_tasks.add(task_id)
                            print(
                                f"Orchestrator: Task {task_id} marked as completed (no handler)."
                            )
                            continue

                        # Execute the task
                        print(f"Orchestrator: Executing task {task_id}")
                        handler_func(task_model, self.state, self.registry)

                        # Mark task as completed in the sorter
                        self.sorter.done(task_id)
                        self.completed_tasks.add(task_id)
                        print(f"Orchestrator: Task {task_id} completed.")

                    except Exception as e:
                        print(
                            f"Orchestrator: FATAL ERROR executing task '{task_id}': {e}"
                        )
                        # In a real engine, this would trigger failure handling
                        raise e  # Stop the workflow

        except Exception as e:
            print(f"Orchestrator: FATAL ERROR in workflow execution: {e}")
            # In a real engine, this would trigger failure handling
        finally:
            # Ensure bulkhead manager is properly shut down
            self.bulkhead_manager.shutdown_all()

        print(f"Orchestrator: Workflow '{self.workflow.name}' finished.")

    def __del__(self):
        """
        Destructor to ensure bulkhead manager is always shut down,
        even if run() was never called or completed due to exception.
        """
        try:
            self.bulkhead_manager.shutdown_all()
        except:
            # Ignore errors during destruction to avoid interfering with shutdown
            pass
