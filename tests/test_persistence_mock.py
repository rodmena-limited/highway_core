from highway_core.persistence.manager import PersistenceManager
from highway_core.engine.state import WorkflowState
from typing import Tuple, Set


class MockPersistenceManager(PersistenceManager):
    def __init__(self):
        self.state_store = {}
        self.completed_tasks_store = {}

    def save_workflow_state(
        self, workflow_run_id: str, state: WorkflowState, completed_tasks: Set[str]
    ):
        """Mock save method"""
        self.state_store[workflow_run_id] = state
        self.completed_tasks_store[workflow_run_id] = completed_tasks

    def load_workflow_state(
        self, workflow_run_id: str
    ) -> Tuple[WorkflowState, Set[str]]:
        """Mock load method"""
        state = self.state_store.get(workflow_run_id)
        completed_tasks = self.completed_tasks_store.get(workflow_run_id, set())
        return state, completed_tasks
