import os
import unittest
import uuid
from pathlib import Path

os.environ["HIGHWAY_ENV"] = "test"
os.environ["NO_DOCKER_USE"] = "true"
os.environ["DATABASE_URL"] = (
    "sqlite:///tests/test_db.sqlite3"  # Use same DB as other tests
)

from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.persistence.sql_persistence_manager import SQLPersistenceManager


class TestSQLPersistenceManager(unittest.TestCase):

    def setUp(self):
        # Set up test environment
        os.environ["HIGHWAY_ENV"] = "test"
        os.environ["NO_DOCKER_USE"] = "true"
        os.environ["DATABASE_URL"] = (
            "sqlite:///tests/test_db.sqlite3"  # Use same DB as other tests
        )
        self.workflow_id = f"test_workflow_{uuid.uuid4().hex[:12]}"

        # Ensure a clean database for each test
        db_file = Path("tests/test_db.sqlite3")
        if db_file.exists():
            db_file.unlink()

    def test_save_and_load_state(self):
        manager = SQLPersistenceManager(is_test=True)
        state = WorkflowState({"var1": "value1"})
        completed_tasks = {"task1"}
        manager.save_workflow_state(self.workflow_id, state, completed_tasks)

        # Load from database
        loaded_state, loaded_tasks = manager.load_workflow_state(self.workflow_id)
        self.assertEqual(loaded_state.variables, state.variables)
        self.assertEqual(loaded_tasks, completed_tasks)
        manager.close()

    def test_start_and_complete_workflow(self):
        manager = SQLPersistenceManager(is_test=True)
        
        # Start workflow
        manager.start_workflow(self.workflow_id, "test_workflow", {"var2": "value2"})
        
        # Verify workflow is running
        loaded_state, loaded_tasks = manager.load_workflow_state(self.workflow_id)
        self.assertEqual(loaded_state.variables, {"var2": "value2"})
        
        # Complete workflow
        manager.complete_workflow(self.workflow_id)
        manager.close()

    def test_start_and_complete_task(self):
        manager = SQLPersistenceManager(is_test=True)
        
        # Start workflow first
        manager.start_workflow(self.workflow_id, "test_workflow", {})
        
        # Start a task
        task = TaskOperatorModel(
            task_id="test_task", operator_type="task", function="test.func"
        )
        success = manager.start_task(self.workflow_id, task)
        self.assertTrue(success)

        # Complete the task
        manager.complete_task(self.workflow_id, "test_task", "result")
        
        # Verify task completion
        loaded_state, loaded_tasks = manager.load_workflow_state(self.workflow_id)
        self.assertIn("test_task", loaded_tasks)
        
        manager.close()

    def test_fail_task(self):
        manager = SQLPersistenceManager(is_test=True)
        
        # Start workflow first
        manager.start_workflow(self.workflow_id, "test_workflow", {})
        
        # Start a task
        task = TaskOperatorModel(
            task_id="failing_task", operator_type="task", function="test.func"
        )
        manager.start_task(self.workflow_id, task)

        # Fail the task
        manager.fail_task(self.workflow_id, "failing_task", "Test error")
        manager.close()


if __name__ == "__main__":
    unittest.main()