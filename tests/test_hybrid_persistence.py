import os
import unittest
from unittest.mock import patch

os.environ["HIGHWAY_ENV"] = "test"
os.environ["USE_FAKE_REDIS"] = "true"
os.environ["DATABASE_URL"] = "sqlite:///:memory:"

from highway_core.persistence.hybrid_persistence import HybridPersistenceManager
from highway_core.engine.state import WorkflowState
from highway_core.engine.models import TaskOperatorModel


class TestHybridPersistenceManager(unittest.TestCase):

    def setUp(self):
        # Set up test environment
        os.environ["HIGHWAY_ENV"] = "test"
        os.environ["USE_FAKE_REDIS"] = "true"
        os.environ["DATABASE_URL"] = "sqlite:///:memory:"
        self.workflow_id = "test_workflow_123"

    def test_start_workflow(self):
        manager = HybridPersistenceManager()
        manager.start_workflow(self.workflow_id, "test_workflow", {})
        # Check SQL
        workflow_data = manager.sql_persistence.db_manager.load_workflow(self.workflow_id)
        self.assertIsNotNone(workflow_data)
        self.assertEqual(workflow_data["name"], "test_workflow")
        # Check Redis
        redis_data = manager.redis_client.hgetall(f"workflow:{self.workflow_id}")
        self.assertEqual(redis_data["name"], "test_workflow")
        self.assertEqual(redis_data["status"], "running")
        manager.close()

    def test_save_and_load_state_redis_hit(self):
        manager = HybridPersistenceManager()
        state = WorkflowState({"var1": "value1"})
        completed_tasks = {"task1"}
        manager.save_workflow_state(self.workflow_id, state, completed_tasks)

        # Load from Redis
        loaded_state, loaded_tasks = manager.load_workflow_state(self.workflow_id)
        self.assertEqual(loaded_state.variables, state.variables)
        self.assertEqual(loaded_tasks, completed_tasks)
        manager.close()

    def test_load_state_redis_miss_sql_hit(self):
        manager = HybridPersistenceManager()
        # Save directly to SQL to simulate Redis miss
        manager.sql_persistence.start_workflow(self.workflow_id, "test_workflow", {"var2": "value2"})
        task = TaskOperatorModel(task_id="task2", operator_type="task", function="test.func")
        manager.sql_persistence.start_task(self.workflow_id, task)
        manager.sql_persistence.complete_task(self.workflow_id, "task2", "result")

        # Load state
        loaded_state, loaded_tasks = manager.load_workflow_state(self.workflow_id)

        # Check that state is loaded from SQL
        self.assertEqual(loaded_state.variables["var2"], "value2")
        self.assertIn("task2", loaded_tasks)

        # Check that Redis is re-hydrated
        redis_state_json = manager.redis_client.hget(f"workflow:{self.workflow_id}", "state")
        self.assertIsNotNone(redis_state_json)
        redis_state = WorkflowState.model_validate_json(redis_state_json)
        self.assertEqual(redis_state.variables, loaded_state.variables)
        manager.close()

    def test_task_locking(self):
        manager = HybridPersistenceManager()
        task = TaskOperatorModel(task_id="test_task", operator_type="task", function="test.func")

        # First attempt to start task should succeed
        self.assertTrue(manager.start_task(self.workflow_id, task))

        # Second attempt should fail because task is locked
        self.assertFalse(manager.start_task(self.workflow_id, task))

        # Complete the task
        manager.complete_task(self.workflow_id, "test_task", "result")

        # Now we should be able to start it again
        self.assertTrue(manager.start_task(self.workflow_id, task))
        manager.close()


if __name__ == "__main__":
    unittest.main()
