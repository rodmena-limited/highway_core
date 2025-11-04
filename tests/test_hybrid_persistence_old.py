import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import redis

os.environ["HIGHWAY_ENV"] = "test"
os.environ["REDIS_DB"] = "1"  # Use Redis DB 1 for tests
os.environ["NO_DOCKER_USE"] = "true"
os.environ["DATABASE_URL"] = (
    "sqlite:///tests/test_db.sqlite3"  # Use same DB as other tests
)

from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.persistence.hybrid_persistence import HybridPersistenceManager


class TestHybridPersistenceManager(unittest.TestCase):

    def setUp(self):
        # Set up test environment
        os.environ["HIGHWAY_ENV"] = "test"
        os.environ["REDIS_DB"] = "1"  # Use Redis DB 1 for tests
        os.environ["NO_DOCKER_USE"] = "true"
        os.environ["DATABASE_URL"] = (
            "sqlite:///tests/test_db.sqlite3"  # Use same DB as other tests
        )
        self.workflow_id = f"test_workflow_{uuid.uuid4().hex[:12]}"

        # Ensure a clean database for each test
        db_file = Path("tests/test_db.sqlite3")
        if db_file.exists():
            db_file.unlink()

        # Flush Redis DB 1 for a clean state
        try:
            r = redis.Redis(host="localhost", port=6379, db=1)
            r.flushdb()
            r.close()
        except redis.exceptions.ConnectionError:
            # If Redis is not running, tests will fall back to SQL-only mode
            pass

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
        manager = HybridPersistenceManager(is_test=True)
        # Save directly to SQL to simulate Redis miss
        manager.sql_persistence.start_workflow(
            self.workflow_id, "test_workflow", {"var2": "value2"}
        )
        task = TaskOperatorModel(
            task_id="task2", operator_type="task", function="test.func"
        )
        manager.sql_persistence.start_task(self.workflow_id, task)
        manager.sql_persistence.complete_task(self.workflow_id, "task2", "result")

        # Load state
        loaded_state, loaded_tasks = manager.load_workflow_state(self.workflow_id)

        # Check that state is loaded from SQL
        self.assertEqual(loaded_state.variables["var2"], "value2")
        self.assertIn("task2", loaded_tasks)

        # Check that Redis is re-hydrated
        redis_state_json = manager.redis_client.hget(
            f"workflow:{self.workflow_id}", "state"
        )
        self.assertIsNotNone(redis_state_json)
        redis_state = WorkflowState.model_validate_json(redis_state_json)
        self.assertEqual(redis_state.variables, loaded_state.variables)
        manager.close()

    def test_task_locking(self):
        manager = HybridPersistenceManager()
        task = TaskOperatorModel(
            task_id="test_task", operator_type="task", function="test.func"
        )

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
