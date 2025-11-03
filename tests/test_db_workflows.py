import os
import time
import uuid

import pytest

os.environ["HIGHWAY_ENV"] = "test"
os.environ["USE_FAKE_REDIS"] = "true"
os.environ["DATABASE_URL"] = "sqlite:///:memory:"

from highway_core.engine.engine import run_workflow_from_yaml
from highway_core.persistence.hybrid_persistence import HybridPersistenceManager


def run_workflow_and_verify_db(workflow_path: str, expected_workflow_name: str):
    """
    Run a workflow and verify that it's correctly stored in the database.
    """
    run_id = f"test-run-{str(uuid.uuid4())}"

    # Create a persistence manager for the test
    manager = HybridPersistenceManager()

    # Run the workflow
    run_workflow_from_yaml(yaml_path=workflow_path, workflow_run_id=run_id, persistence_manager=manager)

    # Verify in database with retries
    db_manager = manager.sql_persistence.db_manager
    for _ in range(20): # Increased retries
        workflow_data = db_manager.load_workflow(run_id)
        if workflow_data and workflow_data["status"] == "completed":
            tasks = db_manager.get_tasks_by_workflow(run_id)
            if all(task["status"] == "completed" for task in tasks):
                break
        time.sleep(1)

    workflow_data = db_manager.load_workflow(run_id)
    assert workflow_data is not None, f"Workflow run {run_id} not found in database"
    assert workflow_data["name"] == expected_workflow_name
    assert workflow_data["status"] == "completed"

    tasks = db_manager.get_tasks_by_workflow(run_id)
    assert len(tasks) > 0, "No tasks found for workflow in database"

    return run_id, manager


class TestPersistenceWorkflow:
    """Test the persistence workflow functionality using HybridPersistenceManager."""

    def test_persistence_workflow_db(self):
        """Test that persistence workflow runs and stores results in database."""
        workflow_path = "tests/data/persistence_test_workflow.yaml"
        expected_name = "tier_4_persistence_test"

        run_id, manager = run_workflow_and_verify_db(workflow_path, expected_name)

        # Create persistence manager for verification
        db_manager = manager.sql_persistence.db_manager

        # Check that the workflow has the expected variables
        workflow_data = db_manager.load_workflow(run_id)
        assert workflow_data is not None
        variables = workflow_data.get("variables", {})
        assert variables.get("run_id") == "persistent-run-123"

        # Verify all expected tasks are present in the tasks table
        tasks = db_manager.get_tasks_by_workflow(run_id)
        task_ids = {task["task_id"] for task in tasks}

        # The persistence test workflow should have these tasks
        expected_task_ids = {"log_start", "step_2", "log_end"}
        assert expected_task_ids.issubset(
            task_ids
        ), f"Missing tasks in database, expected: {expected_task_ids}, actual: {task_ids}"

        # All tasks should be completed
        for task in tasks:
            assert (
                task["status"] == "completed"
            ), f"Task {task['task_id']} was not completed, status: {task['status']}"

        # The workflow should have completed successfully
        assert run_id is not None
        manager.close()


class TestParallelWaitWorkflow:
    """Test the parallel wait workflow functionality using HybridPersistenceManager."""

    def test_parallel_wait_workflow_db(self):
        """Test that parallel wait workflow runs and stores results in database."""
        workflow_path = "tests/data/parallel_wait_test_workflow.yaml"
        expected_name = "tier_2_parallel_wait_test"

        run_id, manager = run_workflow_and_verify_db(workflow_path, expected_name)

        # Create persistence manager for verification
        db_manager = manager.sql_persistence.db_manager

        # Check that specific parallel and wait tasks exist and are executed
        tasks = db_manager.get_tasks_by_workflow(run_id)

        # All tasks should be completed or in 'executing' status for operators like parallel/wait
        for task in tasks:
            assert task["status"] in [
                "completed",
                "executing",
            ], f"Task {task['task_id']} has unexpected status: {task['status']}"

        assert run_id is not None
        manager.close()


class TestLoopWorkflow:
    """Test the loop workflow functionality using HybridPersistenceManager."""

    def test_loop_workflow_db(self):
        """Test that loop workflow runs and stores results in database."""
        workflow_path = "tests/data/loop_test_workflow.yaml"
        expected_name = "tier_3_loop_test"

        run_id, manager = run_workflow_and_verify_db(workflow_path, expected_name)

        # Create persistence manager for verification
        db_manager = manager.sql_persistence.db_manager

        # Check that while and foreach tasks were correctly recorded
        tasks = db_manager.get_tasks_by_workflow(run_id)

        # Check that while task exists and was executed
        while_tasks = [task for task in tasks if task["operator_type"] == "while"]
        assert (
            len(while_tasks) >= 1
        ), f"Expected at least 1 while task, found: {[t['task_id'] for t in while_tasks]}"

        # Check that foreach task exists and was executed
        foreach_tasks = [task for task in tasks if task["operator_type"] == "foreach"]
        assert (
            len(foreach_tasks) >= 1
        ), f"Expected at least 1 foreach task, found: {[t['task_id'] for t in foreach_tasks]}"

        # Verify that all expected tasks are present
        all_task_ids = {task["task_id"] for task in tasks}

        expected_task_ids = {
            "initialize_counter",
            "main_while_loop",
            "log_while_complete",
            "process_users_foreach",
            "log_end",
        }
        assert expected_task_ids.issubset(
            all_task_ids
        ), f"Missing tasks in database, expected: {expected_task_ids}, actual: {all_task_ids}"

        # All tasks should be completed or in 'executing' status for operators like while/foreach
        for task in tasks:
            assert task["status"] in [
                "completed",
                "executing",
            ], f"Task {task['task_id']} has unexpected status: {task['status']}"

        assert run_id is not None
        manager.close()


class TestWhileWorkflow:
    """Test the while workflow functionality using HybridPersistenceManager."""

    def test_while_workflow_db(self):
        """Test that while workflow runs and stores results in database."""
        workflow_path = "tests/data/while_test_workflow.yaml"
        expected_name = "tier_3_final_test"

        run_id, manager = run_workflow_and_verify_db(workflow_path, expected_name)

        # Create persistence manager for verification
        db_manager = manager.sql_persistence.db_manager

        # Check that while and foreach tasks were correctly recorded
        tasks = db_manager.get_tasks_by_workflow(run_id)

        # Check that while task exists and was executed
        while_tasks = [task for task in tasks if task["operator_type"] == "while"]
        assert (
            len(while_tasks) >= 1
        ), f"Expected at least 1 while task, found: {[t['task_id'] for t in while_tasks]}"

        # Check that foreach task exists and was executed
        foreach_tasks = [task for task in tasks if task["operator_type"] == "foreach"]
        assert (
            len(foreach_tasks) >= 1
        ), f"Expected at least 1 foreach task, found: {[t['task_id'] for t in foreach_tasks]}"

        # Verify that all expected tasks are present
        all_task_ids = {task["task_id"] for task in tasks}

        expected_task_ids = {
            "initialize_counter",
            "main_while_loop",
            "log_while_complete",
            "process_users_foreach",
            "log_end",
        }
        assert expected_task_ids.issubset(
            all_task_ids
        ), f"Missing tasks in database, expected: {expected_task_ids}, actual: {all_task_ids}"

        # All tasks should be completed or in 'executing' status for operators like while/foreach
        for task in tasks:
            assert task["status"] in [
                "completed",
                "executing",
            ], f"Task {task['task_id']} has unexpected status: {task['status']}"

        assert run_id is not None
        manager.close()


if __name__ == "__main__":
    pytest.main([__file__])