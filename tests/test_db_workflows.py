import os
import pytest
import uuid
import tempfile
from pathlib import Path
from highway_core.engine.engine import run_workflow_from_yaml
from highway_core.persistence.database_manager import DatabaseManager
import time


def get_db_path():
    """Get the path to the SQLite database for testing."""
    # Use a temporary file to avoid conflicts in parallel tests
    temp_dir = Path(tempfile.gettempdir()) / "highway_tests"
    temp_dir.mkdir(exist_ok=True)
    return str(temp_dir / f"test_db_{os.getpid()}.sqlite3")


def reset_test_database():
    """Reset the test database before running tests."""
    db_path = get_db_path()
    if os.path.exists(db_path):
        os.remove(db_path)


def run_workflow_and_verify_db(workflow_path: str, expected_workflow_name: str):
    """
    Run a workflow and verify that it's correctly stored in the database using SQLAlchemy.

    Args:
        workflow_path: Path to the workflow YAML file
        expected_workflow_name: Expected name of the workflow in the database

    Returns:
        run_id: The run ID of the completed workflow
    """
    # Reset database before running
    reset_test_database()

    # Generate a unique run ID
    run_id = f"test-run-{str(uuid.uuid4())}"

    # Run the workflow
    run_workflow_from_yaml(
        yaml_path=workflow_path, workflow_run_id=run_id, db_path=get_db_path()
    )

    # Add a small delay to ensure all database operations are completed
    time.sleep(0.1)

    # Create database manager instance to verify the contents
    db_manager = DatabaseManager(db_path=get_db_path())

    # Verify in database using the SQLAlchemy-based manager
    max_retries = 10
    last_exception = None
    for attempt in range(max_retries):
        try:
            # Check workflow record exists and is completed
            workflow_data = db_manager.load_workflow(run_id)
            assert workflow_data is not None, (
                f"Workflow run {run_id} not found in database"
            )

            assert workflow_data["workflow_id"] == run_id
            assert workflow_data["name"] == expected_workflow_name
            assert workflow_data["status"] == "completed", (
                f"Expected workflow status 'completed' but got '{workflow_data['status']}'"
            )

            # Verify all tasks for this workflow are in the tasks table
            tasks = db_manager.get_tasks_by_workflow(run_id)
            assert len(tasks) > 0, "No tasks found for workflow in database"

            return run_id
        except Exception as e:
            last_exception = e
            if "database is locked" in str(e) or "database is busy" in str(e):
                # Exponential backoff with jitter
                delay = min(0.01 * (2**attempt) + (time.time() % 0.01), 1.0)
                time.sleep(delay)
                continue
            else:
                raise e

    # If we exhaust all retries, raise the last exception
    raise last_exception


class TestPersistenceWorkflow:
    """Test the persistence workflow functionality using SQLAlchemy."""

    def test_persistence_workflow_db(self):
        """Test that persistence workflow runs and stores results in database."""
        workflow_path = "tests/data/persistence_test_workflow.yaml"
        expected_name = "tier_4_persistence_test"

        run_id = run_workflow_and_verify_db(workflow_path, expected_name)

        # Create database manager instance for verification
        db_manager = DatabaseManager(db_path=get_db_path())

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
        assert expected_task_ids.issubset(task_ids), (
            f"Missing tasks in database, expected: {expected_task_ids}, actual: {task_ids}"
        )

        # All tasks should be completed
        for task in tasks:
            assert task["status"] == "completed", (
                f"Task {task['task_id']} was not completed, status: {task['status']}"
            )

        # The workflow should have completed successfully
        assert run_id is not None


class TestParallelWaitWorkflow:
    """Test the parallel wait workflow functionality using SQLAlchemy."""

    def test_parallel_wait_workflow_db(self):
        """Test that parallel wait workflow runs and stores results in database."""
        workflow_path = "tests/data/parallel_wait_test_workflow.yaml"
        expected_name = "tier_2_parallel_wait_test"

        run_id = run_workflow_and_verify_db(workflow_path, expected_name)

        # Create database manager instance for verification
        db_manager = DatabaseManager(db_path=get_db_path())

        # Check that specific parallel and wait tasks exist and are executed
        tasks = db_manager.get_tasks_by_workflow(run_id)
        operator_tasks = [
            task for task in tasks if task["operator_type"] in ("parallel", "wait")
        ]

        assert len(operator_tasks) >= 2, (
            f"Expected parallel and wait tasks, found: {[t['task_id'] for t in operator_tasks]}"
        )

        # Check that fetch tasks were completed (with potential retry due to parallel execution timing)
        fetch_tasks = [
            task
            for task in tasks
            if task["task_id"].startswith("fetch_") and task["status"] == "completed"
        ]
        # Retry verification if we don't have both fetch tasks completed
        attempts = 0
        max_attempts = 5
        while len(fetch_tasks) < 2 and attempts < max_attempts:
            time.sleep(0.5)  # Additional wait for parallel tasks to complete
            tasks = db_manager.get_tasks_by_workflow(run_id)
            fetch_tasks = [
                task
                for task in tasks
                if task["task_id"].startswith("fetch_")
                and task["status"] == "completed"
            ]
            attempts += 1

        assert len(fetch_tasks) >= 2, (
            f"Expected at least 2 completed fetch tasks, found: {[t['task_id'] for t in fetch_tasks]}. All tasks: {[(t['task_id'], t['status']) for t in tasks]}. Attempted {attempts} retries."
        )

        # Verify that all expected tasks are present
        all_task_ids = {task["task_id"] for task in tasks}

        expected_task_ids = {
            "log_start",
            "run_parallel_fetches",
            "fetch_todo_1",
            "fetch_todo_2",
            "log_todo_2",
            "short_wait",
            "log_parallel_complete",
            "log_end",
        }
        assert expected_task_ids.issubset(all_task_ids), (
            f"Missing tasks in database, expected: {expected_task_ids}, actual: {all_task_ids}"
        )

        # All tasks should be completed or in 'executing' status for operators like parallel/wait
        for task in tasks:
            assert task["status"] in ["completed", "executing"], (
                f"Task {task['task_id']} has unexpected status: {task['status']}"
            )

        assert run_id is not None


class TestLoopWorkflow:
    """Test the loop workflow functionality using SQLAlchemy."""

    def test_loop_workflow_db(self):
        """Test that loop workflow runs and stores results in database."""
        workflow_path = "tests/data/loop_test_workflow.yaml"
        expected_name = "tier_3_loop_test"

        run_id = run_workflow_and_verify_db(workflow_path, expected_name)

        # Create database manager instance for verification
        db_manager = DatabaseManager(db_path=get_db_path())

        # Check that while and foreach tasks were correctly recorded
        tasks = db_manager.get_tasks_by_workflow(run_id)

        # Check that while task exists and was executed
        while_tasks = [task for task in tasks if task["operator_type"] == "while"]
        assert len(while_tasks) >= 1, (
            f"Expected at least 1 while task, found: {[t['task_id'] for t in while_tasks]}"
        )

        # Check that foreach task exists and was executed
        foreach_tasks = [task for task in tasks if task["operator_type"] == "foreach"]
        assert len(foreach_tasks) >= 1, (
            f"Expected at least 1 foreach task, found: {[t['task_id'] for t in foreach_tasks]}"
        )

        # Verify that all expected tasks are present
        all_task_ids = {task["task_id"] for task in tasks}

        expected_task_ids = {
            "initialize_counter",
            "main_while_loop",
            "log_while_complete",
            "process_users_foreach",
            "log_end",
        }
        assert expected_task_ids.issubset(all_task_ids), (
            f"Missing tasks in database, expected: {expected_task_ids}, actual: {all_task_ids}"
        )

        # All tasks should be completed or in 'executing' status for operators like while/foreach
        for task in tasks:
            assert task["status"] in ["completed", "executing"], (
                f"Task {task['task_id']} has unexpected status: {task['status']}"
            )

        assert run_id is not None


class TestWhileWorkflow:
    """Test the while workflow functionality using SQLAlchemy."""

    def test_while_workflow_db(self):
        """Test that while workflow runs and stores results in database."""
        workflow_path = "tests/data/while_test_workflow.yaml"
        expected_name = "tier_3_final_test"

        run_id = run_workflow_and_verify_db(workflow_path, expected_name)

        # Create database manager instance for verification
        db_manager = DatabaseManager(db_path=get_db_path())

        # Check that while and foreach tasks were correctly recorded
        tasks = db_manager.get_tasks_by_workflow(run_id)

        # Check that while task exists and was executed
        while_tasks = [task for task in tasks if task["operator_type"] == "while"]
        assert len(while_tasks) >= 1, (
            f"Expected at least 1 while task, found: {[t['task_id'] for t in while_tasks]}"
        )

        # Check that foreach task exists and was executed
        foreach_tasks = [task for task in tasks if task["operator_type"] == "foreach"]
        assert len(foreach_tasks) >= 1, (
            f"Expected at least 1 foreach task, found: {[t['task_id'] for t in foreach_tasks]}"
        )

        # Verify that all expected tasks are present
        all_task_ids = {task["task_id"] for task in tasks}

        expected_task_ids = {
            "initialize_counter",
            "main_while_loop",
            "log_while_complete",
            "process_users_foreach",
            "log_end",
        }
        assert expected_task_ids.issubset(all_task_ids), (
            f"Missing tasks in database, expected: {expected_task_ids}, actual: {all_task_ids}"
        )

        # All tasks should be completed or in 'executing' status for operators like while/foreach
        for task in tasks:
            assert task["status"] in ["completed", "executing"], (
                f"Task {task['task_id']} has unexpected status: {task['status']}"
            )

        assert run_id is not None


if __name__ == "__main__":
    pytest.main([__file__])
