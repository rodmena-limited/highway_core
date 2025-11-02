import os
import pytest
import sqlite3
import uuid
from pathlib import Path
from highway_core.engine.engine import run_workflow_from_yaml


def get_db_path():
    """Get the path to the SQLite database."""
    return os.path.expanduser("~/.highway.sqlite3")


def reset_test_database():
    """Reset the test database before running tests."""
    db_path = get_db_path()
    if os.path.exists(db_path):
        os.remove(db_path)


def run_workflow_and_verify_db(workflow_path: str, expected_workflow_name: str):
    """
    Run a workflow and verify that it's correctly stored in the database.

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
    run_workflow_from_yaml(yaml_path=workflow_path, workflow_run_id=run_id)

    # Verify in database
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    # Check workflow record exists and is completed - use the specific run_id rather than workflow name
    cursor.execute(
        "SELECT workflow_id, workflow_name, status FROM workflows WHERE workflow_id = ?",
        (run_id,),
    )
    workflow_row = cursor.fetchone()
    assert workflow_row is not None, (
        f"Workflow run {run_id} not found in database"
    )

    db_workflow_id, db_workflow_name, status = workflow_row
    assert db_workflow_id == run_id
    assert db_workflow_name == expected_workflow_name
    assert status == "completed", (
        f"Expected workflow status 'completed' but got '{status}'"
    )

    # Verify all tasks for this workflow are in the tasks table
    cursor.execute(
        "SELECT task_id, operator_type, status, result_value_json FROM tasks WHERE workflow_id = ? ORDER BY created_at",
        (run_id,),
    )
    tasks = cursor.fetchall()
    assert len(tasks) > 0, "No tasks found for workflow in database"

    conn.close()

    return run_id


class TestPersistenceWorkflow:
    """Test the persistence workflow functionality."""

    def test_persistence_workflow_db(self):
        """Test that persistence workflow runs and stores results in database."""
        workflow_path = "tests/data/persistence_test_workflow.yaml"
        expected_name = "tier_4_persistence_test"

        run_id = run_workflow_and_verify_db(workflow_path, expected_name)

        # Verify the specific workflow completed successfully
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # Check that the workflow has the expected variables
        cursor.execute(
            "SELECT variables_json FROM workflows WHERE workflow_id = ?", (run_id,)
        )
        row = cursor.fetchone()
        assert row is not None
        assert '"persistent-run-123"' in row[0]  # Check that the variable was stored

        # Verify all expected tasks are present in the tasks table
        cursor.execute(
            "SELECT task_id, status FROM tasks WHERE workflow_id = ?", (run_id,)
        )
        tasks = cursor.fetchall()
        task_ids = {task[0] for task in tasks}

        # The persistence test workflow should have these tasks
        expected_task_ids = {"log_start", "step_2", "log_end"}
        assert expected_task_ids.issubset(task_ids), (
            f"Missing tasks in database, expected: {expected_task_ids}, actual: {task_ids}"
        )

        # All tasks should be completed
        for task_id, status in tasks:
            assert status == "completed", (
                f"Task {task_id} was not completed, status: {status}"
            )

        conn.close()

        # The workflow should have completed successfully
        assert run_id is not None


class TestParallelWaitWorkflow:
    """Test the parallel wait workflow functionality."""

    def test_parallel_wait_workflow_db(self):
        """Test that parallel wait workflow runs and stores results in database."""
        workflow_path = "tests/data/parallel_wait_test_workflow.yaml"
        expected_name = "tier_2_parallel_wait_test"

        run_id = run_workflow_and_verify_db(workflow_path, expected_name)

        # Verify that parallel and wait tasks were correctly recorded
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # Check that specific parallel and wait tasks exist and are executed
        cursor.execute(
            "SELECT task_id, operator_type, status FROM tasks WHERE workflow_id = ? AND operator_type IN ('parallel', 'wait')",
            (run_id,),
        )
        operator_tasks = cursor.fetchall()

        assert len(operator_tasks) >= 2, (
            f"Expected parallel and wait tasks, found: {operator_tasks}"
        )

        # Check that fetch tasks were completed
        cursor.execute(
            "SELECT task_id, result_value_json FROM tasks WHERE workflow_id = ? AND task_id LIKE 'fetch_%' AND status = 'completed'",
            (run_id,),
        )
        fetch_tasks = cursor.fetchall()
        assert len(fetch_tasks) >= 2, (
            f"Expected at least 2 completed fetch tasks, found: {fetch_tasks}"
        )

        # Verify that all expected tasks are present
        cursor.execute(
            "SELECT task_id, status FROM tasks WHERE workflow_id = ?", (run_id,)
        )
        all_tasks = cursor.fetchall()
        all_task_ids = {task[0] for task in all_tasks}

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
        for task_id, status in all_tasks:
            assert status in ["completed", "executing"], (
                f"Task {task_id} has unexpected status: {status}"
            )

        conn.close()

        assert run_id is not None


class TestLoopWorkflow:
    """Test the loop workflow functionality."""

    def test_loop_workflow_db(self):
        """Test that loop workflow runs and stores results in database."""
        workflow_path = "tests/data/loop_test_workflow.yaml"
        expected_name = "tier_3_loop_test"

        run_id = run_workflow_and_verify_db(workflow_path, expected_name)

        # Verify that while and foreach tasks were correctly recorded
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # Check that while task exists and was executed
        cursor.execute(
            "SELECT task_id, operator_type, status FROM tasks WHERE workflow_id = ? AND operator_type = 'while'",
            (run_id,),
        )
        while_tasks = cursor.fetchall()
        assert len(while_tasks) >= 1, (
            f"Expected at least 1 while task, found: {while_tasks}"
        )

        # Check that foreach task exists and was executed
        cursor.execute(
            "SELECT task_id, operator_type, status FROM tasks WHERE workflow_id = ? AND operator_type = 'foreach'",
            (run_id,),
        )
        foreach_tasks = cursor.fetchall()
        assert len(foreach_tasks) >= 1, (
            f"Expected at least 1 foreach task, found: {foreach_tasks}"
        )

        # Verify that all expected tasks are present
        cursor.execute(
            "SELECT task_id, status FROM tasks WHERE workflow_id = ?", (run_id,)
        )
        all_tasks = cursor.fetchall()
        all_task_ids = {task[0] for task in all_tasks}

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
        for task_id, status in all_tasks:
            assert status in ["completed", "executing"], (
                f"Task {task_id} has unexpected status: {status}"
            )

        conn.close()

        assert run_id is not None


class TestWhileWorkflow:
    """Test the while workflow functionality."""

    def test_while_workflow_db(self):
        """Test that while workflow runs and stores results in database."""
        workflow_path = "tests/data/while_test_workflow.yaml"
        expected_name = "tier_3_final_test"

        run_id = run_workflow_and_verify_db(workflow_path, expected_name)

        # Verify that while and foreach tasks were correctly recorded
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # Check that while task exists and was executed
        cursor.execute(
            "SELECT task_id, operator_type, status FROM tasks WHERE workflow_id = ? AND operator_type = 'while'",
            (run_id,),
        )
        while_tasks = cursor.fetchall()
        assert len(while_tasks) >= 1, (
            f"Expected at least 1 while task, found: {while_tasks}"
        )

        # Check that foreach task exists and was executed
        cursor.execute(
            "SELECT task_id, operator_type, status FROM tasks WHERE workflow_id = ? AND operator_type = 'foreach'",
            (run_id,),
        )
        foreach_tasks = cursor.fetchall()
        assert len(foreach_tasks) >= 1, (
            f"Expected at least 1 foreach task, found: {foreach_tasks}"
        )

        # Verify that all expected tasks are present
        cursor.execute(
            "SELECT task_id, status FROM tasks WHERE workflow_id = ?", (run_id,)
        )
        all_tasks = cursor.fetchall()
        all_task_ids = {task[0] for task in all_tasks}

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
        for task_id, status in all_tasks:
            assert status in ["completed", "executing"], (
                f"Task {task_id} has unexpected status: {status}"
            )

        conn.close()

        assert run_id is not None


if __name__ == "__main__":
    pytest.main([__file__])
