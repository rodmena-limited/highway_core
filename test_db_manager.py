#!/usr/bin/env python3
"""
Test script for the new SQLAlchemy-based DatabaseManager
"""

import tempfile
import os
from pathlib import Path
from datetime import datetime
from highway_core.persistence.database_manager import DatabaseManager


def test_database_manager():
    """Test the database manager functionality."""
    # Create a temporary database file for testing
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        tmp_path = tmp_file.name

    try:
        # Initialize database manager with the temporary database
        db_manager = DatabaseManager(db_path=tmp_path)

        print("✓ Database manager initialized successfully")

        # Test workflow operations
        workflow_id = "test_workflow_123"
        success = db_manager.create_workflow(
            workflow_id=workflow_id,
            name="Test Workflow",
            start_task="start",
            variables={"test": "value"},
        )

        if success:
            print("✓ Workflow created successfully")
        else:
            print("✗ Failed to create workflow")
            return False

        # Load the workflow back
        workflow = db_manager.load_workflow(workflow_id)
        if workflow:
            print(f"✓ Workflow loaded: {workflow['name']}")
        else:
            print("✗ Failed to load workflow")
            return False

        # Test task operations
        task_id = "test_task_123"
        success = db_manager.create_task(
            workflow_id=workflow_id,
            task_id=task_id,
            operator_type="python",
            function="test_function",
        )

        if success:
            print("✓ Task created successfully")
        else:
            print("✗ Failed to create task")
            return False

        # Update task status
        success = db_manager.update_task_status(task_id, "completed")
        if success:
            print("✓ Task status updated successfully")
        else:
            print("✗ Failed to update task status")
            return False

        # Get tasks by workflow
        tasks = db_manager.get_tasks_by_workflow(workflow_id)
        if tasks:
            print(f"✓ Retrieved {len(tasks)} task(s) for workflow")
        else:
            print("✗ Failed to retrieve tasks")
            return False

        # Test storing and retrieving results
        success = db_manager.store_result(
            workflow_id, task_id, "test_result", {"data": "value"}
        )
        if success:
            print("✓ Result stored successfully")
        else:
            print("✗ Failed to store result")
            return False

        results = db_manager.load_results(workflow_id)
        if results:
            print(f"✓ Retrieved results: {results}")
        else:
            print("✗ Failed to retrieve results")
            return False

        # Test storing and retrieving memory
        success = db_manager.store_memory(
            workflow_id, "test_memory", {"state": "value"}
        )
        if success:
            print("✓ Memory stored successfully")
        else:
            print("✗ Failed to store memory")
            return False

        memory = db_manager.load_memory(workflow_id)
        if memory:
            print(f"✓ Retrieved memory: {memory}")
        else:
            print("✗ Failed to retrieve memory")
            return False

        # Test task dependencies
        success = db_manager.store_dependencies(workflow_id, task_id, ["dep1", "dep2"])
        if success:
            print("✓ Dependencies stored successfully")
        else:
            print("✗ Failed to store dependencies")
            return False

        deps = db_manager.get_dependencies(task_id)
        if deps:
            print(f"✓ Retrieved dependencies: {deps}")
        else:
            print("✗ Failed to retrieve dependencies")
            return False

        # Close the database connections
        db_manager.close_all_connections()
        print("✓ Database connections closed successfully")

        print(
            "\n🎉 All tests passed! The SQLAlchemy-based DatabaseManager is working correctly."
        )
        return True

    except Exception as e:
        print(f"✗ Error during testing: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Clean up the temporary database file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
            print(f"✓ Temporary database file {tmp_path} cleaned up")


if __name__ == "__main__":
    success = test_database_manager()
    exit(0 if success else 1)
