"""
Integration tests for SQL persistence manager using real database.
These tests verify that the SQL persistence manager works correctly
with actual database operations.
"""

import os
import pytest
from datetime import datetime, timezone

from highway_core.persistence.sql_persistence_manager import SQLPersistenceManager
from highway_core.engine.state import WorkflowState
from highway_core.persistence.models import Workflow, Task, WorkflowResult, WorkflowMemory


class TestSQLPersistenceIntegration:
    """Integration tests for SQL persistence manager."""

    @pytest.fixture
    def persistence_manager(self):
        """Create a SQL persistence manager for testing."""
        # Use the test database configured in conftest.py
        manager = SQLPersistenceManager(is_test=True)
        
        # For SQLite, we need to ensure the database is properly initialized
        # before attempting cleanup operations
        if manager.sql_persistence.db_manager.engine_url.startswith("sqlite://"):
            # Wait a moment for any pending operations to complete
            import time
            time.sleep(0.1)
            
        # Clean up any existing data before each test
        try:
            with manager.sql_persistence.db_manager.session_scope() as session:
                # Delete in correct order to respect foreign key constraints
                session.query(WorkflowResult).delete()
                session.query(WorkflowMemory).delete()
                session.query(Task).delete()
                session.query(Workflow).delete()
                session.commit()
        except Exception as e:
            # If cleanup fails due to locking, skip cleanup for this test
            # The database will be recreated for the next test session anyway
            print(f"Warning: Could not clean up database before test: {e}")
            
        yield manager
        manager.close()

    def test_workflow_lifecycle(self, persistence_manager):
        """Test complete workflow lifecycle: start, update, complete."""
        workflow_id = "test_workflow_lifecycle_123"
        workflow_name = "test_lifecycle_workflow"
        initial_variables = {"var1": "value1", "counter": 42}

        # Start workflow
        persistence_manager.start_workflow(workflow_id, workflow_name, initial_variables)
        
        # Load workflow state
        state, completed_tasks = persistence_manager.load_workflow_state(workflow_id)
        
        assert state is not None
        assert state.variables == initial_variables
        assert len(completed_tasks) == 0

        # Update workflow state
        updated_variables = {"var1": "updated_value", "counter": 43}
        state.variables = updated_variables
        persistence_manager.save_workflow_state(workflow_id, state, completed_tasks)
        
        # Load updated state
        state, completed_tasks = persistence_manager.load_workflow_state(workflow_id)
        assert state.variables == updated_variables

        # Complete workflow
        persistence_manager.complete_workflow(workflow_id)
        
        # Verify workflow is completed by checking if it can be loaded
        # (completed workflows should still be loadable)
        state, completed_tasks = persistence_manager.load_workflow_state(workflow_id)
        assert state is not None

    def test_task_operations(self, persistence_manager):
        """Test task creation, update, and completion operations."""
        workflow_id = "test_task_ops_456"
        task_id = "test_task_1"
        
        # Start workflow first
        persistence_manager.start_workflow(workflow_id, "test_task_workflow", {})
        
        # Create a simple task model (we'll use a minimal mock)
        from highway_core.engine.models import TaskOperatorModel
        
        task = TaskOperatorModel(
            task_id=task_id,
            operator_type="task",
            function="test_function",
            args=[],
            dependencies=[]
        )
        
        # Start task
        success = persistence_manager.start_task(workflow_id, task)
        assert success is True  # Task should start successfully
        
        # Complete task with result
        result = {"status": "success", "data": {"result": "task_completed"}}
        persistence_manager.complete_task(workflow_id, task_id, result)
        
        # Verify task is in completed tasks
        state, completed_tasks = persistence_manager.load_workflow_state(workflow_id)
        assert task_id in completed_tasks

    def test_workflow_memory_operations(self, persistence_manager):
        """Test workflow memory (variable) storage and retrieval."""
        workflow_id = "test_memory_789"
        
        # Start workflow
        persistence_manager.start_workflow(workflow_id, "test_memory_workflow", {})
        
        # Set memory values
        persistence_manager.set_memory(workflow_id, "user_name", "John Doe")
        persistence_manager.set_memory(workflow_id, "score", 100)
        persistence_manager.set_memory(workflow_id, "config", {"debug": True, "timeout": 30})
        
        # Get memory values
        user_name = persistence_manager.get_memory(workflow_id, "user_name")
        score = persistence_manager.get_memory(workflow_id, "score")
        config = persistence_manager.get_memory(workflow_id, "config")
        
        assert user_name == "John Doe"
        assert score == 100
        assert config == {"debug": True, "timeout": 30}
        
        # Get non-existent memory
        non_existent = persistence_manager.get_memory(workflow_id, "non_existent")
        assert non_existent is None

    def test_multiple_workflows_isolation(self, persistence_manager):
        """Test that multiple workflows are properly isolated."""
        workflow_id_1 = "test_iso_workflow_1"
        workflow_id_2 = "test_iso_workflow_2"
        
        # Start two workflows
        persistence_manager.start_workflow(workflow_id_1, "workflow1", {"var": "value1"})
        persistence_manager.start_workflow(workflow_id_2, "workflow2", {"var": "value2"})
        
        # Set different memory values
        persistence_manager.set_memory(workflow_id_1, "shared_key", "workflow1_value")
        persistence_manager.set_memory(workflow_id_2, "shared_key", "workflow2_value")
        
        # Verify isolation
        value1 = persistence_manager.get_memory(workflow_id_1, "shared_key")
        value2 = persistence_manager.get_memory(workflow_id_2, "shared_key")
        
        assert value1 == "workflow1_value"
        assert value2 == "workflow2_value"
        
        # Complete one workflow
        persistence_manager.complete_workflow(workflow_id_1)
        
        # Verify the other workflow is still accessible
        state2, _ = persistence_manager.load_workflow_state(workflow_id_2)
        assert state2 is not None
        # Note: WorkflowState doesn't have a status attribute, so we only verify it exists

    def test_workflow_results_storage(self, persistence_manager):
        """Test workflow results storage and retrieval."""
        workflow_id = "test_results_999"
        
        # Start workflow
        persistence_manager.start_workflow(workflow_id, "test_results_workflow", {})
        
        # Store workflow result
        result_data = {
            "status": "success",
            "output": {"final_value": 42, "messages": ["step1", "step2", "step3"]},
            "metrics": {"duration": 1.5, "tasks_completed": 3}
        }
        
        persistence_manager.store_workflow_result(workflow_id, result_data)
        
        # Retrieve workflow result
        result = persistence_manager.get_workflow_result(workflow_id)
        
        assert result is not None
        assert result["status"] == "success"
        assert result["output"]["final_value"] == 42
        assert len(result["output"]["messages"]) == 3
        assert result["metrics"]["tasks_completed"] == 3

    def test_concurrent_database_access(self, persistence_manager):
        """Test that the database can handle concurrent access patterns."""
        workflow_id = "test_concurrent_111"
        
        # Start workflow
        persistence_manager.start_workflow(workflow_id, "test_concurrent_workflow", {})
        
        # Perform multiple operations in sequence (simulating concurrent access)
        for i in range(10):
            persistence_manager.set_memory(workflow_id, f"counter_{i}", i)
            persistence_manager.set_memory(workflow_id, "last_update", datetime.now(timezone.utc).isoformat())
        
        # Verify all values were stored
        for i in range(10):
            value = persistence_manager.get_memory(workflow_id, f"counter_{i}")
            assert value == i
        
        # Verify last update was stored
        last_update = persistence_manager.get_memory(workflow_id, "last_update")
        assert last_update is not None

    def test_error_handling(self, persistence_manager):
        """Test error handling for invalid operations."""
        non_existent_workflow = "non_existent_workflow_999"
        
        # Try to load state for non-existent workflow
        state, completed_tasks = persistence_manager.load_workflow_state(non_existent_workflow)
        assert state is None
        assert completed_tasks == set()  # It's a set, not a list
        
        # Try to get memory for non-existent workflow
        memory = persistence_manager.get_memory(non_existent_workflow, "some_key")
        assert memory is None
        
        # Try to complete non-existent workflow (should not crash)
        try:
            persistence_manager.complete_workflow(non_existent_workflow)
            # Should not raise an exception
        except Exception as e:
            pytest.fail(f"Completing non-existent workflow should not raise exception: {e}")

    def test_database_persistence_across_operations(self, persistence_manager):
        """Test that data persists across multiple manager operations."""
        workflow_id = "test_persistence_222"
        
        # Start workflow and store data
        persistence_manager.start_workflow(workflow_id, "test_persistence_workflow", {"initial": "data"})
        persistence_manager.set_memory(workflow_id, "persistent_key", "persistent_value")
        
        # Close and reopen manager (simulating application restart)
        persistence_manager.close()
        
        # Create new manager instance
        new_manager = SQLPersistenceManager(is_test=True)
        
        # Verify data persisted
        state, _ = new_manager.load_workflow_state(workflow_id)
        assert state is not None
        assert state.variables["initial"] == "data"
        
        memory_value = new_manager.get_memory(workflow_id, "persistent_key")
        assert memory_value == "persistent_value"
        
        new_manager.close()