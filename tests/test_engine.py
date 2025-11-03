import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from highway_core.engine.engine import (
    _execute_workflow,
    run_workflow_from_yaml,
)
from highway_core.engine.models import WorkflowModel
from highway_core.engine.orchestrator import Orchestrator


def test_run_workflow_from_yaml_valid_file():
    """Test running a workflow from a valid YAML file."""
    # Create a temporary YAML file with a simple workflow
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as temp_file:
        temp_file.write(
            """
name: test_workflow
version: 1.0.0
description: Test workflow
variables:
  test_var: test_value

start_task: log_start

tasks:
  log_start:
    task_id: log_start
    operator_type: task
    function: tools.log.info
    args: ["Starting workflow..."]
    dependencies: []
        """
        )
        temp_file_path = temp_file.name

    try:
        # Mock the Orchestrator and HybridPersistenceManager
        with patch("highway_core.engine.engine.Orchestrator") as mock_orchestrator_class, \
             patch("highway_core.engine.engine.HybridPersistenceManager") as mock_persistence_class:
            
            mock_orchestrator_instance = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator_instance

            run_workflow_from_yaml(temp_file_path)

            # Verify that the orchestrator was initialized and run
            mock_persistence_class.assert_called_once()
            mock_orchestrator_class.assert_called_once()
            mock_orchestrator_instance.run.assert_called_once()
    finally:
        # Clean up the temporary file
        os.remove(temp_file_path)


def test_run_workflow_from_yaml_with_bulkhead_error():
    """Test workflow execution when bulkhead execute fails."""
    # Create a temporary YAML file with a simple workflow
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as temp_file:
        temp_file.write(
            """
name: test_workflow
version: 1.0.0
description: Test workflow
variables:
  test_var: test_value

start_task: log_start

tasks:
  log_start:
    task_id: log_start
    operator_type: task
    function: tools.log.info
    args: ["Starting workflow..."]
    dependencies: []
        """
        )
        temp_file_path = temp_file.name

    try:
        # Mock the bulkhead's future.result() to raise an exception
        with patch("highway_core.engine.engine.BulkheadManager") as mock_bulkhead_manager_class, \
             patch("highway_core.engine.engine.HybridPersistenceManager") as mock_persistence_class:
            mock_persistence_instance = MagicMock()
            mock_persistence_instance.load_workflow_state.return_value = (None, set())
            mock_persistence_class.return_value = mock_persistence_instance

            mock_bulkhead_instance = MagicMock()
            mock_future = MagicMock()
            mock_future.result.side_effect = Exception("Execution failed")
            mock_bulkhead_instance.execute.return_value = mock_future
            mock_bulkhead_manager_class.return_value.create_bulkhead.return_value = (
                mock_bulkhead_instance
            )

            run_workflow_from_yaml(temp_file_path)
    finally:
        # Clean up the temporary file
        os.remove(temp_file_path)


def test_run_workflow_from_yaml_invalid_file():
    """Test running a workflow from an invalid YAML file."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as temp_file:
        temp_file.write("invalid: yaml: [")  # Invalid YAML
        temp_file_path = temp_file.name

    try:
        with patch("highway_core.engine.engine.HybridPersistenceManager") as mock_persistence_class:
            run_workflow_from_yaml(temp_file_path)
    finally:
        # Clean up the temporary file
        os.remove(temp_file_path)


def test_run_workflow_from_yaml_nonexistent_file():
    """Test running a workflow from a non-existent file."""
    with patch("highway_core.engine.engine.HybridPersistenceManager") as mock_persistence_class:
        run_workflow_from_yaml("/nonexistent/path.yaml")


def test_run_workflow_from_yaml_bulkhead_creation_failure():
    """Test workflow execution when bulkhead creation fails."""
    # Create a temporary YAML file with a simple workflow
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as temp_file:
        temp_file.write(
            """
name: test_workflow
version: 1.0.0
description: Test workflow
variables:
  test_var: test_value

start_task: log_start

tasks:
  log_start:
    task_id: log_start
    operator_type: task
    function: tools.log.info
    args: ["Starting workflow..."]
    dependencies: []
        """
        )
        temp_file_path = temp_file.name

    try:
        # Mock the BulkheadManager.create_bulkhead to raise a ValueError
        with patch("highway_core.engine.engine.BulkheadManager") as mock_bulkhead_manager_class, \
             patch("highway_core.engine.engine.HybridPersistenceManager") as mock_persistence_class:
            mock_persistence_instance = MagicMock()
            mock_persistence_instance.load_workflow_state.return_value = (None, set())
            mock_persistence_class.return_value = mock_persistence_instance

            mock_bulkhead_instance = MagicMock()
            mock_bulkhead_manager_class.return_value.create_bulkhead.side_effect = (
                ValueError("Bulkhead creation failed")
            )
            mock_bulkhead_manager_class.return_value.create_bulkhead.return_value = (
                mock_bulkhead_instance
            )

            # Also mock Orchestrator.run to avoid actual execution
            with patch.object(Orchestrator, "run", return_value=None):
                run_workflow_from_yaml(temp_file_path)
    finally:
        # Clean up the temporary file
        os.remove(temp_file_path)


def test_run_workflow_from_yaml_with_execution_error():
    """Test workflow execution that raises an error."""
    # Create a temporary YAML file with a simple workflow
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as temp_file:
        temp_file.write(
            """
name: test_workflow
version: 1.0.0
description: Test workflow
variables:
  test_var: test_value

start_task: log_start

tasks:
  log_start:
    task_id: log_start
    operator_type: task
    function: tools.log.info
    args: ["Starting workflow..."]
    dependencies: []
        """
        )
        temp_file_path = temp_file.name

    try:
        # Mock the bulkhead's future.result() to raise an exception
        with patch("highway_core.engine.engine.BulkheadManager") as mock_bulkhead_manager_class, \
             patch("highway_core.engine.engine.HybridPersistenceManager") as mock_persistence_class:
            mock_persistence_instance = MagicMock()
            mock_persistence_instance.load_workflow_state.return_value = (None, set())
            mock_persistence_class.return_value = mock_persistence_instance

            mock_bulkhead_instance = MagicMock()
            mock_future = MagicMock()
            mock_future.result.side_effect = Exception("Execution failed")
            mock_bulkhead_instance.execute.return_value = mock_future
            mock_bulkhead_manager_class.return_value.create_bulkhead.return_value = (
                mock_bulkhead_instance
            )

            run_workflow_from_yaml(temp_file_path)
    finally:
        # Clean up the temporary file
        os.remove(temp_file_path)


def test_run_workflow_from_yaml_exception():
    """Test running a workflow when yaml loading throws an exception."""
    # Create a temporary YAML file
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as temp_file:
        temp_file.write("some content")
        temp_file_path = temp_file.name

    try:
        # Mock the yaml.safe_load to raise an exception
        with patch("highway_core.engine.engine.yaml.safe_load") as mock_safe_load, \
             patch("highway_core.engine.engine.HybridPersistenceManager") as mock_persistence_class:
            mock_safe_load.side_effect = Exception("YAML load error")
            run_workflow_from_yaml(temp_file_path)
    finally:
        # Clean up the temporary file
        os.remove(temp_file_path)


def test_execute_workflow():
    """Test the internal _execute_workflow function."""
    # Create a mock orchestrator
    mock_orchestrator = MagicMock()
    mock_orchestrator.run.return_value = "success"

    result = _execute_workflow(mock_orchestrator)

    # Verify the orchestrator's run method was called
    mock_orchestrator.run.assert_called_once()
    assert result == "success"


if __name__ == "__main__":
    pytest.main()
