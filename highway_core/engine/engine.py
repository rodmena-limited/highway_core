# --- engine/engine.py ---
# Purpose: Main entry point for the Tier 1 workflow execution engine.
# Responsibilities:
# - Loads a workflow definition from YAML.
# - Initializes the orchestrator and state.
# - Starts the workflow execution with bulkhead isolation.

import logging
import yaml
from .models import WorkflowModel
from .state import WorkflowState
from .orchestrator import Orchestrator
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager, BulkheadConfig


def run_workflow_from_yaml(yaml_path: str) -> None:
    """
    The main entry point for the Highway Execution Engine with bulkhead isolation.
    """
    print(f"Engine: Loading workflow from: {yaml_path}")

    # 1. Load and Parse YAML
    try:
        with open(yaml_path, "r") as f:
            workflow_data = yaml.safe_load(f)

        workflow_model = WorkflowModel.model_validate(workflow_data)
    except Exception as e:
        print(f"Engine: Failed to load or parse YAML: {e}")
        return

    # 2. Initialize Core Components
    registry = ToolRegistry()
    state = WorkflowState(workflow_model.variables)
    orchestrator = Orchestrator(workflow_model, state, registry)

    # 3. Execute the workflow in a bulkhead for isolation
    bulkhead_manager = BulkheadManager()

    # Create a unique bulkhead for this workflow
    workflow_bulkhead_config = BulkheadConfig(
        name=f"workflow-{workflow_model.name}",
        max_concurrent_calls=5,  # Reasonable default for workflow tasks
        max_queue_size=20,  # Reasonable default queue size
        timeout_seconds=30.0,  # Prevent indefinite hangs
        failure_threshold=3,  # Allow some failures before isolation
        success_threshold=2,  # Number of successes to exit degraded state
        isolation_duration=10.0,  # Duration to isolate after failures
    )

    try:
        workflow_bulkhead = bulkhead_manager.create_bulkhead(workflow_bulkhead_config)
    except ValueError as e:
        print(
            f"Engine: Failed to create bulkhead for workflow {workflow_model.name}: {e}"
        )
        # Fallback: run directly without bulkhead
        orchestrator.run()
        return

    # Execute the workflow run method in the bulkhead
    future = workflow_bulkhead.execute(_execute_workflow, orchestrator)

    try:
        # Wait for the result of the workflow execution
        result = future.result()
        print(f"Engine: Workflow {workflow_model.name} completed successfully.")
    except Exception as e:
        print(f"Engine: Workflow {workflow_model.name} failed with error: {e}")
    finally:
        bulkhead_manager.shutdown_all()


def _execute_workflow(orchestrator: Orchestrator):
    """
    Internal function to execute the orchestrator.run() method.
    This is what gets executed inside the bulkhead.
    """
    return orchestrator.run()


if __name__ == "__main__":
    # This would be used for testing
    pass
