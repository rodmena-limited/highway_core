# --- engine/engine.py ---
# Purpose: Main entry point for the Tier 1 workflow execution engine.
# Responsibilities:
# - Loads a workflow definition from YAML.
# - Initializes the orchestrator and state.
# - Starts the workflow execution with bulkhead isolation.

import logging
import yaml
import uuid
from typing import Any
from .models import WorkflowModel
from .state import WorkflowState
from .orchestrator import Orchestrator
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager, BulkheadConfig
from highway_core.persistence.db_storage import DatabasePersistence  # <--- Import

# Configure root logging before importing other modules
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_workflow_from_yaml(yaml_path: str, workflow_run_id: str | None = None) -> None:
    """
    The main entry point for the Highway Execution Engine with bulkhead isolation.
    """
    logger.info("Engine: Loading workflow from: %s", yaml_path)

    # 1. Load and Parse YAML
    try:
        with open(yaml_path, "r") as f:
            workflow_data = yaml.safe_load(f)

        workflow_model = WorkflowModel.model_validate(workflow_data)
    except Exception as e:
        logger.error("Engine: Failed to load or parse YAML: %s", e)
        return

    # 2. Generate Run ID if not provided
    if not workflow_run_id:
        workflow_run_id = str(uuid.uuid4())
    logger.info("Engine: Starting run for ID: %s", workflow_run_id)

    # 3. Initialize Core Components
    registry = ToolRegistry()
    persistence = DatabasePersistence(connection_string=".workflow_state/")

    # The Orchestrator will load or create its own state
    orchestrator = Orchestrator(
        workflow_run_id=workflow_run_id,
        workflow=workflow_model,
        persistence_manager=persistence,
        registry=registry,
    )

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
        logger.error(
            "Engine: Failed to create bulkhead for workflow %s: %s",
            workflow_model.name,
            e,
        )
        # Fallback: run directly without bulkhead
        orchestrator.run()
        return

    # Execute the workflow run method in the bulkhead
    future = workflow_bulkhead.execute(_execute_workflow, orchestrator)

    try:
        # Wait for the result of the workflow execution
        result = future.result()
        logger.info("Engine: Workflow %s completed successfully.", workflow_model.name)
    except Exception as e:
        logger.error(
            "Engine: Workflow %s failed with error: %s", workflow_model.name, e
        )
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
