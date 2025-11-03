# --- engine/engine.py ---
# Purpose: Main entry point for the Tier 1 workflow execution engine.
# Responsibilities:
# - Loads a workflow definition from YAML.
# - Initializes the orchestrator and state.
# - Starts the workflow execution with bulkhead isolation.

import logging
import uuid
from typing import Any, Dict, Optional

import yaml

from highway_core.persistence.hybrid_persistence import HybridPersistenceManager
from highway_core.tools.bulkhead import BulkheadConfig, BulkheadManager
from highway_core.tools.registry import ToolRegistry

from .models import WorkflowModel
from .orchestrator import Orchestrator

# Configure root logging before importing other modules
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def run_workflow_from_yaml(
    yaml_path: str,
    workflow_run_id: str | None = None,
    persistence_manager: Optional[HybridPersistenceManager] = None,
) -> Dict[str, Any]:
    """
    The main entry point for the Highway Execution Engine with bulkhead isolation.

    Args:
        yaml_path: Path to the workflow YAML file
        workflow_run_id: Optional workflow run ID (will be generated if not provided)
        persistence_manager: Optional persistence manager instance

    Returns:
        Dict containing workflow execution results including status and error information
    """
    logger.info("Engine: Loading workflow from: %s", yaml_path)

    # 1. Load and Parse YAML
    try:
        with open(yaml_path, "r") as f:
            workflow_data = yaml.safe_load(f)

        workflow_model = WorkflowModel.model_validate(workflow_data)
    except Exception as e:
        logger.error("Engine: Failed to load or parse YAML: %s", e)
        return {"status": "failed", "error": f"Failed to load or parse YAML: {str(e)}"}

    # 2. Generate Run ID if not provided
    if not workflow_run_id:
        workflow_run_id = str(uuid.uuid4())
    logger.info("Engine: Starting run for ID: %s", workflow_run_id)

    # 3. Initialize Core Components
    registry = ToolRegistry()
    persistence = persistence_manager or HybridPersistenceManager()

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
        timeout_seconds=120.0,  # Increased for Docker execution overhead
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
        try:
            orchestrator.run()
            # Get final workflow status
            workflow_data = persistence.sql_persistence.db_manager.load_workflow(workflow_run_id)
            return {
                "status": workflow_data.get("status", "completed") if workflow_data else "completed",
                "workflow_id": workflow_run_id,
                "error": workflow_data.get("error_message") if workflow_data else None,
            }
        except Exception as e:
            logger.error(
                "Engine: Workflow %s failed with error: %s", workflow_model.name, e
            )
            return {"status": "failed", "workflow_id": workflow_run_id, "error": str(e)}

    # Execute the workflow run method in the bulkhead
    future = workflow_bulkhead.execute(_execute_workflow, orchestrator)

    try:
        # Wait for the result of the workflow execution
        future.result()
        logger.info("Engine: Workflow %s completed successfully.", workflow_model.name)
        
        # Get final workflow status
        workflow_data = persistence.sql_persistence.db_manager.load_workflow(workflow_run_id)
        return {
            "status": workflow_data.get("status", "completed") if workflow_data else "completed",
            "workflow_id": workflow_run_id,
            "error": workflow_data.get("error_message") if workflow_data else None,
        }
    except Exception as e:
        logger.error(
            "Engine: Workflow %s failed with error: %s", workflow_model.name, e
        )
        # Get final workflow status even on failure
        workflow_data = persistence.sql_persistence.db_manager.load_workflow(workflow_run_id)
        return {
            "status": workflow_data.get("status", "failed") if workflow_data else "failed",
            "workflow_id": workflow_run_id,
            "error": workflow_data.get("error_message", str(e)) if workflow_data else str(e),
        }
    finally:
        bulkhead_manager.shutdown_all()


def _execute_workflow(orchestrator: Orchestrator) -> None:
    """
    Internal function to execute the orchestrator.run() method.
    This is what gets executed inside the bulkhead.
    """
    return orchestrator.run()


if __name__ == "__main__":
    # This would be used for testing
    pass
