# --- engine/engine.py ---
# Purpose: Main entry point for the workflow execution engine.
# Responsibilities:
# - Loads a workflow definition.
# - Initializes the orchestrator and state.
# - Starts the workflow execution.

from highway_dsl import Workflow
from .orchestrator import Orchestrator
from .state import WorkflowState
from persistence.manager import PersistenceManager
from persistence.db_storage import DatabasePersistence


def run_workflow(workflow: Workflow, persistence_manager: PersistenceManager = None):
    """
    Loads and runs a Highway workflow.
    """
    print(f"Engine starting workflow: {workflow.name}")

    if persistence_manager is None:
        persistence_manager = DatabasePersistence()  # Default persistence

    state = WorkflowState(workflow.variables)
    orchestrator = Orchestrator(workflow, state, persistence_manager)

    try:
        orchestrator.run()
        print(f"Workflow {workflow.name} completed successfully.")
    except Exception as e:
        print(f"Workflow {workflow.name} failed: {e}")
        # Log error to persistence
    finally:
        # Final state snapshot
        pass


if __name__ == "__main__":
    # This would be used for testing
    pass
