# --- engine/engine.py ---
# Purpose: Main entry point for the Tier 1 workflow execution engine.
# Responsibilities:
# - Loads a workflow definition from YAML.
# - Initializes the orchestrator and state.
# - Starts the workflow execution.

import yaml
from .models import WorkflowModel
from .state import WorkflowState
from .orchestrator import Orchestrator
from highway_core.tools.registry import ToolRegistry


def run_workflow_from_yaml(yaml_path: str) -> None:
    """
    The main entry point for the Highway Execution Engine.
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

    # 3. Run the workflow
    orchestrator.run()


if __name__ == "__main__":
    # This would be used for testing
    pass
