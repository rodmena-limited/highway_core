# run_test.py
import logging
from highway_core.engine.engine import run_workflow_from_yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    TEST_YAML = "examples/tier_4_persistence_test.yaml"
    STATIC_RUN_ID = "persistent-run-123"

    print(f"--- Starting Tier 4 Test (Run 1) ---")
    run_workflow_from_yaml(TEST_YAML, workflow_run_id=STATIC_RUN_ID)
    print("--- Tier 4 Test (Run 1) Finished ---")

    print("\n\n--- Starting Tier 4 Test (Run 2: Resume) ---")
    run_workflow_from_yaml(TEST_YAML, workflow_run_id=STATIC_RUN_ID)
    print("--- Tier 4 Test (Run 2: Resume) Finished ---")
