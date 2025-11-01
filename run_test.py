# run_test.py
from highway_core.engine.engine import run_workflow_from_yaml

if __name__ == "__main__":
    print("--- Starting Tier 1 Integration Test ---")
    run_workflow_from_yaml("examples/tier_1_5_refactor_test.yaml")
    print("--- Tier 1 Integration Test Finished ---")
