# run_tier3_test.py
import logging

from highway_core.engine.engine import run_workflow_from_yaml

if __name__ == "__main__":
    print("--- Starting Tier 3 Loop Test ---")
    run_workflow_from_yaml("examples/tier_3_loop_test.yaml")
    print("--- Tier 3 Loop Test Finished ---")
