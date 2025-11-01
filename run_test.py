# run_test.py
import logging

from highway_core.engine.engine import run_workflow_from_yaml

if __name__ == "__main__":
    print("--- Starting Tier 2 Parallel/Wait Test ---")
    run_workflow_from_yaml("examples/tier_2_parallel_wait_test.yaml")
    print("--- Tier 2 Parallel/Wait Test Finished ---")
