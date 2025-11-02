#!/usr/bin/env python3
"""
Test script to run the container workflow.
"""
from highway_core.engine.engine import run_workflow_from_yaml

def test_container_workflow():
    """Test the container workflow that fetches URLs"""
    print("Testing container workflow that fetches URLs...")
    try:
        run_workflow_from_yaml("examples/container_test.yaml", "container-test-fresh-2")
        print("Container Workflow Test: PASSED")
        return True
    except Exception as e:
        print(f"Container Workflow Test: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_container_workflow()
