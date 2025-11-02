#!/usr/bin/env python3
"""
Test script to verify the new executor functionality works correctly.
"""
import os
from highway_core.engine.engine import run_workflow_from_yaml
from highway_core.engine.models import WorkflowModel
from highway_core.engine.orchestrator import Orchestrator
from highway_core.tools.registry import ToolRegistry
from highway_core.persistence.db_storage import DatabasePersistence

def test_python_runtime():
    """Test Python runtime functionality"""
    print("Testing Python Runtime...")
    try:
        run_workflow_from_yaml("test_python.yaml", workflow_run_id="test-python-1")
        print("Python Runtime Test: PASSED")
        return True
    except Exception as e:
        print(f"Python Runtime Test: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return False

def test_docker_runtime():
    """Test Docker runtime functionality"""
    print("\nTesting Docker Runtime...")
    try:
        run_workflow_from_yaml("test_docker.yaml", workflow_run_id="test-docker-1")
        print("Docker Runtime Test: PASSED")
        return True
    except Exception as e:
        print(f"Docker Runtime Test: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return False

def test_chaining_runtime():
    """Test chaining of Python and Docker runtimes"""
    print("\nTesting Runtime Chaining...")
    try:
        run_workflow_from_yaml("test_chaining.yaml", workflow_run_id="test-chaining-1")
        print("Runtime Chaining Test: PASSED")
        return True
    except Exception as e:
        print(f"Runtime Chaining Test: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Starting Executor Tests...")
    
    results = []
    results.append(test_python_runtime())
    results.append(test_docker_runtime())
    results.append(test_chaining_runtime())
    
    passed = sum(results)
    total = len(results)
    
    print(f"\nTest Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("All tests passed! Executor functionality is working correctly.")
    else:
        print("Some tests failed. Please check the implementation.")