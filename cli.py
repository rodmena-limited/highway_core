#!/usr/bin/env python3
"""
Highway Core CLI - Run workflows from YAML files or highway_dsl Python files
"""

import argparse
import importlib.util
import os
import sys
import uuid
from pathlib import Path

# Add the highway_core to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from highway_core.engine.engine import run_workflow_from_yaml


def load_workflow_from_python(file_path):
    """Load a workflow from a highway_dsl Python file."""
    try:
        # Load the module
        spec = importlib.util.spec_from_file_location("workflow_module", file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Find and call the workflow function (skip WorkflowBuilder class)
        for attr_name in dir(module):
            if attr_name == 'WorkflowBuilder':
                continue  # Skip the WorkflowBuilder class
                
            attr = getattr(module, attr_name)
            if callable(attr) and hasattr(attr, '__name__') and 'workflow' in attr.__name__.lower():
                return attr()
        
        # If no specific function found, try common names
        if hasattr(module, 'demonstrate_failing_workflow'):
            return module.demonstrate_failing_workflow()
        elif hasattr(module, 'demonstrate_workflow'):
            return module.demonstrate_workflow()
        elif hasattr(module, 'create_workflow'):
            return module.create_workflow()
        elif hasattr(module, 'workflow'):
            return module.workflow()
        
        raise ValueError(f"No workflow function found in {file_path}")
        
    except Exception as e:
        raise RuntimeError(f"Failed to load workflow from {file_path}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Run Highway Core workflows from YAML files or highway_dsl Python files"
    )
    parser.add_argument("workflow_path", help="Path to the workflow file (YAML or Python)")
    parser.add_argument(
        "--run-id",
        help="Unique ID for this workflow run (default: auto-generated UUID)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Validate workflow path
    workflow_path = Path(args.workflow_path)
    if not workflow_path.exists():
        print(
            f"Error: Workflow file '{workflow_path}' does not exist.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not workflow_path.is_file():
        print(f"Error: '{workflow_path}' is not a file.", file=sys.stderr)
        sys.exit(1)

    # Generate a run ID if not provided
    run_id = args.run_id or f"cli-run-{str(uuid.uuid4())}"

    print(f"Running workflow from: {workflow_path}")
    print(f"Run ID: {run_id}")
    print("-" * 50)

    try:
        # Determine file type and load workflow
        if workflow_path.suffix.lower() in ['.yaml', '.yml']:
            # Run YAML workflow
            result = run_workflow_from_yaml(
                yaml_path=str(workflow_path), workflow_run_id=run_id
            )
        elif workflow_path.suffix.lower() == '.py':
            # Load and run highway_dsl Python workflow
            workflow = load_workflow_from_python(str(workflow_path))
            
            # Convert to YAML and run
            yaml_content = workflow.to_yaml()
            
            # Create a temporary YAML file
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
                temp_file.write(yaml_content)
                temp_file_path = temp_file.name
            
            try:
                result = run_workflow_from_yaml(
                    yaml_path=temp_file_path, workflow_run_id=run_id
                )
            finally:
                # Clean up temporary file
                os.unlink(temp_file_path)
        else:
            print(f"Error: Unsupported file format '{workflow_path.suffix}'", file=sys.stderr)
            sys.exit(1)

        print("-" * 50)

        # Check workflow status and report properly
        if result["status"] == "completed":
            print("✅ Workflow completed successfully!")
        elif result["status"] == "failed":
            print("❌ Workflow failed!")
            if result.get("error"):
                print(f"Error: {result['error']}")
            sys.exit(1)
        else:
            print(f"⚠️  Workflow finished with status: {result['status']}")
            if result.get("error"):
                print(f"Error: {result['error']}")

        # Get detailed task status from database
        try:
            from highway_core.config import settings
            from highway_core.persistence.database import get_db_manager

            db_manager = get_db_manager(engine_url=settings.DATABASE_URL)

            # Get all tasks for this workflow
            tasks = db_manager.get_tasks_by_workflow(result["workflow_id"])

            if tasks:
                print("\n📋 Task Summary:")
                completed_tasks = [
                    task for task in tasks if task.get("status") == "completed"
                ]
                failed_tasks = [
                    task for task in tasks if task.get("status") == "failed"
                ]

                print(f"  ✅ Completed: {len(completed_tasks)}")
                print(f"  ❌ Failed: {len(failed_tasks)}")
                print(f"  📊 Total: {len(tasks)}")

                if failed_tasks:
                    print("\n❌ Failed Tasks:")
                    for task in failed_tasks:
                        print(
                            f"  - {task['task_id']}: {task.get('error_message', 'Unknown error')}"
                        )

            db_manager.close()

        except Exception as e:
            print(f"\n⚠️  Could not retrieve detailed task information: {e}")

    except Exception as e:
        print(f"❌ Error running workflow: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
