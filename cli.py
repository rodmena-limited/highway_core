#!/usr/bin/env python3
"""
Highway Core CLI - Run workflows from YAML files
"""

import argparse

# Add the highway_core to the path
import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from highway_core.engine.engine import run_workflow_from_yaml


def main():
    parser = argparse.ArgumentParser(
        description="Run Highway Core workflows from YAML files"
    )
    parser.add_argument("workflow_path", help="Path to the workflow YAML file")
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
        # Run the workflow and get status information
        result = run_workflow_from_yaml(
            yaml_path=str(workflow_path), workflow_run_id=run_id
        )

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
            from highway_core.persistence.database_manager import DatabaseManager

            db_manager = DatabaseManager(engine_url=settings.DATABASE_URL)

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
