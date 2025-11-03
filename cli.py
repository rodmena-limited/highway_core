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
        # Run the workflow
        run_workflow_from_yaml(yaml_path=str(workflow_path), workflow_run_id=run_id)
        print("-" * 50)
        print("✅ Workflow completed successfully!")

    except Exception as e:
        print(f"❌ Error running workflow: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
