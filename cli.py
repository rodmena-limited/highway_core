#!/usr/bin/env python3
"""
Highway Core CLI - Run workflows from YAML files or highway_dsl Python files
"""

import argparse
import asyncio
import importlib.util
import os
import sys
import uuid
from pathlib import Path

# --- THIS IS THE MODIFIED BLOCK ---
# Add the project root to the path
# This ensures that `from highway_core...` imports work
# when running `python cli.py` from the root directory.
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --------------------------------------

from highway_core.engine.engine import run_workflow_from_yaml
from highway_core.persistence.webhook_runner import run_webhook_runner

# New imports for durable mode
import threading
import subprocess
import json
import time


def load_workflow_from_python(file_path):
    """Load a workflow from a highway_dsl Python file."""
    try:
        # Load the module
        spec = importlib.util.spec_from_file_location("workflow_module", file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Find and call the workflow function (skip WorkflowBuilder class)
        for attr_name in dir(module):
            if attr_name == "WorkflowBuilder":
                continue  # Skip the WorkflowBuilder class

            attr = getattr(module, attr_name)
            if (
                callable(attr)
                and hasattr(attr, "__name__")
                and "workflow" in attr.__name__.lower()
            ):
                return attr()

        # If no specific function found, try common names
        if hasattr(module, "demonstrate_failing_workflow"):
            return module.demonstrate_failing_workflow()
        elif hasattr(module, "demonstrate_workflow"):
            return module.demonstrate_workflow()
        elif hasattr(module, "create_workflow"):
            return module.create_workflow()
        elif hasattr(module, "workflow"):
            return module.workflow()

        raise ValueError(f"No workflow function found in {file_path}")

    except Exception as e:
        raise RuntimeError(f"Failed to load workflow from {file_path}: {e}")


def run_local_workflow(args):
    """Existing 'run' command logic for LOCAL mode."""
    workflow_path = Path(args.workflow_path)
    if not workflow_path.exists():
        print(f"Error: Workflow file '{workflow_path}' does not exist.", file=sys.stderr)
        sys.exit(1)
    
    run_id = args.run_id or f"cli-run-{str(uuid.uuid4())}"
    print(f"Running LOCAL workflow from: {workflow_path} (Run ID: {run_id})")
    print("-" * 50)
    
    try:
        if workflow_path.suffix.lower() in [".yaml", ".yml"]:
            result = run_workflow_from_yaml(
                yaml_path=str(workflow_path), workflow_run_id=run_id
            )
        elif workflow_path.suffix.lower() == ".py":
            workflow = load_workflow_from_python(str(workflow_path))
            yaml_content = workflow.to_yaml()
            import tempfile
            with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as temp_file:
                temp_file.write(yaml_content)
                temp_file_path = temp_file.name
            try:
                result = run_workflow_from_yaml(
                    yaml_path=temp_file_path, workflow_run_id=run_id
                )
            finally:
                os.unlink(temp_file_path)
        else:
            print(f"Error: Unsupported file format '{workflow_path.suffix}'", file=sys.stderr)
            sys.exit(1)
        
        print("-" * 50)
        if result["status"] == "completed":
            print("✅ LOCAL Workflow completed successfully!")
        elif result["status"] == "failed":
            print(f"❌ LOCAL Workflow failed: {result.get('error', 'Unknown error')}")
            sys.exit(1)
        
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

        except ImportError as e:
            print(
                f"\n⚠️  Could not import required modules for detailed task information: {e}"
            )
        except Exception as e:
            print(f"\n⚠️  Could not retrieve detailed task information: {e}")
        
    except Exception as e:
        print(f"❌ Error running LOCAL workflow: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def run_durable_services():
    """Starts Scheduler and Worker in separate threads."""
    print("Starting durable services (Scheduler + Worker)... Press Ctrl+C to stop.")
    
    def start_scheduler():
        from highway_core.services.scheduler import Scheduler
        Scheduler().run()
        
    def start_worker():
        from highway_core.services.worker import Worker
        Worker().run()
        
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True, name="SchedulerThread")
    worker_thread = threading.Thread(target=start_worker, daemon=True, name="WorkerThread")
    
    try:
        scheduler_thread.start()
        worker_thread.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down services...")


def run_durable_api():
    """Starts the Flask API server."""
    print("Starting Highway API server on http://127.0.0.1:5000")
    try:
        # We assume cli.py is in the root, so highway_core/api/app.py is the path
        api_path = os.path.join(os.path.dirname(__file__), "highway_core", "api", "app.py")
        if not os.path.exists(api_path):
            print(f"Error: Could not find API app at {api_path}")
            sys.exit(1)
        
        # Use sys.executable to ensure we use the same python env
        subprocess.run([sys.executable, "-m", "flask", "--app", api_path, "run", "--host=0.0.0.0", "--port=5000"], check=True)
    except KeyboardInterrupt:
        print("\nAPI server stopped.")
        
def submit_durable_workflow(args):
    """Client command to submit a new workflow to the API."""
    import requests
    try:
        url = f"http://127.0.0.1:5000/workflow/start/{args.workflow_name}"
        response = requests.post(url)
        print(json.dumps(response.json(), indent=2))
    except requests.ConnectionError:
        print(f"Error: Could not connect to Highway API at {url}. Is it running?")
        print("You can run it with: python cli.py durable start-api")
    except Exception as e:
        print(f"Error submitting workflow: {e}")


def resume_durable_workflow(args):
    """Client command to resume a waiting task."""
    import requests
    try:
        url = f"http://127.0.0.1:5000/workflow/resume?token={args.token}&decision={args.decision}"
        response = requests.get(url)
        print(json.dumps(response.json(), indent=2))
    except requests.ConnectionError:
        print(f"Error: Could not connect to Highway API at {url}. Is it running?")
        print("You can run it with: python cli.py durable start-api")
    except Exception as e:
        print(f"Error resuming workflow: {e}")


def run_webhooks_command(args):
    """Run the webhook runner."""
    print("Starting webhook runner...")
    try:
        asyncio.run(run_webhook_runner())
    except KeyboardInterrupt:
        print("\nWebhook runner stopped by user.")
    except Exception as e:
        print(f"❌ Error running webhook runner: {e}", file=sys.stderr)
        sys.exit(1)


def webhook_status_command(args):
    """Show status of webhook processing."""
    from highway_core.config import settings
    from highway_core.persistence.database import get_db_manager

    try:
        db_manager = get_db_manager(engine_url=settings.DATABASE_URL)

        # Get counts for different webhook statuses
        pending_count = len(db_manager.get_webhooks_by_status(["pending"]))
        sending_count = len(db_manager.get_webhooks_by_status(["sending"]))
        retry_scheduled_count = len(
            db_manager.get_webhooks_by_status(["retry_scheduled"])
        )
        sent_count = len(db_manager.get_webhooks_by_status(["sent"]))
        failed_count = len(db_manager.get_webhooks_by_status(["failed"]))

        print("📊 Webhook Status:")
        print(f"  📥 Pending: {pending_count}")
        print(f"  🚀 Sending: {sending_count}")
        print(f"  🔄 Retry Scheduled: {retry_scheduled_count}")
        print(f"  ✅ Sent: {sent_count}")
        print(f"  ❌ Failed: {failed_count}")

        db_manager.close()

    except Exception as e:
        print(f"❌ Error retrieving webhook status: {e}", file=sys.stderr)
        sys.exit(1)


def webhook_cleanup_command(args):
    """Clean up old completed webhooks."""
    from highway_core.config import settings
    from highway_core.persistence.database import get_db_manager

    try:
        db_manager = get_db_manager(engine_url=settings.DATABASE_URL)

        deleted_count = db_manager.cleanup_completed_webhooks(days_old=args.days_old)

        print(
            f"🧹 Cleaned up {deleted_count} completed webhooks older than {args.days_old} days"
        )

        db_manager.close()

    except Exception as e:
        print(f"❌ Error cleaning up webhooks: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Highway Core - Run workflows from YAML files or highway_dsl Python files"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands", required=True)
    
    # --- 'run' command (LOCAL MODE) ---
    run_parser = subparsers.add_parser(
        "run", 
        help="Run a workflow in-memory (LOCAL mode)"
    )
    run_parser.add_argument(
        "workflow_path", help="Path to the workflow file (YAML or Python)"
    )
    run_parser.add_argument(
        "--run-id",
        help="Unique ID for this workflow run (default: auto-generated UUID)",
    )
    run_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    
    # --- 'durable' command group (DURABLE MODE) ---
    durable_parser = subparsers.add_parser(
        "durable", 
        help="Run durable services (Scheduler, Worker, API)"
    )
    durable_sub = durable_parser.add_subparsers(dest="durable_cmd", help="Durable mode commands", required=True)
    
    # 'durable start-services'
    durable_sub.add_parser(
        "start-services", 
        help="Starts the Scheduler and Worker daemons"
    )
    
    # 'durable start-api'
    durable_sub.add_parser(
        "start-api",
        help="Starts the Flask API server"
    )
    
    # 'durable submit'
    submit_parser = durable_sub.add_parser(
        "submit",
        help="Submits a new durable workflow via the API"
    )
    submit_parser.add_argument(
        "workflow_name", 
        help="Name of the workflow file (e.g., 'long_running_with_pause_and_resume_workflow')"
    )
    
    # 'durable resume'
    resume_parser = durable_sub.add_parser(
        "resume",
        help="Resumes a waiting human task via the API"
    )
    resume_parser.add_argument("--token", required=True, help="The event token")
    resume_parser.add_argument("--decision", required=True, choices=["APPROVED", "DENIED"], help="The decision")

    # --- 'webhooks' command group (unchanged) ---
    webhook_parser = subparsers.add_parser("webhooks", help="Webhook management commands")
    webhook_subparsers = webhook_parser.add_subparsers(dest="webhook_cmd", help="Webhook commands")
    webhook_run_parser = webhook_subparsers.add_parser("run", help="Run the webhook runner")
    webhook_run_parser.add_argument(
        "--batch-size", type=int, default=100, help="Number of webhooks to process in each batch (default: 100)"
    )
    webhook_run_parser.add_argument(
        "--concurrency", type=int, default=20, help="Number of concurrent webhook requests (default: 20)"
    )
    webhook_status_parser = webhook_subparsers.add_parser("status", help="Show webhook processing status")
    webhook_cleanup_parser = webhook_subparsers.add_parser("cleanup", help="Clean up old completed webhooks")
    webhook_cleanup_parser.add_argument(
        "--days-old", type=int, default=30, help="Clean webhooks older than this many days (default: 30)"
    )
    
    args = parser.parse_args()

    if args.command == "run":
        run_local_workflow(args)
    elif args.command == "durable":
        if args.durable_cmd == "start-services":
            run_durable_services()
        elif args.durable_cmd == "start-api":
            run_durable_api()
        elif args.durable_cmd == "submit":
            submit_durable_workflow(args)
        elif args.durable_cmd == "resume":
            resume_durable_workflow(args)
    elif args.command == "webhooks":
        if args.webhook_cmd == "run":
            run_webhooks_command(args)
        elif args.webhook_cmd == "status":
            webhook_status_command(args)
        elif args.webhook_cmd == "cleanup":
            webhook_cleanup_command(args)
        else:
            webhook_parser.print_help()


if __name__ == "__main__":
    main()
