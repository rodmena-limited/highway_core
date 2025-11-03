"""Highway Engine CLI - Command line interface for running highway workflows."""

import sys
import os
import uuid
import time
from pathlib import Path
from typing import Optional
import logging

try:
    from importlib.metadata import version
except ImportError:
    # Python < 3.8
    from importlib_metadata import version

import click
import psutil

from highway_core.engine.engine import run_workflow_from_yaml
from highway_core.utils.docker_detector import is_running_in_docker


def get_system_info():
    """Get detailed system information."""
    cpu_count = psutil.cpu_count(logical=True)
    cpu_freq = psutil.cpu_freq()
    memory = psutil.virtual_memory()
    disk_usage = psutil.disk_usage("/")
    boot_time = psutil.boot_time()

    info = {
        "platform": sys.platform,
        "python_version": sys.version,
        "cpu_count": cpu_count,
        "cpu_freq": f"{cpu_freq.current:.2f} MHz" if cpu_freq else "Unknown",
        "memory_total": f"{memory.total / (1024**3):.2f} GB",
        "memory_available": f"{memory.available / (1024**3):.2f} GB",
        "disk_total": f"{disk_usage.total / (1024**3):.2f} GB",
        "boot_time": time.ctime(boot_time),
        "docker_environment": is_running_in_docker(),
    }
    return info


def format_system_info(info):
    """Format system information for display."""
    lines = [
        "System Information:",
        "=" * 50,
        f"Platform: {info['platform']}",
        f"Python Version: {info['python_version']}",
        f"CPU Count: {info['cpu_count']}",
        f"CPU Frequency: {info['cpu_freq']}",
        f"Total Memory: {info['memory_total']}",
        f"Available Memory: {info['memory_available']}",
        f"Total Disk Space: {info['disk_total']}",
        f"System Boot Time: {info['boot_time']}",
        f"Running in Docker: {info['docker_environment']}",
    ]
    return "\n".join(lines)


@click.group()
@click.version_option(version=version("highway_core"))
def cli():
    """Highway Engine CLI - Run highway workflows with optimized performance."""
    pass


@cli.command()
@click.argument("workflow_path", type=click.Path(exists=True))
@click.option("--db-path", default=None, help="Path to database file")
@click.option(
    "--run-id", default=None, help="Specific run ID (generates random if not provided)"
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--show-system-info", "-s", is_flag=True, help="Show system information")
@click.option(
    "--timeout", default=120.0, type=float, help="Workflow execution timeout in seconds"
)
def start(
    workflow_path: str,
    db_path: Optional[str],
    run_id: Optional[str],
    verbose: bool,
    show_system_info: bool,
    timeout: float,
):
    """Start a highway workflow from a YAML file."""
    # Set up logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger("highway-engine")

    # Show system info if requested
    if show_system_info:
        sys_info = get_system_info()
        click.echo(format_system_info(sys_info))
        click.echo()  # Add a blank line

    # Validate workflow path
    workflow_file = Path(workflow_path)
    if not workflow_file.exists():
        click.echo(f"Error: Workflow file does not exist: {workflow_path}", err=True)
        sys.exit(1)

    if not workflow_file.suffix.lower() in [".yaml", ".yml"]:
        click.echo(f"Error: Workflow file must be YAML: {workflow_path}", err=True)
        sys.exit(1)

    # Generate run ID if not provided
    run_id = run_id or f"engine-run-{str(uuid.uuid4())}"

    click.echo(f"Starting workflow from: {workflow_path}")
    click.echo(f"Run ID: {run_id}")
    click.echo("-" * 50)

    # Prepare database path
    if db_path is None:
        # Use the standard highway database location
        db_path = os.path.expanduser("~/.highway.sqlite3")

    try:
        # Run the workflow
        start_time = time.time()
        logger.info(f"Starting workflow execution with ID: {run_id}")

        run_workflow_from_yaml(
            yaml_path=workflow_path, workflow_run_id=run_id, db_path=db_path
        )

        end_time = time.time()
        execution_time = end_time - start_time

        click.echo()
        click.echo("✅ Workflow completed successfully!")
        click.echo(f"Execution time: {execution_time:.2f} seconds")
        click.echo(f"Database: {db_path}")

    except Exception as e:
        logger.error(f"Workflow execution failed: {str(e)}")
        click.echo(f"\n❌ Workflow failed: {str(e)}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
