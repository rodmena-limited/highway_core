# --- tools/command.py ---
# Implements 'tools.shell.run' for executing shell commands.

import logging
import subprocess
import shlex
from .decorators import tool

logger = logging.getLogger(__name__)


@tool()
def run(*args) -> str:
    """
    Runs a shell command and returns its stdout.
    Accepts multiple arguments to form the complete command.
    """
    # Join the arguments into a single command string
    if len(args) == 1:
        # If there's only one argument, use it directly
        command = args[0]
    else:
        # If there are multiple arguments, join them with spaces like a shell would
        import shlex

        command = " ".join(shlex.quote(str(arg)) for arg in args)

    logger.info("  [Tool.Shell.Run] Executing: %s", command)
    try:
        args_list = shlex.split(command)
        result = subprocess.run(
            args_list,
            capture_output=True,
            text=True,
            check=True,
            timeout=300,  # 5 min timeout
        )
        # Return just the stdout, stripped of trailing whitespace, which is what the workflow expects
        return result.stdout.rstrip()
    except subprocess.CalledProcessError as e:
        logger.error("  [Tool.Shell.Run] FAILED with code %s", e.returncode)
        # Return the error output so the workflow can handle it
        error_output = (
            e.stdout + e.stderr
            if e.stdout or e.stderr
            else f"Error: Command failed with code {e.returncode}"
        )
        return error_output.rstrip()
    except Exception as e:
        logger.error("  [Tool.Shell.Run] FAILED: %s", e)
        return f"Error: {e}".rstrip()
