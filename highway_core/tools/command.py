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
    Raises an exception if the command fails (non-zero exit code).
    """
    # Import shlex here to ensure it's available in the function scope
    import shlex
    
    # Join the arguments into a single command string
    if len(args) == 1:
        # If there's only one argument, use it directly
        command = args[0]
    else:
        # If there are multiple arguments, join them with spaces like a shell would
        command = " ".join(shlex.quote(str(arg)) for arg in args)

    logger.info("  [Tool.Shell.Run] Executing: %s", command)
    try:
        args_list = shlex.split(command)
        result = subprocess.run(
            args_list,
            capture_output=True,
            text=True,
            check=True,  # This will raise subprocess.CalledProcessError on non-zero exit
            timeout=300,  # 5 min timeout
        )
        # Return just the stdout, stripped of trailing whitespace, which is what the workflow expects
        return result.stdout.rstrip()
    except subprocess.CalledProcessError as e:
        logger.error("  [Tool.Shell.Run] FAILED with code %s, stdout: %s, stderr: %s", e.returncode, e.stdout, e.stderr)
        # Raise the exception to halt the workflow task execution
        raise RuntimeError(f"Command failed with exit code {e.returncode}: {e.stderr}") from e
    except Exception as e:
        logger.error("  [Tool.Shell.Run] FAILED: %s", e)
        raise RuntimeError(f"Command execution failed: {e}") from e
