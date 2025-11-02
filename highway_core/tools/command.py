# --- tools/command.py ---
# Implements 'tools.shell.run' for executing shell commands.

import logging
import subprocess
import shlex

logger = logging.getLogger(__name__)


def run(command: str) -> dict[str, object]:
    """
    Runs a shell command and returns its output, error, and status.
    """
    logger.info("  [Tool.Shell.Run] Executing: %s", command)
    try:
        args = shlex.split(command)
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=True,
            timeout=300,  # 5 min timeout
        )
        return {
            "status_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.CalledProcessError as e:
        logger.error("  [Tool.Shell.Run] FAILED with code %s", e.returncode)
        return {
            "status_code": e.returncode,
            "stdout": e.stdout,
            "stderr": e.stderr,
        }
    except Exception as e:
        logger.error("  [Tool.Shell.Run] FAILED: %s", e)
        return {
            "status_code": -1,
            "stdout": "",
            "stderr": str(e),
        }
