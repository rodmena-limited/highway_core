# --- tools/command.py ---
# Implements 'tools.shell.run' for executing shell commands.

import subprocess
import shlex


def run(command: str) -> dict:
    """
    Runs a shell command and returns its output, error, and status.
    """
    print(f"  [Tool.Shell.Run] Executing: {command}")
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
        print(f"  [Tool.Shell.Run] FAILED with code {e.returncode}")
        return {
            "status_code": e.returncode,
            "stdout": e.stdout,
            "stderr": e.stderr,
        }
    except Exception as e:
        print(f"  [Tool.Shell.Run] FAILED: {e}")
        return {
            "status_code": -1,
            "stdout": "",
            "stderr": str(e),
        }
