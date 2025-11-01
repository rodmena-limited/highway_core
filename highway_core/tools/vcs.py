# --- tools/vcs.py ---
# Stubs for 'vcs.git.*' functions.


def clone_and_setup_repo(repo_url: str) -> dict:
    print(f"  [Tool.VCS] STUB: Cloning {repo_url}")
    return {"workspace": "/tmp/ai-build-123"}


def commit_files(workspace: str, message: str) -> dict:
    print(f"  [Tool.VCS] STUB: Committing to {workspace}: {message}")
    return {"commit_hash": "abc1234"}
