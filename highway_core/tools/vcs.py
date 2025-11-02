# --- tools/vcs.py ---
# Stubs for 'vcs.git.*' functions.


import logging

logger = logging.getLogger(__name__)


def clone_and_setup_repo(repo_url: str) -> dict[str, str]:
    logger.info("  [Tool.VCS] STUB: Cloning %s", repo_url)
    return {"workspace": "/tmp/ai-build-123"}


def commit_files(workspace: str, message: str) -> dict[str, str]:
    logger.info("  [Tool.VCS] STUB: Committing to %s: %s", workspace, message)
    return {"commit_hash": "abc1234"}
