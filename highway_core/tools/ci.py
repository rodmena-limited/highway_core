# --- tools/ci.py ---
# Stubs for 'ci.*' functions.


import logging

logger = logging.getLogger(__name__)


def run_all_tests(workspace: str) -> dict[str, object]:
    logger.info("  [Tool.CI] STUB: Running all tests in %s", workspace)
    return {"all_passed": False, "failing_tests": ["test_create_user"]}
