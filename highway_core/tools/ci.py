# --- tools/ci.py ---
# Stubs for 'ci.*' functions.


def run_all_tests(workspace: str) -> dict:
    print(f"  [Tool.CI] STUB: Running all tests in {workspace}")
    return {"all_passed": False, "failing_tests": ["test_create_user"]}
