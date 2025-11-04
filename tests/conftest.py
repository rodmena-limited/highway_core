import os
import tempfile

import pytest

# Global test database path - will be set per test session
TEST_DB_PATH = None


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing test collection and execution.

    Conditionally sets up the test environment for SQLite (default)
    or PostgreSQL (if USE_PG=true).
    """
    global TEST_DB_PATH

    if os.environ.get("USE_PG", "false").lower() in ("true", "1"):
        # --- PostgreSQL Test Mode ---
        print("\nCONFTEST: USE_PG=true detected. Forcing HIGHWAY_ENV=production.")
        print("CONFTEST: Tests will run against the PostgreSQL DB from .env file.")
        os.environ["HIGHWAY_ENV"] = "production"
        # We DO NOT set DATABASE_URL here. We let config.py read it from .env.

    else:
        # --- Default SQLite Test Mode ---
        # Create a unique database file for this test session
        TEST_DB_PATH = tempfile.mktemp(suffix="_highway_test.sqlite3")
        print(
            f"\nCONFTEST: Defaulting to SQLite. Setting up test environment, DB_PATH={TEST_DB_PATH}"
        )

        os.environ["HIGHWAY_ENV"] = "test"
        os.environ["NO_DOCKER_USE"] = "true"
        # Force the DATABASE_URL to our temp SQLite file
        os.environ["DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH}"
        print(f"CONFTEST: DATABASE_URL set to {os.environ['DATABASE_URL']}")


def pytest_sessionfinish(session, exitstatus):
    """Clean up test database file after session."""
    global TEST_DB_PATH

    if TEST_DB_PATH and os.path.exists(TEST_DB_PATH):
        try:
            os.remove(TEST_DB_PATH)
            print(f"CONFTEST: Cleaned up test database: {TEST_DB_PATH}")
        except OSError as e:
            print(f"CONFTEST: Warning - could not remove {TEST_DB_PATH}: {e}")
