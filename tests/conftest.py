import os

import pytest

DB_PATH = "/tmp/highway_test.sqlite3"


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing test collection and execution.
    """
    print(f"CONFTEST: Setting up test environment, DB_PATH={DB_PATH}")
    if os.path.exists(DB_PATH):
        print(f"CONFTEST: Removing existing database at {DB_PATH}")
        os.remove(DB_PATH)
    os.environ["HIGHWAY_ENV"] = "test"
    os.environ["REDIS_DB"] = "1"  # Use Redis DB 1 for tests
    os.environ["NO_DOCKER_USE"] = "true"
    os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"  # Use 4 slashes for absolute path
    print(f"CONFTEST: DATABASE_URL set to {os.environ['DATABASE_URL']}")
