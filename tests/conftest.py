import os

import pytest

DB_PATH = "/tmp/highway_test.sqlite3"


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing test collection and execution.
    """
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    os.environ["HIGHWAY_ENV"] = "test"
    os.environ["REDIS_DB"] = "1"  # Use Redis DB 1 for tests
    os.environ["NO_DOCKER_USE"] = "true"
    os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"
