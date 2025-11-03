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
    os.environ["USE_FAKE_REDIS"] = "true"
    os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"
