import os

import pytest

DB_PATH = "/tmp/highway_test.sqlite3"


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing test collection and execution.
    
    Conditionally sets up the test environment for SQLite (default)
    or PostgreSQL (if USE_PG=true).
    """
    
    if os.environ.get("USE_PG", "false").lower() in ("true", "1"):
        # --- PostgreSQL Test Mode ---
        print("\nCONFTEST: USE_PG=true detected. Forcing HIGHWAY_ENV=production.")
        print("CONFTEST: Tests will run against the PostgreSQL DB from .env file.")
        os.environ["HIGHWAY_ENV"] = "production"
        # We DO NOT set DATABASE_URL here. We let config.py read it from .env.
    
    else:
        # --- Default SQLite Test Mode ---
        print(f"\nCONFTEST: Defaulting to SQLite. Setting up test environment, DB_PATH={DB_PATH}")
        if os.path.exists(DB_PATH):
            print(f"CONFTEST: Removing existing database at {DB_PATH}")
            try:
                os.remove(DB_PATH)
            except OSError as e:
                print(f"CONFTEST: Warning - could not remove {DB_PATH}: {e}")
        
        os.environ["HIGHWAY_ENV"] = "test"
        os.environ["NO_DOCKER_USE"] = "true"
        # Force the DATABASE_URL to our temp SQLite file
        os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"
        print(f"CONFTEST: DATABASE_URL set to {os.environ['DATABASE_URL']}")
