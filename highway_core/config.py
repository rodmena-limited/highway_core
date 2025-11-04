import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Determine the environment (e.g., 'development', 'test', 'production')
# Default to 'development' if not set
ENV = os.getenv("HIGHWAY_ENV", "development")

# Load the appropriate .env file
# For tests, we'll load 'test.env'. For other environments, we load '.env'
if ENV == "test":
    dotenv_path = Path(".") / "test.env"
else:
    dotenv_path = Path(".") / ".env"

if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path)


class Settings:
    # --- Test settings ---
    # Check for USE_PG=true for testing against prod PG
    USE_PG_FOR_TESTS: bool = os.getenv("USE_PG", "false").lower() in ("true", "1")

    # --- Database settings ---
    # Support both POSTGRES_* and DB_* naming conventions
    POSTGRES_USER: Optional[str] = os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
    POSTGRES_PASSWORD: Optional[str] = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD")
    POSTGRES_HOST: Optional[str] = os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST")
    POSTGRES_PORT: Optional[str] = os.getenv("POSTGRES_PORT") or os.getenv("DB_PORT")
    POSTGRES_DB: Optional[str] = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")

    # Check if all PG variables are set
    use_postgres = all(
        [
            POSTGRES_USER,
            POSTGRES_PASSWORD,
            POSTGRES_HOST,
            POSTGRES_PORT,
            POSTGRES_DB,
        ]
    )

    if use_postgres:
        # Build PostgreSQL connection string for psycopg (v3)
        DATABASE_URL: str = f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    else:
        # Fallback to SQLite
        raw_db_url = os.getenv("DATABASE_URL", "sqlite:///~/.highway.sqlite3")
        DATABASE_URL: str = raw_db_url.replace("~", os.path.expanduser("~"))

    # --- Test Environment Override ---
    # If we are in 'test' mode AND we are NOT explicitly told to use PG,
    # then we use the DATABASE_URL set by conftest.py (which should be /tmp/highway_test.sqlite3)
    if ENV == "test" and not USE_PG_FOR_TESTS:
        # Use the DATABASE_URL already set by conftest.py, don't override it
        pass

    # Docker settings for tests
    NO_DOCKER_USE: bool = os.getenv("NO_DOCKER_USE", "false").lower() in (
        "true",
        "1",
        "t",
    )


settings = Settings()

# In test environment, if USE_PG is true, we let the PG URL stand.
# If USE_PG is false, we use the DATABASE_URL set by conftest.py
if ENV == "test":
    if settings.USE_PG_FOR_TESTS:
        if not settings.use_postgres:
            # This is an error state, user wants PG test but .env is not set
            print(
                "WARNING: USE_PG=true but PostgreSQL env vars not set. Falling back to SQLite."
            )
            # Don't override DATABASE_URL, let conftest.py handle it
        else:
            # Using PG for tests, print a clear message
            print(f"TESTING with POSTGRES: {settings.POSTGRES_HOST}")
    else:
        # Default test environment: use the DATABASE_URL set by conftest.py
        pass
