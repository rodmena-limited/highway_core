import os
from pathlib import Path
from dotenv import load_dotenv

# Determine the environment (e.g., 'development', 'test', 'production')
# Default to 'development' if not set
ENV = os.getenv("HIGHWAY_ENV", "development")

# Load the appropriate .env file
# For tests, we'll load 'test.env'. For other environments, we load '.env'
if ENV == "test":
    dotenv_path = Path('.') / 'test.env'
else:
    dotenv_path = Path('.') / '.env'

if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path)


class Settings:
    # Database settings
    raw_db_url = os.getenv("DATABASE_URL", "sqlite:///~/.highway/highway.sqlite3")
    DATABASE_URL: str = raw_db_url.replace("~", os.path.expanduser("~"))

    # Redis settings
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    REDIS_PASSWORD: str | None = os.getenv("REDIS_PASSWORD")
    REDIS_DB: int = int(os.getenv("REDIS_DB", 0))

    # Test settings
    USE_FAKE_REDIS: bool = os.getenv("USE_FAKE_REDIS", "false").lower() in ("true", "1", "t")
    
    # Docker settings for tests
    NO_DOCKER_USE: bool = os.getenv("NO_DOCKER_USE", "false").lower() in ("true", "1", "t")


settings = Settings()

# In test environment, use the DATABASE_URL from environment variable if set, 
# otherwise use a default test database file
if ENV == "test":
    if os.getenv("DATABASE_URL") is None:
        settings.DATABASE_URL = "sqlite:///tests/test_db.sqlite3"
