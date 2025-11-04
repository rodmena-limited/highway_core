# PostgreSQL Integration Plan for Highway Core

## Overview
This plan implements PostgreSQL support for the Highway Core workflow engine while maintaining backward compatibility with SQLite. The implementation follows a phased approach to ensure stability and test coverage.

## Current State Analysis
- **Redis Integration**: Found in `pyproject.toml`, `config.py`, `.env.example`, `test.env.example`, and `.github/workflows/publish.yml`
- **SQLite Locking Issues**: Current `@retry_on_lock_error` decorator only handles SQLite-specific errors
- **Test Framework**: Hardcoded SQLite usage in `test_workflows_concurrent.py` with direct `sqlite3` calls
- **Database Manager**: Currently optimized for SQLite with specific PRAGMA settings

## Phase 1: Remove Redis Dependencies
### Files to Modify:
- `pyproject.toml` - Remove Redis dependencies
- `highway_core/config.py` - Remove Redis configuration
- `.env.example` - Remove Redis environment variables
- `test.env.example` - Remove Redis environment variables
- `.github/workflows/publish.yml` - Remove Redis service

### Verification:
- Run `./factcheck.sh` to ensure tests still pass
- Verify Redis-related code is completely removed

## Phase 2: Add PostgreSQL Support
### Files to Modify:
- `pyproject.toml` - Add `psycopg[binary]>=3.0.0` dependency
- `.env.example` - Add PostgreSQL environment variables
- `highway_core/config.py` - Complete rewrite with PostgreSQL fallback logic
- `highway_core/persistence/database.py` - Major updates:
  - Add PostgreSQL-specific imports (`inspect`, `OperationalError`)
  - Update `@retry_on_lock_error` decorator for PostgreSQL deadlocks
  - Refactor `__init__` method with dialect-specific engine configuration
  - Update `_initialize_schema` method to be dialect-agnostic
- `highway_core/persistence/models.py` - Add performance indexes:
  - Workflow: `idx_workflows_status_updated_at`, `idx_workflows_name`
  - TaskExecution: `idx_task_executions_wfid_tid_status`
  - WorkflowResult: `idx_workflow_results_wfid_key`

### Verification:
- Run `./factcheck.sh` to ensure SQLite tests still pass
- Test PostgreSQL configuration manually (if available)

## Phase 3: Update Test Framework
### Files to Modify:
- `tests/conftest.py` - Update `pytest_sessionstart` to respect `USE_PG` environment variable
- `test_workflows_concurrent.py` - Major refactor:
  - Replace direct `sqlite3` calls with SQLAlchemy via `DatabaseManager`
  - Update all helper functions (`get_workflow_results_from_db`, `get_workflow_by_name`, etc.)
  - Make test setup conditional based on `USE_PG` flag
  - Update test methods to be database-agnostic

### Verification:
- Run `./factcheck.sh` to ensure SQLite tests pass
- Test with `USE_PG=true pytest` (if PostgreSQL available)

## Phase 4: Create Documentation
### Files to Create:
- `docs/postgres_setup.md` - PostgreSQL setup and configuration guide

## Implementation Details

### PostgreSQL Configuration Logic
```python
# Check if all PostgreSQL variables are set
use_postgres = all([
    POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
])

if use_postgres:
    DATABASE_URL = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
else:
    DATABASE_URL = "sqlite:///~/.highway.sqlite3"  # Fallback
```

### Database Engine Configuration
- **PostgreSQL**: `QueuePool` with size 10, max_overflow 5
- **SQLite**: `QueuePool` with size 1, WAL mode, specific PRAGMA settings

### Test Environment Control
- **Default**: `pytest` uses temporary SQLite database
- **Override**: `USE_PG=true pytest` uses PostgreSQL from `.env`

## Enterprise-Grade Improvements (Future)
1. **Database Migrations (Alembic)** - Handle schema changes safely
2. **PostgreSQL JSONB Support** - Better performance for JSON data
3. **Configurable Connection Pooling** - Environment variable control
4. **Graceful Shutdown** - Proper cleanup for webhook runner

## Risk Mitigation
- Maintain full backward compatibility with SQLite
- Extensive testing after each phase
- Clear fallback mechanisms
- Comprehensive documentation

## Success Criteria
- All existing tests pass with SQLite
- PostgreSQL configuration works when environment variables are set
- `USE_PG=true pytest` runs tests against PostgreSQL
- No breaking changes to existing functionality

## Timeline
1. **Phase 1**: 1-2 hours (Redis removal)
2. **Phase 2**: 3-4 hours (PostgreSQL core implementation)
3. **Phase 3**: 2-3 hours (Test framework updates)
4. **Phase 4**: 1 hour (Documentation)

Total estimated time: 7-10 hours

## Verification Commands
After each phase, run:
```bash
./factcheck.sh  # Comprehensive test suite
pytest tests/    # Individual test runs
```

This plan ensures a smooth transition to PostgreSQL while maintaining full backward compatibility and comprehensive test coverage.