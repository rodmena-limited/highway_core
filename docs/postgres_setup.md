# PostgreSQL Setup for Highway Core

Highway Core supports PostgreSQL as a production-ready database backend, falling back to SQLite if PostgreSQL is not configured.

## 1. Install and Configure PostgreSQL

First, install PostgreSQL on your server.

```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
```

## 2. Create Database and User

You need to create a dedicated database and user for Highway.

1.  Switch to the `postgres` user:
    ```bash
    sudo -u postgres psql
    ```

2.  Inside the `psql` shell, run the following commands. Replace `highway_user` and `strong_password` with your desired credentials.

    ```sql
    -- Create the database
    CREATE DATABASE highway_db;
    
    -- Create the user
    CREATE USER highway_user WITH PASSWORD 'strong_password';
    
    -- Grant all privileges on the new database to the new user
    GRANT ALL PRIVILEGES ON DATABASE highway_db TO highway_user;
    
    -- Exit the psql shell
    \q
    ```

## 3. Configure Environment Variables

Highway Core reads database configuration from a `.env` file in the project's root directory.

Create a file named `.env` and add the following lines, replacing the values with the ones you just created:

```ini
# --- PostgreSQL Settings ---
# If all 5 are set, Highway will use PostgreSQL.
POSTGRES_USER=highway_user
POSTGRES_PASSWORD=strong_password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=highway_db
```

As long as these 5 `POSTGRES_` variables are set, Highway will automatically use PostgreSQL. If any are missing, it will fall back to using a SQLite file at `~/.highway.sqlite3`.

## 4. Running Tests with PostgreSQL

By default, all tests (`pytest` or `./factcheck.sh`) run against a temporary SQLite database for safety and speed.

If you want to run the test suite against your live PostgreSQL database (e.g., in a staging environment), you can use the `USE_PG=true` environment variable:

```bash
# This command will read your .env file and run all tests against PostgreSQL
USE_PG=true pytest
```

## 5. Performance Considerations

### Connection Pooling

PostgreSQL uses a connection pool with the following default settings:
- **Pool Size**: 10 connections
- **Max Overflow**: 5 additional connections
- **Pool Recycle**: 30 minutes

You can customize these settings by adding the following to your `.env` file:

```ini
# Optional: Customize PostgreSQL connection pool
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=10
```

### Indexes

The system automatically creates the following indexes for optimal performance:

- **Workflows**: `idx_workflows_status_updated_at`, `idx_workflows_name`
- **Task Executions**: `idx_task_executions_wfid_tid_status`
- **Workflow Results**: `idx_workflow_results_wfid_key`

### Locking and Concurrency

PostgreSQL handles concurrent access much better than SQLite. The system includes automatic retry logic for:
- Deadlock detection
- Lock timeouts
- Connection pool exhaustion

## 6. Troubleshooting

### Common Issues

1. **Connection Refused**: Ensure PostgreSQL is running and the host/port are correct
2. **Authentication Failed**: Verify the username and password in your `.env` file
3. **Database Not Found**: Make sure the database exists and the user has permissions

### Debug Mode

To enable detailed logging for database operations, set:

```ini
# Enable SQLAlchemy debug logging
SQLALCHEMY_ECHO=true
```

This will print all SQL queries to the console, which can help diagnose connection or query issues.

## 7. Migration from SQLite to PostgreSQL

If you're migrating an existing SQLite database to PostgreSQL:

1. **Backup your SQLite database**:
   ```bash
   cp ~/.highway.sqlite3 ~/.highway.sqlite3.backup
   ```

2. **Export data from SQLite**: Use a tool like `sqlite3` to export your data to SQL format

3. **Import into PostgreSQL**: Use `psql` to import the data into your new PostgreSQL database

4. **Update your `.env` file**: Add the PostgreSQL connection settings

5. **Test thoroughly**: Run your workflows to ensure everything works correctly

**Note**: The schema is automatically created by Highway Core, so you only need to migrate the data, not the structure.

## 8. Monitoring

For production use, consider monitoring:
- **Connection pool usage**: Watch for connection exhaustion
- **Query performance**: Use PostgreSQL's built-in query analysis tools
- **Lock contention**: Monitor for deadlocks and long-running transactions

PostgreSQL provides excellent monitoring capabilities through its system catalogs and the `pg_stat_*` views.