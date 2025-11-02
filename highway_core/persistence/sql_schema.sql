-- highway_core/persistence/sql_schema.sql
-- Schema for the Highway Core Workflow Engine SQLite3 database

-- Workflows table
CREATE TABLE IF NOT EXISTS workflows (
    workflow_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    start_task TEXT,
    variables_json TEXT, -- JSON serialized
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'running' -- running, completed, failed, paused
);

-- Index for faster workflow lookups
CREATE INDEX IF NOT EXISTS idx_workflows_status ON workflows(status);
CREATE INDEX IF NOT EXISTS idx_workflows_created_at ON workflows(created_at);

-- Tasks table 
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    operator_type TEXT NOT NULL,
    runtime TEXT DEFAULT 'python',
    function TEXT,
    image TEXT,
    command_json TEXT, -- JSON serialized command array
    args_json TEXT, -- JSON serialized args array
    kwargs_json TEXT, -- JSON serialized kwargs dict
    result_key TEXT,
    dependencies_json TEXT, -- JSON serialized dependencies array
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'pending', -- pending, executing, completed, failed
    PRIMARY KEY (workflow_id, task_id), -- Composite primary key
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id) ON DELETE CASCADE
);

-- Indexes for faster task lookups
CREATE INDEX IF NOT EXISTS idx_tasks_workflow_id ON tasks(workflow_id);
CREATE INDEX IF NOT EXISTS idx_tasks_workflow_status ON tasks(workflow_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_operator_type ON tasks(operator_type);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

-- Task Executions table (for execution history)
CREATE TABLE IF NOT EXISTS task_executions (
    execution_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    executor_runtime TEXT,
    execution_args_json TEXT,
    execution_kwargs_json TEXT,
    result_json TEXT,
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_ms INTEGER,
    status TEXT DEFAULT 'pending', -- pending, running, completed, failed
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id) ON DELETE CASCADE
);

-- Indexes for faster task execution lookups
CREATE INDEX IF NOT EXISTS idx_task_executions_task_id ON task_executions(task_id);
CREATE INDEX IF NOT EXISTS idx_task_executions_workflow_id ON task_executions(workflow_id);
CREATE INDEX IF NOT EXISTS idx_task_executions_workflow_task ON task_executions(workflow_id, task_id);
CREATE INDEX IF NOT EXISTS idx_task_executions_status ON task_executions(status);

-- Workflow Results table (for storing results by key)
CREATE TABLE IF NOT EXISTS workflow_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    result_key TEXT NOT NULL,
    result_value_json TEXT, -- JSON serialized result
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    UNIQUE(workflow_id, result_key)
);

-- Indexes for faster result lookups
CREATE INDEX IF NOT EXISTS idx_workflow_results_workflow_id ON workflow_results(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_results_key ON workflow_results(result_key);
CREATE INDEX IF NOT EXISTS idx_workflow_results_workflow_key ON workflow_results(workflow_id, result_key);

-- Workflow Memory table (for storing workflow state memory)
CREATE TABLE IF NOT EXISTS workflow_memory (
    memory_id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    memory_key TEXT NOT NULL,
    memory_value_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    UNIQUE(workflow_id, memory_key)
);

-- Indexes for faster memory lookups
CREATE INDEX IF NOT EXISTS idx_workflow_memory_workflow_id ON workflow_memory(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_memory_key ON workflow_memory(memory_key);
CREATE INDEX IF NOT EXISTS idx_workflow_memory_workflow_key ON workflow_memory(workflow_id, memory_key);

-- Dependencies table (for tracking task dependencies)
CREATE TABLE IF NOT EXISTS task_dependencies (
    dependency_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    depends_on_task_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    FOREIGN KEY (depends_on_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    UNIQUE(task_id, depends_on_task_id)
);

-- Indexes for faster dependency lookups
CREATE INDEX IF NOT EXISTS idx_task_dependencies_task_id ON task_dependencies(task_id);
CREATE INDEX IF NOT EXISTS idx_task_dependencies_depends_on ON task_dependencies(depends_on_task_id);
CREATE INDEX IF NOT EXISTS idx_task_dependencies_workflow_id ON task_dependencies(workflow_id);
CREATE INDEX IF NOT EXISTS idx_task_dependencies_workflow_tasks ON task_dependencies(workflow_id, task_id, depends_on_task_id);