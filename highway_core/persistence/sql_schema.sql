-- Workflows table: Track workflow execution instances
CREATE TABLE IF NOT EXISTS workflows (
    workflow_id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT DEFAULT 'running', -- running, completed, failed
    error_message TEXT,
    variables_json TEXT, -- Store initial workflow variables
    start_task TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tasks table: Track individual task executions
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    operator_type TEXT NOT NULL,
    runtime TEXT DEFAULT 'python',
    function TEXT,
    image TEXT,
    command_json TEXT,
    args_json TEXT,
    kwargs_json TEXT,
    result_key TEXT,
    dependencies_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'pending', -- pending, executing, completed, failed
    error_message TEXT,
    result_value_json TEXT,
    PRIMARY KEY (workflow_id, task_id),
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id) ON DELETE CASCADE
);

-- Workflow results table: Store task results (separate from tasks for flexibility)
CREATE TABLE IF NOT EXISTS workflow_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    result_key TEXT NOT NULL,
    result_value_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id, task_id) REFERENCES tasks(workflow_id, task_id) ON DELETE CASCADE
);

-- Indices for performance
CREATE INDEX IF NOT EXISTS idx_workflows_status ON workflows(status);
CREATE INDEX IF NOT EXISTS idx_tasks_workflow_id ON tasks(workflow_id);
CREATE INDEX IF NOT EXISTS idx_tasks_workflow_status ON tasks(workflow_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_workflow_results_workflow ON workflow_results(workflow_id);
