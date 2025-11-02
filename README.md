# Highway Core

Highway Core is a powerful and flexible workflow engine that enables you to define, execute, and manage complex workflows with ease. Built with resilience and scalability in mind, it supports persistence, conditional flows, loops, and parallel execution.

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Workflow Definition](#workflow-definition)
- [Operators](#operators)
- [Persistence and Resumability](#persistence-and-resumability)
- [Testing](#testing)
- [Development](#development)
- [License](#license)

## Features

- **Declarative Workflows**: Define workflows in YAML format
- **Task Execution**: Execute functions with dependency management
- **Conditional Logic**: Support for if/else branches
- **Parallel Execution**: Run multiple tasks concurrently with bulkhead isolation
- **Looping Constructs**: foreach and while loop support
- **State Management**: Variables, results, and memory management
- **Persistence**: Save and restore workflow state for resumability
- **Bulkhead Pattern**: Isolate different workflows and operations
- **Pydantic Integration**: Strong typing and validation

## Installation

Install Highway Core using pip:

```bash
pip install highway-core
```

Or install the development version:

```bash
pip install git+https://github.com/rodmena-limited/highway_core.git
```

## Quick Start

### Define a Workflow

Create a YAML file (`simple_workflow.yaml`):

```yaml
name: simple_example
version: 1.0.0
description: A simple example workflow

start_task: log_start

variables:
  message: "Hello from Highway Core!"

tasks:
  log_start:
    task_id: log_start
    operator_type: task
    function: tools.log.info
    args: ["{{variables.message}}"]
    dependencies: []
    result_key: "start_result"

  process_data:
    task_id: process_data
    operator_type: task
    function: tools.memory.set
    args: ["processed_value", "Data processed successfully"]
    dependencies: ["log_start"]
    result_key: "process_result"

  log_end:
    task_id: log_end
    operator_type: task
    function: tools.log.info
    args: ["Workflow completed with result: {{results.process_result}}"]
    dependencies: ["process_data"]
```

### Execute the Workflow

```python
from highway_core.engine.engine import run_workflow_from_yaml

# Execute the workflow
run_workflow_from_yaml("simple_workflow.yaml")
```

## Workflow Definition

A Highway Core workflow is defined in YAML format with the following structure:

```yaml
name: workflow_name
version: 1.x.x
description: Optional description of the workflow
variables: # Initial variables for the workflow
  key: value
start_task: task_id_to_start_with
tasks: # Dictionary of tasks
  task_id:
    task_id: task_id
    operator_type: task | condition | parallel | wait | while | foreach
    function: tools.module.function
    args: [list, of, arguments]
    dependencies: [list, of, task, ids]
    result_key: optional_key_to_store_result
```

## Operators

### Task Operator
Executes a function with provided arguments.
```yaml
my_task:
  task_id: my_task
  operator_type: task
  function: tools.log.info
  args: ["Hello World"]
  dependencies: []
```

### Condition Operator
Executes different branches based on a condition.
```yaml
conditional_task:
  task_id: conditional_task
  operator_type: condition
  condition: "{{variables.some_value}} == true"
  if_true: task_if_true
  if_false: task_if_false
  dependencies: []
```

### Parallel Operator
Executes multiple tasks in parallel.
```yaml
parallel_task:
  task_id: parallel_task
  operator_type: parallel
  tasks: ["task1", "task2", "task3"]
  dependencies: []
```

### While Operator
Repeats execution while a condition is true.
```yaml
while_task:
  task_id: while_task
  operator_type: while
  condition: "{{variables.counter}} < 10"
  body: task_to_repeat
  dependencies: []
```

### ForEach Operator
Iterates over a collection and executes a task for each item.
```yaml
foreach_task:
  task_id: foreach_task
  operator_type: foreach
  items: "{{variables.list_of_items}}"
  body: task_to_repeat
  dependencies: []
```

## Persistence and Resumability

Highway Core supports persistence to save workflow state after each task execution and resume from where it left off if interrupted.

```python
from highway_core.engine.engine import run_workflow_from_yaml

# Run workflow with a specific ID to enable persistence
run_workflow_from_yaml("workflow.yaml", workflow_run_id="my-run-123")

# Later, resume the same workflow with the same ID
run_workflow_from_yaml("workflow.yaml", workflow_run_id="my-run-123")
```

The workflow will automatically detect completed tasks and skip re-execution.

## Testing

Run the test suite:

```bash
pytest
```

Run with coverage:

```bash
pytest --cov=highway_core
```

## Development

### Setup

```bash
git clone https://github.com/rodmena-limited/highway_core.git
cd highway_core
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
pip install -e ".[dev]"
```

### Running Type Checks

```bash
mypy .
```

### Project Structure

```
highway_core/
├── engine/           # Core execution engine
│   ├── state.py      # Workflow state management
│   ├── orchestrator.py # Task orchestration
│   ├── engine.py     # Main engine entry point
│   └── models.py     # Data models
├── tools/            # Available tools and functions
│   ├── registry.py   # Tool registry
│   ├── memory.py     # Memory operations
│   ├── log.py        # Logging operations
│   └── ...           # Other tools
└── persistence/      # Persistence implementations
    ├── manager.py    # Persistence interface
    └── db_storage.py # File-based persistence
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for your changes
5. Run the test suite (`pytest`)
6. Run type checks (`mypy .`)
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to the branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

If you encounter any issues, please file them in our [Issues](https://github.com/rodmena-limited/highway_core/issues) section.

## Acknowledgments

- Built with Pydantic for robust data validation
- Uses graphlib for topological sorting of task dependencies
- Follows bulkhead pattern for isolation