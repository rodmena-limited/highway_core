Here is the full, descriptive prompt for your agent to implement the Tier 1 features.
The bellow are just basic sample recommendations for structuring your work, you should feel free to adapt as needed and make sure it will have utmost quality.

-----

**Agent Task: Implement Tier 1 of the Highway Execution Engine**

Your mission is to implement the Tier 1 (Minimum Viable Product) of the Highway workflow execution engine. This foundational tier must be capable of parsing a workflow from a YAML file, executing a simple linear chain of tasks, managing state, and resolving variables.

This is the foundation for an enterprise-grade engine. Robustness, clear error handling, and testability are the highest priorities.

### Core Requirements

1.  **YAML First (Critical):** The engine MUST operate directly on the YAML workflow definition. It **CANNOT** import or depend on the `highway_dsl` Python library.
2.  **Internal Pydantic Models:** Because you cannot use `highway_dsl`, you must create a new file, `highway_core/engine/models.py`, to define your own Pydantic models for parsing and validating the workflow YAML.
3.  **Test-Driven Development (TDD):** All new logic must be accompanied by `pytest` unit tests. You must create a new `tests/` directory at the root of the project. Aim for 100% test coverage on the new logic.
4.  **No `eval()`:** Do not use `eval()` for any part of this. All logic must be handled explicitly.

### Target Workflow to Execute

Your implementation will be considered successful when it can correctly parse and execute the following YAML content (which is located at `examples/base_tier_one_workflow.yaml`):

```yaml
---
name: tier1_core_test
version: 1.0.0
description: A simple linear workflow to test the core Tier 1 engine.
variables:
  # The engine should be able to load and use these,
  # but this first test doesn't require it.
  initial_var: "test_value"
start_task: log_start
tasks:
  log_start:
    task_id: log_start
    operator_type: task
    function: tools.log.info
    args:
      - "Starting Tier 1 test..."
    dependencies: []

  set_memory_value:
    task_id: set_memory_value
    operator_type: task
    function: tools.memory.set
    args:
      - "test_key"
      - "hello_world"
    dependencies:
      - log_start
    result_key: "mem_report" # The engine must save the return value

  log_result:
    task_id: log_result
    operator_type: task
    function: tools.log.info
    args:
      # The engine must resolve these variables
      - "Memory set report: Key={{mem_report.key}}, Status={{mem_report.status}}"
    dependencies:
      - set_memory_value

  log_finish:
    task_id: log_finish
    operator_type: task
    function: tools.log.info
    args:
      - "Tier 1 test complete."
    dependencies:
      - log_result
```

-----

### Implementation Plan (Step-by-Step)

You will modify and create files within the `highway_core` directory.

#### Step 1: `highway_core/engine/models.py` (New File)

Create this file to define the Pydantic models for parsing the YAML.

```python
# highway_core/engine/models.py
from pydantic import BaseModel, Field
from typing import Any, List, Dict, Optional

class TaskOperatorModel(BaseModel):
    """Parses a single task from the YAML 'tasks' dict."""
    task_id: str
    operator_type: str  # For Tier 1, this will always be 'task'
    function: str
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    dependencies: List[str] = Field(default_factory=list)
    result_key: Optional[str] = None
    # Add other fields as needed (e.g., retry_policy)
    
    class Config:
        extra = 'allow' # Allow other operator keys for now

class WorkflowModel(BaseModel):
    """Parses the root YAML file."""
    name: str
    start_task: str
    variables: Dict[str, Any] = Field(default_factory=dict)
    tasks: Dict[str, TaskOperatorModel]
```

#### Step 2: `highway_core/tools/log.py`

Implement the logging tools.

```python
# highway_core/tools/log.py
import logging

# Configure a single, shared logger for the engine
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [HighwayEngine] - %(message)s",
)
logger = logging.getLogger("HighwayEngine")

def info(message: str) -> None:
    """Logs a message at the INFO level."""
    logger.info(message)

def error(message: str) -> None:
    """Logs a message at the ERROR level."""
    logger.error(message)
```

#### Step 3: `highway_core/engine/state.py`

Implement the `WorkflowState` class. This is the "memory" of the workflow run.

```python
# highway_core/engine/state.py
import re
from copy import deepcopy

class WorkflowState:
    """Manages all data for a single workflow run."""
    
    # Regex to find {{ variable.name }}
    TEMPLATE_REGEX = re.compile(r"\{\{([\s\w.-]+)\}\}")

    def __init__(self, initial_variables: dict):
        # Deepcopy to ensure isolation
        self._data = {
            "variables": deepcopy(initial_variables),
            "results": {},    # Stores task outputs, e.g., "mem_report": {...}
            "memory": {},     # For tools.memory.set
        }
        print("WorkflowState initialized.")

    def set_result(self, result_key: str, value: Any):
        """Saves the output of a task (from 'result_key')."""
        print(f"State: Setting result for key: {result_key}")
        self._data["results"][result_key] = value

    def _get_value(self, path: str) -> Any:
        """
        Retrieves a value from the state using dot-delimited path.
        e.g., "mem_report.key" -> self._data['results']['mem_report']['key']
        """
        path = path.strip()
        parts = path.split('.')
        
        # Determine the root context (e.g., 'variables' or 'results')
        root_key = parts[0]
        if root_key in self._data:
            current_val = self._data[root_key]
        else:
            # Fallback for keys that are not explicitly namespaced
            # e.g., 'mem_report' instead of 'results.mem_report'
            current_val = self._data["results"].get(root_key)
            if current_val is None:
                 print(f"State: Warning - could not find root key: {root_key}")
                 return None
        
        # Traverse the nested path
        for part in parts[1:]:
            if isinstance(current_val, dict):
                current_val = current_val.get(part)
            else:
                print(f"State: Error - Cannot access property '{part}' on non-dict.")
                return None
        return current_val

    def resolve_templating(self, input_data: Any) -> Any:
        """
        Recursively resolves templated strings like '{{mem_report.key}}'.
        """
        if isinstance(input_data, str):
            # Check if the *entire string* is a variable
            match = self.TEMPLATE_REGEX.fullmatch(input_data)
            if match:
                return self._get_value(match.group(1))

            # Otherwise, replace all occurrences within the string
            def replacer(m):
                val = self._get_value(m.group(1))
                return str(val) if val is not None else m.group(0)
                
            return self.TEMPLATE_REGEX.sub(replacer, input_data)

        if isinstance(input_data, list):
            return [self.resolve_templating(item) for item in input_data]

        if isinstance(input_data, dict):
            return {k: self.resolve_templating(v) for k, v in input_data.items()}
            
        # Return non-templatable types as-is (e.g., int, bool)
        return input_data
```

#### Step 4: `highway_core/tools/memory.py`

Implement the special `set_memory` tool.

```python
# highway_core/tools/memory.py
# This is a special tool that requires the WorkflowState.
# The task_handler will inject the 'state' argument.

from highway_core.engine.state import WorkflowState

def set_memory(state: WorkflowState, key: str, value: Any) -> dict:
    """
    Saves a value to the workflow's volatile memory.
    This tool MUST return a dict for the 'mem_report' result_key.
    """
    print(f"Tool.Memory: Setting key '{key}'")
    state._data["memory"][key] = value # Accessing internal state directly
    
    # Return value as specified by the test workflow
    return {
        "key": key,
        "status": "ok"
    }
```

#### Step 5: `highway_core/tools/registry.py`

Implement the `ToolRegistry`.

```python
# highway_core/tools/registry.py
from typing import Callable, Dict
from . import log
from . import memory

class ToolRegistry:
    def __init__(self):
        self.functions: Dict[str, Callable] = {}
        self._register_tools()
        print(f"ToolRegistry loaded with {len(self.functions)} functions.")

    def _register_tools(self):
        """Internal method to load all Tier 1 tools."""
        self.register_tool("tools.log.info", log.info)
        self.register_tool("tools.log.error", log.error)
        self.register_tool("tools.memory.set", memory.set_memory)

    def register_tool(self, name: str, func: Callable):
        self.functions[name] = func

    def get_function(self, name: str) -> Callable:
        func = self.functions.get(name)
        if func is None:
            print(f"Error: Tool '{name}' not found in registry.")
            raise KeyError(f"Tool '{name}' not found.")
        return func
```

#### Step 6: `highway_core/engine/operator_handlers/task_handler.py`

Implement the `TaskOperator` handler.

```python
# highway_core/engine/operator_handlers/task_handler.py
from highway_core.engine.models import TaskOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry

def execute(task: TaskOperatorModel, state: WorkflowState, registry: ToolRegistry) -> None:
    """
    Executes a single TaskOperator.
    """
    print(f"TaskHandler: Executing task: {task.task_id}")
    
    # 1. Get the tool from the registry
    tool_name = task.function
    try:
        tool_func = registry.get_function(tool_name)
    except KeyError:
        print(f"TaskHandler: Error - {tool_name} not found.")
        raise
        
    # 2. Resolve arguments
    resolved_args = state.resolve_templating(task.args)
    resolved_kwargs = state.resolve_templating(task.kwargs)
    
    # 3. Special check for tools that need state
    if tool_name == "tools.memory.set":
        # Inject the state object as the first argument
        resolved_args.insert(0, state)
        
    # 4. Execute the tool
    try:
        print(f"TaskHandler: Calling {tool_name} with args={resolved_args}")
        result = tool_func(*resolved_args, **resolved_kwargs)
    except Exception as e:
        print(f"TaskHandler: Error executing {tool_name}: {e}")
        raise
        
    # 5. Save the result
    if task.result_key:
        state.set_result(task.result_key, result)
```

#### Step 7: `highway_core/engine/orchestrator.py`

Implement the `Orchestrator`.

```python
# highway_core/engine/orchestrator.py
from collections import deque
from highway_core.engine.models import WorkflowModel
from highway_core.engine.state import WorkflowState
from highway_core.tools.registry import ToolRegistry
from highway_core.engine.operator_handlers import task_handler

class Orchestrator:
    def __init__(self, workflow: WorkflowModel, state: WorkflowState, registry: ToolRegistry):
        self.workflow = workflow
        self.state = state
        self.registry = registry
        
        # Use a deque for an efficient LILO queue
        self.task_queue = deque([self.workflow.start_task])
        self.completed_tasks = set()
        print("Orchestrator initialized.")

    def _dependencies_met(self, dependencies: list[str]) -> bool:
        """Checks if all dependencies for a task are in the completed set."""
        return all(dep_id in self.completed_tasks for dep_id in dependencies)

    def _find_next_runnable_tasks(self) -> list[str]:
        """
        Finds all tasks whose dependencies are now met.
        This is a simple but non-performant O(n^2) check.
        It is fine for Tier 1.
        """
        runnable = []
        for task_id, task in self.workflow.tasks.items():
            if task_id not in self.completed_tasks and task_id not in self.task_queue:
                if self._dependencies_met(task.dependencies):
                    runnable.append(task_id)
        return runnable

    def run(self):
        """
        Runs the workflow execution loop.
        """
        print(f"Orchestrator: Starting workflow '{self.workflow.name}'")
        
        while self.task_queue:
            task_id = self.task_queue.popleft()
            
            task_model = self.workflow.tasks.get(task_id)
            if not task_model:
                print(f"Orchestrator: Error - Task ID '{task_id}' not found in workflow tasks.")
                continue

            # 1. Check dependencies
            if not self._dependencies_met(task_model.dependencies):
                # This shouldn't happen with our current logic, but as a safeguard
                self.task_queue.append(task_id) # Put it back
                continue
                
            # 2. Execute the task (only TaskHandler for Tier 1)
            try:
                if task_model.operator_type == "task":
                    task_handler.execute(task_model, self.state, self.registry)
                else:
                    print(f"Orchestrator: Warning - Skipping operator type '{task_model.operator_type}'.")
            
                # 3. Mark as complete
                self.completed_tasks.add(task_id)
                print(f"Orchestrator: Task {task_id} completed.")
                
                # 4. Find and queue the next tasks
                next_tasks = self._find_next_runnable_tasks()
                for next_task_id in next_tasks:
                    if next_task_id not in self.task_queue:
                        self.task_queue.append(next_task_id)
                        
            except Exception as e:
                print(f"Orchestrator: FATAL ERROR executing task '{task_id}': {e}")
                # In a real engine, this would trigger failure handling
                break # Stop the workflow

        print(f"Orchestrator: Workflow '{self.workflow.name}' finished.")
```

#### Step 8: `highway_core/engine/engine.py`

Implement the main entry point.

```python
# highway_core/engine/engine.py
import yaml
from .models import WorkflowModel
from .state import WorkflowState
from .orchestrator import Orchestrator
from highway_core.tools.registry import ToolRegistry

def run_workflow_from_yaml(yaml_path: str) -> None:
    """
    The main entry point for the Highway Execution Engine.
    """
    print(f"Engine: Loading workflow from: {yaml_path}")
    
    # 1. Load and Parse YAML
    try:
        with open(yaml_path, 'r') as f:
            workflow_data = yaml.safe_load(f)
        
        workflow_model = WorkflowModel.model_validate(workflow_data)
    except Exception as e:
        print(f"Engine: Failed to load or parse YAML: {e}")
        return

    # 2. Initialize Core Components
    registry = ToolRegistry()
    state = WorkflowState(workflow_model.variables)
    orchestrator = Orchestrator(workflow_model, state, registry)
    
    # 3. Run the workflow
    orchestrator.run()
```

### Testing and Validation

1.  Create a new root-level directory `tests/`.
2.  Create `tests/test_tier1_state.py` to unit test the `WorkflowState` class.
    ```python
    # tests/test_tier1_state.py
    from highway_core.engine.state import WorkflowState

    def test_resolve_simple_string():
        state = WorkflowState({})
        state.set_result("mem_report", {"key": "test_key", "status": "ok"})
        
        test_str = "Key={{mem_report.key}}, Status={{mem_report.status}}"
        resolved_str = state.resolve_templating(test_str)
        
        assert resolved_str == "Key=test_key, Status=ok"

    def test_resolve_full_string_replacement():
        state = WorkflowState({})
        state.set_result("mem_report", {"key": "test_key"})
        
        test_str = "{{mem_report.key}}"
        resolved_str = state.resolve_templating(test_str)
        
        assert resolved_str == "test_key"

    def test_resolve_list():
        state = WorkflowState({"initial_var": "hello"})
        test_list = ["Static", "{{variables.initial_var}}"]
        resolved_list = state.resolve_templating(test_list)
        
        assert resolved_list == ["Static", "hello"]
    ```
3.  Create a new root-level file `run_test.py` to run the integration test.
    ```python
    # run_test.py
    from highway_core.engine.engine import run_workflow_from_yaml

    if __name__ == "__main__":
        print("--- Starting Tier 1 Integration Test ---")
        run_workflow_from_yaml("examples/base_tier_one_workflow.yaml")
        print("--- Tier 1 Integration Test Finished ---")
    ```
4.  Run `python run_test.py`.
5.  **Success Criteria:** The console output should log the following messages **in order**:
    1.  `[HighwayEngine] - Starting Tier 1 test...`
    2.  `[HighwayEngine] - Memory set report: Key=test_key, Status=ok`
    3.  `[HighwayEngine] - Tier 1 test complete.`
