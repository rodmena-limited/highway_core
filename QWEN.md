You have successfully implemented the basic Tier 1 proof-of-concept. The log output confirms that the linear execution, task handling, state management, and variable resolution are all working at a fundamental level.

However, as you noted, this foundation is not yet the "enterprise-grade" system we're aiming for. The current implementation has several critical shortcuts (like the O(n^2) dependency check and hardcoded handlers) that will not scale. You've also introduced a `bulkhead` pattern, which is an excellent idea for resiliency, but its current implementation is inefficient.

Here is a complete action plan to refactor your Tier 1 implementation into a robust, scalable, and resilient foundation. This "Tier 1.5 Refactor" is the most critical step to get right.

-----

### Agent Task: Refactor Tier 1 for Enterprise-Grade Resiliency

Your task is to refactor the existing Tier 1 components. You will not add new operators (like `parallel` or `while`) yet. Instead, you will harden the core parsing, orchestration, and execution logic to make it scalable, resilient, and ready for advanced features.

### Core Requirements

1.  **Full Operator Parsing:** The engine must parse *all* operator types from the YAML, not just `task`.
2.  **Resilient Execution:** Tool execution must be wrapped in a persistent `bulkhead` to protect the engine from failing or slow tools.
3.  **Scalable Orchestration:** The `Orchestrator`'s dependency resolution must be rewritten to use a proper topological sort (DAG) to avoid O(n^2) performance issues.
4.  **Dynamic Tool Loading:** The `ToolRegistry` must be refactored to *dynamically* load tools instead of hardcoding them.
5.  **Explicit State:** `WorkflowState` variable resolution must be made explicit to avoid ambiguity.
6.  **Full Test Coverage:** All new and refactored logic must be fully unit-tested.

-----

### Step-by-Step Refactor Plan

#### Step 1: Update `highway_core/engine/models.py` (Critical)

Your current models only parse `TaskOperatorModel`. This is insufficient. You must use Pydantic's **Discriminated Unions** to parse *any* valid operator.

```python
# highway_core/engine/models.py
from pydantic import BaseModel, Field, ConfigDict
from typing import Any, List, Dict, Optional, Literal, Union

# Define the "kind" for each operator
OperatorType = Literal[
    "task", "condition", "parallel", "foreach", "while", "wait"
]

# --- Base Operator Model ---
class BaseOperatorModel(BaseModel):
    """The base model all operators share."""
    task_id: str
    operator_type: OperatorType
    dependencies: List[str] = Field(default_factory=list)
    model_config = ConfigDict(extra="allow")

# --- Specific Operator Models ---
class TaskOperatorModel(BaseOperatorModel):
    operator_type: Literal["task"]
    function: str
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    result_key: Optional[str] = None

class ConditionOperatorModel(BaseOperatorModel):
    operator_type: Literal["condition"]
    condition: str
    if_true: str
    if_false: str

class ParallelOperatorModel(BaseOperatorModel):
    operator_type: Literal["parallel"]
    branches: Dict[str, List[str]] # Just parsing for now

class WaitOperatorModel(BaseOperatorModel):
    operator_type: Literal["wait"]
    wait_for: Any # Can be str, int (for timedelta), etc.

class ForEachOperatorModel(BaseObject):
    operator_type: Literal["foreach"]
    items: str
    loop_body: List[Any] # Just parsing for now

class WhileOperatorModel(BaseObject):
    operator_type: Literal["while"]
    condition: str
    loop_body: List[Any] # Just parsing for now

# --- The Discriminated Union ---
# This allows Pydantic to automatically parse the correct model
# based on the 'operator_type' field.
AnyOperatorModel = Union[
    TaskOperatorModel,
    ConditionOperatorModel,
    ParallelOperatorModel,
    WaitOperatorModel,
    ForEachOperatorModel,
    WhileOperatorModel,
]

class WorkflowModel(BaseModel):
    """Parses the root YAML file."""
    name: str
    start_task: str
    variables: Dict[str, Any] = Field(default_factory=dict)
    # This is the key change:
    tasks: Dict[str, AnyOperatorModel] = Field(discriminator="operator_type")
```

#### Step 2: Refactor `highway_core/tools/registry.py`

Make this dynamic. Tools should "register themselves."

1.  Create `highway_core/tools/decorators.py`:
    ```python
    # highway_core/tools/decorators.py
    TOOL_REGISTRY = {}

    def tool(name: str):
        """Decorator to register a function as a Highway tool."""
        def decorator(func):
            if name in TOOL_REGISTRY:
                raise ValueError(f"Duplicate tool name: {name}")
            TOOL_REGISTRY[name] = func
            return func
        return decorator
    ```
2.  Update `highway_core/tools/log.py`:
    ```python
    # highway_core/tools/log.py
    import logging
    from .decorators import tool

    logger = logging.getLogger("HighwayEngine") # Use shared logger

    @tool("tools.log.info")
    def info(message: str) -> None:
        logger.info(message)

    @tool("tools.log.error")
    def error(message: str) -> None:
        logger.error(message)
    ```
3.  Update `highway_core/tools/memory.py`:
    ```python
    # highway_core/tools/memory.py
    from .decorators import tool
    from highway_core.engine.state import WorkflowState

    @tool("tools.memory.set")
    def set_memory(state: WorkflowState, key: str, value: Any) -> dict:
        # ... (rest of your existing code)
    ```
4.  Refactor `highway_core/tools/registry.py`:
    ```python
    # highway_core/tools/registry.py
    import pkgutil
    import importlib
    from .decorators import TOOL_REGISTRY

    class ToolRegistry:
        def __init__(self):
            # The registry is now just a reference to the one
            # populated by the @tool decorator.
            self.functions = TOOL_REGISTRY
            self._discover_tools()
            print(f"ToolRegistry loaded with {len(self.functions)} functions.")

        def _discover_tools(self):
            """Dynamically imports all modules in 'highway_core.tools'."""
            import highway_core.tools
            
            # This iterates over all modules in the 'tools' package
            # and imports them, which triggers their @tool decorators.
            for _, name, _ in pkgutil.walk_packages(
                highway_core.tools.__path__,
                highway_core.tools.__name__ + '.'
            ):
                importlib.import_module(name)

        def get_function(self, name: str) -> Callable:
            # ... (your existing get_function logic)
    ```
5.  **Important:** Create `highway_core/tools/__init__.py` and import all tool files (`log.py`, `memory.py`, `fetch.py`, etc.) to ensure `walk_packages` can find them.

#### Step 3: Refactor `highway_core/engine/state.py`

Make variable resolution explicit and safer.

1.  Update `_get_value(self, path: str)`:
      * It should *no longer* have the fallback logic.
      * If `path.startswith("variables.")`, it must check `self._data["variables"]`.
      * If `path.startswith("results.")`, it must check `self._data["results"]`.
      * If `path.startswith("memory.")`, it must check `self._data["memory"]`.
      * If `path == "item"`, it must check `self._data["loop_context"]`.
      * **This is a breaking change.** Your test YAML `{{mem_report.key}}` must become `{{results.mem_report.key}}`.

#### Step 4: Refactor `highway_core/engine/orchestrator.py`

This is the biggest change. You must replace the simple queue with a proper DAG (Directed Acyclic Graph) resolver.

1.  **Add `graphlib`:** Your engine will now use Python's built-in `graphlib.TopologicalSorter`.
2.  **Refactor `__init__`:**
      * Create the task graph: `self.graph = {task_id: set(task.dependencies) for task_id, task in workflow.tasks.items()}`.
      * Create the sorter: `self.sorter = TopologicalSorter(self.graph)`.
      * Call `self.sorter.prepare()`. This readies the DAG.
      * `self.task_queue` is no longer needed.
3.  **Refactor `run()`:**
      * The `while self.task_queue:` loop must be replaced with `while self.sorter.is_active():`.
      * Get runnable tasks: `runnable_tasks = self.sorter.get_ready()`.
      * **This is the key:** You must now be able to run these tasks *concurrently* (e.g., using a `ThreadPoolExecutor`).
      * For each `task_id` in `runnable_tasks`:
          * Execute it (see Step 5).
          * Mark it as complete: `self.sorter.done(task_id)`.
4.  **Remove `_find_next_runnable_tasks()`:** This method is no longer needed. `graphlib` does this for you.
5.  **Implement a Handler Map:** The orchestrator needs to call the *correct* handler, not just `task_handler`.
    ```python
    # In __init__:
    from .operator_handlers import task, condition, ...
    self.handler_map = {
        "task": task_handler.execute,
        "condition": condition_handler.execute,
        # ... all other stubs
    }

    # In run(), when executing a task:
    task_model = self.workflow.tasks[task_id]
    handler_func = self.handler_map.get(task_model.operator_type)
    if handler_func:
        # The handler must now return the list of *next* tasks
        # e.g., for 'condition', it returns [task.if_true] or [task.if_false]
        # These are NOT added to the queue. They are just marked
        # as "done" in the sorter so their children can run.
        # This part is tricky. A simpler way:
        # Handlers should just *do their job* (e.g., run a task)
        # The Orchestrator's loop is *only* responsible for the DAG.
        # Let's adjust:
        
        # New `run()` logic:
        while self.sorter.is_active():
            for task_id in self.sorter.get_ready():
                # Submit self._execute_task(task_id) to a thread pool
            
            # Wait for all submitted tasks to finish...
            
            for task_id, result in finished_tasks:
                if result.success:
                    self.sorter.done(task_id)
                else:
                    # Handle task failure
                    
    def _execute_task(self, task_id: str):
        # This is where the handler map logic goes
        task_model = self.workflow.tasks[task_id]
        handler_func = self.handler_map.get(task_model.operator_type)
        # ...
        # handler_func(task_model, self.state, self.registry)
        # ...
    ```

#### Step 5: Refactor `highway_core/engine/operator_handlers/*`

1.  **All Handlers:** Update all handler stubs (like `condition_handler.py`) to import from `highway_core.engine.models` (your new models), **not** `highway_dsl`.
2.  **`condition_handler.py`:** This must be implemented.
      * It must evaluate the condition (using a safe library like `py_expression_eval`).
      * It must **return the *next* task to run** (e.g., `[task.if_true]` or `[task.if_false]`).
      * The `Orchestrator` must be updated to handle this. The `Orchestrator`'s `run` loop will get this list and *dynamically add* that task and its dependencies to the `TopologicalSorter`. (This is advanced. A simpler model for now: a `condition` operator is just a task that *results in* a "next\_task\_id", and another task depends on that.)
      * **Let's simplify:** The `Orchestrator`'s job is to run the DAG. A `condition` task's *dependents* will be *both* `if_true` and `if_false`. The `condition` handler's job is just to *run*. The `if_true`/`if_false` tasks will have a *runtime dependency* check in the Orchestrator. This is too complex for now.
      * **Revised Tier 1.5 Orchestrator Logic:** We will keep the O(n^2) check for now, but refactor the `run` loop to support all operators.
    <!-- end list -->
    ```python
    # Refactored Orchestrator.run() (Tier 1.5)
    def run(self):
        while self.task_queue:
            task_id = self.task_queue.popleft()
            task_model = self.workflow.tasks.get(task_id)
            
            if not self._dependencies_met(task_model.dependencies):
                self.task_queue.append(task_id) # Put it back
                continue
            
            # --- NEW LOGIC ---
            handler_func = self.handler_map.get(task_model.operator_type)
            if not handler_func:
                print(f"Orchestrator: Error - No handler for {task_model.operator_type}")
                # Handle failure
                continue
                
            try:
                # Handlers now return the list of next task IDs
                next_task_ids = handler_func(task_model, self.state, self.registry)
                
                self.completed_tasks.add(task_id)
                
                # Add the *specific* next tasks
                for next_id in next_task_ids:
                    if next_id not in self.task_queue:
                        self.task_queue.append(next_id)
                        
                # Also add any tasks that are now unblocked
                self.task_queue.extend(self._find_next_runnable_tasks())
                
            except Exception as e:
                # Handle task failure
                print(f"Orfchestrator: FATAL ERROR in {task_id}: {e}")
                break
    ```
3.  **Refactor `task_handler.py`:**
      * It must now `return []` (an empty list, as a `task` operator doesn't define its own *next* step, its dependents do).
      * **Bulkhead:** The `BulkheadManager` should be created *once* in the `Orchestrator` and *passed into* the `task_handler.execute` function. The `TaskHandler` should *not* create its own manager.
      * The `BulkheadManager` should create and hold *one bulkhead per tool function* (e.g., one for `"tools.fetch.get"`, one for `"tools.shell.run"`). It should *not* create a new bulkhead for every task.
    <!-- end list -->
    ```python
    # In Orchestrator.__init__
    self.bulkhead_manager = BulkheadManager()

    # In Orchestrator.run()
    handler_func(task_model, self.state, self.registry, self.bulkhead_manager)

    # In task_handler.execute()
    def execute(task, state, registry, bulkhead_manager):
        tool_name = task.function
        # ...
        
        # Get or create a bulkhead FOR THAT TOOL
        bulkhead = bulkhead_manager.get_bulkhead(tool_name)
        if not bulkhead:
            # Create a default config for the tool
            config = BulkheadConfig(name=tool_name, ...)
            bulkhead = bulkhead_manager.create_bulkhead(config)
            
        # ... (rest of your logic to execute via bulkhead.execute)
    ```

-----

### New Test Workflow: `examples/tier_1_5_refactor_test.yaml`

This new workflow will validate your refactored engine. It specifically tests the new features:

  * `Condition` operator handling.
  * `fetch` tool (which will test the new `Bulkhead` logic).
  * `tools.log.error` tool.
  * Explicit `{{results...}}` and `{{variables...}}` state resolution.

<!-- end list -->
this yaml is in examples/tier_1_5_refactor_test.yaml

```yaml
name: tier1_5_refactor_test
version: 1.0.0
description: Tests the refactored Tier 1.5 engine (handlers, state, and bulkheads).
variables:
  base_api_url: "https://jsonplaceholder.typicode.com"
  
start_task: log_start

tasks:
  log_start:
    task_id: log_start
    operator_type: task
    function: tools.log.info
    args: ["Starting Tier 1.5 Refactor Test..."]
    dependencies: []

  # Tests tools.fetch.get, result_key, and bulkhead
  fetch_todo_1:
    task_id: fetch_todo_1
    operator_type: task
    function: tools.fetch.get
    args: ["{{variables.base_api_url}}/todos/1"]
    dependencies: ["log_start"]
    result_key: "todo_1" # Saves to results.todo_1

  # Tests ConditionOperator and explicit state resolution
  check_fetch_status:
    task_id: check_fetch_status
    operator_type: condition
    condition: "{{results.todo_1.status}} == 200" # Explicit state
    dependencies: ["fetch_todo_1"]
    if_true: "log_success"
    if_false: "log_failure"

  # Tests the 'if_true' branch
  log_success:
    task_id: log_success
    operator_type: task
    function: tools.log.info
    args:
      - "Fetch successful. Title: {{results.todo_1.data.title}}" # Nested resolution
    dependencies: ["check_fetch_status"]

  # Tests the 'if_false' branch
  log_failure:
    task_id: log_failure
    operator_type: task
    function: tools.log.error
    args:
      - "Fetch failed. Status: {{results.todo_1.status}}"
    dependencies: ["check_fetch_status"]

  # Tests that the orchestrator can continue after a branch
  log_end:
    task_id: log_end
    operator_type: task
    function: tools.log.info
    args: ["Test finished."]
    # Depends on BOTH branches. The orchestrator must wait
    # for the completed path (log_success or log_failure).
    dependencies: ["log_success", "log_failure"]
```

### Pass Criteria (Expected Output)

When you run your refactored engine with this new YAML, I will fact-check the output. It **must** look like this (assuming the fetch is successful):

```
Engine: Loading workflow from: examples/tier_1_5_refactor_test.yaml
ToolRegistry loaded with X functions.
WorkflowState initialized.
Orchestrator initialized.
Orchestrator: Starting workflow 'tier1_5_refactor_test'
Orchestrator: Executing task log_start
TaskHandler: Executing task: log_start
TaskHandler: Calling tools.log.info with args=['Starting Tier 1.5 Refactor Test...']
[HighwayEngine] - Starting Tier 1.5 Refactor Test...
Orchestrator: Task log_start completed.
Orchestrator: Executing task fetch_todo_1
TaskHandler: Executing task: fetch_todo_1
TaskHandler: Getting or creating bulkhead for 'tools.fetch.get'
TaskHandler: Calling tools.fetch.get via bulkhead...
  [Tool.Fetch.Get] Fetching https://jsonplaceholder.typicode.com/todos/1...
State: Setting result for key: todo_1
Orchestrator: Task fetch_todo_1 completed.
Orchestrator: Executing task check_fetch_status
ConditionHandler: Evaluating '{{results.todo_1.status}} == 200'
ConditionHandler: Resolved to '200 == 200'. Result: True
Orchestrator: Task check_fetch_status completed. Adding 'log_success' to queue.
Orchestrator: Executing task log_success
TaskHandler: Executing task: log_success
TaskHandler: Calling tools.log.info with args=['Fetch successful. Title: delectus aut autem']
[HighwayEngine] - Fetch successful. Title: delectus aut autem
Orchestrator: Task log_success completed.
Orchestrator: Executing task log_end
TaskHandler: Executing task: log_end
TaskHandler: Calling tools.log.info with args=['Test finished.']
[HighwayEngine] - Test finished.
Orchestrator: Task log_end completed.
Orchestrator: Workflow 'tier1_5_refactor_test' finished.
```

*(Note: The "Task log\_failure" path should be correctly skipped.)*
