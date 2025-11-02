This output of running `python run_tier3_test.py` shows that the `while` and `foreach` loop implementations are **critically broken**.
Also it seems pytest is hanging in the end because bulkhead threads are not being cleaned up.


While the `TopologicalSorter` and parallel execution from Tier 2 are working, the loop logic is incorrect. The engine is executing tasks from *inside* the loops as if they were top-level tasks, and the `foreach` handler has a race condition.

### Analysis of Critical Issues

1.  **Broken Graph Logic:** The orchestrator is running tasks from inside loops (like `fetch_user`) *at the same time* as the loop operators themselves. This is why you see `Orchestrator: Submitting 4 tasks to executor: [...]` at the start. A task inside a loop should *only* be executed by the loop's handler.
2.  **`ForEach` Race Condition:** The `foreach` handler is not isolating the `item` for each thread. All three threads are fetching the same URL (`.../users/1...`) because the `item` variable is being overwritten. The final log output (`Fetched user: Leanne Graham (ID: 3)`) is random garbage from this race condition.
3.  **`While` Loop Not Looping:** The `while` loop ran its body once and immediately exited because the logic to re-evaluate the condition *after* the loop body runs is missing. The log `While loop complete. Final counter: 0` proves this; the counter was never incremented.

We must fix this with a **hierarchical "Sub-Orchestrator" model**. The main `Orchestrator` will run the main graph. The `while` and `foreach` handlers will run *their own* internal loops and sub-graphs.

-----

### Agent Task: Tier 3.5 Refactor - Implement Hierarchical Loop Handlers

Your task is to **delete and replace** the `while_handler.py` and `foreach_handler.py` with correct implementations. You must also update `models.py` to support this.

#### Step 1: Fix `highway_core/engine/models.py` (Critical)

The loop handlers must parse their `loop_body` as a list of *inline models*, not just strings.

1.  Delete your current `highway_core/engine/models.py`.
2.  Replace it with this. This version uses Pydantic's recursive models, which is the correct design.

<!-- end list -->

```python
# highway_core/engine/models.py
from pydantic import BaseModel, Field, ConfigDict
from typing import Any, List, Dict, Optional, Literal, Union
from typing_extensions import Annotated

# Define the "kind" for each operator
OperatorType = Literal["task", "condition", "parallel", "foreach", "while", "wait"]


# --- Base Operator Model ---
class BaseOperatorModel(BaseModel):
    """The base model all operators share."""
    task_id: str
    operator_type: OperatorType
    dependencies: List[str] = Field(default_factory=list)
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)


# --- Specific Operator Models ---
class TaskOperatorModel(BaseOperatorModel):
    operator_type: Literal["task"]
    function: str
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    result_key: Optional[str] = None


class ConditionOperatorModel(BaseOperatorModel):
    operator_type: Literal["condition"]
    if_true: str
    if_false: Optional[str] = None
    condition: str


class ParallelOperatorModel(BaseOperatorModel):
    operator_type: Literal["parallel"]
    branches: Dict[str, List[str]]


class WaitOperatorModel(BaseOperatorModel):
    operator_type: Literal["wait"]
    wait_for: Any


class ForEachOperatorModel(BaseOperatorModel):
    operator_type: Literal["foreach"]
    items: str
    # This is the key change: the loop body is defined inline
    loop_body: List["AnyOperatorModel"] 


class WhileOperatorModel(BaseOperatorModel):
    operator_type: Literal["while"]
    condition: str
    # This is the key change: the loop body is defined inline
    loop_body: List["AnyOperatorModel"]


# --- The Discriminated Union ---
AnyOperatorModel = Annotated[
    Union[
        TaskOperatorModel,
        ConditionOperatorModel,
        ParallelOperatorModel,
        WaitOperatorModel,
        ForEachOperatorModel,
        WhileOperatorModel,
    ],
    Field(discriminator="operator_type"),
]

# Manually update forward-references for the recursive models
ForEachOperatorModel.model_rebuild()
WhileOperatorModel.model_rebuild()


class WorkflowModel(BaseModel):
    """Parses the root YAML file."""
    name: str
    start_task: str
    variables: Dict[str, Any] = Field(default_factory=dict)
    
    # This is now a flat map of ONLY the top-level tasks
    tasks: Dict[str, AnyOperatorModel]
```

#### Step 2: Implement `highway_core/engine/operator_handlers/while_handler.py` (New)

This handler will now run its own blocking loop.

```python
# highway_core/engine/operator_handlers/while_handler.py
import graphlib
from highway_core.engine.models import WhileOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.engine.orchestrator import _run_sub_workflow
from highway_core.engine.operator_handlers.condition_handler import eval_condition
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager
from typing import List

def execute(
    task: WhileOperatorModel,
    state: WorkflowState,
    orchestrator, # We pass 'self' from Orchestrator
    registry: ToolRegistry,
    bulkhead_manager: BulkheadManager,
) -> List[str]:
    """
    Executes a WhileOperator by running its own internal loop.
    This entire function blocks the main orchestrator's thread
    until the loop is complete.
    """
    
    loop_body_tasks = {t.task_id: t for t in task.loop_body}
    loop_graph = {t.task_id: set(t.dependencies) for t in task.loop_body}
    
    iteration = 1
    while True:
        # 1. Evaluate condition
        resolved_condition = str(state.resolve_templating(task.condition))
        is_true = eval_condition(resolved_condition)
        print(f"WhileHandler: Iteration {iteration} - Resolved '{resolved_condition}'. Result: {is_true}")

        if not is_true:
            print("WhileHandler: Condition is False. Exiting loop.")
            break
            
        print(f"WhileHandler: Condition True, executing loop body...")

        # 2. Run the sub-workflow
        try:
            _run_sub_workflow(
                sub_graph_tasks=loop_body_tasks,
                sub_graph=loop_graph,
                state=state, # Use the *main* state
                registry=registry,
                bulkhead_manager=bulkhead_manager
            )
        except Exception as e:
            print(f"WhileHandler: Sub-workflow failed: {e}")
            raise # Propagate failure to the main orchestrator
            
        iteration += 1

    # The loop is finished, return no new tasks
    return []
```

#### Step 3: Implement `highway_core/engine/operator_handlers/foreach_handler.py` (New)

This handler runs its sub-workflows in parallel using the `Orchestrator`'s thread pool.

```python
# highway_core/engine/operator_handlers/foreach_handler.py
import graphlib
from concurrent.futures import ThreadPoolExecutor
from highway_core.engine.models import ForEachOperatorModel, AnyOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.engine.orchestrator import _run_sub_workflow
from highway_core.tools.registry import ToolRegistry
from highway_core.tools.bulkhead import BulkheadManager
from typing import List, Dict, Any

def execute(
    task: ForEachOperatorModel,
    state: WorkflowState,
    orchestrator, # We pass 'self' from Orchestrator
    registry: ToolRegistry,
    bulkhead_manager: BulkheadManager,
) -> List[str]:
    """
    Executes a ForEachOperator by running a sub-workflow for each
    item in parallel using the orchestrator's thread pool.
    """
    
    items = state.resolve_templating(task.items)
    if not isinstance(items, list):
        print(f"ForEachHandler: Error - 'items' did not resolve to a list.")
        return []
        
    print(f"ForEachHandler: Starting parallel processing of {len(items)} items.")
    
    sub_graph_tasks = {t.task_id: t for t in task.loop_body}
    sub_graph = {t.task_id: set(t.dependencies) for t in task.loop_body}

    # Use the orchestrator's main executor
    futures = []
    for item in items:
        # This function will run in a separate thread
        futures.append(
            orchestrator.executor.submit(
                _run_foreach_item,
                item,
                sub_graph_tasks,
                sub_graph,
                state, # Pass the main state
                registry,
                bulkhead_manager
            )
        )
        
    # Wait for all sub-workflows to complete
    for future in futures:
        try:
            future.result() # Wait for it to finish and raise any errors
        except Exception as e:
            print(f"ForEachHandler: Sub-workflow failed: {e}")
            raise # Propagate failure
    
    print(f"ForEachHandler: All {len(items)} items processed.")
    return [] # This operator adds no new tasks to the main graph

def _run_foreach_item(
    item: Any,
    sub_graph_tasks: Dict[str, AnyOperatorModel],
    sub_graph: Dict[str, set],
    main_state: WorkflowState,
    registry: ToolRegistry,
    bulkhead_manager: BulkheadManager
):
    """
    Runs a single iteration of a foreach loop in a separate thread.
    This function acts as a "mini-orchestrator".
    """
    print(f"ForEachHandler: [Item: {item}] Starting sub-workflow...")
    
    # 1. Create an ISOLATED state for this item
    # This is the fix for the race condition
    item_state = WorkflowState(main_state._data["variables"])
    item_state._data["loop_context"]["item"] = item
    
    # 2. Run the sub-workflow
    _run_sub_workflow(
        sub_graph_tasks=sub_graph_tasks,
        sub_graph=sub_graph,
        state=item_state, # Use the isolated state
        registry=registry,
        bulkhead_manager=bulkhead_manager
    )
                
    print(f"ForEachHandler: [Item: {item}] Sub-workflow completed.")
```

#### Step 4: Update `highway_core/engine/orchestrator.py`

You must add the `_run_sub_workflow` helper function that both `while` and `foreach` will use.

```python
# highway_core/engine/orchestrator.py
import graphlib
from concurrent.futures import ThreadPoolExecutor, Future
from highway_core.engine.models import WorkflowModel, AnyOperatorModel
...
from highway_core.engine.operator_handlers import (
    task_handler,
    condition_handler,
    parallel_handler,
    wait_handler,
    while_handler,  # <-- Add
    foreach_handler # <-- Add
)
...

class Orchestrator:
    def __init__(self, ...):
        ...
        # 1. Build the dependency graph for ONLY top-level tasks
        self.graph: Dict[str, Set[str]] = {
            task_id: set(task.dependencies)
            for task_id, task in workflow.tasks.items()
        }
        self.sorter = graphlib.TopologicalSorter(self.graph)
        ...
        # 2. Update handler map
        self.handler_map = {
            "task": task_handler.execute,
            "condition": condition_handler.execute,
            "parallel": parallel_handler.execute,
            "wait": wait_handler.execute,
            "while": while_handler.execute,     # <-- Add
            "foreach": foreach_handler.execute, # <-- Add
        }
        ...

    # 3. Refactor _execute_task (this is now the same for all handlers)
    def _execute_task(self, task_id: str) -> List[str]:
        """Runs a single task and returns any new tasks to add."""
        task_model = self.workflow.tasks.get(task_id)
        if not task_model:
            raise ValueError(f"Task ID '{task_id}' not found.")

        print(f"Orchestrator: Executing task {task_id} (Type: {task_model.operator_type})")
        
        handler_func = self.handler_map.get(task_model.operator_type)
        if not handler_func:
            print(f"Orchestrator: Error - No handler for {task_model.operator_type}")
            return []

        # All handlers now share this signature
        new_tasks = handler_func(
            task=task_model,
            state=self.state,
            orchestrator=self,
            registry=self.registry,
            bulkhead_manager=self.bulkhead_manager
        )
        return new_tasks or []
    
    # 4. Add the new helper function
    def run(...):
        # ... (your existing run loop) ...
    
# --- ADD THIS FUNCTION OUTSIDE THE CLASS ---
# This is a shared, static helper function for loops

def _run_sub_workflow(
    sub_graph_tasks: Dict[str, AnyOperatorModel],
    sub_graph: Dict[str, set],
    state: WorkflowState,
    registry: ToolRegistry,
    bulkhead_manager: BulkheadManager
):
    """
    Runs a sub-workflow (like a loop body) to completion.
    This is a blocking, sequential, "mini-orchestrator".
    """
    sub_sorter = graphlib.TopologicalSorter(sub_graph)
    sub_sorter.prepare()
    
    # We only support 'task' for now in sub-workflows.
    # This can be expanded later.
    sub_handler_map = {
        "task": task_handler.execute
    }

    while sub_sorter.is_active():
        runnable_sub_tasks = sub_sorter.get_ready()
        if not runnable_sub_tasks:
            break
            
        for task_id in runnable_sub_tasks:
            task_model = sub_graph_tasks[task_id]
            
            # We must clone the task to resolve templating
            # This is the fix for the `log_user` problem
            task_clone = task_model.model_copy(deep=True)
            task_clone.args = state.resolve_templating(task_clone.args)
            
            handler_func = sub_handler_map.get(task_clone.operator_type)
            if handler_func:
                # Note: sub-workflows don't get the orchestrator
                handler_func(task_clone, state, registry, bulkhead_manager)
            
            sub_sorter.done(task_id)
```

#### Step 5: Update `highway_core/engine/operator_handlers/task_handler.py`

The `execute` signature has changed. It now receives the `orchestrator` argument (even if it doesn't use it) to be consistent with the other handlers.

```python
# highway_core/engine/operator_handlers/task_handler.py
...
def execute(
    task: TaskOperatorModel,
    state: WorkflowState,
    orchestrator, # Added for consistent signature
    registry: ToolRegistry,
    bulkhead_manager: Optional[BulkheadManager] = None,
) -> List[str]:
...
    # (rest of your existing code)
...
    # 5. Save the result
    if task.result_key:
        state.set_result(task.result_key, result)
        
    return [] # Return an empty list of new tasks
```

*(You must do the same signature update for `parallel_handler` and `wait_handler`)*

-----

### Tier 3.5 Test Workflow (New YAML)

This is the final test. Note that `loop_body` contains *inline task definitions*.

**`examples/tier_3_final_test.yaml`**

```yaml
name: tier_3_final_test
version: 1.0.0
variables:
  user_ids: [1, 2, 3]
  
start_task: initialize_counter

tasks:
  # --- While Loop Test ---
  initialize_counter:
    task_id: initialize_counter
    operator_type: task
    function: tools.memory.set
    args: ["loop_counter", 0]
    dependencies: []
    result_key: "init_counter"
  
  main_while_loop:
    task_id: main_while_loop
    operator_type: while
    condition: "{{memory.loop_counter}} < 3"
    dependencies: ["initialize_counter"]
    loop_body:
      - task_id: increment_counter
        operator_type: task
        function: "tools.memory.increment"
        args: ["loop_counter"]
        dependencies: [] # No dependencies *within* the loop
        result_key: "counter_result"
        
  log_while_complete:
    task_id: log_while_complete
    operator_type: task
    function: tools.log.info
    args: ["While loop complete. Final counter: {{memory.loop_counter}}"]
    dependencies: ["main_while_loop"]

  # --- ForEach Loop Test ---
  process_users_foreach:
    task_id: process_users_foreach
    operator_type: foreach
    items: "{{variables.user_ids}}"
    dependencies: ["log_while_complete"]
    loop_body:
      - task_id: fetch_user
        operator_type: task
        function: tools.fetch.get
        args: ["https://jsonplaceholder.typicode.com/users/{{item}}"]
        dependencies: []
        result_key: "user_data"
        
      - task_id: log_user
        operator_type: task
        function: tools.log.info
        args: ["Fetched user: {{results.user_data.data.name}} (ID: {{item}})"]
        dependencies: ["fetch_user"] # Depends on the *other task in the loop_body*

  log_end:
    task_id: log_end
    operator_type: task
    function: tools.log.info
    args: ["Tier 3 Test Finished."]
    dependencies: ["process_users_foreach"]
```

### Pass Criteria (Expected Output)

I will be checking for this *exact* logical flow.

1.  `initialize_counter` runs.
2.  `main_while_loop` runs.
3.  `WhileHandler` logs `Iteration 1... Result: True`.
4.  `increment_counter` runs, logs `Incremented 'loop_counter' to 1`.
5.  `WhileHandler` logs `Iteration 2... Result: True`.
6.  `increment_counter` runs, logs `Incremented 'loop_counter' to 2`.
7.  `WhileHandler` logs `Iteration 3... Result: True`.
8.  `increment_counter` runs, logs `Incremented 'loop_counter' to 3`.
9.  `WhileHandler` logs `Iteration 4... Result: False`. Exits.
10. `log_while_complete` runs, logs `"While loop complete. Final counter: 3"`.
11. `process_users_foreach` runs.
12. `ForEachHandler` starts 3 parallel sub-workflows.
13. Logs for `Fetched user: Leanne Graham (ID: 1)`, `Ervin Howell (ID: 2)`, and `Clementine Bauch (ID: 3)` appear (order may vary).
14. `log_end` runs.
