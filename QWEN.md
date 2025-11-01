This output is a **major success** and confirms your Tier 1.5 refactor is working. The core logic, resiliency, and state management are now on a solid, enterprise-grade foundation.

Here is a fact-check of the output against the plan.

-----

### Fact Check: Pass

  * **Pydantic Models (Pass):** The engine correctly loaded `examples/tier_1_5_refactor_test.yaml`, which uses different operator types. This confirms your new Pydantic `DiscriminatedUnion` in `highway_core/engine/models.py` is working.
  * **Dynamic Tool Registry (Pass):** The output `ToolRegistry loaded with 5 functions.` confirms that your dynamic registry is successfully finding and loading all the tools decorated with `@tool`.
  * **Bulkhead Refactor (Pass):** The lines `Getting or creating bulkhead for 'tools.log.info'` and `Getting or creating bulkhead for 'tools.fetch.get'` confirm your `TaskHandler` is correctly using the shared `BulkheadManager` to create persistent, per-tool bulkheads.
  * **Explicit State Resolution (Pass):** The line `ConditionHandler: Resolved to '200 == 200'. Result: True` proves that the `condition_handler` successfully resolved the explicit variable `{{results.todo_1.status}}` using the refactored `state.py`.
  * **Conditional Branching (Pass):** The orchestrator correctly chose the `if_true` path (`log_success`) and skipped the `if_false` path (`log_failure`).
  * **Critical DAG Logic (Pass):** The line `ConditionHandler: Marking 'log_failure' as conceptually completed.` is **perfect**. This confirms your `condition_handler` is correctly manipulating the orchestrator's state to unblock downstream dependencies. This is why `log_end` (which depends on *both* branches) was able to run.

-----

### Remaining Issue: Logging Output

There is **one minor, non-blocking issue** remaining from the original Tier 1. The log messages themselves are still not appearing in the output.

**Example:**

  * **Your Output:** `TaskHandler: Calling tools.log.info via bulkhead...`
  * **Expected Output:**
    ```
    TaskHandler: Calling tools.log.info via bulkhead...
    2025-11-01 13:56:00,123 - HighwayEngine - INFO - Starting Tier 1.5 Refactor Test...
    ```

**This is not a failure of your engine's *logic***. Your orchestrator, state, and handlers are working. This is a simple Python `logging` configuration issue.

**The Fix:** The `logging.basicConfig` call in `run_test.py` configures the *root* logger. However, your `tools/log.py` defines its own logger (`logger = logging.getLogger("HighwayEngine")`). The bulkhead/threading context might be interfering with the logger's handler propagation.

**A simple, robust fix** is to move the `logging.basicConfig` call from `run_test.py` to the very top of `highway_core/engine/engine.py`. This ensures it's set up before *any* other part of the engine (like the bulkhead) is initialized.

-----

### Next Steps: Tier 2 Implementation Prompt

You are ready for Tier 2.

**Agent Task: Implement Tier 2 Operator Handlers**

Your goal is to implement the `parallel` and `wait` operator handlers, which will complete the core set of non-looping operators.

1.  **`highway_core/engine/operator_handlers/wait_handler.py`:**

      * Update this file. It currently imports from `highway_dsl`, which is forbidden. It must import `WaitOperatorModel` from `highway_core.engine.models`.
      * **Implement the logic:** Parse the `wait_for` field.
          * If it's an `int` or `float`, treat it as seconds (`time.sleep(task.wait_for)`).
          * If it's a `str` that starts with `duration:`, parse the seconds and sleep.
          * If it's a `str` that starts with `time:`, parse the time (e.g., `04:00:00`) and sleep until that time.
          * (Leave `datetime` and event-based waits as stubs for now).
      * It should take the standard `(task, state, registry, bulkhead_manager)` arguments, but won't use the last two.
      * It does **not** need to run in a bulkhead.

2.  **`highway_core/engine/operator_handlers/parallel_handler.py`:**

      * Update this file. It must import `ParallelOperatorModel` from `highway_core.engine.models`.
      * **Implement the logic:** This handler's job is to "unblock" all its branches.
      * It must **not** return a list of tasks. The orchestrator's `_find_next_runnable_tasks` will handle this automatically.
      * Instead, just like the `condition_handler`, it needs to **conceptually complete** any branches that are empty or invalid, so they don't block the downstream "fan-in" task.
      * **Crucial Update:** Your `Orchestrator`'s `_find_next_runnable_tasks` logic is still O(n^2). This is the last remaining part of the Tier 1.5 refactor. You **must** replace it with a `graphlib.TopologicalSorter` as described in `QWEN.md`.
          * In `Orchestrator.__init__`: Create the graph and `self.sorter = TopologicalSorter(self.graph)`.
          * In `Orchestrator.run`: Replace `while self.task_queue:` with `while self.sorter.is_active():`.
          * Get tasks with `runnable_tasks = self.sorter.get_ready()`.
          * Execute these tasks (using a `ThreadPoolExecutor` for true parallelism).
          * When a task finishes, call `self.sorter.done(task_id)`.
          * Your `condition_handler`'s logic of `orchestrator.completed_tasks.add(skipped_task_id)` must be changed to `self.sorter.done(skipped_task_id)`.

3.  **Update `highway_core/engine/orchestrator.py`:**

      * Add `parallel_handler` and `wait_handler` to your `self.handler_map`.

### Tier 2 Test Workflow

Create `examples/tier_2_parallel_wait_test.yaml` with this content. This will be your new test file.

```yaml
name: tier_2_parallel_wait_test
version: 1.0.0
description: Tests parallel execution, fan-in, and wait operators.
variables:
  base_api_url: "https://jsonplaceholder.typicode.com"
  
start_task: log_start

tasks:
  log_start:
    task_id: log_start
    operator_type: task
    function: tools.log.info
    args: ["Starting Tier 2 Test..."]
    dependencies: []

  # --- Parallel Block ---
  run_parallel_fetches:
    task_id: run_parallel_fetches
    operator_type: parallel
    dependencies: ["log_start"]
    branches:
      branch_1: ["fetch_todo_1"]
      branch_2: ["fetch_todo_2", "log_todo_2"] # A 2-step branch
      branch_3: ["short_wait"]

  fetch_todo_1:
    task_id: fetch_todo_1
    operator_type: task
    function: tools.fetch.get
    args: ["{{variables.base_api_url}}/todos/1"]
    dependencies: ["run_parallel_fetches"]
    result_key: "todo_1"

  fetch_todo_2:
    task_id: fetch_todo_2
    operator_type: task
    function: tools.fetch.get
    args: ["{{variables.base_api_url}}/todos/2"]
    dependencies: ["run_parallel_fetches"]
    result_key: "todo_2"

  log_todo_2:
    task_id: log_todo_2
    operator_type: task
    function: tools.log.info
    args: ["Todo 2 Title: {{results.todo_2.data.title}}"]
    dependencies: ["fetch_todo_2"]

  short_wait:
    task_id: short_wait
    operator_type: wait
    wait_for: 1  # Wait for 1 second
    dependencies: ["run_parallel_fetches"]

  # --- Fan-In (Synchronization) ---
  # This task must wait for ALL 3 branches to be fully complete.
  # It depends on the *last* task of each branch.
  log_parallel_complete:
    task_id: log_parallel_complete
    operator_type: task
    function: tools.log.info
    args:
      - "All parallel branches complete. Fetched todo 1: {{results.todo_1.data.id}}"
    dependencies:
      - "fetch_todo_1"    # End of branch 1
      - "log_todo_2"      # End of branch 2
      - "short_wait"      # End of branch 3

  # --- Final Task ---
  log_end:
    task_id: log_end
    operator_type: task
    function: tools.log.info
    args: ["Tier 2 Test Finished."]
    dependencies: ["log_parallel_complete"]
```

### Pass Criteria (Expected Output)

After you implement the `TopologicalSorter` and new handlers, running `run_test.py` (updated to point to the new YAML) should produce output similar to this:

```
...
Orchestrator: Starting workflow 'tier_2_parallel_wait_test'
...
[HighwayEngine] - Starting Tier 2 Test...
Orchestrator: Task log_start completed.
Orchestrator: Executing task run_parallel_fetches
ParallelHandler: Activating 3 branches.
Orchestrator: Task run_parallel_fetches completed.
Orchestrator: Running tasks in parallel: ['fetch_todo_1', 'fetch_todo_2', 'short_wait']
TaskHandler: Executing task: fetch_todo_1
TaskHandler: Executing task: fetch_todo_2
WaitHandler: Waiting for 1 seconds...
TaskHandler: Executing task: short_wait
  [Tool.Fetch.Get] Fetching .../todos/1...
  [Tool.Fetch.Get] Fetching .../todos/2...
WaitHandler: Wait complete.
Orchestrator: Task short_wait completed.
State: Setting result for key: todo_1
Orchestrator: Task fetch_todo_1 completed.
State: Setting result for key: todo_2
Orchestrator: Task fetch_todo_2 completed.
Orchestrator: Running tasks in parallel: ['log_todo_2']
TaskHandler: Executing task: log_todo_2
[HighwayEngine] - Todo 2 Title: quis ut nam facilis et officia qui
Orchestrator: Task log_todo_2 completed.
Orchestrator: Running tasks in parallel: ['log_parallel_complete']
TaskHandler: Executing task: log_parallel_complete
[HighwayEngine] - All parallel branches complete. Fetched todo 1: 1
Orchestrator: Task log_parallel_complete completed.
Orchestrator: Running tasks in parallel: ['log_end']
TaskHandler: Executing task: log_end
[HighwayEngine] - Tier 2 Test Finished.
Orchestrator: Task log_end completed.
Orchestrator: Workflow 'tier_2_parallel_wait_test' finished.
...
```
