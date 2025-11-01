Here is the priority order for implementation, designed to get a simple, linear workflow running first (the "Minimum Viable Product") and then build up to the complex features.

### Tier 1: The "Hello, World" Linear Workflow

This is the absolute minimum you need to execute a simple chain of `TaskOperators` (e.g., `task1 -> task2 -> task3`).

1.  **`tools/log.py`**: Start by implementing `info()` and `error()`. This is your "print statement." It's the simplest tool, has no dependencies, and gives you immediate feedback.
2.  **`tools/registry.py`**: Implement this to register your new `tools.log.info` function. This module is the "phonebook" that connects a string (`"tools.log.info"`) to the actual Python function.
3.  **`engine/state.py`**: This is the engine's "memory." Implement the `WorkflowState` class, including `set_result` and, most importantly, `resolve_templating`. The engine is useless without its ability to resolve variables like `"{{my_result}}"`.
4.  **`engine/operator_handlers/task_handler.py`**: This is the workhorse. It needs to:
    * Get the function from the `ToolRegistry` (Priority 2).
    * Get the arguments from the `TaskOperator`.
    * Resolve the arguments using `WorkflowState` (Priority 3).
    * Call the function.
    * Save the return value back into the `WorkflowState` using the `result_key`.
5.  **`engine/orchestrator.py`**: This is the "brain." Implement a simple, *linear* version first. It needs to:
    * Load the workflow.
    * Find the `start_task`.
    * Call the `task_handler` (Priority 4) to execute it.
    * Implement the `_dependencies_met` function.
    * Find the next task based on the completed task's dependencies and run it.
6.  **`engine/engine.py`**: This is the "ignition switch." It's a simple file that just creates the `Orchestrator` and `WorkflowState` and calls `orchestrator.run()`.

***
**Result:** At the end of Tier 1, you can run a simple workflow like `log.info("start") -> log.info("middle") -> log.info("end")`.

### Tier 2: Core Logic (To run your `fetch` example)

Now, add the core control flow operators and the tools needed for your test workflow.

1.  **`tools/fetch.py`**: Implement `get()` using the `requests` library. This is your first real-world tool.
2.  **`tools/memory.py`**: Implement the `set_memory` function. This is a "special" tool, so you will need to update `task_handler.py` to recognize `tools.memory.set` and pass the `WorkflowState` object to it as a first argument.
3.  **`engine/operator_handlers/condition_handler.py`**: Implement this to handle `if/else` logic. You must **use a safe expression evaluator library** (like `py_expression_eval` or `asteval`)—do **not** use Python's built-in `eval()` as it is a major security risk.
4.  **`engine/operator_handlers/parallel_handler.py`**: This is more complex. The handler should return *all* the starting tasks from *all* branches to the Orchestrator's queue. You will also need to update the Orchestrator's `_dependencies_met` logic to handle "fan-in" (i.e., a task that depends on all branches of a parallel operator).

***
**Result:** At the end of Tier 2, you can run your `execution_engine_test_v1.py` workflow.

### Tier 3: Full Feature Set (Loops & Agentic Tools)

This tier adds the remaining complex operators and tools needed for the massive agentic workflows.

1.  **`engine/operator_handlers/wait_handler.py`**: This is the easiest of the remaining operators. Implement `time.sleep()` for `timedelta` and a simple check for `datetime` or `str` values.
2.  **`engine/operator_handlers/while_handler.py`**: Implement the `while` loop logic. This will be similar to the `condition` handler, but if `True`, it returns the first task of its `loop_body`. The final task in the `loop_body` must depend on the `while` operator itself to create the loop.
3.  **`engine/operator_handlers/foreach_handler.py`**: This is the most complex operator. It needs to:
    * Resolve the `items` list.
    * Iterate over the list.
    * For each item, set the `item` in a special `state.loop_context`.
    * Run the *entire* `loop_body` as a sub-workflow for that item.
4.  **`tools/command.py`**: Implement `tools.shell.run` using the `subprocess` module.
5.  **All other `tools/` modules**: Add the stubs for `llm.py`, `vcs.py`, `ansible.py`, `filesystem.py`, etc.

***
**Result:** At the end of Tier 3, your engine can run *all* the massive workflows we designed.

### Tier 4: Reliability (Persistence)

Implement this last. This layer doesn't add new features but makes the engine robust.

1.  **All `persistence/` modules**: Build the `PersistenceManager` and the `DatabasePersistence` implementation.
2.  **Update `Orchestrator`**: Modify the orchestrator to call `persistence.save_workflow_state()` after every completed task.
3.  **Update `engine.py`**: Add logic to `run_workflow` to check if a workflow ID already exists in persistence and, if so, load its state and resume.
