from highway_dsl import WorkflowBuilder

def demonstrate_parallel_wait_workflow():
    """
    Defines a workflow that tests parallel execution, fan-in, and wait operators.
    """

    builder = WorkflowBuilder("tier_2_parallel_wait_test")

    builder.task(
        "log_start",
        "tools.log.info",
        args=["Starting Tier 2 Test..."],
    )

    # --- Parallel Block ---
    builder.parallel(
        "run_parallel_fetches",
        branches={
            "branch_1": lambda b: b.task(
                "fetch_todo_1",
                "tools.fetch.get",
                args=["{{variables.base_api_url}}/todos/1"],
                result_key="todo_1",
            ),
            "branch_2": lambda b: b.task(
                "fetch_todo_2",
                "tools.fetch.get",
                args=["{{variables.base_api_url}}/todos/2"],
                result_key="todo_2",
            ).task(
                "log_todo_2",
                "tools.log.info",
                args=["Todo 2 Title: {{results.todo_2.data.title}}"],
                dependencies=["fetch_todo_2"],
            ),
            "branch_3": lambda b: b.wait(
                "short_wait",
                wait_for=1,
            ),
        },
        dependencies=["log_start"],
    )

    # --- Fan-In (Synchronization) ---
    builder.task(
        "log_parallel_complete",
        "tools.log.info",
        args=["All parallel branches complete. Fetched todo 1: {{results.todo_1.data.id}}"],
        dependencies=["fetch_todo_1", "log_todo_2", "short_wait"],
    )

    # --- Final Task ---
    builder.task(
        "log_end",
        "tools.log.info",
        args=["Tier 2 Test Finished."],
        dependencies=["log_parallel_complete"],
    )

    workflow = builder.build()

    workflow.set_variables({
        "base_api_url": "https://jsonplaceholder.typicode.com",
    })

    return workflow


if __name__ == "__main__":
    parallel_workflow = demonstrate_parallel_wait_workflow()
    print(parallel_workflow.to_yaml())