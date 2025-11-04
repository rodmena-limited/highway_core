from highway_dsl import WorkflowBuilder


def demonstrate_loop_test_workflow():
    """
    Defines a workflow that tests while loops and foreach loops.
    """

    builder = WorkflowBuilder("tier_3_loop_test")

    # --- Main Graph Tasks ---
    builder.task(
        "initialize_counter",
        "tools.memory.set",
        args=["loop_counter", 0],
        result_key="init_counter",
    )

    builder.while_loop(
        "main_while_loop",
        condition="{{memory.loop_counter}} < 3",
        loop_body=lambda b: b.task(
            "increment_counter",
            "tools.memory.increment",
            args=["loop_counter"],
            result_key="counter_result",
        ),
        dependencies=["initialize_counter"],
    )

    builder.task(
        "log_while_complete",
        "tools.log.info",
        args=["While loop complete. Final counter: {{memory.loop_counter}}"],
        dependencies=["main_while_loop"],
    )

    builder.foreach(
        "process_users_foreach",
        items="{{variables.user_ids}}",
        loop_body=lambda fb: fb.task(
            "fetch_user",
            "tools.fetch.get",
            args=["https://jsonplaceholder.typicode.com/users/{{item}}"],
            result_key="user_data",
        ).task(
            "log_user",
            "tools.log.info",
            args=["Fetched user: {{results.user_data.data.name}} (ID: {{item}})"],
            dependencies=["fetch_user"],
        ),
        dependencies=["log_while_complete"],
    )

    builder.task(
        "log_end",
        "tools.log.info",
        args=["Tier 3 Test Finished."],
        dependencies=["process_users_foreach"],
    )

    workflow = builder.build()

    workflow.set_variables(
        {
            "user_ids": [1, 2, 3],
        }
    )

    return workflow


if __name__ == "__main__":
    loop_workflow = demonstrate_loop_test_workflow()
    print(loop_workflow.to_yaml())
