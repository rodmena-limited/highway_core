from highway_dsl import WorkflowBuilder

def demonstrate_failing_workflow():
    """
    Defines a workflow that intentionally fails to test error handling.
    """

    builder = WorkflowBuilder("Failing Workflow Test")

    # Set the multiplier in memory
    builder.task(
        "set_multiplier",
        "tools.memory.set",
        args=["multiplier", 7],
        result_key="set_ok",
    )

    # This task will fail - using a non-existent function
    builder.task(
        "failing_task",
        "non.existent.function",
        args=[],
        dependencies=["set_multiplier"],
        result_key="fail_result",
    )

    workflow = builder.build()

    workflow.set_variables({
        "base": 100,
    })

    return workflow


if __name__ == "__main__":
    failing_workflow = demonstrate_failing_workflow()
    print(failing_workflow.to_yaml())