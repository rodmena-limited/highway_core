from highway_dsl import WorkflowBuilder


def demonstrate_persistence_test_workflow():
    """
    Defines a workflow that tests persistence functionality.
    """

    builder = WorkflowBuilder("tier_4_persistence_test")

    builder.task(
        "log_start",
        "tools.log.info",
        args=["(Run: {{variables.run_id}}) - Step 1: Logging start..."],
        result_key="step1",
    )

    builder.task(
        "step_2",
        "tools.memory.set",
        args=["final_step", "Step 2 was here"],
        dependencies=["log_start"],
        result_key="step2",
    )

    builder.task(
        "log_end",
        "tools.log.info",
        args=["(Run: {{variables.run_id}}) - Step 3: Workflow finished."],
        dependencies=["step_2"],
    )

    workflow = builder.build()

    workflow.set_variables(
        {
            "run_id": "persistent-run-123",  # A static ID for testing
        }
    )

    return workflow


if __name__ == "__main__":
    persistence_workflow = demonstrate_persistence_test_workflow()
    print(persistence_workflow.to_yaml())
