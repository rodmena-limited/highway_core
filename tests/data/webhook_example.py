from highway_dsl import WorkflowBuilder


def create_webhook_example_workflow():
    """
    Create a workflow that demonstrates webhook functionality.
    Webhooks are registered to send the result and status of tasks automatically.
    """
    builder = WorkflowBuilder("webhook_example")

    # Define the start task
    builder.task(
        "task_1",
        "tools.log.info",
        args=["Hello from task 1!"],
        result_key="task_1_result",
    )

    # Register webhook to send task_1 status when it completes
    builder.task(
        "register_webhook_task_1_completed",
        "tools.webhook.post",
        args=["task_1", "on_completed", "http://127.0.0.1:7666/index.json"],
    )

    # Register webhook to send when task_1 fails
    builder.task(
        "register_webhook_task_1_failed",
        "tools.webhook.post",
        args=["task_1", "on_failed", "http://127.0.0.1:7666/index.json"],
    )

    # Another task
    builder.task(
        "task_2",
        "tools.log.info",
        args=["Hello from task 2!"],
        result_key="task_2_result",
        dependencies=["task_1"],  # Wait for task_1 to complete
    )

    # Register webhook to send task_2 status when it completes
    builder.task(
        "register_webhook_task_2_completed",
        "tools.webhook.post",
        args=["task_2", "on_completed", "http://127.0.0.1:7666/index.json"],
    )

    return builder.build()


def webhook_example():
    """
    Main function that returns the webhook example workflow.
    This follows the pattern expected by our CLI.
    """
    return create_webhook_example_workflow()


if __name__ == "__main__":
    workflow = create_webhook_example_workflow()
    print(workflow.to_yaml())
