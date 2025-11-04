import sys
from datetime import timedelta

from highway_dsl import RetryPolicy, WorkflowBuilder

builder = WorkflowBuilder("data_processing_pipeline")

builder.task("start", "workflows.tasks.initialize", result_key="init_data")
builder.task(
    "validate",
    "workflows.tasks.validate_data",
    args=["{{init_data}}"],
    result_key="validated_data",
)

builder.condition(
    "check_quality",
    condition="{{validated_data.quality_score}} > 0.8",
    if_true=lambda b: b.task(
        "high_quality_processing",
        "workflows.tasks.advanced_processing",
        args=["{{validated_data}}"],
        retry_policy=RetryPolicy(
            max_retries=5, delay=timedelta(seconds=10), backoff_factor=2.0
        ),
    ),
    if_false=lambda b: b.task(
        "standard_processing",
        "workflows.tasks.basic_processing",
        args=["{{validated_data}}"],
    ),
)

workflow = builder.build()
print(workflow.to_yaml(), file=sys.stdout)
