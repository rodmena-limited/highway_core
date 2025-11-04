import json
from datetime import timedelta, time, datetime

try:
    from highway_dsl import (
        Workflow,
        WorkflowBuilder,
        TaskOperator,
        ConditionOperator,
        ParallelOperator,
        WaitOperator,
        ForEachOperator,
        WhileOperator,
        RetryPolicy,
        TimeoutPolicy,
        OperatorType,
    )
except ImportError:
    print("Error: highway_dsl library not found. Please install it.")
    exit()


def demonstrate_ecommerce_workflow():
    """
    Defines a comprehensive e-commerce order processing workflow.
    """

    builder = WorkflowBuilder("ecommerce_order_processing_v1")

    # --- PHASE 1: ORDER VALIDATION ---
    builder.task("validate_order", "tools.memory.set", args=["order_status", "validating"])
    
    builder.parallel(
        "validate_items_availability",
        branches={
            "check_customer": lambda b: b.task(
                "check_customer_credit",
                "tools.fetch.get",
                args=["{{variables.api_base_url}}/users/{{variables.customer_id}}"],
                result_key="customer_credit",
            ),
            "check_inventory": lambda b: b.task(
                "check_inventory_stock",
                "tools.fetch.get",
                args=["{{variables.api_base_url}}/posts/{{variables.item_id}}"],
                result_key="inventory_status",
            ),
            "check_pricing": lambda b: b.task(
                "validate_item_prices",
                "tools.memory.set",
                args=["price_validated", "true"],
            ),
        },
        dependencies=["validate_order"]
    )

    # --- PHASE 2: PAYMENT PROCESSING ---
    builder.task(
        "process_payment",
        "tools.memory.set",
        args=["payment_method", "credit"],
        dependencies=["validate_items_availability"],
    )

    # --- PHASE 3: PARALLEL OPERATIONS ---
    builder.parallel(
        "execute_parallel_operations",
        branches={
            "update_inventory": lambda b: b.task(
                "update_inventory_levels",
                "tools.memory.set",
                args=["inventory_updated", "true"],
            ),
            "send_notifications": lambda b: b.task(
                "send_customer_notification",
                "tools.log.info",
                args=["Order confirmed! Payment method: {{memory.payment_method}}"],
            ).task(
                "send_warehouse_notification",
                "tools.log.info",
                args=["New order received for item {{variables.item_id}}"],
            ),
            "generate_documents": lambda b: b.task(
                "generate_invoice",
                "tools.memory.set",
                args=["invoice_generated", "true"],
            ).task(
                "generate_shipping_label",
                "tools.memory.set",
                args=["shipping_label_generated", "true"],
            ),
        },
        dependencies=["process_payment"]
    )

    # --- PHASE 4: WAIT AND RETRY LOGIC ---
    builder.wait(
        "wait_for_payment_confirmation",
        wait_for=1,
        dependencies=["execute_parallel_operations"],
    )

    builder.task(
        "check_payment_status",
        "tools.memory.set",
        args=["payment_attempts", "1"],
        dependencies=["wait_for_payment_confirmation"],
    )

    builder.condition(
        "verify_payment_complete",
        condition="{{memory.payment_attempts}} <= {{variables.max_retry_attempts}}",
        if_true=lambda b: b.task(
            "finalize_order",
            "tools.memory.set",
            args=["order_status", "completed"],
        ),
        if_false=lambda b: b.task(
            "handle_payment_failure",
            "tools.log.error",
            args=["Payment failed after {{variables.max_retry_attempts}} attempts"],
        ),
        dependencies=["check_payment_status"],
    )

    # --- PHASE 5: WHILE LOOP FOR INVENTORY CHECK ---
    builder.while_loop(
        "check_low_stock",
        condition="{{memory.loop_counter | default(0)}} < 2",
        loop_body=lambda b: b.task(
            "notify_low_stock",
            "tools.log.warning",
            args=["Low stock alert! Loop iteration: {{memory.loop_counter | default(0)}}"],
        ).task(
            "increment_counter",
            "tools.memory.increment",
            args=["loop_counter"],
        ).wait(
            "wait_for_restock",
            wait_for=1,
        ),
        dependencies=["verify_payment_complete"],
    )

    # --- PHASE 6: FOREACH FOR MULTIPLE ITEMS ---
    builder.foreach(
        "process_order_items",
        items="{{variables.order_items}}",
        loop_body=lambda fb: fb.task(
            "process_single_item",
            "tools.log.info",
            args=["Processing item: {{loop_context.item}}"],
        ),
        dependencies=["check_low_stock"],
    )

    # --- PHASE 7: FINALIZATION ---
    builder.task(
        "send_final_confirmation",
        "tools.log.info",
        args=["Order {{variables.order_id}} completed successfully!"],
        dependencies=["process_order_items"],
    )

    workflow = builder.build()

    workflow.set_variables(
        {
            "api_base_url": "https://jsonplaceholder.typicode.com",
            "max_retry_attempts": 3,
            "low_stock_threshold": 5,
            "customer_id": 1,
            "item_id": 1,
            "quantity": 2,
            "order_total": 100,
            "order_id": "ORD-12345",
            "order_items": ["item1", "item2", "item3"],
        }
    )

    return workflow


if __name__ == "__main__":
    ecommerce_workflow = demonstrate_ecommerce_workflow()

    # Write only the YAML content without extra text
    try:
        yaml_content = ecommerce_workflow.to_yaml()
        print(yaml_content)
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")