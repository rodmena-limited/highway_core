import sys
from datetime import timedelta

try:
    from highway_dsl import (
        WorkflowBuilder,
        RetryPolicy,
        TimeoutPolicy
    )
except ImportError:
    print("Error: highway_dsl library not found.")
    sys.exit(1)


def create_expense_workflow():
    """
    Defines the Expense Report Approval workflow using Highway DSL.
    
    This workflow models a human-in-the-loop process with a timeout,
    conditional branching, and parallel execution.
    """

    builder = WorkflowBuilder("ExpenseReportApproval")

    # 1. save the report
    builder.task(
        "save_report",
        "db.save_report",
        args=["{{variables.report_data}}"],
        result_key="report",
        retry_policy=RetryPolicy(max_retries=2, delay=timedelta(seconds=1))
    )

    # 2. Request manager approval.
    # This task will durably pause. We set a 3-day timeout
    builder.task(
        "request_manager_approval",
        "human.request_approval",
        # We pass the prompt and manager email
        args=[
            "Approve expense report {{report.id}} for ${{report.amount}}?",
            "{{report.manager_email}}"
        ],
        result_key="approval", # The result will be {"status": "APPROVED" | "DENIED"}
        timeout_policy=TimeoutPolicy(
            timeout=timedelta(days=3),
            kill_on_timeout=False # We want it to report a timeout
        )
    )

    # 3. Main conditional logic
    builder.condition(
        "check_approval",
        condition="{{approval.status}} == 'APPROVED'",
        
        # --- 3a. IF TRUE (Approved) ---
        if_true=lambda approved_branch: approved_branch.parallel(
            "process_approved_branch",
            branches={
                "payments": lambda pb: pb.task(
                    "run_payment",
                    "api.payment_gateway.pay",
                    args=["{{report.id}}", "{{report.amount}}"]
                ),
                "notify": lambda pb: pb.task(
                    "notify_employee_approved",
                    "email.send_approved_notification",
                    args=["{{report.user_email}}"]
                )
            }
        ),

        # --- 3b. IF FALSE (Denied or Timeout) ---
        if_false=lambda denied_branch: denied_branch.condition(
            "check_denial_or_timeout",
            condition="{{approval.reason}} == 'TIMEOUT'",
            
            # --- IF TRUE (Timeout) ---
            if_true=lambda timeout_branch: timeout_branch.task(
                "escalate_to_finance",
                "email.escalate_finance_queue",
                args=["{{report.id}}", "Overdue approval"]
            ),
            
            # --- IF FALSE (Explicitly Denied) ---
            if_false=lambda regular_denial_branch: regular_denial_branch.task(
                "notify_employee_denied",
                "email.send_denied_notification",
                args=["{{report.user_email}}"]
            )
        )
    )
    
    # --- 4. Final "fan-in" task ---
    # This task runs after any of the three final outcomes.
    builder.task(
        "close_report",
        "tools.log.info", # Just log it
        args=["Closing report {{report.id}}"],
        dependencies=[
            "process_approved_branch", # The parallel operator
            "escalate_to_finance",     # The timeout task
            "notify_employee_denied"   # The denied task
        ]
    )

    workflow = builder.build()

    workflow.set_variables({
        "report_data": {
            "id": "exp-test-123",
            "amount": 500,
            "user_email": "employee@company.com",
            "manager_email": "manager@company.com"
        }
    })

    return workflow


if __name__ == "__main__":
    expense_workflow = create_expense_workflow()
    print(expense_workflow.to_yaml())