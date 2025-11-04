#!/usr/bin/env python3
"""
Test script to verify that the webhook functionality is working properly.
"""
import os
import tempfile

from highway_core.persistence.database import get_db_manager


def test_webhook_registration():
    """Test that the webhook table exists and can register webhooks."""

    # Create a temporary database for testing
    temp_db_path = tempfile.mktemp(suffix=".db")

    try:
        # Create database manager with temporary database
        db_manager = get_db_manager(db_path=temp_db_path)

        # Test creating a webhook
        success = db_manager.create_webhook(
            task_id="test_task_1",
            execution_id="test_execution_1",
            workflow_id="test_workflow_1",
            url="http://127.0.0.1:7666/index.json",
            method="POST",
            headers={"Content-Type": "application/json"},
            payload={"event": "test", "message": "Test webhook"},
            webhook_type="on_complete",
        )

        print(f"Webhook creation success: {success}")

        # Test getting ready webhooks
        ready_webhooks = db_manager.get_ready_webhooks(limit=10)
        print(f"Ready webhooks count: {len(ready_webhooks)}")

        if ready_webhooks:
            print("First webhook details:")
            first_wh = ready_webhooks[0]
            print(f"  ID: {first_wh['id']}")
            print(f"  Task: {first_wh['task_id']}")
            print(f"  URL: {first_wh['url']}")
            print(f"  Method: {first_wh['method']}")
            print(f"  Status: {first_wh['status']}")

        # Test webhook status counts
        pending_webhooks = db_manager.get_webhooks_by_status(["pending"])
        print(f"Pending webhooks: {len(pending_webhooks)}")

        # Close the database
        db_manager.close()

        # Clean up
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)

        print("✅ Webhook functionality test passed!")

    except Exception as e:
        print(f"❌ Webhook functionality test failed: {e}")
        # Clean up in case of error
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)
        raise


if __name__ == "__main__":
    test_webhook_registration()
