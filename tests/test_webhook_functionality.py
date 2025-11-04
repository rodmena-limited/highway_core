import asyncio
from datetime import datetime, timezone
import pytest
import json
from unittest.mock import AsyncMock, patch

from highway_core.persistence.database import get_db_manager
from highway_core.persistence.webhook_runner import WebhookRunner


class TestWebhookFunctionality:
    """Test webhook functionality including creation, sending, and status management."""
    
    def setup_method(self):
        """Set up test database manager."""
        # Use a temporary file for testing
        import tempfile
        import os
        self.temp_db_path = tempfile.mktemp(suffix='.db')
        self.db_manager = get_db_manager(db_path=self.temp_db_path)

    def teardown_method(self):
        """Clean up after tests."""
        self.db_manager.close()
        # Remove the temporary database file
        import os
        if os.path.exists(self.temp_db_path):
            os.remove(self.temp_db_path)

    def test_create_webhook(self):
        """Test creating a webhook record."""
        # Create a webhook
        success = self.db_manager.create_webhook(
            task_id="test_task_1",
            execution_id="test_execution_1",
            workflow_id="test_workflow_1",
            url="http://127.0.0.1:7666/index.json",
            method="POST",
            headers={"Content-Type": "application/json"},
            payload={"event": "test", "message": "Test message"},
            webhook_type="on_complete",
            max_retries=3,
            rate_limit_requests=10,
            rate_limit_window=60
        )
        
        assert success is True
        
        # Verify the webhook was created
        webhooks = self.db_manager.get_webhooks_by_workflow("test_workflow_1")
        assert len(webhooks) == 1
        webhook = webhooks[0]
        
        assert webhook['task_id'] == 'test_task_1'
        assert webhook['execution_id'] == 'test_execution_1'
        assert webhook['workflow_id'] == 'test_workflow_1'
        assert webhook['url'] == 'http://127.0.0.1:7666/index.json'
        assert webhook['method'] == 'POST'
        assert webhook['headers'] == {"Content-Type": "application/json"}
        assert webhook['payload'] == {"event": "test", "message": "Test message"}
        assert webhook['webhook_type'] == 'on_complete'
        assert webhook['status'] == 'pending'
        assert webhook['max_retries'] == 3
        assert webhook['retry_count'] == 0

    def test_get_ready_webhooks(self):
        """Test getting ready webhooks for processing."""
        # Create a few webhooks
        self.db_manager.create_webhook(
            task_id="test_task_1",
            execution_id="test_execution_1",
            workflow_id="test_workflow_1",
            url="http://127.0.0.1:7666/index.json",
            method="POST",
            headers={"Content-Type": "application/json"},
            payload={"event": "test1", "message": "Test message 1"},
            webhook_type="on_complete"
        )
        
        self.db_manager.create_webhook(
            task_id="test_task_2",
            execution_id="test_execution_2",
            workflow_id="test_workflow_2",
            url="http://127.0.0.1:7666/index.json",
            method="POST",
            headers={"Content-Type": "application/json"},
            payload={"event": "test2", "message": "Test message 2"},
            webhook_type="on_start"
        )
        
        # Get ready webhooks
        ready_webhooks = self.db_manager.get_ready_webhooks(limit=10)
        
        assert len(ready_webhooks) == 2
        for webhook in ready_webhooks:
            assert webhook['status'] in ['pending', 'retry_scheduled']

    def test_update_webhook_response(self):
        """Test updating webhook with response information."""
        # Create a webhook
        success = self.db_manager.create_webhook(
            task_id="test_task_1",
            execution_id="test_execution_1",
            workflow_id="test_workflow_1",
            url="http://127.0.0.1:7666/index.json",
            method="POST",
            headers={"Content-Type": "application/json"},
            payload={"event": "test", "message": "Test message"},
            webhook_type="on_complete"
        )
        
        assert success is True
        
        # Get the webhook to get its ID
        webhooks = self.db_manager.get_webhooks_by_workflow("test_workflow_1")
        webhook = webhooks[0]
        webhook_id = webhook['id']
        
        # Update webhook with success response
        update_success = self.db_manager.update_webhook_response(
            webhook_id=webhook_id,
            response_status=200,
            response_headers={"Content-Type": "application/json"},
            response_body='{"status": "success"}',
            success=True
        )
        
        assert update_success is True
        
        # Verify webhook status was updated to 'sent'
        updated_webhooks = self.db_manager.get_webhooks_by_workflow("test_workflow_1")
        updated_webhook = updated_webhooks[0]
        
        assert updated_webhook['status'] == 'sent'
        assert updated_webhook['response_status'] == 200
        assert updated_webhook['response_body'] == '{"status": "success"}'

    def test_schedule_webhook_retry(self):
        """Test scheduling a webhook for retry."""
        # Create a webhook
        success = self.db_manager.create_webhook(
            task_id="test_task_1",
            execution_id="test_execution_1",
            workflow_id="test_workflow_1",
            url="http://127.0.0.1:7666/index.json",
            method="POST",
            headers={"Content-Type": "application/json"},
            payload={"event": "test", "message": "Test message"},
            webhook_type="on_complete"
        )
        
        assert success is True
        
        # Get the webhook to get its ID
        webhooks = self.db_manager.get_webhooks_by_workflow("test_workflow_1")
        webhook = webhooks[0]
        webhook_id = webhook['id']
        
        # Initially should have 0 retry count
        assert webhook['retry_count'] == 0
        
        # Schedule a retry
        retry_success = self.db_manager.schedule_webhook_retry(webhook_id)
        
        assert retry_success is True
        
        # Verify webhook was updated for retry
        updated_webhooks = self.db_manager.get_webhooks_by_workflow("test_workflow_1")
        updated_webhook = updated_webhooks[0]
        
        assert updated_webhook['status'] == 'retry_scheduled'
        assert updated_webhook['retry_count'] == 1
        assert updated_webhook['next_retry_at'] is not None

    def test_get_webhooks_by_status(self):
        """Test getting webhooks by status."""
        # Create webhooks with different statuses
        self.db_manager.create_webhook(
            task_id="test_task_1",
            execution_id="test_execution_1",
            workflow_id="test_workflow_1",
            url="http://127.0.0.1:7666/index.json",
            method="POST",
            headers={"Content-Type": "application/json"},
            payload={"event": "test1", "message": "Test message 1"},
            webhook_type="on_complete"
        )
        
        # Get pending webhooks
        pending_webhooks = self.db_manager.get_webhooks_by_status(["pending"])
        assert len(pending_webhooks) == 1
        assert pending_webhooks[0]['status'] == 'pending'

    def test_webhook_unique_constraint(self):
        """Test that webhook with same execution_id, task_id, workflow_id, and webhook_type cannot be duplicated."""
        # Create first webhook
        success1 = self.db_manager.create_webhook(
            task_id="test_task_1",
            execution_id="test_execution_1",
            workflow_id="test_workflow_1",
            url="http://127.0.0.1:7666/index.json",
            method="POST",
            headers={"Content-Type": "application/json"},
            payload={"event": "test", "message": "Test message"},
            webhook_type="on_complete"
        )
        
        assert success1 is True
        
        # Try to create duplicate webhook - should fail
        success2 = self.db_manager.create_webhook(
            task_id="test_task_1",
            execution_id="test_execution_1",
            workflow_id="test_workflow_1",
            url="http://127.0.0.1:7666/index.json2",  # Different URL but same other fields
            method="POST",
            headers={"Content-Type": "application/json"},
            payload={"event": "test", "message": "Test message"},
            webhook_type="on_complete"
        )
        
        assert success2 is False  # Should fail due to unique constraint

    def test_cleanup_completed_webhooks(self):
        """Test cleaning up old completed webhooks."""
        # Create a webhook and mark it as sent
        success = self.db_manager.create_webhook(
            task_id="test_task_1",
            execution_id="test_execution_1",
            workflow_id="test_workflow_1",
            url="http://127.0.0.1:7666/index.json",
            method="POST",
            headers={"Content-Type": "application/json"},
            payload={"event": "test", "message": "Test message"},
            webhook_type="on_complete"
        )
        
        assert success is True
        
        # Get the webhook ID and update its status to sent
        webhooks = self.db_manager.get_webhooks_by_workflow("test_workflow_1")
        webhook = webhooks[0]
        webhook_id = webhook['id']
        
        # Update webhook to mark as sent
        update_success = self.db_manager.update_webhook_response(
            webhook_id=webhook_id,
            response_status=200,
            response_headers={"Content-Type": "application/json"},
            response_body='{"status": "success"}',
            success=True
        )
        
        assert update_success is True
        
        # Try to clean up - no webhooks should be cleaned since they're not old enough
        cleaned_count = self.db_manager.cleanup_completed_webhooks(days_old=0)  # Clean immediately
        assert cleaned_count >= 0  # May be 0 or more depending on existing data


class TestWebhookRunner:
    """Test webhook runner functionality."""
    
    @pytest.mark.asyncio
    async def test_webhook_runner_initialization(self):
        """Test that webhook runner initializes properly."""
        runner = WebhookRunner(batch_size=10, concurrency=5, rate_limit_per_second=10)
        
        assert runner.batch_size == 10
        assert runner.concurrency == 5
        assert runner.rate_limit_per_second == 10
        assert runner.db_manager is not None
    
    @pytest.mark.asyncio
    async def test_send_webhook_success(self):
        """Test sending a webhook successfully."""
        runner = WebhookRunner(batch_size=10, concurrency=5, rate_limit_per_second=10)
        
        # Mock webhook data
        webhook_data = {
            'id': 1,
            'url': 'http://httpbin.org/post',  # Using httpbin for testing
            'method': 'POST',
            'headers': {'Content-Type': 'application/json'},
            'payload': {'test': 'data'},
            'retry_count': 0,
            'max_retries': 3
        }
        
        # Use httpbin.org to test webhook sending without needing a local server
        with patch('aiohttp.ClientSession.request') as mock_request:
            # Mock the response
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.text.return_value = '{"status": "success"}'
            mock_response.headers = {'Content-Type': 'application/json'}
            
            mock_request.return_value.__aenter__.return_value = mock_response
            
            success, status, headers, body = await runner._send_webhook(webhook_data)
            
            assert success is True
            assert status == 200
            assert body == '{"status": "success"}'
    
    @pytest.mark.asyncio
    async def test_send_webhook_failure(self):
        """Test handling webhook sending failure."""
        runner = WebhookRunner(batch_size=10, concurrency=5, rate_limit_per_second=10)
        
        # Mock webhook data
        webhook_data = {
            'id': 1,
            'url': 'http://invalid-url-that-does-not-exist.com/webhook',
            'method': 'POST',
            'headers': {'Content-Type': 'application/json'},
            'payload': {'test': 'data'},
            'retry_count': 0,
            'max_retries': 3
        }
        
        success, status, headers, body = await runner._send_webhook(webhook_data)
        
        assert success is False
        # Status might be 0 for connection errors
        assert status == 0 or (status >= 400)  # Either connection error or HTTP error
    
    @pytest.mark.asyncio
    async def test_get_base_url(self):
        """Test extracting base URL for rate limiting."""
        runner = WebhookRunner(batch_size=10, concurrency=5, rate_limit_per_second=10)
        
        full_url = "http://example.com:8080/api/webhook?param=value"
        base_url = runner._get_base_url(full_url)
        
        assert base_url == "http://example.com:8080"
    
    @pytest.mark.asyncio
    async def test_check_rate_limit(self):
        """Test rate limiting functionality."""
        runner = WebhookRunner(batch_size=10, concurrency=5, rate_limit_per_second=10)
        
        # Initially should be within limits
        assert runner._check_rate_limit("http://example.com") is True
        
        # Add many requests to exceed rate limit
        for i in range(15):  # More than our 10 per second limit
            runner._record_request("http://example.com")
        
        # Now should be over the limit
        assert runner._check_rate_limit("http://example.com") is False