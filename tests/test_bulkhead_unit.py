"""Unit tests for the bulkhead pattern implementation."""

import threading
import time
from concurrent.futures import Future, TimeoutError
from unittest.mock import Mock, patch

import pytest

from highway_core.tools.bulkhead import (
    Bulkhead,
    BulkheadCircuitOpenError,
    BulkheadConfig,
    BulkheadError,
    BulkheadFullError,
    BulkheadIsolationError,
    BulkheadManager,
    BulkheadState,
    BulkheadTimeoutError,
    CircuitBreaker,
    ExecutionResult,
    get_default_manager,
    set_default_manager,
    shutdown_default_manager,
    with_bulkhead,
    with_bulkhead_async,
)


def test_bulkhead_initialization():
    """Test bulkhead initialization with config."""
    config = BulkheadConfig(name="test", max_concurrent_calls=3, max_queue_size=5)
    bulkhead = Bulkhead(config)

    assert bulkhead.name == "test"
    assert bulkhead.config.max_concurrent_calls == 3
    assert bulkhead.config.max_queue_size == 5
    assert bulkhead.get_state() == BulkheadState.HEALTHY
    bulkhead.shutdown()


def test_bulkhead_execute_success():
    """Test successful execution through bulkhead."""
    config = BulkheadConfig(name="test", timeout_seconds=1.0)
    bulkhead = Bulkhead(config)

    def test_func(x, y):
        return x + y

    future = bulkhead.execute(test_func, 2, 3)
    result = future.result(timeout=1.0)

    assert isinstance(result, ExecutionResult)
    assert result.success is True
    assert result.result == 5
    assert result.error is None
    bulkhead.shutdown()


def test_bulkhead_execute_sync():
    """Test synchronous execution through bulkhead."""
    config = BulkheadConfig(name="test", timeout_seconds=1.0)
    bulkhead = Bulkhead(config)

    def test_func(x):
        return x * 2

    result = bulkhead.execute_sync(test_func, 5)

    assert isinstance(result, ExecutionResult)
    assert result.success is True
    assert result.result == 10
    assert result.error is None
    bulkhead.shutdown()


def test_bulkhead_execute_with_exception():
    """Test execution that raises an exception."""
    config = BulkheadConfig(name="test", timeout_seconds=1.0)
    bulkhead = Bulkhead(config)

    def failing_func():
        raise ValueError("Test error")

    future = bulkhead.execute(failing_func)

    # The future should complete with an exception
    with pytest.raises(BulkheadError):
        future.result(timeout=2.0)

    bulkhead.shutdown()


def test_bulkhead_queue_full():
    """Test behavior when queue is full."""
    config = BulkheadConfig(name="test", max_concurrent_calls=1, max_queue_size=1)
    bulkhead = Bulkhead(config)

    def slow_func():
        time.sleep(0.5)
        return "done"

    # Submit first task (will be running)
    future1 = bulkhead.execute(slow_func)
    time.sleep(0.1)  # Give time for the first task to start being processed

    # Submit second task (will be queued)
    future2 = bulkhead.execute(slow_func)
    time.sleep(0.1)  # Give time for the second task to enter the queue

    # Try to submit third task (should fail - queue is full)
    with pytest.raises(BulkheadFullError):
        bulkhead.execute(slow_func)

    # Verify the first two tasks still complete
    result1 = future1.result(timeout=2.0)
    result2 = future2.result(timeout=2.0)
    assert result1.success is True
    assert result2.success is True
    bulkhead.shutdown()


def test_bulkhead_execution_timeout():
    """Test execution timeout behavior."""
    config = BulkheadConfig(name="test", timeout_seconds=0.1)
    bulkhead = Bulkhead(config)

    def slow_func():
        time.sleep(0.2)
        return "completed"

    future = bulkhead.execute(slow_func)

    # This should raise a timeout error
    with pytest.raises(BulkheadTimeoutError):
        future.result(timeout=0.5)

    bulkhead.shutdown()


def test_bulkhead_with_concurrent_calls():
    """Test concurrent execution within limits."""
    config = BulkheadConfig(name="test", max_concurrent_calls=2, timeout_seconds=2.0)
    bulkhead = Bulkhead(config)

    def slow_func(duration):
        time.sleep(duration)
        return f"completed after {duration}s"

    # Submit multiple tasks
    futures = []
    for i in range(3):  # 3 tasks, but only 2 can run concurrently
        future = bulkhead.execute(slow_func, 0.1)
        futures.append(future)

    # All should complete successfully
    for future in futures:
        result = future.result(timeout=1.0)
        assert isinstance(result, ExecutionResult)
        assert result.success is True

    bulkhead.shutdown()


def test_bulkhead_stats():
    """Test bulkhead statistics."""
    config = BulkheadConfig(name="test", timeout_seconds=1.0)
    bulkhead = Bulkhead(config)

    def test_func():
        return "success"

    # Execute a few tasks
    future1 = bulkhead.execute(test_func)
    future2 = bulkhead.execute(test_func)

    result1 = future1.result(timeout=1.0)
    result2 = future2.result(timeout=1.0)

    stats = bulkhead.get_stats()
    assert stats["total_executions"] >= 2
    assert stats["successful_executions"] >= 2

    bulkhead.shutdown()


def test_bulkhead_context_manager():
    """Test bulkhead context manager."""
    config = BulkheadConfig(name="test")
    bulkhead = Bulkhead(config)

    with bulkhead.context() as bh:
        assert bh is bulkhead

    bulkhead.shutdown()


def test_bulkhead_shutdown():
    """Test bulkhead shutdown functionality."""
    config = BulkheadConfig(name="test", timeout_seconds=1.0)
    bulkhead = Bulkhead(config)

    def test_func():
        return "test"

    # Submit a task
    future = bulkhead.execute(test_func)
    result = future.result(timeout=1.0)
    assert result.success is True

    # Shutdown bulkhead
    bulkhead.shutdown()

    # Verify shutdown completed cleanly


def test_bulkhead_is_healthy():
    """Test is_healthy method."""
    config = BulkheadConfig(name="test")
    bulkhead = Bulkhead(config)

    assert bulkhead.is_healthy() is True
    assert bulkhead.get_state() == BulkheadState.HEALTHY

    bulkhead.shutdown()


def test_bulkhead_reset_stats():
    """Test reset stats functionality."""
    config = BulkheadConfig(name="test", timeout_seconds=1.0)
    bulkhead = Bulkhead(config)

    def test_func():
        return "success"

    future = bulkhead.execute(test_func)
    result = future.result(timeout=1.0)

    stats_before = bulkhead.get_stats()
    assert stats_before["total_executions"] > 0

    bulkhead.reset_stats()
    stats_after = bulkhead.get_stats()

    assert stats_after["total_executions"] == 0
    assert stats_after["successful_executions"] == 0
    assert stats_after["failed_executions"] == 0
    assert stats_after["rejected_executions"] == 0

    bulkhead.shutdown()


def test_circuit_breaker_initially_healthy():
    """Test circuit breaker starts in healthy state."""
    cb = CircuitBreaker(5, 3, 30.0, "test")
    assert cb.get_state() == BulkheadState.HEALTHY
    assert cb.is_request_allowed() is True


def test_circuit_breaker_success_transition():
    """Test circuit breaker transition on success."""
    cb = CircuitBreaker(2, 2, 30.0, "test")

    # Should transition from healthy to degraded on failures
    cb.record_failure()
    assert cb.get_state() == BulkheadState.DEGRADED

    # Should transition back to healthy after enough successes
    cb.record_success()
    cb.record_success()
    assert cb.get_state() == BulkheadState.HEALTHY


def test_circuit_breaker_failure_transition():
    """Test circuit breaker isolation on multiple failures."""
    cb = CircuitBreaker(2, 2, 0.1, "test")

    # Should transition to isolated after threshold failures
    cb.record_failure()
    cb.record_failure()
    assert cb.get_state() == BulkheadState.ISOLATED

    # Requests should be denied while isolated
    assert cb.is_request_allowed() is False

    # Wait for isolation period to end
    time.sleep(0.15)
    assert cb.is_request_allowed() is True  # Should auto-close after isolation


def test_circuit_breaker_stats():
    """Test circuit breaker statistics."""
    cb = CircuitBreaker(5, 3, 30.0, "test")

    cb.record_failure()
    stats = cb.get_stats()
    assert stats["failure_count"] == 1
    assert stats["success_count"] == 0
    assert stats["state"] == BulkheadState.DEGRADED.value

    cb.record_success()
    stats = cb.get_stats()
    assert stats["failure_count"] == 0  # Success resets failure count
    assert stats["success_count"] == 1
    assert (
        stats["state"] == BulkheadState.DEGRADED.value
    )  # Still degraded since we didn't have enough successes to recover


def test_bulkhead_manager_create_bulkhead():
    """Test bulkhead manager creation."""
    manager = BulkheadManager()

    config = BulkheadConfig(name="test", max_concurrent_calls=2)
    bulkhead = manager.create_bulkhead(config)

    assert bulkhead is not None
    assert manager.get_bulkhead("test") is not None

    manager.shutdown_all()


def test_bulkhead_manager_get_or_create():
    """Test get_or_create functionality."""
    manager = BulkheadManager()

    config = BulkheadConfig(name="test", max_concurrent_calls=2)

    # Create first bulkhead
    bh1 = manager.get_or_create_bulkhead(config)
    # Get the same one again
    bh2 = manager.get_or_create_bulkhead(config)

    assert bh1 is bh2  # Same instance

    manager.shutdown_all()


def test_bulkhead_manager_execute_in_bulkhead():
    """Test executing in specific bulkhead."""
    manager = BulkheadManager()

    config = BulkheadConfig(name="test", timeout_seconds=1.0)
    manager.create_bulkhead(config)

    def test_func(x):
        return x * 2

    future = manager.execute_in_bulkhead("test", test_func, 5)
    result = future.result(timeout=1.0)

    assert isinstance(result, ExecutionResult)
    assert result.success is True
    assert result.result == 10

    manager.shutdown_all()


def test_bulkhead_manager_execute_nonexistent_bulkhead():
    """Test executing in nonexistent bulkhead."""
    manager = BulkheadManager()

    def test_func():
        return "test"

    with pytest.raises(ValueError):
        manager.execute_in_bulkhead("nonexistent", test_func)

    manager.shutdown_all()


def test_bulkhead_manager_get_all_stats():
    """Test getting stats for all bulkheads."""
    manager = BulkheadManager()

    config1 = BulkheadConfig(name="bh1", timeout_seconds=1.0)
    config2 = BulkheadConfig(name="bh2", timeout_seconds=1.0)

    manager.create_bulkhead(config1)
    manager.create_bulkhead(config2)

    stats = manager.get_all_stats()
    assert "bh1" in stats
    assert "bh2" in stats

    manager.shutdown_all()


def test_bulkhead_manager_get_health_status():
    """Test getting health status for all bulkheads."""
    manager = BulkheadManager()

    config1 = BulkheadConfig(name="bh1")
    config2 = BulkheadConfig(name="bh2")

    manager.create_bulkhead(config1)
    manager.create_bulkhead(config2)

    health = manager.get_health_status()
    assert "bh1" in health
    assert "bh2" in health
    assert health["bh1"] is True
    assert health["bh2"] is True

    manager.shutdown_all()


def test_bulkhead_manager_shutdown_all():
    """Test shutting down all bulkheads."""
    manager = BulkheadManager()

    config1 = BulkheadConfig(name="bh1")
    config2 = BulkheadConfig(name="bh2")

    manager.create_bulkhead(config1)
    manager.create_bulkhead(config2)

    # Verify they exist
    assert manager.get_bulkhead("bh1") is not None
    assert manager.get_bulkhead("bh2") is not None

    # Shutdown all
    manager.shutdown_all()

    # Bulkheads should be gone
    assert manager.get_bulkhead("bh1") is None
    assert manager.get_bulkhead("bh2") is None


def test_bulkhead_manager_context_manager():
    """Test bulkhead manager as context manager."""
    with BulkheadManager() as manager:
        config = BulkheadConfig(name="test")
        bulkhead = manager.create_bulkhead(config)
        assert manager.get_bulkhead("test") is not None

        # The bulkhead should be automatically shut down when exiting context
        # We'll just verify the context manager works


def test_with_bulkhead_decorator():
    """Test the with_bulkhead decorator."""
    manager = BulkheadManager()
    config = BulkheadConfig(name="test", timeout_seconds=1.0)
    manager.create_bulkhead(config)

    def test_func(x, y):
        return x + y

    # Apply decorator
    decorated_func = with_bulkhead("test", manager)(test_func)

    future = decorated_func(2, 3)
    result = future.result(timeout=1.0)

    assert isinstance(result, ExecutionResult)
    assert result.success is True
    assert result.result == 5

    manager.shutdown_all()


def test_with_bulkhead_decorator_exception():
    """Test decorator with function that raises an exception."""
    manager = BulkheadManager()
    config = BulkheadConfig(name="test", timeout_seconds=1.0)
    manager.create_bulkhead(config)

    def failing_func():
        raise ValueError("Test error")

    decorated_func = with_bulkhead("test", manager)(failing_func)

    future = decorated_func()

    # The future should complete with an exception
    with pytest.raises(BulkheadError):
        future.result(timeout=2.0)

    manager.shutdown_all()


def test_default_manager():
    """Test default manager functionality."""
    # Use a separate manager to avoid conflicts with other tests
    temp_manager = BulkheadManager()
    set_default_manager(temp_manager)

    manager = get_default_manager()
    assert manager is temp_manager

    shutdown_default_manager()
    assert get_default_manager() is not None  # Should recreate default manager
    shutdown_default_manager()  # Clean up
