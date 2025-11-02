"""Additional unit tests for the bulkhead async functionality."""

import asyncio
import pytest
from highway_core.tools.bulkhead import (
    Bulkhead,
    BulkheadConfig,
    with_bulkhead_async,
    BulkheadManager,
    BulkheadError,
)


@pytest.mark.asyncio
async def test_bulkhead_execute_async():
    """Test async execution through bulkhead."""
    config = BulkheadConfig(name="test_async", timeout_seconds=2.0)
    bulkhead = Bulkhead(config)

    async def async_func(x, y):
        await asyncio.sleep(0.1)  # Simulate async work
        return x + y

    result = await bulkhead.execute_async(async_func, 2, 3)

    assert result == 5
    bulkhead.shutdown()


@pytest.mark.asyncio
async def test_bulkhead_execute_async_with_exception():
    """Test async execution with exception."""
    config = BulkheadConfig(name="test_async", timeout_seconds=2.0)
    bulkhead = Bulkhead(config)

    async def failing_async_func():
        await asyncio.sleep(0.1)
        raise ValueError("Async error")

    with pytest.raises(BulkheadError):
        await bulkhead.execute_async(failing_async_func)

    bulkhead.shutdown()


@pytest.mark.asyncio
async def test_with_bulkhead_async_decorator():
    """Test the async decorator functionality."""
    manager = BulkheadManager()
    config = BulkheadConfig(name="async_test", timeout_seconds=2.0)
    bulkhead = manager.create_bulkhead(config)

    @with_bulkhead_async("async_test", manager)
    async def async_test_func(x):
        await asyncio.sleep(0.1)
        return x * 2

    result = await async_test_func(5)
    assert result == 10

    manager.shutdown_all()


@pytest.mark.asyncio
async def test_with_bulkhead_async_decorator_exception():
    """Test the async decorator with exception."""
    manager = BulkheadManager()
    config = BulkheadConfig(name="async_test", timeout_seconds=2.0)
    bulkhead = manager.create_bulkhead(config)

    @with_bulkhead_async("async_test", manager)
    async def failing_async_func():
        await asyncio.sleep(0.1)
        raise ValueError("Async test error")

    with pytest.raises(BulkheadError):
        await failing_async_func()

    manager.shutdown_all()


@pytest.mark.asyncio
async def test_bulkhead_async_timeout():
    """Test async execution timeout."""
    config = BulkheadConfig(name="test_async", timeout_seconds=0.1)
    bulkhead = Bulkhead(config)

    async def slow_async_func():
        await asyncio.sleep(0.2)  # Longer than timeout
        return "result"

    with pytest.raises(BulkheadError):
        await bulkhead.execute_async(slow_async_func)

    bulkhead.shutdown()
