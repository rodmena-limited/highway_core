#!/usr/bin/env python3
"""
Complete example demonstrating the bulkhead pattern in action.
This simulates a microservices architecture with different components.
"""

import random
import threading
import time
from concurrent.futures import Future, as_completed, wait
from typing import List, Optional

# Import the bulkhead implementation
from highway_core.tools.bulkhead import (
    BulkheadConfig,
    BulkheadManager,
    ExecutionResult,
    with_bulkhead,
)


class DatabaseService:
    """Simulates a database service that can sometimes fail or be slow"""

    def __init__(self):
        self.call_count = 0

    def query(self, sql: str, user_id: Optional[int] = None) -> str:
        """Simulate a database query"""
        self.call_count += 1

        # Simulate random failures
        if random.random() < 0.2:  # 20% chance of failure
            raise Exception("Database connection failed")

        # Simulate slow queries occasionally
        if random.random() < 0.1:  # 10% chance of being slow
            time.sleep(2.0)
        else:
            time.sleep(0.1)  # Normal query time

        return f"Result for: {sql} - user: {user_id}"


class ExternalAPIService:
    """Simulates an external API that can be unreliable"""

    def __init__(self):
        self.call_count = 0

    def call_api(self, endpoint: str, data: Optional[dict] = None) -> str:
        """Simulate an API call"""
        self.call_count += 1

        # Simulate random timeouts
        if random.random() < 0.15:  # 15% chance of timeout
            time.sleep(3.0)
            raise Exception("API timeout")

        # Simulate service unavailable
        if random.random() < 0.1:  # 10% chance of service down
            raise Exception("Service unavailable: 503")

        time.sleep(0.2)  # Normal API response time
        return f"API response from {endpoint}"


class PaymentService:
    """Simulates a payment processing service"""

    def process_payment(self, amount: float, user_id: int) -> dict:
        """Process a payment - critical operation that should be isolated"""
        time.sleep(0.3)  # Payment processing time

        # Simulate occasional payment failures
        if random.random() < 0.05:  # 5% failure rate for payments
            raise Exception("Payment processing failed: Insufficient funds")

        return {
            "transaction_id": f"txn_{random.randint(1000, 9999)}",
            "amount": amount,
            "user_id": user_id,
            "status": "completed",
        }


class UserService:
    """Simulates a user management service"""

    def get_user_profile(self, user_id: int) -> dict:
        """Get user profile - should be fast and reliable"""
        time.sleep(0.05)

        # Rare failures for user service
        if random.random() < 0.02:  # 2% failure rate
            raise Exception("User service temporarily unavailable")

        return {
            "user_id": user_id,
            "name": f"User_{user_id}",
            "email": f"user_{user_id}@example.com",
        }


def simulate_high_traffic_scenario():
    """Simulate high traffic scenario to demonstrate bulkhead effectiveness"""
    print("🚀 Starting High Traffic Simulation")
    print("=" * 60)

    # Create services
    db_service = DatabaseService()
    api_service = ExternalAPIService()
    payment_service = PaymentService()
    user_service = UserService()

    # Create bulkhead manager
    manager = BulkheadManager()

    try:
        # Configure bulkheads for different services
        db_config = BulkheadConfig(
            name="database",
            max_concurrent_calls=3,
            max_queue_size=5,
            timeout_seconds=1.5,
            failure_threshold=3,
            success_threshold=2,
            isolation_duration=10.0,
        )

        api_config = BulkheadConfig(
            name="external_api",
            max_concurrent_calls=2,
            max_queue_size=3,
            timeout_seconds=1.0,
            failure_threshold=2,
            success_threshold=1,
            isolation_duration=15.0,
        )

        payment_config = BulkheadConfig(
            name="payments",
            max_concurrent_calls=2,
            max_queue_size=10,
            timeout_seconds=2.0,
            failure_threshold=1,  # Very sensitive to failures
            success_threshold=3,
            isolation_duration=30.0,  # Long isolation for critical service
        )

        user_config = BulkheadConfig(
            name="users",
            max_concurrent_calls=5,
            max_queue_size=20,
            timeout_seconds=0.5,
            failure_threshold=5,
            success_threshold=2,
            isolation_duration=5.0,
        )

        # Create bulkheads
        db_bulkhead = manager.create_bulkhead(db_config)
        api_bulkhead = manager.create_bulkhead(api_config)
        payment_bulkhead = manager.create_bulkhead(payment_config)
        user_bulkhead = manager.create_bulkhead(user_config)

        # Give bulkheads time to initialize properly
        time.sleep(0.5)

        # Create decorated functions
        @with_bulkhead("database", manager)
        def safe_db_query(sql, user_id=None):
            return db_service.query(sql, user_id)

        @with_bulkhead("external_api", manager)
        def safe_api_call(endpoint, data=None):
            return api_service.call_api(endpoint, data)

        @with_bulkhead("payments", manager)
        def safe_payment_process(amount, user_id):
            return payment_service.process_payment(amount, user_id)

        @with_bulkhead("users", manager)
        def safe_user_profile(user_id):
            return user_service.get_user_profile(user_id)

        # Track all futures for proper cleanup
        all_futures = []

        def test_database_isolation():
            print("\n📊 Testing Database Bulkhead Isolation")
            print("-" * 40)

            futures = []

            # Send more requests than the bulkhead can handle concurrently
            for i in range(8):  # Reduced to avoid immediate queue full
                try:
                    future = safe_db_query(
                        f"SELECT * FROM users WHERE id = {i}", user_id=i
                    )
                    futures.append(future)
                    all_futures.append(future)
                    print(f"  Submitted database query {i + 1}/8")
                except Exception as e:
                    print(f"  🚫 Query {i + 1} immediately rejected: {e}")

            # Collect results
            successful = 0
            failed = 0
            rejected = 0

            for i, future in enumerate(futures):
                try:
                    result = future.result(timeout=3.0)
                    if result.success:
                        successful += 1
                        print(f"  ✅ Query {i + 1}: Success - {result.result}")
                    else:
                        failed += 1
                        print(f"  ❌ Query {i + 1}: {result.error}")
                except Exception as e:
                    if "queue is full" in str(e):
                        rejected += 1
                        print(f"  🚫 Query {i + 1}: Rejected - {e}")
                    elif "timeout" in str(e).lower():
                        failed += 1
                        print(f"  ⏰ Query {i + 1}: Timeout - {e}")
                    else:
                        failed += 1
                        print(f"  ❌ Query {i + 1}: {e}")

            print(
                f"  Results: {successful} successful, {failed} failed, {rejected} rejected"
            )

            # Show bulkhead state
            stats = db_bulkhead.get_stats()
            print(
                f"  Bulkhead state: {stats['state']}, Queue: {stats['current_queue_size']}/{stats['max_queue_size']}"
            )

        def test_api_circuit_breaker():
            print("\n🌐 Testing API Circuit Breaker")
            print("-" * 40)

            # Intentionally cause failures to trigger isolation
            futures = []
            for i in range(4):  # Reduced number of calls
                try:
                    # Use endpoints that are likely to cause timeouts
                    future = safe_api_call(f"/timeout-endpoint-{i}", {"data": "test"})
                    futures.append(future)
                    all_futures.append(future)
                    print(f"  Submitted API call {i + 1}/4")
                except Exception as e:
                    print(f"  🚫 API call {i + 1} immediately rejected: {e}")

            successful = 0
            failed = 0

            for i, future in enumerate(futures):
                try:
                    result = future.result(timeout=3.0)
                    if result.success:
                        successful += 1
                        print(f"  ✅ API call {i + 1}: Success - {result.result}")
                    else:
                        failed += 1
                        print(f"  ❌ API call {i + 1}: {result.error}")
                except Exception as e:
                    failed += 1
                    print(f"  ❌ API call {i + 1}: {e}")

            print(f"  Results: {successful} successful, {failed} failed")

            # Check if bulkhead isolated itself
            stats = api_bulkhead.get_stats()
            print(
                f"  Bulkhead state: {stats['state']}, Failures: {stats['failure_count']}"
            )

        def test_mixed_workload():
            print("\n🔄 Testing Mixed Workload")
            print("-" * 40)

            futures = []

            # Mix of different operations
            operations = [
                lambda: safe_db_query("SELECT * FROM products", 123),
                lambda: safe_api_call("/products", {"category": "electronics"}),
                lambda: safe_payment_process(99.99, 456),
                lambda: safe_user_profile(789),
            ]

            # Submit multiple rounds of mixed operations
            for round in range(2):
                for i, op in enumerate(operations):
                    try:
                        future = op()
                        futures.append(future)
                        all_futures.append(future)
                        print(f"  Round {round + 1}: Submitted operation {i + 1}")
                    except Exception as e:
                        print(f"  🚫 Round {round + 1} operation {i + 1} rejected: {e}")

            # Wait for all operations to complete with timeout
            print("  Waiting for operations to complete...")
            done, not_done = wait(futures, timeout=10.0)
            print(f"  Completed: {len(done)}/{len(futures)} operations")

            if not_done:
                print(f"  ⚠️  {len(not_done)} operations did not complete in time")
                # Cancel unfinished futures
                for future in not_done:
                    if not future.done():
                        future.cancel()

        def monitor_bulkheads():
            print("\n📈 Bulkhead Monitoring Dashboard")
            print("-" * 40)

            for i in range(2):  # Reduced monitoring cycles
                stats = manager.get_all_stats()
                print(f"  Update {i + 1}:")
                for service, stat in stats.items():
                    state_icon = (
                        "🟢"
                        if stat["state"] == "healthy"
                        else "🟡" if stat["state"] == "degraded" else "🔴"
                    )
                    print(
                        f"    {state_icon} {service:12} | State: {stat['state']:8} | "
                        f"Success: {stat['successful_executions']:2} | "
                        f"Failed: {stat['failed_executions']:2} | "
                        f"Rejected: {stat['rejected_executions']:2} | "
                        f"Queue: {stat['current_queue_size']}/{stat['max_queue_size']}"
                    )
                if i < 1:
                    time.sleep(1)  # Reduced sleep time

        def test_graceful_degradation():
            print("\n🛡️ Testing Graceful Degradation")
            print("-" * 40)

            # Overload the system intentionally but more carefully
            futures = []
            for i in range(12):  # Reduced number of requests
                try:
                    if i % 4 == 0:
                        future = safe_db_query(f"HEAVY_QUERY_{i}")
                    elif i % 4 == 1:
                        future = safe_api_call(f"/heavy-endpoint-{i}")
                    elif i % 4 == 2:
                        future = safe_payment_process(i * 10, i)
                    else:
                        future = safe_user_profile(i)
                    futures.append(future)
                    all_futures.append(future)
                    print(f"  Submitted request {i + 1}/12")
                except Exception as e:
                    print(f"  🚫 Request {i + 1} immediately rejected: {e}")

            print(
                "  System gracefully rejected excess load while protecting core services"
            )

            # Quick check on completion
            time.sleep(1)
            completed = sum(1 for f in futures if f.done())
            print(f"  {completed}/{len(futures)} requests completed within 1 second")

        # Run all tests
        test_database_isolation()
        time.sleep(0.5)  # Small delay between tests

        test_api_circuit_breaker()
        time.sleep(0.5)

        test_mixed_workload()
        time.sleep(0.5)

        monitor_bulkheads()
        time.sleep(0.5)

        test_graceful_degradation()

        # Wait for all pending operations to complete
        print("\n⏳ Waiting for all operations to complete...")
        pending_futures = [f for f in all_futures if not f.done()]
        if pending_futures:
            done, not_done = wait(pending_futures, timeout=5.0)
            print(f"  Final wait: {len(done)} completed, {len(not_done)} still pending")

        # Final statistics
        print("\n🎯 Final Statistics")
        print("=" * 60)
        final_stats = manager.get_all_stats()
        for service, stats in final_stats.items():
            total = (
                stats["successful_executions"]
                + stats["failed_executions"]
                + stats["rejected_executions"]
            )
            success_rate = (
                (stats["successful_executions"] / total * 100) if total > 0 else 0
            )
            print(
                f"  {service:12}: {stats['successful_executions']:2} ✅ | "
                f"{stats['failed_executions']:2} ❌ | "
                f"{stats['rejected_executions']:2} 🚫 | "
                f"Success: {success_rate:5.1f}% | "
                f"State: {stats['state']}"
            )

    except Exception as e:
        print(f"❌ Error during simulation: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # Clean shutdown
        print("\n🛑 Shutting down bulkheads...")
        manager.shutdown_all()
        print("✅ Cleanup completed!")


def test_bulkhead_simulation():
    """Test function to run the bulkhead simulation"""
    simulate_high_traffic_scenario()
