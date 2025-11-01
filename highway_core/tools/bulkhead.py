import threading
import queue
import time
import logging
from typing import Callable, Any, Optional, Dict, List
from enum import Enum
from dataclasses import dataclass
from concurrent.futures import Future
import functools

from .decorators import tool

logger = logging.getLogger("BulkheadPattern")


class BulkheadState(Enum):
    """Enum representing the state of a bulkhead"""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    ISOLATED = "isolated"
    FAILED = "failed"


@dataclass
class BulkheadConfig:
    """Configuration for a bulkhead"""

    name: str
    max_concurrent_calls: int = 10
    max_queue_size: int = 100
    timeout_seconds: Optional[float] = None
    failure_threshold: int = 5
    success_threshold: int = 3
    isolation_duration: float = 30.0  # seconds


@dataclass
class ExecutionResult:
    """Result of a function execution through bulkhead"""

    success: bool
    result: Any
    error: Optional[Exception]
    execution_time: float
    bulkhead_name: str


class BulkheadIsolationError(Exception):
    """Exception raised when bulkhead is isolated"""

    pass


class BulkheadTimeoutError(Exception):
    """Exception raised when bulkhead operation times out"""

    pass


class BulkheadFullError(Exception):
    """Exception raised when bulkhead queue is full"""

    pass


class Bulkhead:
    """
    Implements the bulkhead pattern to isolate function executions
    and prevent cascading failures.
    """

    def __init__(self, config: BulkheadConfig):
        self.config = config
        self.name = config.name

        # Execution control
        self._semaphore = threading.Semaphore(config.max_concurrent_calls)
        self._task_queue: queue.Queue = queue.Queue(maxsize=config.max_queue_size)
        self._worker_threads: List[threading.Thread] = []
        self._stop_event = threading.Event()

        # State management
        self._state = BulkheadState.HEALTHY
        self._state_lock = threading.RLock()
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._isolation_end_time: Optional[float] = None

        # Statistics
        self._total_executions = 0
        self._successful_executions = 0
        self._failed_executions = 0
        self._rejected_executions = 0
        self._stats_lock = threading.RLock()

        # Start worker threads
        self._start_workers()

        logger.info(
            f"Bulkhead '{self.name}' initialized with {config.max_concurrent_calls} concurrent calls and queue size {config.max_queue_size}"
        )

    def _start_workers(self):
        """Start worker threads to process queued tasks"""

        def worker():
            while not self._stop_event.is_set():
                try:
                    # Get task from queue with timeout to allow checking stop event
                    task = self._task_queue.get(timeout=0.1)
                    self._process_task(task)
                    self._task_queue.task_done()
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"Worker thread error in bulkhead '{self.name}': {e}")

        # Create worker threads (one per concurrent call capacity)
        for i in range(self.config.max_concurrent_calls):
            thread = threading.Thread(
                target=worker,
                name=f"BulkheadWorker-{self.name}-{i}",
                daemon=False,  # Changed to non-daemon for proper shutdown
            )
            thread.start()
            self._worker_threads.append(thread)

    def _process_task(self, task):
        """Process a single task from the queue"""
        func, args, kwargs, future, start_time = task

        try:
            # Check if bulkhead is isolated
            if self._state == BulkheadState.ISOLATED:
                if self._isolation_end_time and time.time() < self._isolation_end_time:
                    future.set_exception(
                        BulkheadIsolationError(
                            f"Bulkhead '{self.name}' is isolated until {self._isolation_end_time}"
                        )
                    )
                    return
                else:
                    # Isolation period ended, transition to healthy
                    with self._state_lock:
                        if (
                            self._state == BulkheadState.ISOLATED
                            and self._isolation_end_time
                            and time.time() >= self._isolation_end_time
                        ):
                            self._state = BulkheadState.HEALTHY
                            self._failure_count = 0
                            self._success_count = 0
                            logger.info(
                                f"Bulkhead '{self.name}' isolation period ended, returning to healthy state"
                            )

            # Execute the function
            with self._semaphore:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time

                # Update success metrics
                with self._state_lock:
                    self._success_count += 1
                    self._failure_count = 0

                    # Check if we should transition from degraded to healthy
                    if (
                        self._state == BulkheadState.DEGRADED
                        and self._success_count >= self.config.success_threshold
                    ):
                        self._state = BulkheadState.HEALTHY
                        logger.info(f"Bulkhead '{self.name}' returned to healthy state")

                # Update statistics
                with self._stats_lock:
                    self._successful_executions += 1

                future.set_result(
                    ExecutionResult(
                        success=True,
                        result=result,
                        error=None,
                        execution_time=execution_time,
                        bulkhead_name=self.name,
                    )
                )

        except Exception as e:
            execution_time = time.time() - start_time

            # Update failure metrics
            with self._state_lock:
                self._failure_count += 1
                self._success_count = 0
                self._last_failure_time = time.time()

                # Check state transitions
                if self._failure_count >= self.config.failure_threshold:
                    if self._state != BulkheadState.ISOLATED:
                        self._state = BulkheadState.ISOLATED
                        self._isolation_end_time = (
                            time.time() + self.config.isolation_duration
                        )
                        logger.warning(
                            f"Bulkhead '{self.name}' isolated due to {self._failure_count} consecutive failures"
                        )
                elif self._state == BulkheadState.HEALTHY and self._failure_count > 0:
                    self._state = BulkheadState.DEGRADED
                    logger.warning(f"Bulkhead '{self.name}' degraded due to failures")

            # Update statistics
            with self._stats_lock:
                self._failed_executions += 1

            future.set_exception(e)

    def execute(self, func: Callable, *args, **kwargs) -> Future:
        """
        Execute a function through the bulkhead with isolation.

        Args:
            func: The function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Future that will contain the ExecutionResult

        Raises:
            BulkheadFullError: If the bulkhead queue is full
            BulkheadIsolationError: If the bulkhead is isolated
        """
        # Check bulkhead state
        if self._state == BulkheadState.ISOLATED:
            if self._isolation_end_time and time.time() < self._isolation_end_time:
                raise BulkheadIsolationError(f"Bulkhead '{self.name}' is isolated")
            else:
                # Reset state if isolation period ended
                with self._state_lock:
                    if (
                        self._state == BulkheadState.ISOLATED
                        and self._isolation_end_time
                        and time.time() >= self._isolation_end_time
                    ):
                        self._state = BulkheadState.HEALTHY
                        self._failure_count = 0
                        self._success_count = 0

        # Create future for result
        future: Future = Future()
        start_time = time.time()

        # Try to add task to queue
        try:
            self._task_queue.put_nowait((func, args, kwargs, future, start_time))

            # Update statistics
            with self._stats_lock:
                self._total_executions += 1

        except queue.Full:
            with self._stats_lock:
                self._rejected_executions += 1
            raise BulkheadFullError(f"Bulkhead '{self.name}' queue is full")

        # Wait for result with timeout if configured
        if self.config.timeout_seconds:

            def timeout_handler():
                if not future.done():
                    future.set_exception(
                        BulkheadTimeoutError(
                            f"Bulkhead '{self.name}' operation timed out after {self.config.timeout_seconds} seconds"
                        )
                    )

            timer = threading.Timer(self.config.timeout_seconds, timeout_handler)
            timer.start()
            future.add_done_callback(lambda _: timer.cancel())

        return future

    def execute_sync(self, func: Callable, *args, **kwargs) -> ExecutionResult:
        """
        Synchronously execute a function through the bulkhead.

        Args:
            func: The function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            ExecutionResult containing the result or error
        """
        future = self.execute(func, *args, **kwargs)
        try:
            return future.result()
        except Exception as e:
            return ExecutionResult(
                success=False,
                result=None,
                error=e,
                execution_time=0.0,
                bulkhead_name=self.name,
            )

    def get_state(self) -> BulkheadState:
        """Get the current state of the bulkhead"""
        return self._state

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for the bulkhead"""
        with self._stats_lock:
            queue_size = self._task_queue.qsize()
            return {
                "name": self.name,
                "state": self._state.value,
                "total_executions": self._total_executions,
                "successful_executions": self._successful_executions,
                "failed_executions": self._failed_executions,
                "rejected_executions": self._rejected_executions,
                "current_queue_size": queue_size,
                "max_queue_size": self.config.max_queue_size,
                "concurrent_capacity": self.config.max_concurrent_calls,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "isolation_end_time": self._isolation_end_time,
            }

    def reset_stats(self):
        """Reset bulkhead statistics"""
        with self._stats_lock:
            self._total_executions = 0
            self._successful_executions = 0
            self._failed_executions = 0
            self._rejected_executions = 0

    def shutdown(self, timeout: float = 5.0):
        """Shutdown the bulkhead and all worker threads"""
        logger.info(f"Shutting down bulkhead '{self.name}'")
        self._stop_event.set()

        # Clear the queue and cancel pending tasks
        while not self._task_queue.empty():
            try:
                task = self._task_queue.get_nowait()
                func, args, kwargs, future, start_time = task
                if not future.done():
                    future.set_exception(
                        Exception(f"Bulkhead '{self.name}' shutdown during execution")
                    )
                self._task_queue.task_done()
            except queue.Empty:
                break

        # Wait for all worker threads to finish
        for thread in self._worker_threads:
            thread.join(timeout=timeout)
            if thread.is_alive():
                logger.warning(
                    f"Worker thread {thread.name} did not shutdown gracefully"
                )


class BulkheadManager:
    """
    Manages multiple bulkheads for different system components.
    """

    def __init__(self):
        self._bulkheads: Dict[str, Bulkhead] = {}
        self._lock = threading.RLock()

    def create_bulkhead(self, config: BulkheadConfig) -> Bulkhead:
        """Create and register a new bulkhead"""
        with self._lock:
            if config.name in self._bulkheads:
                raise ValueError(f"Bulkhead with name '{config.name}' already exists")

            bulkhead = Bulkhead(config)
            self._bulkheads[config.name] = bulkhead
            return bulkhead

    def get_bulkhead(self, name: str) -> Optional[Bulkhead]:
        """Get a bulkhead by name"""
        with self._lock:
            return self._bulkheads.get(name)

    def execute_in_bulkhead(
        self, bulkhead_name: str, func: Callable, *args, **kwargs
    ) -> Future:
        """Execute a function in a specific bulkhead"""
        bulkhead = self.get_bulkhead(bulkhead_name)
        if not bulkhead:
            raise ValueError(f"Bulkhead '{bulkhead_name}' not found")
        return bulkhead.execute(func, *args, **kwargs)

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all bulkheads"""
        with self._lock:
            return {
                name: bulkhead.get_stats() for name, bulkhead in self._bulkheads.items()
            }

    def shutdown_all(self, timeout: float = 5.0):
        """Shutdown all bulkheads"""
        with self._lock:
            for name, bulkhead in self._bulkheads.items():
                try:
                    bulkhead.shutdown(timeout)
                except Exception as e:
                    logger.error(f"Error shutting down bulkhead '{name}': {e}")


# Decorator for easy bulkhead usage
def with_bulkhead(bulkhead_name: str, manager: BulkheadManager):
    """
    Decorator to execute a function through a bulkhead.

    Example:
        @with_bulkhead("database", bulkhead_manager)
        def query_database(query):
            return db.execute(query)
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return manager.execute_in_bulkhead(bulkhead_name, func, *args, **kwargs)

        return wrapper

    return decorator
