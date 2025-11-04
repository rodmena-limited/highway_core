import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, Tuple

import redis

from highway_core.config import settings
from highway_core.engine.models import AnyOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.persistence.manager import PersistenceManager
from highway_core.persistence.sql_persistence import SQLPersistence

logger = logging.getLogger(__name__)


class HybridPersistenceManager(PersistenceManager):
    def __init__(self, db_path: Optional[str] = None, is_test: bool = False):
        # Initialize SQL persistence
        self.sql_persistence = SQLPersistence(
            db_path=db_path, is_test=is_test or db_path is not None
        )

        # Initialize Redis connection
        self.redis_client = None
        self.redis_enabled = False
        try:
            self.redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                password=settings.REDIS_PASSWORD,
                db=settings.REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            self.redis_client.ping()
            self.redis_enabled = True
            logger.info("Redis connection successful.")
        except redis.exceptions.ConnectionError as e:
            logger.warning(
                f"Redis connection failed: {e}. Falling back to SQL-only mode."
            )
        except redis.exceptions.TimeoutError as e:
            logger.warning(
                f"Redis connection timed out: {e}. Falling back to SQL-only mode."
            )
        except Exception as e:
            logger.warning(
                f"Redis connection failed with unexpected error: {e}. Falling back to SQL-only mode."
            )

    def _get_workflow_key(self, workflow_run_id: str) -> str:
        return f"workflow:{workflow_run_id}"

    def _get_completed_tasks_key(self, workflow_run_id: str) -> str:
        return f"workflow:{workflow_run_id}:completed_tasks"

    def start_workflow(
        self, workflow_id: str, workflow_name: str, variables: Dict[str, Any]
    ) -> None:
        self.sql_persistence.start_workflow(workflow_id, workflow_name, variables)
        if self.redis_enabled:
            try:
                workflow_key = self._get_workflow_key(workflow_id)
                self.redis_client.hset(
                    workflow_key,
                    mapping={
                        "name": workflow_name,
                        "status": "running",
                        "start_time": datetime.now(timezone.utc).isoformat(),
                    },
                )
            except Exception as e:
                logger.warning(f"Failed to store workflow start in Redis: {e}")
                self.redis_enabled = False

    def complete_workflow(self, workflow_id: str) -> None:
        self.sql_persistence.complete_workflow(workflow_id)
        if self.redis_enabled:
            try:
                workflow_key = self._get_workflow_key(workflow_id)
                self.redis_client.hset(workflow_key, "status", "completed")
                self.redis_client.expire(workflow_key, 3600)  # Expire in 1 hour
            except Exception as e:
                logger.warning(f"Failed to store workflow completion in Redis: {e}")
                self.redis_enabled = False

    def fail_workflow(self, workflow_id: str, error_message: str) -> None:
        self.sql_persistence.fail_workflow(workflow_id, error_message)
        if self.redis_enabled:
            try:
                workflow_key = self._get_workflow_key(workflow_id)
                self.redis_client.hset(
                    workflow_key,
                    mapping={
                        "status": "failed",
                        "error_message": error_message,
                    },
                )
                self.redis_client.expire(workflow_key, 3600)  # Expire in 1 hour
            except Exception as e:
                logger.warning(f"Failed to store workflow failure in Redis: {e}")
                self.redis_enabled = False

    def save_workflow_state(
        self, workflow_run_id: str, state: WorkflowState, completed_tasks: Set[str]
    ) -> None:
        if self.redis_enabled:
            try:
                workflow_key = self._get_workflow_key(workflow_run_id)
                self.redis_client.hset(workflow_key, "state", state.model_dump_json())
                completed_tasks_key = self._get_completed_tasks_key(workflow_run_id)
                if completed_tasks:
                    self.redis_client.delete(
                        completed_tasks_key
                    )  # Clear existing tasks
                    self.redis_client.sadd(
                        completed_tasks_key, *[str(t) for t in completed_tasks]
                    )
                else:
                    self.redis_client.delete(completed_tasks_key)
            except Exception as e:
                logger.warning(f"Failed to save workflow state to Redis: {e}")
                self.redis_enabled = False

    def load_workflow_state(
        self, workflow_run_id: str
    ) -> Tuple[WorkflowState | None, Set[str]]:
        if self.redis_enabled:
            try:
                workflow_key = self._get_workflow_key(workflow_run_id)
                state_json = self.redis_client.hget(workflow_key, "state")
                if state_json:
                    state = WorkflowState.model_validate_json(state_json)
                    completed_tasks_key = self._get_completed_tasks_key(workflow_run_id)
                    completed_tasks = self.redis_client.smembers(completed_tasks_key)
                    return state, completed_tasks
            except Exception as e:
                logger.warning(f"Failed to load workflow state from Redis: {e}")
                self.redis_enabled = False

        # Fallback to SQL if Redis is disabled or state not in Redis
        try:
            state, completed_tasks = self.sql_persistence.load_workflow_state(
                workflow_run_id
            )
            if state and self.redis_enabled:
                # Re-hydrate Redis
                self.save_workflow_state(workflow_run_id, state, completed_tasks)
            return state, completed_tasks
        except Exception as e:
            logger.error(f"Failed to load workflow state from SQL: {e}")
            return None, set()

    def _get_task_lock_key(self, workflow_run_id: str, task_id: str) -> str:
        return f"workflow:{workflow_run_id}:task:{task_id}:lock"

    def start_task(self, workflow_id: str, task: AnyOperatorModel) -> bool:
        """Tries to acquire a lock and start a task."""
        if self.redis_enabled:
            try:
                lock_key = self._get_task_lock_key(workflow_id, task.task_id)
                if self.redis_client.set(
                    lock_key, "locked", nx=True, ex=3600
                ):  # Lock for 1 hour
                    self.sql_persistence.start_task(workflow_id, task)
                    return True
                else:
                    logger.info(f"Task {task.task_id} is already locked.")
                    return False
            except Exception as e:
                logger.warning(f"Failed to acquire task lock in Redis: {e}")
                self.redis_enabled = False
                # Fall back to SQL-only mode
                self.sql_persistence.start_task(workflow_id, task)
                return True
        else:
            # In SQL-only mode, we don't have a lock, just proceed
            self.sql_persistence.start_task(workflow_id, task)
            return True

    def complete_task(self, workflow_id: str, task_id: str, result: Any) -> None:
        self.sql_persistence.complete_task(workflow_id, task_id, result)
        if self.redis_enabled:
            try:
                completed_tasks_key = self._get_completed_tasks_key(workflow_id)
                self.redis_client.sadd(completed_tasks_key, task_id)
                lock_key = self._get_task_lock_key(workflow_id, task_id)
                self.redis_client.delete(lock_key)
            except Exception as e:
                logger.warning(f"Failed to update task completion in Redis: {e}")
                self.redis_enabled = False

    def fail_task(self, workflow_id: str, task_id: str, error_message: str) -> None:
        self.sql_persistence.fail_task(workflow_id, task_id, error_message)
        if self.redis_enabled:
            try:
                lock_key = self._get_task_lock_key(workflow_id, task_id)
                self.redis_client.delete(lock_key)
            except Exception as e:
                logger.warning(f"Failed to release task lock in Redis: {e}")
                self.redis_enabled = False

    def save_task_result(self, workflow_run_id: str, task_id: str, result: Any) -> bool:
        return self.sql_persistence.save_task_result(workflow_run_id, task_id, result)

    def save_task_execution(
        self, workflow_run_id: str, task_id: str, executor_runtime: str, **kwargs
    ) -> bool:
        return self.sql_persistence.save_task_execution(
            workflow_run_id, task_id, executor_runtime, **kwargs
        )

    def mark_task_completed(self, workflow_run_id: str, task_id: str) -> bool:
        return self.sql_persistence.mark_task_completed(workflow_run_id, task_id)

    def save_task(
        self, workflow_run_id: str, task_id: str, operator_type: str, **kwargs
    ) -> bool:
        return self.sql_persistence.save_task(
            workflow_run_id, task_id, operator_type, **kwargs
        )

    def save_task_if_not_exists(
        self, workflow_run_id: str, task_id: str, operator_type: str, **kwargs
    ) -> bool:
        return self.sql_persistence.save_task_if_not_exists(
            workflow_run_id, task_id, operator_type, **kwargs
        )

    def close(self) -> None:
        self.sql_persistence.close()
        if self.redis_enabled and self.redis_client:
            try:
                self.redis_client.close()
            except Exception as e:
                logger.warning(f"Failed to close Redis connection: {e}")
