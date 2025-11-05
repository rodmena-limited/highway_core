import logging
import time
import json
from typing import Dict, Any

from highway_core.persistence.database import get_db_manager, TaskQueue, Task
from highway_core.engine.orchestrator import Orchestrator
from highway_core.engine.models import WorkflowModel, AnyOperatorModel
from highway_core.persistence.sql_persistence_manager import SQLPersistenceManager
from highway_core.tools.registry import ToolRegistry

logger = logging.getLogger("HighwayWorker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def model_to_dict(model: Task) -> Dict[str, Any]:
    """Converts an SQLAlchemy model instance to a dict, handling JSON fields."""
    task_dict = {c.name: getattr(model, c.name) for c in model.__table__.columns}
    
    # Manually deserialize JSON fields for Pydantic
    task_dict['args'] = model.args
    task_dict['kwargs'] = model.kwargs
    task_dict['command'] = model.command
    task_dict['dependencies'] = model.dependencies_list
    return task_dict

class Worker:
    def __init__(self):
        self.db_manager = get_db_manager()
        self.poll_interval = 1  # Poll every 1 second
        self.tool_registry = ToolRegistry()

    def run(self):
        logger.info("Highway Worker started...")
        try:
            while True:
                job = self.db_manager.get_next_queued_task_atomic()
                if job:
                    logger.info(f"Picked up job {job.id} for task {job.task_id}")
                    self.execute_task_from_job(job)
                else:
                    time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            logger.info("Highway Worker shutting down...")
        finally:
            self.db_manager.close_all_connections()

    def execute_task_from_job(self, job: TaskQueue):
        task_db = None
        orchestrator_context = None
        try:
            # 1. Get task and workflow models
            workflow_db = self.db_manager.load_workflow(job.workflow_id)
            if not workflow_db:
                raise Exception(f"Workflow {job.workflow_id} not found.")
            
            task_db = self.db_manager.get_task_by_id(job.workflow_id, job.task_id)
            if not task_db:
                raise Exception(f"Task {job.task_id} not found.")
            
            # 2. Update status to RUNNING
            self.db_manager.update_task_status_by_workflow(job.workflow_id, job.task_id, "running")
            
            # 3. Create a 'lite' Orchestrator context
            workflow_model = WorkflowModel(
                name=workflow_db['name'],
                start_task=workflow_db['start_task'],
                variables=workflow_db['variables'],
                tasks={} # Not needed for single task execution
            )
            # Set the mode on the pydantic model for handlers to read
            setattr(workflow_model, 'mode', workflow_db.get('mode', 'LOCAL')) 
            
            persistence = SQLPersistenceManager(engine_url=self.db_manager.engine_url)
            
            orchestrator_context = Orchestrator(
                workflow_run_id=job.workflow_id,
                workflow=workflow_model,
                persistence_manager=persistence,
                registry=self.tool_registry
            )
            
            # Load the current state for the handler
            orchestrator_context.state, _ = persistence.load_workflow_state(job.workflow_id)
            
            # 4. Get the handler and executor
            task_dict = model_to_dict(task_db)
            task_model: AnyOperatorModel = AnyOperatorModel.model_validate(task_dict)
            
            handler_func = orchestrator_context.handler_map.get(task_model.operator_type)
            if not handler_func:
                raise ValueError(f"No handler for operator type: {task_model.operator_type}")

            selected_executor = None
            if hasattr(task_model, 'runtime'):
                selected_executor = orchestrator_context.executors.get(task_model.runtime)
                if not selected_executor:
                    raise ValueError(f"No executor registered for runtime: {task_model.runtime}")

            # 5. Execute the task handler
            handler_func(
                task=task_model,
                state=orchestrator_context.state,
                orchestrator=orchestrator_context,
                registry=orchestrator_context.registry,
                bulkhead_manager=orchestrator_context.bulkhead_manager,
                executor=selected_executor,
                resource_manager=orchestrator_context.resource_manager,
                workflow_run_id=orchestrator_context.run_id,
            )
            
            # 6. Mark as COMPLETED (if not durably paused)
            task_final_status = self.db_manager.get_task_by_id(job.workflow_id, job.task_id).status
            if task_final_status == "running":
                logger.info(f"Task {job.task_id} handler finished. Marking COMPLETED.")
                # We need to get the result from the state
                result_to_save = None
                if task_model.result_key:
                    result_to_save = orchestrator_context.state.get_result(task_model.result_key)
                self.db_manager.complete_task(job.workflow_id, job.task_id, result_to_save)
            else:
                logger.info(f"Task {job.task_id} handler finished with durable status: {task_final_status}")

        except Exception as e:
            logger.error(f"Error executing task {job.task_id}: {e}", exc_info=True)
            if task_db:
                self.db_manager.fail_task(job.workflow_id, job.task_id, str(e))
        finally:
            if orchestrator_context:
                orchestrator_context.close()
            self.db_manager.delete_queue_job(job.id)
            logger.info(f"Finished job {job.id} for task {job.task_id}")

if __name__ == "__main__":
    w = Worker()
    w.run()