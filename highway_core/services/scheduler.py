import logging
import time
from datetime import datetime, timezone

from highway_core.persistence.database import get_db_manager

logger = logging.getLogger("HighwayScheduler")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class Scheduler:
    def __init__(self):
        self.db_manager = get_db_manager()
        self.poll_interval = 5  # Poll every 5 seconds

    def run(self):
        logger.info("Highway Scheduler started...")
        try:
            while True:
                self.poll_for_ready_workflows()
                self.poll_for_runnable_tasks()
                self.poll_for_waiting_tasks()
                self.poll_for_completed_workflows()
                
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            logger.info("Highway Scheduler shutting down...")
        finally:
            self.db_manager.close_all_connections()

    def poll_for_ready_workflows(self):
        """Finds 'PENDING' workflows and starts them."""
        try:
            workflows = self.db_manager.get_workflows_by_status("PENDING", mode="DURABLE")
            for wf in workflows:
                logger.info(f"Starting new durable workflow {wf.workflow_id}")
                self.db_manager.update_workflow_status(wf.workflow_id, "RUNNING")
                # Enqueue the start task
                self.db_manager.enqueue_task(wf.workflow_id, wf.start_task)
        except Exception as e:
            logger.error(f"Scheduler error in poll_for_ready_workflows: {e}", exc_info=True)

    def poll_for_runnable_tasks(self):
        """Finds 'PENDING' tasks whose dependencies are met."""
        try:
            workflows = self.db_manager.get_workflows_by_status("RUNNING", mode="DURABLE")
            for wf in workflows:
                pending_tasks = self.db_manager.get_tasks_by_status(wf.workflow_id, "PENDING")
                for task in pending_tasks:
                    if self.db_manager.are_task_dependencies_met(wf.workflow_id, task.task_id):
                        logger.info(f"🎯 SCHEDULER: Dependencies met for task '{task.task_id}'. Enqueuing for workflow {wf.workflow_id}")
                        self.db_manager.enqueue_task(wf.workflow_id, task.task_id)
                        
                    # Special logging for tasks waiting for events
                    waiting_tasks = self.db_manager.get_tasks_by_status(wf.workflow_id, "WAITING_FOR_EVENT")
                    for waiting_task in waiting_tasks:
                        if waiting_task.event_token:
                            logger.info(f"⏳ SCHEDULER: Task '{waiting_task.task_id}' waiting for event with token: {waiting_task.event_token}")
                            logger.info(f"📋 To resume: curl -X GET 'http://127.0.0.1:5000/workflow/resume?token={waiting_task.event_token}&decision=YOUR_DECISION'")
                        else:
                            logger.info(f"⏳ SCHEDULER: Task '{waiting_task.task_id}' waiting for event (no token)")
                            
        except Exception as e:
            logger.error(f"Scheduler error in poll_for_runnable_tasks: {e}", exc_info=True)

    def poll_for_waiting_tasks(self):
        """Finds 'WAITING_FOR_TIMER' tasks whose timer is up."""
        try:
            tasks_to_wake = self.db_manager.find_tasks_to_wake()
            for task in tasks_to_wake:
                logger.info(f"Waking up task {task.task_id} from timer.")
                # Set status to PENDING, not QUEUED. This lets the
                # poll_for_runnable_tasks check its dependencies first
                # (in case a task depends on both a timer and another task).
                # For a simple timer, it will be enqueued on the *next* cycle.
                self.db_manager.update_task_status_by_workflow(task.workflow_id, task.task_id, "PENDING")
        except Exception as e:
            logger.error(f"Scheduler error in poll_for_waiting_tasks: {e}", exc_info=True)

    def poll_for_completed_workflows(self):
        """Checks if any running workflows are now complete."""
        try:
            workflows = self.db_manager.get_workflows_by_status("RUNNING", mode="DURABLE")
            for wf in workflows:
                active_tasks = 0
                statuses_to_check = ["PENDING", "QUEUED", "RUNNING", "WAITING_FOR_TIMER", "WAITING_FOR_EVENT"]
                for status in statuses_to_check:
                    active_tasks += len(self.db_manager.get_tasks_by_status(wf.workflow_id, status))
                    if active_tasks > 0:
                        break # No need to check other statuses
                
                if active_tasks == 0:
                    logger.info(f"Workflow {wf.workflow_id} has no active tasks. Marking as COMPLETED.")
                    self.db_manager.update_workflow_status(wf.workflow_id, "completed")
        except Exception as e:
            logger.error(f"Scheduler error in poll_for_completed_workflows: {e}", exc_info=True)

if __name__ == "__main__":
    s = Scheduler()
    s.run()