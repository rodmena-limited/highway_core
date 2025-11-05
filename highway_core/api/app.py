import logging
import os
import json
import uuid
from flask import Flask, request, jsonify

from highway_core.persistence.database import get_db_manager
from highway_core.engine.models import AnyOperatorModel

# We must use the highway_dsl library to load the workflow
try:
    from highway_dsl import Workflow
    from cli import load_workflow_from_python
except ImportError:
    print("Error: highway_dsl library not found. Please install 'highway-dsl'.")
    exit(1)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HighwayAPI")

# Use a single DB manager for the Flask app
db_manager = get_db_manager()

@app.route("/workflow/start/<path:workflow_name>", methods=["POST"])
def start_workflow(workflow_name: str):
    """
    Starts a new DURABLE workflow.
    Finds the workflow .py file, loads it, and registers it with the DB.
    The Scheduler will pick it up.
    """
    logger.info(f"Received request to start durable workflow: {workflow_name}")
    
    # 1. Find the workflow file (assume it's in tests/data for now)
    workflow_path = os.path.join(
        os.path.dirname(__file__), 
        '..', '..', 'tests', 'data', f"{workflow_name}.py"
    )
    
    if not os.path.exists(workflow_path):
        logger.warning(f"Workflow definition '{workflow_name}.py' not found at {workflow_path}")
        return jsonify({"error": f"Workflow definition '{workflow_name}.py' not found."}), 404
        
    try:
        # 2. Load the highway_dsl.Workflow object
        workflow_dsl: Workflow = load_workflow_from_python(workflow_path)
        
        # 3. Convert Pydantic models to a list of dicts for persistence
        tasks_list = [
            task.model_dump(mode='json') for task in workflow_dsl.tasks.values()
        ]
        
        # 4. Create the durable workflow in the DB
        workflow_id = str(uuid.uuid4())
        db_manager.create_durable_workflow(
            workflow_id=workflow_id,
            workflow_name=workflow_dsl.name,
            start_task_id=workflow_dsl.start_task,
            variables=workflow_dsl.variables,
            tasks=tasks_list
        )
        
        logger.info(f"🚀 WORKFLOW CREATED: '{workflow_dsl.name}' (ID: {workflow_id})")
        logger.info(f"📋 Total tasks: {len(tasks_list)}")
        logger.info(f"🎯 Starting task: {workflow_dsl.start_task}")
        logger.info(f"📊 Workflow will be picked up by scheduler shortly")
        
        return jsonify({
            "message": f"Durable workflow '{workflow_dsl.name}' created successfully",
            "workflow_id": workflow_id,
            "workflow_name": workflow_dsl.name,
            "total_tasks": len(tasks_list),
            "status": "PENDING",
            "next_steps": "Monitor status with /workflow/status/{workflow_id} or wait for scheduler pickup"
        }), 202
        
    except Exception as e:
        logger.error(f"Error starting workflow {workflow_name}: {e}", exc_info=True)
        return jsonify({"error": f"Failed to start workflow: {str(e)}"}), 500

@app.route("/workflow/resume", methods=["GET"])
def resume_workflow():
    """
    Resumes a workflow task that is 'WAITING_FOR_EVENT'.
    e.g., /workflow/resume?token=...&decision=APPROVED
    """
    token = request.args.get("token")
    decision = request.args.get("decision")
    
    if not token or not decision:
        return jsonify({"error": "Missing 'token' or 'decision' query parameter"}), 400
        
    try:
        task = db_manager.get_task_by_token(token)
        
        if not task:
            return jsonify({"error": "Invalid or expired token"}), 404
            
        if task.status != "WAITING_FOR_EVENT":
            return jsonify({"error": "Task is not waiting for an event"}), 400
        
        # 1. Save the result
        result_data = {"status": decision, "reason": "HUMAN_INPUT"}
        if task.result_key:
            db_manager.store_result(
                task.workflow_id, task.task_id, task.result_key, result_data
            )
            logger.info(f"Stored human decision '{decision}' for result key '{task.result_key}' in workflow {task.workflow_id}")
        
        # 2. Mark the human task as COMPLETED
        db_manager.update_task_status_by_workflow(task.workflow_id, task.task_id, "completed")
        
        logger.info(f"🎯 WORKFLOW EVENT: Human task '{task.task_id}' in workflow '{task.workflow_id}' was {decision} with token '{token}'")
        logger.info(f"📋 Next steps: Workflow will proceed to evaluate condition and execute appropriate branches")
        
        return jsonify({
            "message": f"Workflow task resumed with decision: {decision}",
            "workflow_id": task.workflow_id,
            "task_id": task.task_id,
            "decision": decision,
            "status": "COMPLETED",
            "next_steps": "Workflow will proceed to condition evaluation and branch execution"
        }), 200

    except Exception as e:
        logger.error(f"Error resuming with token {token}: {e}", exc_info=True)
        return jsonify({"error": f"Failed to resume workflow: {str(e)}"}), 500

@app.route("/workflow/status/<workflow_id>", methods=["GET"])
def get_workflow_status(workflow_id: str):
    """
    Gets the status of a workflow and all its tasks.
    """
    logger.info(f"📊 Status request for workflow: {workflow_id}")
    
    workflow = db_manager.load_workflow(workflow_id)
    if not workflow:
        logger.warning(f"❌ Workflow not found: {workflow_id}")
        return jsonify({"error": "Workflow not found"}), 404
        
    tasks = db_manager.get_tasks_by_workflow(workflow_id)
    
    # Find any tasks waiting for events
    waiting_tasks = []
    for t in tasks:
        if t.status == 'WAITING_FOR_EVENT':
            waiting_tasks.append({
                "task_id": t.task_id,
                "event_token": t.event_token if t.event_token else 'No token generated',
                "result_key": t.result_key if t.result_key else 'No result key',
                "description": "This task is waiting for human input or external event"
            })
    
    tasks_status = [
        {
            "task_id": t.task_id, 
            "status": t.status, 
            "error": t.error_message if t.error_message else None,
            "event_token": t.event_token if t.status == 'WAITING_FOR_EVENT' else None
        } for t in tasks
    ]
    
    response = {
        "workflow_id": workflow_id,
        "workflow_name": workflow['name'],
        "workflow_status": workflow['status'],
        "tasks": tasks_status,
        "summary": {
            "total_tasks": len(tasks),
            "completed_tasks": len([t for t in tasks if t.status == 'completed']),
            "waiting_for_event": len(waiting_tasks),
            "pending_tasks": len([t for t in tasks if t.status == 'PENDING']),
            "failed_tasks": len([t for t in tasks if t.status == 'failed'])
        }
    }
    
    # Add special information for workflows waiting for events
    if waiting_tasks:
        response["waiting_for_input"] = {
            "message": "🎯 Workflow is waiting for human input!",
            "tasks_waiting": waiting_tasks,
            "instructions": "Use the event_token with /workflow/resume endpoint to provide input"
        }
        logger.info(f"🎯 Workflow {workflow_id} has {len(waiting_tasks)} task(s) waiting for human input")
        for task in waiting_tasks:
            logger.info(f"📋 Task '{task['task_id']}' waiting with token: {task['event_token']}")
    else:
        logger.info(f"📊 Workflow {workflow_id} status: {workflow['status']} - {len([t for t in tasks if t['status'] == 'completed'])}/{len(tasks)} tasks completed")
    
    return jsonify(response), 200

@app.route("/workflows", methods=["GET"])
def list_workflows():
    """
    Lists all workflows with their basic status information.
    """
    logger.info("📋 Listing all workflows")
    
    try:
        # Use existing method to get workflows by different statuses
        pending_workflows = db_manager.get_workflows_by_status("PENDING", mode="DURABLE")
        running_workflows = db_manager.get_workflows_by_status("RUNNING", mode="DURABLE")
        completed_workflows = db_manager.get_workflows_by_status("completed", mode="DURABLE")
        failed_workflows = db_manager.get_workflows_by_status("failed", mode="DURABLE")
        
        workflows = pending_workflows + running_workflows + completed_workflows + failed_workflows
        logger.info(f"📋 Found {len(workflows)} workflows")
        workflow_list = []
        
        for wf in workflows:
            tasks = db_manager.get_tasks_by_workflow(wf.workflow_id)
            waiting_tasks = [t for t in tasks if t.status == 'WAITING_FOR_EVENT']
            
            workflow_info = {
                "workflow_id": wf.workflow_id,
                "workflow_name": wf.workflow_name,
                "status": wf.status,
                "created_at": wf.start_time.isoformat() if wf.start_time else None,
                "updated_at": wf.updated_at.isoformat() if wf.updated_at else None,
                "total_tasks": len(tasks),
                "completed_tasks": len([t for t in tasks if t.status == 'completed']),
                "waiting_for_input": len(waiting_tasks),
                "has_waiting_tasks": len(waiting_tasks) > 0
            }
            
            # Add waiting task tokens if any
            if waiting_tasks:
                workflow_info["waiting_tasks"] = [{
                    "task_id": t.task_id,
                    "event_token": t.event_token if t.event_token else 'No token'
                } for t in waiting_tasks]
            
            workflow_list.append(workflow_info)
        
        logger.info(f"📊 Found {len(workflow_list)} workflows")
        return jsonify({
            "workflows": workflow_list,
            "total_count": len(workflow_list),
            "summary": {
                "running": len([w for w in workflow_list if w['status'] == 'RUNNING']),
                "completed": len([w for w in workflow_list if w['status'] == 'completed']),
                "pending": len([w for w in workflow_list if w['status'] == 'PENDING']),
                "failed": len([w for w in workflow_list if w['status'] == 'failed']),
                "waiting_for_input": len([w for w in workflow_list if w['has_waiting_tasks']])
            }
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Error listing workflows: {e}", exc_info=True)
        return jsonify({"error": f"Failed to list workflows: {str(e)}"}), 500

if __name__ == "__main__":
    run_server()