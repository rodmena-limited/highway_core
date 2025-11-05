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
        
        return jsonify({
            "message": "Durable workflow created",
            "workflow_id": workflow_id,
            "status": "PENDING"
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
        
        # 2. Mark the human task as COMPLETED
        db_manager.update_task_status_by_workflow(task.workflow_id, task.task_id, "completed")
        
        logger.info(f"Resumed task {task.task_id} for workflow {task.workflow_id} with decision: {decision}")
        
        return jsonify({
            "message": "Workflow task resumed",
            "workflow_id": task.workflow_id,
            "task_id": task.task_id,
            "status": "COMPLETED"
        }), 200

    except Exception as e:
        logger.error(f"Error resuming with token {token}: {e}", exc_info=True)
        return jsonify({"error": f"Failed to resume workflow: {str(e)}"}), 500

@app.route("/workflow/status/<workflow_id>", methods=["GET"])
def get_workflow_status(workflow_id: str):
    """
    Gets the status of a workflow and all its tasks.
    """
    workflow = db_manager.load_workflow(workflow_id)
    if not workflow:
        return jsonify({"error": "Workflow not found"}), 404
        
    tasks = db_manager.get_tasks_by_workflow(workflow_id)
    tasks_status = [
        {
            "task_id": t['task_id'], 
            "status": t['status'], 
            "error": t.get('error_message')
        } for t in tasks
    ]
    
    return jsonify({
        "workflow_id": workflow_id,
        "workflow_name": workflow['name'],
        "workflow_status": workflow['status'],
        "tasks": tasks_status
    }), 200

def run_server():
    app.run(debug=True, port=5000, host='0.0.0.0')

if __name__ == "__main__":
    run_server()