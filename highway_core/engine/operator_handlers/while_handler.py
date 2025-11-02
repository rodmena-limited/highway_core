# highway_core/engine/operator_handlers/while_handler.py
import graphlib
from highway_core.engine.common import WhileOperatorModel
from highway_core.engine.state import WorkflowState
from highway_core.engine.operator_handlers.condition_handler import eval_condition
from highway_core.engine.operator_handlers import task_handler


def execute(
    task: WhileOperatorModel,
    state: WorkflowState,
    orchestrator,
    registry,
    bulkhead_manager,
):
    counter = 0
    max_iterations = 100  # Safety break

    while counter < max_iterations:
        resolved_condition = str(state.resolve_templating(task.condition))
        is_true = eval_condition(resolved_condition)
        print(
            f"WhileHandler: Iteration {counter + 1} - Resolved '{resolved_condition}'. Result: {is_true}"
        )

        if not is_true:
            print("WhileHandler: Condition is False. Exiting loop.")
            break

        print(f"WhileHandler: Condition True, executing loop body: {task.loop_body}")

        # Like foreach, create a sub-orchestrator for the loop body
        sub_graph_tasks = {
            task_id: orchestrator.workflow.tasks[task_id] for task_id in task.loop_body
        }
        loop_graph = {tid: set(t.dependencies) for tid, t in sub_graph_tasks.items()}
        loop_sorter = graphlib.TopologicalSorter(loop_graph)
        loop_sorter.prepare()

        handler_map = {"task": task_handler.execute}

        while loop_sorter.is_active():
            for task_id in loop_sorter.get_ready():
                task_model = sub_graph_tasks[task_id]
                task_clone = task_model.model_copy(deep=True)
                task_clone.args = state.resolve_templating(task_clone.args)

                handler_func = handler_map.get(task_clone.operator_type)
                if handler_func:
                    handler_func(task_clone, state, registry, bulkhead_manager)

                loop_sorter.done(task_id)

        counter += 1

    if counter >= max_iterations:
        print("WhileHandler: Maximum iterations reached, exiting loop.")
