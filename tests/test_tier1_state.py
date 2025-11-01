# tests/test_tier1_state.py
from highway_core.engine.state import WorkflowState


def test_resolve_simple_string():
    state = WorkflowState({})
    state.set_result("mem_report", {"key": "test_key", "status": "ok"})

    test_str = "Key={{mem_report.key}}, Status={{mem_report.status}}"
    resolved_str = state.resolve_templating(test_str)

    assert resolved_str == "Key=test_key, Status=ok"


def test_resolve_full_string_replacement():
    state = WorkflowState({})
    state.set_result("mem_report", {"key": "test_key"})

    test_str = "{{mem_report.key}}"
    resolved_str = state.resolve_templating(test_str)

    assert resolved_str == "test_key"


def test_resolve_list():
    state = WorkflowState({"initial_var": "hello"})
    test_list = ["Static", "{{variables.initial_var}}"]
    resolved_list = state.resolve_templating(test_list)

    assert resolved_list == ["Static", "hello"]
