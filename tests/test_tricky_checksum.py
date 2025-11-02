import pytest
from highway_core.engine.engine import run_workflow_from_yaml
import sqlite3
import json
from pathlib import Path


def test_tricky_checksum():
    """Test the tricky checksum workflow with mixed runtimes and complex dependencies."""
    # Remove any existing database
    db_path = Path.home() / ".highway.sqlite3"
    if db_path.exists():
        db_path.unlink()

    # Run the workflow
    run_workflow_from_yaml(
        yaml_path="tests/data/tricky_checksum.yaml",
        workflow_run_id="tricky-checksum-test",
    )

    # Connect to database and get results
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get results for this workflow
    cursor = conn.execute(
        "SELECT result_key, result_value_json FROM workflow_results WHERE workflow_id = ?",
        ("tricky-checksum-test",),
    )
    results = {}
    for row in cursor:
        results[row["result_key"]] = json.loads(row["result_value_json"])

    conn.close()

    # The final number should be 5746
    # Calculation: ((100 * 2) + (7 * 3)) * ((100 / 4) + 1) = (200 + 21) * (25 + 1) = 221 * 26 = 5746
    expected_result = 5746

    # Extract the final number from results
    final_number_str = results.get("the_final_number", "0")
    final_number = int(float(final_number_str))

    assert final_number == expected_result, (
        f"Expected {expected_result}, but got {final_number}"
    )

    # Also verify the intermediate results are correct
    assert results.get("alpha_p1") == "200", (
        f"Expected alpha_p1='200', got '{results.get('alpha_p1')}'"
    )
    assert results.get("alpha_p2") == "21", (
        f"Expected alpha_p2='21', got '{results.get('alpha_p2')}'"
    )
    assert results.get("beta_p1") == "25", (
        f"Expected beta_p1='25', got '{results.get('beta_p1')}'"
    )
    assert results.get("alpha_final") == "221", (
        f"Expected alpha_final='221', got '{results.get('alpha_final')}'"
    )
    assert results.get("beta_final") == "26", (
        f"Expected beta_final='26', got '{results.get('beta_final')}'"
    )

    print(f"✅ Test passed! Final number: {final_number}")
