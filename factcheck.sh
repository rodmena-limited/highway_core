#!/bin/bash

# Define paths and search terms
DB_FILE="$HOME/.highway.sqlite3"
PYTHON_SCRIPT="cli.py"
WORKFLOW_FILE="tests/data/container_checksum_test_workflow.yaml"
SEARCH_TERM="5746"

rm -f "$DB_FILE"

# Execute the python script
python "$PYTHON_SCRIPT" "$WORKFLOW_FILE"

# Check if the python script executed successfully
if [ $? -ne 0 ]; then
    echo "Error: Python script failed to execute." >&2
    exit 1
fi

# Query sqlite, pipe to egrep, and store the output
output=$(sqlite3 "$DB_FILE" "SELECT result_value_json FROM workflow_results WHERE created_at > datetime('now', '-1 minute') ORDER BY created_at DESC LIMIT 1;" | egrep "$SEARCH_TERM")

# Check if egrep found anything
if [ -z "$output" ]; then
    echo "Error: Search term '$SEARCH_TERM' not found in the latest result created within the last minute." >&2
    exit 1
else
    # Print the found output if successful
    echo "$output"
fi
