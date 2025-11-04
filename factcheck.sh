#!/bin/bash

# Highway Core Python Workflow Factcheck Script
# Runs all Python workflow tests and verifies results in the database

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track overall test results
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to clean database
clean_database() {
    echo "🧹 Cleaning database..."
    # For PostgreSQL, we'll truncate tables instead of deleting the file
    python3 -c "
from highway_core.persistence.database import get_db_manager
from highway_core.config import settings
from sqlalchemy import text

db_manager = get_db_manager()
with db_manager.session_scope() as session:
    # Truncate all tables to clean the database
    session.execute(text('TRUNCATE TABLE workflows, tasks, workflow_results, task_dependencies, webhooks, admin_tasks, workflow_templates, workflow_memory CASCADE'))
    session.commit()
print('Database cleaned')
"
}

# Function to verify workflow results in database
verify_workflow_in_database() {
    local workflow_name="$1"
    local expected_total_tasks="$2"
    
    # Wait a moment for database writes to complete
    sleep 1
    
    # Query the database to verify workflow results
    python3 -c "
import sys
from highway_core.persistence.database import get_db_manager
from highway_core.config import settings
from sqlalchemy import text

workflow_name = '$workflow_name'
expected_total_tasks = '$expected_total_tasks'

db_manager = get_db_manager()
with db_manager.session_scope() as session:
    # Get the latest workflow with the given name
    workflow = session.execute(text(f\"SELECT workflow_id, status FROM workflows WHERE workflow_name = '{workflow_name}' ORDER BY start_time DESC LIMIT 1\")).fetchone()
    
    if not workflow:
        print(f'❌ No workflow found with name: {workflow_name}')
        sys.exit(1)
    
    workflow_id, status = workflow
    
    if status != 'completed':
        print(f'❌ Workflow {workflow_name} status is {status}, expected completed')
        sys.exit(1)
    
    # Count total tasks
    total_tasks = session.execute(text(f\"SELECT COUNT(*) FROM tasks WHERE workflow_id = '{workflow_id}'\")).fetchone()[0]
    
    # Count completed tasks
    completed_tasks = session.execute(text(f\"SELECT COUNT(*) FROM tasks WHERE workflow_id = '{workflow_id}' AND status = 'completed'\")).fetchone()[0]
    
    # Count failed tasks
    failed_tasks = session.execute(text(f\"SELECT COUNT(*) FROM tasks WHERE workflow_id = '{workflow_id}' AND status = 'failed'\")).fetchone()[0]
    
    print(f'📊 Database verification for {workflow_name}:')
    print(f'  Workflow ID: {workflow_id}')
    print(f'  Status: {status}')
    print(f'  Total tasks: {total_tasks}')
    print(f'  Completed tasks: {completed_tasks}')
    print(f'  Failed tasks: {failed_tasks}')
    
    # Verify results
    if total_tasks == 0:
        print('❌ No tasks found for workflow')
        sys.exit(1)
    
    if failed_tasks > 0:
        print(f'❌ {failed_tasks} tasks failed')
        sys.exit(1)
    
    if completed_tasks != total_tasks:
        print(f'❌ Not all tasks completed: {completed_tasks}/{total_tasks}')
        sys.exit(1)
    
    if expected_total_tasks and total_tasks != int(expected_total_tasks):
        print(f'❌ Expected {expected_total_tasks} tasks, but found {total_tasks}')
        sys.exit(1)
    
    print('✅ Database verification passed')
"
}

# Function to run a Python workflow test
run_python_workflow_test() {
    local workflow_file="$1"
    local test_name="$2"
    local expected_result="$3"
    local expect_failure="$4"  # New parameter: "true" if we expect this workflow to fail
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo ""
    echo "🧪 Testing: $test_name"
    echo "📁 Workflow: $workflow_file"
    
    # Clean database before each test to ensure consistent state
    clean_database
    
    # Run the workflow
    echo "🚀 Running workflow..."
    output=$(python cli.py run "tests/data/$workflow_file" 2>&1)
    exit_code=$?
    
    # Handle expected failure case
    if [ "$expect_failure" = "true" ]; then
        if [ $exit_code -ne 0 ]; then
            echo -e "${GREEN}✅ PASS: Workflow failed as expected (testing error handling)${NC}"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            echo -e "${RED}❌ FAIL: Workflow should have failed but succeeded${NC}"
            echo "Output: $output"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
        return
    fi
    
    # Handle normal case (expecting success)
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✅ PASS: Workflow completed successfully${NC}"
        
        # Verify results in database instead of trusting CLI output
        # Get the actual workflow name from the database
        workflow_name=$(python3 -c "
from highway_core.persistence.database import get_db_manager
from highway_core.config import settings
from sqlalchemy import text

db_manager = get_db_manager()
with db_manager.session_scope() as session:
    workflow = session.execute(text('SELECT workflow_name FROM workflows ORDER BY start_time DESC LIMIT 1')).fetchone()
    if workflow:
        print(workflow[0])
    else:
        print('')
")
        
        if [ -z "$workflow_name" ]; then
            echo -e "${RED}❌ FAIL: Could not find workflow in database${NC}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            return
        fi
        
        # Map workflow files to expected task counts
        case "$workflow_file" in
            "container_checksum_test_workflow.py")
                expected_tasks=7
                ;;
            "loop_test_workflow.py")
                expected_tasks=6
                ;;
            "parallel_wait_test_workflow.py")
                expected_tasks=8
                ;;
            "persistence_test_workflow.py")
                expected_tasks=3
                ;;
            "while_test_workflow.py")
                expected_tasks=6
                ;;
            *)
                expected_tasks=""
                ;;
        esac
        
        if verify_workflow_in_database "$workflow_name" "$expected_tasks"; then
            echo -e "${GREEN}✅ PASS: Database verification successful${NC}"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            echo -e "${RED}❌ FAIL: Database verification failed${NC}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    else
        echo -e "${RED}❌ FAIL: Workflow failed with exit code $exit_code${NC}"
        echo "Error output: $output"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# Main test execution
echo "🚀 Starting Highway Core Python Workflow Tests"
echo "============================================="

# Test 1: Container Checksum Test
run_python_workflow_test \
    "container_checksum_test_workflow.py" \
    "Container Checksum Test" \
    "5746"

# Test 2: Failing Workflow (should complete but may have error tasks)
run_python_workflow_test \
    "failing_workflow.py" \
    "Failing Workflow Test" \
    "" \
    "true"  # Expect this workflow to fail (testing error handling)

# Test 3: Loop Test
run_python_workflow_test \
    "loop_test_workflow.py" \
    "Loop Test" \
    ""

# Test 4: Parallel Wait Test
run_python_workflow_test \
    "parallel_wait_test_workflow.py" \
    "Parallel Wait Test" \
    ""

# Test 5: Persistence Test
run_python_workflow_test \
    "persistence_test_workflow.py" \
    "Persistence Test" \
    ""

# Test 6: While Test
run_python_workflow_test \
    "while_test_workflow.py" \
    "While Test" \
    ""

# Final summary
echo ""
echo "======================================="
echo "📊 TEST SUMMARY"
echo "======================================="
echo "Total tests: $TOTAL_TESTS"
echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed: ${RED}$FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}❌ Some tests failed!${NC}"
    exit 1
fi