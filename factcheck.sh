#!/bin/bash

# Highway Core Python Workflow Factcheck Script
# Runs all Python workflow tests to verify correct execution

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
    rm -f "$HOME/.highway.sqlite3"
}

# Function to run a Python workflow test
run_python_workflow_test() {
    local workflow_file="$1"
    local test_name="$2"
    local expected_result="$3"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo ""
    echo "🧪 Testing: $test_name"
    echo "📁 Workflow: $workflow_file"
    
    # Clean database before each test to ensure consistent state
    clean_database
    
    # Run the workflow
    echo "🚀 Running workflow..."
    output=$(python cli.py "tests/data/$workflow_file" 2>&1)
    exit_code=$?
    
    # Check if workflow completed successfully
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✅ PASS: Workflow completed successfully${NC}"
        
        # Extract completed tasks count from CLI output
        completed_count=$(echo "$output" | grep -o "✅ Completed: [0-9]*" | grep -o "[0-9]*" | head -n 1)
        total_count=$(echo "$output" | grep -o "📊 Total: [0-9]*" | grep -o "[0-9]*" | head -n 1)
        
        if [ -n "$completed_count" ] && [ -n "$total_count" ]; then
            echo "📊 Tasks: $completed_count completed out of $total_count total"
            
            # For most tests, all tasks should be completed
            if [ "$completed_count" -eq "$total_count" ] && [ "$total_count" -gt 0 ]; then
                echo -e "${GREEN}✅ PASS: All tasks completed successfully${NC}"
                PASSED_TESTS=$((PASSED_TESTS + 1))
            else
                echo -e "${RED}❌ FAIL: Not all tasks completed - $completed_count/$total_count${NC}"
                FAILED_TESTS=$((FAILED_TESTS + 1))
            fi
        else
            echo -e "${RED}❌ FAIL: Could not extract task completion info${NC}"
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
    ""

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