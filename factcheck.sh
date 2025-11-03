#!/bin/bash

# Define paths and configuration
DB_FILE="$HOME/.highway.sqlite3"
PYTHON_SCRIPT="cli.py"
WORKFLOWS_DIR="tests/data"

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
    rm -f "$DB_FILE"
}

# Function to run a workflow test
run_workflow_test() {
    local workflow_file="$1"
    local expected_result="$2"
    local test_name="$3"
    local should_fail="$4"  # "true" if workflow should fail, "false" otherwise
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo ""
    echo "🧪 Testing: $test_name"
    echo "📁 Workflow: $workflow_file"
    
    # Clean database before each test
    clean_database
    
    # Run the workflow
    echo "🚀 Running workflow..."
    output=$(python "$PYTHON_SCRIPT" "$WORKFLOWS_DIR/$workflow_file" 2>&1)
    exit_code=$?
    
    echo "📊 Exit code: $exit_code"
    
    # Check if workflow should fail
    if [ "$should_fail" = "true" ]; then
        if [ $exit_code -ne 0 ]; then
            echo -e "${GREEN}✅ PASS: Workflow failed as expected${NC}"
            
            # Check if error message contains expected text
            if echo "$output" | grep -q "$expected_result"; then
                echo -e "${GREEN}✅ PASS: Error message contains expected text${NC}"
                PASSED_TESTS=$((PASSED_TESTS + 1))
            else
                echo -e "${RED}❌ FAIL: Error message does not contain expected text${NC}"
                echo "Expected: $expected_result"
                echo "Got: $output"
                FAILED_TESTS=$((FAILED_TESTS + 1))
            fi
        else
            echo -e "${RED}❌ FAIL: Workflow should have failed but succeeded${NC}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    else
        # Workflow should succeed
        if [ $exit_code -eq 0 ]; then
            echo -e "${GREEN}✅ PASS: Workflow completed successfully${NC}"
            
            # Check for expected result in database or output
            if [ -n "$expected_result" ]; then
                if echo "$output" | grep -q "$expected_result"; then
                    echo -e "${GREEN}✅ PASS: Output contains expected result: $expected_result${NC}"
                    PASSED_TESTS=$((PASSED_TESTS + 1))
                else
                    # Check database for result
                    db_result=$(sqlite3 "$DB_FILE" "SELECT result_value_json FROM workflow_results WHERE created_at > datetime('now', '-1 minute') ORDER BY created_at DESC LIMIT 1;" 2>/dev/null)
                    if echo "$db_result" | grep -q "$expected_result"; then
                        echo -e "${GREEN}✅ PASS: Database contains expected result: $expected_result${NC}"
                        PASSED_TESTS=$((PASSED_TESTS + 1))
                    else
                        echo -e "${RED}❌ FAIL: Neither output nor database contains expected result: $expected_result${NC}"
                        echo "Database result: $db_result"
                        echo "Output: $output"
                        FAILED_TESTS=$((FAILED_TESTS + 1))
                    fi
                fi
            else
                # No specific result expected, just check success
                PASSED_TESTS=$((PASSED_TESTS + 1))
            fi
        else
            echo -e "${RED}❌ FAIL: Workflow should have succeeded but failed${NC}"
            echo "Error output: $output"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    fi
}

# Main test execution
echo "🚀 Starting Highway Core Workflow Tests"
echo "======================================="

# Test 1: Container Checksum Test (should produce 5746)
run_workflow_test \
    "container_checksum_test_workflow.yaml" \
    "5746" \
    "Container Checksum Test" \
    "false"

# Test 2: Failing Workflow (should fail with specific error)
run_workflow_test \
    "failing_workflow.yaml" \
    "Function 'non.existent.function' not found in registry" \
    "Failing Workflow Test" \
    "true"

# Test 3: Loop Test (should complete successfully)
run_workflow_test \
    "loop_test_workflow.yaml" \
    "" \
    "Loop Test" \
    "false"

# Test 4: Parallel Wait Test (should complete successfully)
run_workflow_test \
    "parallel_wait_test_workflow.yaml" \
    "" \
    "Parallel Wait Test" \
    "false"

# Test 5: Persistence Test (should complete successfully)
run_workflow_test \
    "persistence_test_workflow.yaml" \
    "" \
    "Persistence Test" \
    "false"

# Test 6: While Test (should complete successfully)
run_workflow_test \
    "while_test_workflow.yaml" \
    "" \
    "While Test" \
    "false"

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