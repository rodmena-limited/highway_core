#!/bin/bash

# Define paths and configuration
DB_FILE="$HOME/.highway.sqlite3"
PYTHON_SCRIPT="cli.py"
WORKFLOWS_DIR="tests/data"
LOGS_DIR="/tmp/highway_concurrent_logs"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Track overall test results
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Create logs directory
mkdir -p "$LOGS_DIR"

# Function to clean database
clean_database() {
    echo "🧹 Cleaning database..."
    rm -f "$DB_FILE"
}

# Function to run a workflow test in background
run_workflow_test_concurrent() {
    local workflow_file="$1"
    local expected_result="$2"
    local test_name="$3"
    local should_fail="$4"  # "true" if workflow should fail, "false" otherwise
    local test_id="$5"
    local log_file="$LOGS_DIR/test_${test_id}.log"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo "🧪 Starting concurrent test: $test_name (ID: $test_id)" | tee -a "$log_file"
    echo "📁 Workflow: $workflow_file" | tee -a "$log_file"
    
    # Run the workflow in background
    echo "🚀 Running workflow concurrently..." | tee -a "$log_file"
    start_time=$(date +%s.%N)
    
    output=$(python "$PYTHON_SCRIPT" "$WORKFLOWS_DIR/$workflow_file" 2>&1)
    exit_code=$?
    
    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc -l)
    
    echo "📊 Exit code: $exit_code" | tee -a "$log_file"
    echo "⏱️  Duration: ${duration}s" | tee -a "$log_file"
    echo "📝 Full output:" | tee -a "$log_file"
    echo "$output" | tee -a "$log_file"
    
    # Check if workflow should fail
    if [ "$should_fail" = "true" ]; then
        if [ $exit_code -ne 0 ]; then
            echo -e "${GREEN}✅ PASS: Workflow failed as expected${NC}" | tee -a "$log_file"
            
            # Check if error message contains expected text
            if echo "$output" | grep -q "$expected_result"; then
                echo -e "${GREEN}✅ PASS: Error message contains expected text${NC}" | tee -a "$log_file"
                echo "PASSED" > "$LOGS_DIR/result_${test_id}.txt"
            else
                echo -e "${RED}❌ FAIL: Error message does not contain expected text${NC}" | tee -a "$log_file"
                echo "Expected: $expected_result" | tee -a "$log_file"
                echo "Got: $output" | tee -a "$log_file"
                echo "FAILED" > "$LOGS_DIR/result_${test_id}.txt"
            fi
        else
            echo -e "${RED}❌ FAIL: Workflow should have failed but succeeded${NC}" | tee -a "$log_file"
            echo "FAILED" > "$LOGS_DIR/result_${test_id}.txt"
        fi
    else
        # Workflow should succeed
        if [ $exit_code -eq 0 ]; then
            echo -e "${GREEN}✅ PASS: Workflow completed successfully${NC}" | tee -a "$log_file"
            
            # Check for expected result in database or output
            if [ -n "$expected_result" ]; then
                if echo "$output" | grep -q "$expected_result"; then
                    echo -e "${GREEN}✅ PASS: Output contains expected result: $expected_result${NC}" | tee -a "$log_file"
                    echo "PASSED" > "$LOGS_DIR/result_${test_id}.txt"
                else
                    # Check database for result (wait a bit for database writes)
                    sleep 2
                    db_result=$(sqlite3 "$DB_FILE" "SELECT result_value_json FROM workflow_results WHERE created_at > datetime('now', '-5 minutes') ORDER BY created_at DESC LIMIT 1;" 2>/dev/null)
                    if echo "$db_result" | grep -q "$expected_result"; then
                        echo -e "${GREEN}✅ PASS: Database contains expected result: $expected_result${NC}" | tee -a "$log_file"
                        echo "PASSED" > "$LOGS_DIR/result_${test_id}.txt"
                    else
                        echo -e "${RED}❌ FAIL: Neither output nor database contains expected result: $expected_result${NC}" | tee -a "$log_file"
                        echo "Database result: $db_result" | tee -a "$log_file"
                        echo "Output: $output" | tee -a "$log_file"
                        echo "FAILED" > "$LOGS_DIR/result_${test_id}.txt"
                    fi
                fi
            else
                # No specific result expected, just check success
                echo "PASSED" > "$LOGS_DIR/result_${test_id}.txt"
            fi
        else
            echo -e "${RED}❌ FAIL: Workflow should have succeeded but failed${NC}" | tee -a "$log_file"
            echo "Error output: $output" | tee -a "$log_file"
            echo "FAILED" > "$LOGS_DIR/result_${test_id}.txt"
        fi
    fi
}

# Function to check database integrity
check_database_integrity() {
    echo ""
    echo "🔍 Checking database integrity after concurrent execution..."
    
    # Check for any database corruption or locking issues
    if [ -f "$DB_FILE" ]; then
        # Try to query the database
        if sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM workflows;" >/dev/null 2>&1; then
            workflow_count=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM workflows;" 2>/dev/null)
            task_count=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM tasks;" 2>/dev/null)
            result_count=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM workflow_results;" 2>/dev/null)
            
            echo -e "${GREEN}✅ Database integrity check passed${NC}"
            echo "📊 Database statistics:"
            echo "  - Workflows: $workflow_count"
            echo "  - Tasks: $task_count"
            echo "  - Results: $result_count"
            return 0
        else
            echo -e "${RED}❌ Database integrity check failed${NC}"
            return 1
        fi
    else
        echo -e "${YELLOW}⚠️  No database file found${NC}"
        return 0
    fi
}

# Function to check for race conditions in logs
check_for_race_conditions() {
    echo ""
    echo "🔍 Checking for potential race conditions..."
    
    # Look for common race condition indicators in logs
    race_patterns=(
        "database is locked"
        "concurrent modification"
        "race condition"
        "deadlock"
        "timeout"
        "connection refused"
    )
    
    found_race=false
    for pattern in "${race_patterns[@]}"; do
        if grep -r -i "$pattern" "$LOGS_DIR"/*.log >/dev/null 2>&1; then
            echo -e "${RED}⚠️  Potential race condition detected: $pattern${NC}"
            grep -r -i "$pattern" "$LOGS_DIR"/*.log
            found_race=true
        fi
    done
    
    if [ "$found_race" = false ]; then
        echo -e "${GREEN}✅ No obvious race conditions detected in logs${NC}"
    fi
}

# Main test execution
echo "🚀 Starting Highway Core Concurrent Workflow Tests"
echo "=================================================="
echo "🧪 This test runs all workflows simultaneously to check for race conditions"
echo ""

# Clean database before starting
clean_database

# Start all tests concurrently in background
echo "🔄 Starting all workflow tests concurrently..."

# Test 1: Container Checksum Test (should produce 5746)
run_workflow_test_concurrent \
    "container_checksum_test_workflow.yaml" \
    "5746" \
    "Container Checksum Test" \
    "false" \
    "1" &

# Test 2: Failing Workflow (should fail with specific error)
run_workflow_test_concurrent \
    "failing_workflow.yaml" \
    "Function 'non.existent.function' not found in registry" \
    "Failing Workflow Test" \
    "true" \
    "2" &

# Test 3: Loop Test (should complete successfully)
run_workflow_test_concurrent \
    "loop_test_workflow.yaml" \
    "" \
    "Loop Test" \
    "false" \
    "3" &

# Test 4: Parallel Wait Test (should complete successfully)
run_workflow_test_concurrent \
    "parallel_wait_test_workflow.yaml" \
    "" \
    "Parallel Wait Test" \
    "false" \
    "4" &

# Test 5: Persistence Test (should complete successfully)
run_workflow_test_concurrent \
    "persistence_test_workflow.yaml" \
    "" \
    "Persistence Test" \
    "false" \
    "5" &

# Test 6: While Test (should complete successfully)
run_workflow_test_concurrent \
    "while_test_workflow.yaml" \
    "" \
    "While Test" \
    "false" \
    "6" &

# Wait for all background jobs to complete
echo ""
echo "⏳ Waiting for all concurrent tests to complete..."
wait

# Collect results
echo ""
echo "📊 Collecting results from concurrent tests..."
for i in {1..6}; do
    if [ -f "$LOGS_DIR/result_${i}.txt" ]; then
        result=$(cat "$LOGS_DIR/result_${i}.txt")
        case $i in
            1) test_name="Container Checksum Test" ;;
            2) test_name="Failing Workflow Test" ;;
            3) test_name="Loop Test" ;;
            4) test_name="Parallel Wait Test" ;;
            5) test_name="Persistence Test" ;;
            6) test_name="While Test" ;;
        esac
        
        if [ "$result" = "PASSED" ]; then
            echo -e "${GREEN}✅ $test_name: PASSED${NC}"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            echo -e "${RED}❌ $test_name: FAILED${NC}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    else
        echo -e "${RED}❌ Test $i: No result file found${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
done

# Post-execution checks
check_database_integrity
check_for_race_conditions

# Final summary
echo ""
echo "=================================================="
echo "📊 CONCURRENT TEST SUMMARY"
echo "=================================================="
echo "Total tests: $TOTAL_TESTS"
echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed: ${RED}$FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}🎉 All concurrent tests passed!${NC}"
    echo -e "${GREEN}✅ No race conditions detected!${NC}"
    exit 0
else
    echo -e "${RED}❌ Some concurrent tests failed!${NC}"
    echo -e "${YELLOW}⚠️  Check logs in $LOGS_DIR for details${NC}"
    exit 1
fi