#!/bin/bash

LOG_FILE="test_results_$(date +%Y%m%d_%H%M%S).log"

echo "=== Test Run: $(date) ===" | tee $LOG_FILE
echo "" | tee -a $LOG_FILE

PASSED=0
FAILED=0

# Run tests in tests/ directory
for test in tests/test_*.py; do
    echo "=============================================" | tee -a $LOG_FILE
    echo "Running: $test" | tee -a $LOG_FILE
    echo "=============================================" | tee -a $LOG_FILE
    if python3 "$test" >> $LOG_FILE 2>&1; then
        echo "✓ PASSED" | tee -a $LOG_FILE
        PASSED=$((PASSED + 1))
    else
        echo "✗ FAILED (exit code: $?)" | tee -a $LOG_FILE
        FAILED=$((FAILED + 1))
    fi
    echo "" | tee -a $LOG_FILE
done

# Run test in root
if [ -f "test_anticipated_bundling_unified.py" ]; then
    test="test_anticipated_bundling_unified.py"
    echo "=============================================" | tee -a $LOG_FILE
    echo "Running: $test" | tee -a $LOG_FILE
    echo "=============================================" | tee -a $LOG_FILE
    if python3 "$test" >> $LOG_FILE 2>&1; then
        echo "✓ PASSED" | tee -a $LOG_FILE
        PASSED=$((PASSED + 1))
    else
        echo "✗ FAILED (exit code: $?)" | tee -a $LOG_FILE
        FAILED=$((FAILED + 1))
    fi
    echo "" | tee -a $LOG_FILE
fi

echo "=============================================" | tee -a $LOG_FILE
echo "SUMMARY: $PASSED passed, $FAILED failed" | tee -a $LOG_FILE
echo "Log file: $(pwd)/$LOG_FILE" | tee -a $LOG_FILE

echo "$LOG_FILE"
