#!/bin/bash
set -e

# Build tools
go build -o stress ./cmd/stress
go build -o verify ./cmd/verify

TEST_DIR=$(mktemp -d)
echo "Test Dir: $TEST_DIR"

for i in {1..5}; do
    echo "Iteration $i..."
    # Start stress in background
    ./stress -dir "$TEST_DIR" -duration 10s -workers 4 &
    PID=$!
    
    # Let it run for a bit
    sleep 2
    
    # Kill it
    echo "Killing $PID..."
    kill -9 $PID || true
    wait $PID || true
    
    # Verify
    echo "Verifying..."
    ./verify -dir "$TEST_DIR"
    
    if [ $? -ne 0 ]; then
        echo "Verification failed!"
        exit 1
    fi
    echo "Recovery OK."
done

rm -rf "$TEST_DIR"
rm stress verify
echo "Crash test passed."
