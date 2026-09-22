#!/bin/bash
set -e

# verify_treedb_crash.sh
# Runs a crash recovery loop for TreeDB.
# Usage: ./scripts/verify_treedb_crash.sh

# Ensure we are in root
if [ ! -d "TreeDB" ]; then
    echo "Please run from repo root."
    exit 1
fi

mkdir -p bin

# Build tools
echo "Building stress tools..."
go build -o bin/treedb-stress ./TreeDB/cmd/stress
go build -o bin/treedb-verify ./TreeDB/cmd/verify

TEST_DIR=$(mktemp -d)
echo "Test Dir: $TEST_DIR"

for i in {1..5}; do
    echo "Iteration $i..."
    # Start stress in background
    ./bin/treedb-stress -dir "$TEST_DIR" -duration 10s -workers 4 &
    PID=$!
    
    # Let it run for a bit
    sleep 2
    
    # Kill it
    echo "Killing $PID..."
    kill -9 $PID || true
    wait $PID || true
    
    # Verify
    echo "Verifying..."
    ./bin/treedb-verify -dir "$TEST_DIR"
    
    if [ $? -ne 0 ]; then
        echo "Verification failed!"
        exit 1
    fi
    echo "Recovery OK."
done

rm -rf "$TEST_DIR"
echo "Crash test passed."