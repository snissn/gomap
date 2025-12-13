#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_commits_back>"
  exit 1
fi

N=$1

# Ensure the working directory is clean before switching commits to avoid losing work
if [ -n "$(git status --porcelain)" ]; then 
  echo "Error: Working directory is not clean. Please commit or stash your changes."
  exit 1
fi

CURRENT_COMMIT=$(git rev-parse HEAD)

echo "Current commit: $CURRENT_COMMIT"
echo "Going back $N commits..."

if ! git checkout HEAD~$N; then
    echo "Error: Could not checkout HEAD~$N"
    exit 1
fi

echo "Running tests at HEAD~$N..."
go test .
TEST_EXIT_CODE=$?

echo "Returning to original commit $CURRENT_COMMIT..."
git checkout "$CURRENT_COMMIT"

exit $TEST_EXIT_CODE
