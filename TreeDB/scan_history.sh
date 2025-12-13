#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <N>"
  exit 1
fi

MAX_N=$1

echo "Scanning history for test results..."
printf "% -15s | % -10s\n" "Commits Back" "Result"
printf "% -15s | % -10s\n" "---------------" "----------"

for i in $(seq 1 $MAX_N); do
  # Run the test script, suppressing stdout/stderr to keep the table clean.
  # We rely on the exit code to determine pass/fail.
  ./run_past_tests.sh "$i" > /dev/null 2>&1
  EXIT_CODE=$?
  
  if [ $EXIT_CODE -eq 0 ]; then
    RESULT="PASS"
  else
    RESULT="FAIL"
  fi
  
  printf "% -15s | % -10s\n" "$i" "$RESULT"
done
