#!/bin/bash

# run_perf.sh
# Executes the 9 steps of the TreeDB Performance Sprint using Gemini (legacy).
# Loops each step up to 5 times until a completion file is detected.
# If the completion file is found (even on the 6th check), it proceeds to the next step.
# If the file is missing on the 6th check (after 5 runs), it errors out.

set -e

MAX_ATTEMPTS=5

for i in {1..9}; do
    # Format i with a leading zero (e.g., 01, 02) for the prompt file search
    printf -v padded_i "%02d" "$i"
    
    # Find the specific prompt file matching the pattern (e.g., prompts/perf_01_plan.txt)
    prompt_files=(prompts/perf_${padded_i}_*.txt)
    
    if [ ! -e "${prompt_files[0]}" ]; then
        echo "Error: No prompt file found matching prompts/perf_${padded_i}_*.txt"
        exit 1
    fi
    
    prompt_file="${prompt_files[0]}"
    complete_file="PHASE_PERF_${i}_COMPLETE"

    echo "=================================================="
    echo "Processing Perf Step $i"
    echo "Prompt: $prompt_file"
    echo "Goal: Create $complete_file"
    echo "=================================================="

    # Loop 1 to 6. 
    # 1-5: Run tool if file missing.
    # 6: Check file one last time. If missing, error.
    for attempt in {1..6}; do
        if [ -f "$complete_file" ]; then
            echo "Success: $complete_file detected. Moving to next step."
            break
        fi

        if [ "$attempt" -gt "$MAX_ATTEMPTS" ]; then
            echo "Error: Failed to complete Step $i after $MAX_ATTEMPTS attempts."
            echo "File $complete_file was not created."
            exit 1
        fi

        echo "--- Attempt $attempt of $MAX_ATTEMPTS for Step $i ---"
        # Run Gemini with the prompt content
        npx https://github.com/google-gemini/gemini-cli --yolo "$(cat "$prompt_file")"
    done
done

echo "=================================================="
echo "All 9 Performance Sprint Steps Executed Successfully."
echo "=================================================="
