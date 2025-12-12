#!/bin/bash

# run_perf.sh
# Executes the 9 steps of the TreeDB Performance Sprint using Gemini.
# Loops each step up to N times until a completion file is detected.

set -e

MAX_RETRIES=10

for i in {1..9}; do
    # Format i with a leading zero (e.g., 01, 02) for the prompt file search
    printf -v padded_i "%02d" "$i"
    
    # Find the specific prompt file matching the pattern (e.g., prompts/perf_01_plan.txt)
    # We use a glob here because the filenames have suffixes.
    prompt_files=(prompts/perf_${padded_i}_*.txt)
    
    # check if file exists
    if [ ! -e "${prompt_files[0]}" ]; then
        echo "Error: No prompt file found matching prompts/perf_${padded_i}_*.txt"
        exit 1
    fi
    
    prompt_file="${prompt_files[0]}"
    
    # Completion file format defined in the prompts (e.g., PHASE_PERF_1_COMPLETE)
    complete_file="PHASE_PERF_${i}_COMPLETE"

    if [ -f "$complete_file" ]; then
        echo "Perf Step $i already complete ($complete_file exists). Skipping."
        continue
    fi

    echo "=================================================="
    echo "Starting Perf Step $i"
    echo "Prompt: $prompt_file"
    echo "Goal: Create $complete_file"
    echo "=================================================="
    
    # Run Gemini with the prompt content ONE TIME
    npx https://github.com/google-gemini/gemini-cli --yolo "$(cat "$prompt_file")"
    
    if [ -f "$complete_file" ]; then
        echo "Perf Step $i completed successfully."
    else
        echo "Perf Step $i executed. Run again to verify or continue."
    fi

    # Exit after running the active step, so the user can review or re-run.
    exit 0
done

echo "All 9 Performance Sprint Steps Executed Successfully."
