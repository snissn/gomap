#!/bin/bash

# run_phases.sh
# Executes the 7 phases of TreeDB development using Gemini (legacy).
# Loops each phase up to N times until a completion file is detected.

set -e

MAX_RETRIES=10

for i in {8..8}; do
#for i in {1..7}; do
    prompt_file="prompts/phase${i}.txt"
    complete_file="PHASE_${i}_COMPLETE"
    
    if [ ! -f "$prompt_file" ]; then
        echo "Error: $prompt_file not found!"
        exit 1
    fi

    if [ -f "$complete_file" ]; then
        echo "Phase $i already complete ($complete_file exists). Skipping."
        continue
    fi

    echo "=================================================="
    echo "Starting Phase $i"
    echo "Prompt: $prompt_file"
    echo "=================================================="
    
    for (( attempt=1; attempt<=MAX_RETRIES; attempt++ )); do
        echo "Run $attempt/$MAX_RETRIES for Phase $i"
        
        # Run Gemini with the prompt content
        npx https://github.com/google-gemini/gemini-cli --yolo "$(cat "$prompt_file")"
        
        if [ -f "$complete_file" ]; then
            echo "Phase $i completed successfully on attempt $attempt."
            echo ""
            break
        fi

        if [ $attempt -eq $MAX_RETRIES ]; then
            echo "Error: Phase $i failed to complete after $MAX_RETRIES attempts."
            exit 1
        fi

        echo "Phase $i not yet complete (file $complete_file not found). Retrying..."
        echo ""
    done
done

echo "All 7 Phases Executed Successfully."
