#!/bin/bash

# run_phases.sh
# Executes the 7 phases of TreeDB development using Gemini.
# Runs each prompt twice to ensure idempotency and completion.

set -e

for i in {1..7}; do
    prompt_file="prompts/phase${i}.txt"
    
    if [ ! -f "$prompt_file" ]; then
        echo "Error: $prompt_file not found!"
        exit 1
    fi

    echo "=================================================="
    echo "Starting Phase $i (Run 1/2)"
    echo "Prompt: $prompt_file"
    echo "=================================================="
    
    # Run Gemini with the prompt content
    npx https://github.com/google-gemini/gemini-cli --yolo "$(cat "$prompt_file")"
    
    echo ""
    echo "=================================================="
    echo "Verifying Phase $i (Run 2/2 - Idempotency Check)"
    echo "Prompt: $prompt_file"
    echo "=================================================="
    
    # Run Gemini again to verify/fix/complete
    npx https://github.com/google-gemini/gemini-cli --yolo "$(cat "$prompt_file")"
    
    echo ""
    echo "Phase $i completed."
    echo ""
done

echo "All 7 Phases Executed."
