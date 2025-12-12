#!/usr/bin/env bash
set -euo pipefail

# Maximum attempts per perf step before failing.
MAX_RUNS="${MAX_RUNS:-5}"

prompt_files=(
  prompts/perf-00-plan.md
  prompts/perf-01-collect-data.md
  prompts/perf-02-review-data.md
  prompts/perf-03-design-improvements.md
  prompts/perf-04-security-correctness-review.md
  prompts/perf-05-update-spec.md
  prompts/perf-06-review-spec.md
  prompts/perf-07-update-agents-todo.md
  prompts/perf-08-implement.md
)

for prompt_file in "${prompt_files[@]}"; do
  base="$(basename "$prompt_file")"
  if [[ "$base" =~ ^perf-([0-9]+) ]]; then
    step_raw="${BASH_REMATCH[1]}"
    step_num=$((10#$step_raw))
  else
    echo "ERROR: Could not parse perf step number from $prompt_file" >&2
    exit 1
  fi

  marker="@PERF_$(printf '%02d' "$step_num")_COMPLETE"

  if [[ -f "$marker" ]]; then
    echo "== Perf step $step_num already complete ($marker present); skipping =="
    continue
  fi

  for attempt in $(seq 1 "$MAX_RUNS"); do
    echo "== Running $prompt_file (attempt $attempt/$MAX_RUNS) =="
    npx @openai/codex@latest exec --dangerously-bypass-approvals-and-sandbox "$(cat "$prompt_file")"

    if [[ -f "$marker" ]]; then
      echo "== Perf step $step_num complete ($marker created) =="
      break
    fi

    if [[ "$attempt" -eq "$MAX_RUNS" ]]; then
      echo "ERROR: Perf step $step_num did not complete after $MAX_RUNS attempts." >&2
      exit 1
    fi
  done
done

