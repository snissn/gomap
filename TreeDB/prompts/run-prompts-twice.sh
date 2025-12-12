#!/usr/bin/env bash
set -euo pipefail

# Maximum attempts per phase before failing.
MAX_RUNS="${MAX_RUNS:-5}"

prompt_files=(
  prompts/phase-00-bootstrap.md
  prompts/phase-01-primitives.md
  prompts/phase-02-pager.md
  prompts/phase-03-slotted-pages.md
  prompts/phase-04-btree-core.md
  prompts/phase-05-slab-manager.md
  prompts/phase-06-mvcc-pruning.md
  prompts/phase-07-batch-commit.md
  prompts/phase-08-iterators.md
  prompts/phase-09-compaction.md
  prompts/phase-10-adaptive-threshold.md
  prompts/phase-11-public-api.md
)

for prompt_file in "${prompt_files[@]}"; do
  base="$(basename "$prompt_file")"
  if [[ "$base" =~ ^phase-([0-9]+) ]]; then
    phase_raw="${BASH_REMATCH[1]}"
    phase_num=$((10#$phase_raw))
  else
    echo "ERROR: Could not parse phase number from $prompt_file" >&2
    exit 1
  fi

  marker="@PHASE_${phase_num}_COMPLETE"

  if [[ -f "$marker" ]]; then
    echo "== Phase $phase_num already complete ($marker present); skipping =="
    continue
  fi

  for attempt in $(seq 1 "$MAX_RUNS"); do
    echo "== Running $prompt_file (attempt $attempt/$MAX_RUNS) =="
    npx @openai/codex@latest exec --dangerously-bypass-approvals-and-sandbox "$(cat "$prompt_file")"

    if [[ -f "$marker" ]]; then
      echo "== Phase $phase_num complete ($marker created) =="
      break
    fi

    if [[ "$attempt" -eq "$MAX_RUNS" ]]; then
      echo "ERROR: Phase $phase_num did not complete after $MAX_RUNS attempts." >&2
      exit 1
    fi
  done
done
