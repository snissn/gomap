#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

while [[ ! -f SLAB_WORK_HALT ]]; do
  npx --yes @openai/codex@latest --dangerously-bypass-approvals-and-sandbox -m 'gpt-5.2-codex' exec "$(cat mainnet_parity_prompt.md)"
  sleep 1
done

echo "SLAB_WORK_HALT exists; exiting."
