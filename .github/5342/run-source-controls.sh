#!/usr/bin/env bash
set -euo pipefail
evidence="$RUNNER_TEMP/5342-populate"
controls="$GITHUB_WORKSPACE/harness/.github/5342"
mkdir -p "$evidence/tmp"
export TMPDIR="$evidence/tmp"
result=0
for name in control-symlinks.sh control-stat-failure.sh control-timeout.sh control-basic.sh control-workflow.py control-guardrail.sh; do
  if [[ "$name" == *.py ]]; then command=(python3 "$controls/$name");
  elif [[ "$name" == control-guardrail.sh ]]; then command=(bash "$controls/$name" "$GITHUB_WORKSPACE/product/scripts/check_read_snapshot_guardrail.sh");
  else command=(bash "$controls/$name"); fi
  printf '%q ' "${command[@]}" > "$evidence/$name.command"
  printf '\n' >> "$evidence/$name.command"
  if "${command[@]}" > "$evidence/$name.log" 2>&1; then rc=0; else rc=$?; fi
  printf '%s=%s\n' "$name" "$rc" | tee -a "$evidence/control-exits.txt"
  if [[ $rc != 0 ]] || ! grep -q '^PASS:' "$evidence/$name.log"; then result=1; fi
done
for name in test_refresh_ci_impact_inventory.py test_ci_impact.py test_treedb_ci_contract.py test_treedb_windows_core_headroom.py; do
  printf 'PYTHONDONTWRITEBYTECODE=1 uv run --with pyyaml python .github/scripts/%s\n' "$name" > "$evidence/$name.command"
  if PYTHONDONTWRITEBYTECODE=1 uv run --with pyyaml python ".github/scripts/$name" > "$evidence/$name.log" 2>&1; then rc=0; else rc=$?; fi
  printf '%s=%s\n' "$name" "$rc" | tee -a "$evidence/contract-exits.txt"
  if [[ $rc != 0 ]]; then result=1; fi
done
if PYTHONDONTWRITEBYTECODE=1 uv run --with pyyaml python .github/scripts/refresh_ci_impact_inventory.py --check > "$evidence/inventory-check.log" 2>&1; then rc=0; else rc=$?; fi
printf 'inventory-check=%s\n' "$rc" | tee -a "$evidence/contract-exits.txt"
if [[ $rc != 0 ]]; then result=1; fi
exit "$result"
