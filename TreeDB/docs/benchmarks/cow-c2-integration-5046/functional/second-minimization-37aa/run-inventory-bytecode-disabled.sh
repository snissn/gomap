#!/bin/bash
set -u
export PYTHONDONTWRITEBYTECODE=1
COW_ARTIFACTS=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/second-minimization-37aa
COW_SOURCE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-second-minimization-37aa
cd "$COW_SOURCE"
python3 -B "$COW_ARTIFACTS/inventory-bytecode-repair.py" "$COW_ARTIFACTS" "$COW_SOURCE" > "$COW_ARTIFACTS/cleanup.log" 2>&1
COW_EXIT=$?
printf 'owned-bytecode-cleanup\t%s\n' "$COW_EXIT" > "$COW_ARTIFACTS/inventory-repair-exits.tsv"
if [ "$COW_EXIT" != 0 ]; then exit "$COW_EXIT"; fi
python3 -B -m unittest discover -s .github/scripts -p 'test_ci_impact.py' > "$COW_ARTIFACTS/ci-impact-bytecode-disabled.log" 2>&1
COW_EXIT=$?
printf 'ci-impact-bytecode-disabled\t%s\n' "$COW_EXIT" >> "$COW_ARTIFACTS/inventory-repair-exits.tsv"
if [ "$COW_EXIT" != 0 ]; then exit "$COW_EXIT"; fi
python3 -B -m unittest discover -s .github/scripts -p 'test_refresh_ci_impact_inventory.py' > "$COW_ARTIFACTS/ci-inventory-bytecode-disabled.log" 2>&1
COW_EXIT=$?
printf 'ci-inventory-bytecode-disabled\t%s\n' "$COW_EXIT" >> "$COW_ARTIFACTS/inventory-repair-exits.tsv"
if [ "$COW_EXIT" != 0 ]; then exit "$COW_EXIT"; fi
python3 -B "$COW_ARTIFACTS/verify.py" "$COW_ARTIFACTS" "$COW_SOURCE" > "$COW_ARTIFACTS/final-source-check.json"
COW_EXIT=$?
printf 'final-no-extra-proof\t%s\n' "$COW_EXIT" >> "$COW_ARTIFACTS/inventory-repair-exits.tsv"
exit "$COW_EXIT"
