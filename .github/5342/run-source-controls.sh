#!/usr/bin/env bash
set -euo pipefail
evidence="$RUNNER_TEMP/5342-populate"
controls="$GITHUB_WORKSPACE/harness/.github/5342"
mkdir -p "$evidence/tmp"
export TMPDIR="$evidence/tmp"
result=0
# The six source/metadata/wrapper controls already PASS on this exact product
# in retained run41151731211/artifact13023835466; do not repeat unchanged controls.
printf '%s\n' 'reuse six controls: run41151731211 job124389568147 artifact13023835466, exact product5e1b9990' > "$evidence/reused-control-evidence.txt"
printf '%s\n' 'python3 -m pip install --disable-pip-version-check --only-binary=:all: --no-deps --target <retained-pythondeps> PyYAML==6.0.2' > "$evidence/python-bootstrap.command"
if python3 -m pip install --disable-pip-version-check --only-binary=:all: --no-deps --target "$evidence/pythondeps" 'PyYAML==6.0.2' > "$evidence/python-bootstrap.log" 2>&1; then rc=0; else rc=$?; fi
printf 'python-bootstrap=%s\n' "$rc" > "$evidence/python-bootstrap.exit"
if [[ $rc != 0 ]]; then exit "$rc"; fi
export PYTHONPATH="$evidence/pythondeps"
export PYTHONDONTWRITEBYTECODE=1
python3 -c 'import sys,yaml; print(sys.version); print("PyYAML="+yaml.__version__)' > "$evidence/python-tools.txt"
for name in test_refresh_ci_impact_inventory.py test_ci_impact.py test_treedb_ci_contract.py test_treedb_windows_core_headroom.py; do
  printf 'PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/%s\n' "$name" > "$evidence/$name.command"
  if PYTHONDONTWRITEBYTECODE=1 python3 ".github/scripts/$name" > "$evidence/$name.log" 2>&1; then rc=0; else rc=$?; fi
  printf '%s=%s\n' "$name" "$rc" | tee -a "$evidence/contract-exits.txt"
  if [[ $rc != 0 ]]; then result=1; fi
done
if PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/refresh_ci_impact_inventory.py --check > "$evidence/inventory-check.log" 2>&1; then rc=0; else rc=$?; fi
printf 'inventory-check=%s\n' "$rc" | tee -a "$evidence/contract-exits.txt"
if [[ $rc != 0 ]]; then result=1; fi
exit "$result"
