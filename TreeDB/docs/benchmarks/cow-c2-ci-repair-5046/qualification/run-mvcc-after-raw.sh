#!/bin/bash
set -uo pipefail
export PYTHONDONTWRITEBYTECODE=1
out=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/pr5069-integrated-974d
source=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-pr5069-integrated-974d/source
run(){ name=$1; shift; "$@" > "$out/$name.stdout" 2> "$out/$name.stderr"; status=$?; printf '%s\n' "$status" > "$out/$name.exit"; return "$status"; }
run raw-analysis python3 "$out/analyze.py" /mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/cost-integrated-974d || exit $?
run mvcc-driver python3 "$out/collect_mvcc.py" --source "$source" --identity "$out/source-identity.json" --out /mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/mvcc-integrated-974d || exit $?
run mvcc-analysis python3 "$out/analyze_mvcc.py" /mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/mvcc-integrated-974d || exit $?
