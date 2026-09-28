#!/usr/bin/env bash
set -euo pipefail

# Run each document snapshot evidence leaf in a fresh Go test process. CPU and
# heap profiles cover the whole process, including fixture construction; the
# benchmark's DOCUMENT_SNAPSHOT_EVIDENCE record identifies its measured phase.
if [[ $# -ne 1 ]]; then
  echo "usage: $0 OUTPUT_DIRECTORY" >&2
  exit 2
fi
repo=$(cd "$(dirname "$0")/.." && pwd)
if [[ -n $(git -C "$repo" status --porcelain --untracked-files=all) ]]; then
  echo "document snapshot capture requires a clean source checkout" >&2
  exit 1
fi
out=$1
mkdir -p "$out"
out=$(cd "$out" && pwd)
cd "$repo"
git rev-parse HEAD > "$out/source-head.txt"
go version > "$out/go-version.txt"

run_leaf() {
  local name=$1 regex=$2
  GOWORK=off go test -json ./TreeDB/internal/raftfsm \
    -run '^$' -bench "$regex" -benchtime=1x -count=1 -benchmem \
    -timeout "${TIMEOUT:-30m}" \
    -cpuprofile "$out/cpu_${name}.pprof" \
    -memprofile "$out/heap_${name}.pprof" \
    > "$out/${name}.json"
  python3 - "$out/${name}.json" <<'PY'
import json
import sys

events = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8")]
output = "".join(event.get("Output", "") for event in events)
if "DOCUMENT_SNAPSHOT_EVIDENCE " not in output or "BenchmarkDocumentSnapshot" not in output:
    raise SystemExit(f"missing benchmark/evidence output in {sys.argv[1]}")
PY
}

for rows in 1000 10000 100000; do
  run_leaf "growth_${rows}" "^BenchmarkDocumentSnapshotGrowthV1/rows=${rows}$"
done
for snapshot in false true; do
  run_leaf "foreground_${snapshot}" "^BenchmarkDocumentSnapshotForegroundV1/snapshot=${snapshot}$"
done
