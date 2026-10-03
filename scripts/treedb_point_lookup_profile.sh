#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"
RUN_DIR="${RUN_DIR:-/tmp/treedb_point_lookup_profile_$(date +%Y%m%d_%H%M%S)}"
BENCHTIME="${BENCHTIME:-200ms}"
mkdir -p "$RUN_DIR"
RUN_DIR=$(cd "$RUN_DIR" && pwd)
git rev-parse HEAD > "$RUN_DIR/source-head.txt"
git status --porcelain > "$RUN_DIR/source-status.txt"
git diff HEAD > "$RUN_DIR/source-diff.patch"
go version > "$RUN_DIR/go-version.txt"
printf 'GOWORK=off\nBENCHTIME=%s\nCOUNT=1\n' "$BENCHTIME" > "$RUN_DIR/settings.txt"

# Each benchmark family gets a fresh process; profiles include fixture setup.
capture() {
  local name=$1 package=$2 benchmark=$3
  GOWORK=off go test "$package" -run '^$' -bench "$benchmark" \
    -benchmem -benchtime "$BENCHTIME" -count=1 -timeout=2m \
    -cpuprofile "$RUN_DIR/${name}_cpu.pprof" \
    -memprofile "$RUN_DIR/${name}_allocs.pprof" \
    -o "$RUN_DIR/${name}.test" 2>&1 | tee "$RUN_DIR/${name}.txt"
  go tool pprof -top "$RUN_DIR/${name}.test" \
    "$RUN_DIR/${name}_cpu.pprof" > "$RUN_DIR/${name}_cpu_top.txt"
  go tool pprof -top -alloc_space "$RUN_DIR/${name}.test" \
    "$RUN_DIR/${name}_allocs.pprof" > "$RUN_DIR/${name}_allocs_top.txt"
}

capture node_metadata ./TreeDB/node '^BenchmarkLeafPointMetadata$'
capture node_search ./TreeDB/node '^BenchmarkLeafCommonPrefixSuffixSearch$'
capture tree_metadata ./TreeDB/tree '^BenchmarkPointValueMetadata$'
printf 'point lookup profiles: %s\n' "$RUN_DIR"
