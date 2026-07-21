#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP_ROOT=${TMPDIR:-/tmp}
RUN_DIR=${RUN_DIR:-$(mktemp -d "$TMP_ROOT/treedb_index_vacuum_m4_XXXXXX")}
COUNT=${COUNT:-10}
RUN_FULL_TESTS=${RUN_FULL_TESTS:-true}
RUN_RACE_TESTS=${RUN_RACE_TESTS:-true}

mkdir -p "$RUN_DIR"/{tests,benchmarks,profiles,m0,compact-storage}
RUN_DIR=$(cd "$RUN_DIR" && pwd -P)
cd "$ROOT"

run_tree_test() {
  env GOWORK=off TMPDIR="$TMP_ROOT" go test "$@"
}

run_and_log() {
  local name=$1
  shift
  printf '%q ' "$@" >>"$RUN_DIR/commands.txt"
  printf '\n' >>"$RUN_DIR/commands.txt"
  "$@" 2>&1 | tee "$RUN_DIR/tests/$name.txt"
}

{
  printf 'source_sha=%s\n' "$(git rev-parse HEAD)"
  printf 'source_status_lines=%s\n' "$(git status --short | wc -l)"
  printf 'go_version=%s\n' "$(go version)"
  printf 'kernel=%s\n' "$(uname -srvmo)"
  printf 'goos=%s\n' "$(go env GOOS)"
  printf 'goarch=%s\n' "$(go env GOARCH)"
  printf 'cpu=%s\n' "$(lscpu | awk -F: '/Model name/{sub(/^[ \t]+/, "", $2); print $2; exit}')"
  printf 'memory=%s\n' "$(free -h | awk '/^Mem:/{print $2 " total, " $7 " available"}')"
  printf 'filesystem=%s\n' "$(findmnt -no SOURCE,FSTYPE,OPTIONS --target "$TMP_ROOT")"
  printf 'count=%s\n' "$COUNT"
  printf 'run_full_tests=%s\n' "$RUN_FULL_TESTS"
  printf 'run_race_tests=%s\n' "$RUN_RACE_TESTS"
} >"$RUN_DIR/environment.txt"
: >"$RUN_DIR/commands.txt"

if rg -n 'deferred to #3681' TreeDB >"$RUN_DIR/deferred-3681.txt"; then
  printf 'stale deferred-to-3681 assertions remain\n' >&2
  exit 1
fi
rg -n 'ErrVacuumRecoverableRootSetRequired' TreeDB >"$RUN_DIR/recoverable-root-required-inventory.txt" || true

run_and_log backend run_tree_test ./TreeDB/db -run 'TestVacuumM0ProductionOnlineVacuumIsSupported|TestVacuumIndexOnline_.*(Preserves|Reopen|Collection|Stable)|TestCapture.*BlocksVacuum' -count=1
run_and_log public run_tree_test ./TreeDB -run 'TestVacuumIndexOnline|TestPublicVacuum|TestCached.*Vacuum' -count=1
run_and_log background run_tree_test ./TreeDB -run 'TestBackgroundIndexVacuum' -count=1
run_and_log compact-storage run_tree_test ./TreeDB/... -run 'TestCompactStorage(IndexVacuum|Full|Cached)' -count=1
run_and_log offline run_tree_test ./TreeDB/... -run 'TestVacuumIndexOffline' -count=1
run_and_log close-opt-in env GOWORK=off TMPDIR="$TMP_ROOT" TREEDB_CLOSE_VACUUM_INDEX_ONLINE=1 go test ./TreeDB -run 'Test.*Close.*Vacuum|TestVacuum.*Close' -count=1

if [[ "$RUN_FULL_TESTS" == "true" ]]; then
  run_and_log full-tree run_tree_test ./TreeDB/... -count=1
fi
if [[ "$RUN_RACE_TESTS" == "true" ]]; then
  run_and_log race run_tree_test -race ./TreeDB ./TreeDB/db ./TreeDB/collections ./TreeDB/internal/dictdb ./TreeDB/internal/templatedb \
    -run 'Test.*Vacuum|TestCompactStorage|Test.*StableResource|Test.*Close' -count=1
fi

env GOWORK=off TMPDIR="$TMP_ROOT" go test ./TreeDB/db -run '^$' \
  -bench 'BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn|BenchmarkPL06ExternalVacuumCollectionForegroundChurn' \
  -count="$COUNT" -benchtime=1x -benchmem | tee "$RUN_DIR/benchmarks/production.txt"

RUN_DIR="$RUN_DIR/m0" COUNT="$COUNT" scripts/treedb_vacuum_m0_capture.sh
RUN_DIR="$RUN_DIR/compact-storage" COUNT="$COUNT" scripts/compact_storage_m0_profile.sh

PROFILE_BENCH='^BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn/bytes_64x$'
env GOWORK=off TMPDIR="$TMP_ROOT" go test ./TreeDB/db -run '^$' -bench "$PROFILE_BENCH" -benchtime=3x -count=1 \
  -cpuprofile "$RUN_DIR/profiles/cpu.pprof" >"$RUN_DIR/profiles/cpu_raw.txt"
env GOWORK=off TMPDIR="$TMP_ROOT" go test ./TreeDB/db -run '^$' -bench "$PROFILE_BENCH" -benchtime=1x -count=1 \
  -memprofile "$RUN_DIR/profiles/allocs.pprof" -memprofilerate=1 >"$RUN_DIR/profiles/allocs_raw.txt"
env GOWORK=off TMPDIR="$TMP_ROOT" go test ./TreeDB/db -run '^$' -bench "$PROFILE_BENCH" -benchtime=3x -count=1 \
  -mutexprofile "$RUN_DIR/profiles/mutex.pprof" >"$RUN_DIR/profiles/mutex_raw.txt"
env GOWORK=off TMPDIR="$TMP_ROOT" go test ./TreeDB/db -run '^$' -bench "$PROFILE_BENCH" -benchtime=3x -count=1 \
  -blockprofile "$RUN_DIR/profiles/block.pprof" >"$RUN_DIR/profiles/block_raw.txt"
env GOWORK=off TMPDIR="$TMP_ROOT" go test ./TreeDB/db -run '^$' -bench "$PROFILE_BENCH" -benchtime=1x -count=1 \
  -trace "$RUN_DIR/profiles/trace.out" >"$RUN_DIR/profiles/trace_raw.txt"

go tool pprof -top "$RUN_DIR/profiles/cpu.pprof" >"$RUN_DIR/profiles/cpu_top.txt"
go tool pprof -top -alloc_space "$RUN_DIR/profiles/allocs.pprof" >"$RUN_DIR/profiles/allocs_top.txt"
go tool pprof -top "$RUN_DIR/profiles/mutex.pprof" >"$RUN_DIR/profiles/mutex_top.txt"
go tool pprof -top "$RUN_DIR/profiles/block.pprof" >"$RUN_DIR/profiles/block_top.txt"

printf 'complete\n' >"$RUN_DIR/COMPLETE"
printf '%s\n' "$RUN_DIR"
