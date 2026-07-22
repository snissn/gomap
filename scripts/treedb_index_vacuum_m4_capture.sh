#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP_ROOT=${TMPDIR:-/tmp}
RUN_DIR=${RUN_DIR:-}
COUNT=${COUNT:-10}
RUN_FULL_TESTS=${RUN_FULL_TESTS:-true}
RUN_RACE_TESTS=${RUN_RACE_TESTS:-true}
M0_PACKET_DIR=${M0_PACKET_DIR:-}

cd "$ROOT"
if [[ "$COUNT" != "10" ]]; then
  printf 'M4 certification requires exactly 10 M0 repetitions\n' >&2
  exit 1
fi
if [[ -n "$(git status --porcelain)" ]]; then
  printf 'refusing certification from a dirty worktree\n' >&2
  exit 1
fi

if [[ -z "$RUN_DIR" ]]; then
  RUN_DIR=$(mktemp -d "$TMP_ROOT/treedb_index_vacuum_m4_XXXXXX")
elif [[ -e "$RUN_DIR" ]]; then
  if [[ ! -d "$RUN_DIR" ]]; then
    printf 'RUN_DIR exists but is not a directory: %s\n' "$RUN_DIR" >&2
    exit 1
  fi
  if [[ -n "$(find "$RUN_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    printf 'refusing non-empty RUN_DIR: %s\n' "$RUN_DIR" >&2
    exit 1
  fi
else
  mkdir -p "$RUN_DIR"
fi
mkdir -p "$RUN_DIR"/{tests,benchmarks,profiles,m0,compact-storage}
RUN_DIR=$(cd "$RUN_DIR" && pwd -P)

for command in lscpu free findmnt; do
  if ! command -v "$command" >/dev/null 2>&1; then
    printf 'unsupported platform metadata command: %s\n' "$command" >&2
    exit 1
  fi
done
if ! cpu=$(lscpu | awk -F: '/Model name/{sub(/^[ \t]+/, "", $2); print $2; exit}'); then
  printf 'unsupported platform metadata: lscpu failed\n' >&2
  exit 1
fi
if ! memory=$(free -h | awk '/^Mem:/{print $2 " total, " $7 " available"}'); then
  printf 'unsupported platform metadata: free failed\n' >&2
  exit 1
fi
if ! filesystem=$(findmnt -no SOURCE,FSTYPE,OPTIONS --target "$TMP_ROOT"); then
  printf 'unsupported platform metadata: findmnt failed for %s\n' "$TMP_ROOT" >&2
  exit 1
fi
if [[ -z "$cpu" || -z "$memory" || -z "$filesystem" ]]; then
  printf 'unsupported platform metadata: cpu=%q memory=%q filesystem=%q\n' "$cpu" "$memory" "$filesystem" >&2
  exit 1
fi

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
  printf 'source_status_lines=0\n'
  printf 'go_version=%s\n' "$(go version)"
  printf 'kernel=%s\n' "$(uname -srvmo)"
  printf 'goos=%s\n' "$(go env GOOS)"
  printf 'goarch=%s\n' "$(go env GOARCH)"
  printf 'cpu=%s\n' "$cpu"
  printf 'memory=%s\n' "$memory"
  printf 'filesystem=%s\n' "$filesystem"
  printf 'count=%s\n' "$COUNT"
  printf 'run_full_tests=%s\n' "$RUN_FULL_TESTS"
  printf 'run_race_tests=%s\n' "$RUN_RACE_TESTS"
  printf 'm0_packet_dir=%s\n' "$M0_PACKET_DIR"
} >"$RUN_DIR/environment.txt"
: >"$RUN_DIR/commands.txt"

if rg -n 'deferred to #3681' TreeDB >"$RUN_DIR/deferred-3681.txt"; then
  printf 'stale deferred-to-3681 assertions remain\n' >&2
  exit 1
fi
rg -n 'ErrVacuumRecoverableRootSetRequired' TreeDB >"$RUN_DIR/recoverable-root-required-inventory.txt" || true

run_and_log matrix env GOWORK=off TMPDIR="$TMP_ROOT" \
  TREEDB_INDEX_VACUUM_M4_MATRIX_OUT="$RUN_DIR/tests/index_vacuum_m4_matrix.json" \
  go test ./TreeDB -run '^(TestIndexVacuumM4CertificationMatrix|TestIndexVacuumM4MatrixHarnessContract)$' \
  -count=1 -timeout 20m
run_and_log backend run_tree_test ./TreeDB/db -run 'TestVacuumM0ProductionOnlineVacuumIsSupported|TestVacuumIndexOnline|TestCapture.*(Blocks|Vacuum)' -count=1
run_and_log public run_tree_test ./TreeDB -run 'TestVacuumIndexOnline|TestPublicVacuum|TestCached.*Vacuum' -count=1
run_and_log background run_tree_test ./TreeDB -run 'TestBackgroundIndexVacuum' -count=1
run_and_log compact-storage run_tree_test ./TreeDB/... -run 'TestCompactStorage(IndexVacuum|Full|Exhaustive|Cached)' -count=1
run_and_log offline run_tree_test ./TreeDB/... -run 'TestVacuumIndexOffline' -count=1
run_and_log close-opt-in env GOWORK=off TMPDIR="$TMP_ROOT" TREEDB_CLOSE_VACUUM_INDEX_ONLINE=1 go test ./TreeDB \
  -run '^TestCloseOptInVacuumIndexOnlineShrinksAndReopens$' -v -count=1
if ! rg -q '^--- PASS: TestCloseOptInVacuumIndexOnlineShrinksAndReopens' "$RUN_DIR/tests/close-opt-in.txt"; then
  printf 'close opt-in certification test did not execute successfully\n' >&2
  exit 1
fi

if [[ "$RUN_FULL_TESTS" == "true" ]]; then
  run_and_log full-tree run_tree_test ./TreeDB/... -timeout 20m -count=1
fi
if [[ "$RUN_RACE_TESTS" == "true" ]]; then
  run_and_log race run_tree_test -race ./TreeDB ./TreeDB/db ./TreeDB/collections ./TreeDB/internal/dictdb ./TreeDB/internal/templatedb \
    -run 'Test.*Vacuum|TestCompactStorage|Test.*StableResource|Test.*Close' -timeout 30m -count=1
fi

env GOWORK=off TMPDIR="$TMP_ROOT" go test ./TreeDB/db -run '^$' \
  -bench 'BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn|BenchmarkPL06ExternalVacuumCollectionForegroundChurn' \
  -count="$COUNT" -benchtime=1x -benchmem | tee "$RUN_DIR/benchmarks/production.txt"

if [[ -n "$M0_PACKET_DIR" ]]; then
  M0_PACKET_DIR=$(cd "$M0_PACKET_DIR" && pwd -P)
  source_sha=$(git rev-parse HEAD)
  if ! jq -e --arg sha "$source_sha" --argjson count "$COUNT" '
    .schema_version == 1 and
    .environment.git_sha == $sha and
    .environment.dirty_state == "clean" and
    .environment.repetitions == $count and
    .gates == {
      "legacy_completed_without_abort": true,
      "legacy_cv_at_most_10_percent": true,
      "public_status_explicit": true
    }
  ' \
    "$M0_PACKET_DIR/results.json" >/dev/null; then
    printf 'reused M0 packet is not a complete, clean, exact-head all-gates-pass packet\n' >&2
    exit 1
  fi
  rm -rf "$RUN_DIR/m0"
  cp -a "$M0_PACKET_DIR" "$RUN_DIR/m0"
  printf 'reuse M0_PACKET_DIR=%q\n' "$M0_PACKET_DIR" >>"$RUN_DIR/commands.txt"
else
  RUN_DIR="$RUN_DIR/m0" COUNT="$COUNT" scripts/treedb_vacuum_m0_capture.sh
fi
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
