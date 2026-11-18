#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
FIXTURE=${TREEDB_LEAFGEN_PLAN_FIXTURE:-${1:?usage: leafgen_plan_profile.sh <application.db-or-maindb> [output-dir]}}
COUNT=${LEAFGEN_PLAN_PROFILE_COUNT:-3}

fail() {
  printf '%s\n' "$*" >&2
  exit 1
}

FIXTURE=$(cd -- "$FIXTURE" && pwd -P)
[[ -f "$FIXTURE/index.db" || -f "$FIXTURE/maindb/index.db" ]] || fail "missing index.db in saved store: $FIXTURE"
MAIN_DIR=$FIXTURE
if [[ ! -f "$FIXTURE/index.db" ]]; then
  MAIN_DIR=$(cd -- "$FIXTURE/maindb" && pwd -P)
fi
[[ "$COUNT" =~ ^[1-9][0-9]*$ ]] || fail 'LEAFGEN_PLAN_PROFILE_COUNT must be a positive integer'

outside_fixture() {
  case "$1/" in
    "${FIXTURE%/}/"*|"${MAIN_DIR%/}/"*) fail "output must be outside the saved store: $1" ;;
  esac
}

if [[ -n ${2:-} ]]; then
  if [[ -d "$2" ]]; then
    OUT=$(cd -- "$2" && pwd -P)
    outside_fixture "$OUT"
    (shopt -s dotglob nullglob; set -- "$OUT"/*; [[ $# -eq 0 ]]) || fail "output directory is not empty: $OUT"
  else
    PARENT=$(cd -- "$(dirname -- "$2")" && pwd -P)
    OUT="$PARENT/$(basename -- "$2")"
    outside_fixture "$OUT"
    mkdir -- "$OUT"
  fi
else
  PARENT=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
  outside_fixture "$PARENT"
  OUT=$(mktemp -d "$PARENT/leafgen_plan_profile.XXXXXXXX")
fi

cd "$ROOT"
BIN="$OUT/treedb_db.test"
GOWORK=off go test -c -o "$BIN" ./TreeDB/db

profile_case() {
  local label=$1 benchmark=$2
  local rows="$OUT/bench_${label}.txt" cpu="$OUT/cpu_${label}.pprof" allocs="$OUT/allocs_${label}.pprof"
  TREEDB_LEAFGEN_PLAN_FIXTURE="$FIXTURE" "$BIN" \
    -test.run '^$' -test.bench "^${benchmark}$" -test.benchtime=1x -test.count "$COUNT" -test.benchmem \
    -test.cpuprofile "$cpu" -test.memprofile "$allocs" | tee "$rows"
  awk -v name="$benchmark" -v count="$COUNT" '
    ($1 == name || $1 ~ ("^" name "-[0-9]+$")) && $2 == 1 && $4 == "ns/op" { rows++ }
    END { exit rows != count }
  ' "$rows" || fail "expected $COUNT benchmark rows for $benchmark in $rows"
  go tool pprof -top "$BIN" "$cpu" > "$OUT/cpu_${label}_top.txt"
  go tool pprof -top -alloc_space "$BIN" "$allocs" > "$OUT/alloc_space_${label}_top.txt"
  go tool pprof -top -alloc_objects "$BIN" "$allocs" > "$OUT/alloc_objects_${label}_top.txt"
}

profile_case reopen BenchmarkLeafGenerationPlanRetainedSourceReopen_SavedHome
profile_case warm BenchmarkLeafGenerationPlanCached_SavedHome

echo "leafgen plan profile complete"
echo "  fixture: $FIXTURE"
echo "  results and profiles: $OUT"
