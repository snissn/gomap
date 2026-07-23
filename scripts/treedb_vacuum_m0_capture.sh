#!/usr/bin/env bash
set -euo pipefail

# Capture the public vacuum status, offline shrink ceiling, and ten
# interleaved legacy/public samples across the M0-to-M1 transition.
ROOT=$(git rev-parse --show-toplevel)
RUN_DIR=${RUN_DIR:-"$ROOT/artifacts/treedb-vacuum-m0/$(date +%Y%m%d_%H%M%S)"}
SHA=$(git rev-parse HEAD)
mkdir -p "$RUN_DIR"
RUN_DIR=$(cd "$RUN_DIR" && pwd -P)
mkdir -p "$RUN_DIR/raw"

default_cpu_set() {
  local allowed range start end cpu
  local -a selected=()
  allowed=$(awk '/^Cpus_allowed_list:/ { print $2; exit }' /proc/self/status)
  IFS=',' read -ra ranges <<<"$allowed"
  for range in "${ranges[@]}"; do
    if [[ "$range" == *-* ]]; then
      start=${range%-*}
      end=${range#*-}
    else
      start=$range
      end=$range
    fi
    for ((cpu = start; cpu <= end && ${#selected[@]} < 2; cpu++)); do
      selected+=("$cpu")
    done
    ((${#selected[@]} == 2)) && break
  done
  ((${#selected[@]} > 0)) || return 1
  (IFS=,; printf '%s\n' "${selected[*]}")
}

export CPU_SET=${CPU_SET:-$(default_cpu_set)}
export GOMAXPROCS=${GOMAXPROCS:-2}
export GOMEMLIMIT=${GOMEMLIMIT:-8GiB}
DIRTY_STATE=clean
if [[ -n $(git status --porcelain) ]]; then
  DIRTY_STATE=dirty
fi
read -r DEVICE FILESYSTEM < <(df -PT "$RUN_DIR" | awk 'NR == 2 {print $1, $2}')

LEGACY_BENCH='^BenchmarkVacuumIndexOnlineCollectionForegroundChurn/bytes_64x$'
PUBLIC_BENCH='^BenchmarkPL06ExternalVacuumCollectionForegroundChurn/bytes_64x$'
LEGACY_COMMAND="GOWORK=off GOMAXPROCS=$GOMAXPROCS GOMEMLIMIT=$GOMEMLIMIT taskset -c $CPU_SET go test ./TreeDB/db -run '^$' -bench '$LEGACY_BENCH' -benchtime=1x -count=1 -benchmem"
PUBLIC_COMMAND="GOWORK=off GOMAXPROCS=$GOMAXPROCS GOMEMLIMIT=$GOMEMLIMIT taskset -c $CPU_SET go test ./TreeDB/db -run '^$' -bench '$PUBLIC_BENCH' -benchtime=1x -count=1 -benchmem"
printf '%s\n%s\n' "$LEGACY_COMMAND" "$PUBLIC_COMMAND" >"$RUN_DIR/commands.txt"

run_go_test() {
  GOWORK=off GOMAXPROCS="$GOMAXPROCS" GOMEMLIMIT="$GOMEMLIMIT" \
    taskset -c "$CPU_SET" go test ./TreeDB/db "$@"
}

capture_fixture() {
  TREEDB_VACUUM_M0_ARTIFACT="$RUN_DIR/fixture.json" \
  TREEDB_VACUUM_M0_COMMAND="$*" \
  TREEDB_VACUUM_M0_GIT_SHA="$SHA" \
  TREEDB_VACUUM_M0_DIRTY_STATE="$DIRTY_STATE" \
  TREEDB_VACUUM_M0_FILESYSTEM="$FILESYSTEM" \
  TREEDB_VACUUM_M0_DEVICE="$DEVICE" \
  TREEDB_VACUUM_M0_TIMING_BOUNDARY="one fixed-work vacuum operation; setup excluded" \
  run_go_test -run '^TestVacuumM0WriteArtifact$' -count=1 -v
}

cd "$ROOT"
run_go_test -run '^TestVacuumM0FixtureDeterministicDebtAndOfflineCeiling$' -count=1
capture_fixture "scripts/treedb_vacuum_m0_capture.sh"

for sample in $(seq 1 10); do
  run_go_test -run '^$' -bench "$LEGACY_BENCH" -benchtime=1x -count=1 -benchmem >"$RUN_DIR/raw/legacy-${sample}.txt"
  run_go_test -run '^$' -bench "$PUBLIC_BENCH" -benchtime=1x -count=1 -benchmem >"$RUN_DIR/raw/public-${sample}.txt"
done

cat "$RUN_DIR"/raw/legacy-*.txt >"$RUN_DIR/legacy-benchstat-input.txt"
if command -v benchstat >/dev/null 2>&1; then
  benchstat "$RUN_DIR/legacy-benchstat-input.txt" >"$RUN_DIR/legacy-benchstat.txt"
fi

python3 "$ROOT/scripts/treedb_vacuum_m0_summarize_test.py"
python3 "$ROOT/scripts/treedb_vacuum_m0_summarize.py" --repo-root "$ROOT" --run-dir "$RUN_DIR"

echo "M0 artifacts written to $RUN_DIR"
