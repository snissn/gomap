#!/usr/bin/env bash
set -euo pipefail

# Capture the public vacuum status, offline shrink ceiling, and ten
# interleaved production/public samples with the coordinator enabled.
ROOT=$(git rev-parse --show-toplevel)
RUN_DIR=${RUN_DIR:-"$ROOT/artifacts/treedb-vacuum-m0/$(date +%Y%m%d_%H%M%S)"}
SHA=$(git rev-parse HEAD)
export M0_SOURCE_SHA="$SHA"
# Atomically claim a new packet directory before creating or replacing any
# packet content. Existing directories (even empty) belong to their caller.
mkdir -p "$(dirname "$RUN_DIR")"
if ! mkdir "$RUN_DIR"; then
  echo 'capture directory already exists; use a fresh RUN_DIR' >&2
  exit 2
fi
RUN_DIR=$(cd "$RUN_DIR" && pwd -P)
mkdir "$RUN_DIR/raw"

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

# The EXIT receipt runs on setup, admission, measurement and parser failure.
# It never replaces the failing command's status with a telemetry success.
export M0_QUALIFICATION_PROFILE=${M0_QUALIFICATION_PROFILE:-controlled-performance}
export M0_LAST_PHASE=initializing
terminal_receipt() {
  local status=$?
  trap - EXIT
  python3 "$ROOT/scripts/treedb_vacuum_m0_environment.py" --run-dir "$RUN_DIR" \
    --phase terminal --exit-code "$status" || { ((status != 0)) || status=2; }
  exit "$status"
}
trap terminal_receipt EXIT

case "$M0_QUALIFICATION_PROFILE:${M0_CONTROLLED:-0}" in
  hosted-semantic:0|controlled-performance:1) ;;
  *) echo 'qualification profile and explicit custody mode disagree' >&2; exit 2 ;;
esac

# Controlled placement must be explicitly configured; defaulting here would
# conceal a missing administrator topology/lease declaration.
if [[ ${M0_CONTROLLED:-0} == 1 && ( -z ${CPU_SET:-} || -z ${GOMAXPROCS:-} || -z ${M0_LEASE_PATH:-} ) ]]; then
  echo 'controlled mode requires explicit CPU_SET, GOMAXPROCS and M0_LEASE_PATH' >&2
  exit 2
fi
if [[ ${M0_CONTROLLED:-0} == 1 && ! ${M0_BASELINE_SHA:-} =~ ^[0-9a-f]{40}$ ]]; then
  echo 'controlled paired proposal requires explicitly approved full M0_BASELINE_SHA' >&2
  exit 2
fi
export CPU_SET=${CPU_SET:-$(default_cpu_set)}
export GOMAXPROCS=${GOMAXPROCS:-2}
export GOMEMLIMIT=${GOMEMLIMIT:-8GiB}
DIRTY_STATE=clean
if [[ -n $(git status --porcelain) ]]; then
  DIRTY_STATE=dirty
fi
read -r DEVICE FILESYSTEM < <(df -PT "$RUN_DIR" | awk 'NR == 2 {print $1, $2}')

PRODUCTION_BENCH='^BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn/bytes_64x$'
PUBLIC_BENCH='^BenchmarkPL06ExternalVacuumCollectionForegroundChurn/bytes_64x$'
PRODUCTION_COMMAND="GOWORK=off GOMAXPROCS=$GOMAXPROCS GOMEMLIMIT=$GOMEMLIMIT taskset -c $CPU_SET go test ./TreeDB/db -run '^$' -bench '$PRODUCTION_BENCH' -benchtime=1x -count=1 -benchmem"
PUBLIC_COMMAND="GOWORK=off GOMAXPROCS=$GOMAXPROCS GOMEMLIMIT=$GOMEMLIMIT taskset -c $CPU_SET go test ./TreeDB/db -run '^$' -bench '$PUBLIC_BENCH' -benchtime=1x -count=1 -benchmem"
printf '%s\n%s\n' "$PRODUCTION_COMMAND" "$PUBLIC_COMMAND" >"$RUN_DIR/commands.txt"

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
export M0_LAST_PHASE=admission
python3 "$ROOT/scripts/treedb_vacuum_m0_environment.py" --run-dir "$RUN_DIR" --phase admission
GOWORK=off go env -json GOVERSION GOROOT GOTOOLDIR GOOS GOARCH GOFLAGS GOTOOLCHAIN >"$RUN_DIR/go-environment.json"
go tool compile -V >"$RUN_DIR/compiler-version.txt"
export M0_LAST_PHASE=fixture
run_go_test -run '^TestVacuumM0FixtureDeterministicDebtAndOfflineCeiling$' -count=1
capture_fixture "scripts/treedb_vacuum_m0_capture.sh"

if [[ ${M0_CONTROLLED:-0} == 1 ]]; then
  export M0_LAST_PHASE=baseline-build
  python3 "$ROOT/scripts/treedb_vacuum_m0_baseline.py" --repo-root "$ROOT" --run-dir "$RUN_DIR" --source "$M0_BASELINE_SHA"
  TREEDB_VACUUM_M0_ARTIFACT="$RUN_DIR/baseline-fixture.json" \
  TREEDB_VACUUM_M0_GIT_SHA="$M0_BASELINE_SHA" \
  TREEDB_VACUUM_M0_COMMAND="baseline-db.test -test.run '^TestVacuumM0WriteArtifact$' -test.count=1 -test.v" \
  TREEDB_VACUUM_M0_DIRTY_STATE=baseline-with-candidate-test-overlay \
  TREEDB_VACUUM_M0_FILESYSTEM="$FILESYSTEM" \
  TREEDB_VACUUM_M0_DEVICE="$DEVICE" \
  TREEDB_VACUUM_M0_TIMING_BOUNDARY="one fixed-work vacuum operation; setup excluded" \
  taskset -c "$CPU_SET" "$RUN_DIR/baseline-db.test" -test.run '^TestVacuumM0WriteArtifact$' -test.count=1 -test.v
  printf '%s\n' "baseline source=$M0_BASELINE_SHA; taskset -c $CPU_SET baseline-db.test -test.run '^$' -test.bench '$PRODUCTION_BENCH' -test.benchtime=1x -test.count=1 -test.benchmem" >>"$RUN_DIR/commands.txt"
fi

baseline_sample() {
  local sample=$1
  export M0_LAST_PHASE="baseline-$sample"
  python3 "$ROOT/scripts/treedb_vacuum_m0_environment.py" --run-dir "$RUN_DIR" --phase "baseline-$sample-before"
  taskset -c "$CPU_SET" "$RUN_DIR/baseline-db.test" -test.run '^$' -test.bench "$PRODUCTION_BENCH" -test.benchtime=1x -test.count=1 -test.benchmem >"$RUN_DIR/raw/baseline-${sample}.txt" 2>&1
  python3 "$ROOT/scripts/treedb_vacuum_m0_environment.py" --run-dir "$RUN_DIR" --phase "baseline-$sample-after"
}

for sample in $(seq 1 10); do
  if [[ ${M0_CONTROLLED:-0} == 1 ]] && ((sample % 2 == 1)); then baseline_sample "$sample"; fi
  export M0_LAST_PHASE="production-$sample"
  python3 "$ROOT/scripts/treedb_vacuum_m0_environment.py" --run-dir "$RUN_DIR" --phase "production-$sample-before"
  run_go_test -run '^$' -bench "$PRODUCTION_BENCH" -benchtime=1x -count=1 -benchmem >"$RUN_DIR/raw/production-${sample}.txt" 2>&1
  python3 "$ROOT/scripts/treedb_vacuum_m0_environment.py" --run-dir "$RUN_DIR" --phase "production-$sample-after"
  if [[ ${M0_CONTROLLED:-0} == 1 ]] && ((sample % 2 == 0)); then baseline_sample "$sample"; fi
  export M0_LAST_PHASE="public-$sample"
  python3 "$ROOT/scripts/treedb_vacuum_m0_environment.py" --run-dir "$RUN_DIR" --phase "public-$sample-before"
  run_go_test -run '^$' -bench "$PUBLIC_BENCH" -benchtime=1x -count=1 -benchmem >"$RUN_DIR/raw/public-${sample}.txt" 2>&1
  python3 "$ROOT/scripts/treedb_vacuum_m0_environment.py" --run-dir "$RUN_DIR" --phase "public-$sample-after"
done

cat "$RUN_DIR"/raw/production-*.txt >"$RUN_DIR/production-benchstat-input.txt"
if command -v benchstat >/dev/null 2>&1; then
  benchstat "$RUN_DIR/production-benchstat-input.txt" >"$RUN_DIR/production-benchstat.txt"
fi

ACCEPTANCE_CONTRACT=v3
if [[ ${M0_CONTROLLED:-0} == 1 ]]; then ACCEPTANCE_CONTRACT=v4-proposal; fi
export M0_LAST_PHASE=summary
python3 "$ROOT/scripts/treedb_vacuum_m0_summarize_test.py"
python3 "$ROOT/scripts/treedb_vacuum_m0_paired_test.py"
python3 "$ROOT/scripts/treedb_vacuum_m0_ci_test.py"
python3 "$ROOT/scripts/treedb_vacuum_m0_summarize.py" --repo-root "$ROOT" --run-dir "$RUN_DIR" --stability-arm production --acceptance-contract "$ACCEPTANCE_CONTRACT" --qualification-profile "${M0_QUALIFICATION_PROFILE:-controlled-performance}"

echo "M0 artifacts written to $RUN_DIR"
