#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BASELINE_HASH="${BASELINE_HASH:?BASELINE_HASH is required}"
CANDIDATE_HASH="${CANDIDATE_HASH:-HEAD}"
RUNS="${RUNS:-8}"
BENCHTIME="${BENCHTIME:-2s}"
PROFILE_BENCHTIME="${PROFILE_BENCHTIME:-1s}"
CPUSET="${CPUSET:-0}"
MAX_REGRESSION_PERCENT="${MAX_REGRESSION_PERCENT:-5}"
MAX_BYTES_REGRESSION_PERCENT="${MAX_BYTES_REGRESSION_PERCENT:-1}"
MAX_BYTES_REGRESSION_ABSOLUTE="${MAX_BYTES_REGRESSION_ABSOLUTE:-64}"
MAX_ADAPTER_RATIO="${MAX_ADAPTER_RATIO:-2}"
SCRIPT_GOWORK="${SCRIPT_GOWORK:-off}"
OUT_DIR="${OUT_DIR:-$ROOT/artifacts/mvcc_adapter_overhead_gate}"

COMMIT_REGEX='^BenchmarkCommitAt/(DirectTreeDB|MVCC)/1$'
GET_REGEX='^BenchmarkGetAt/(DirectSeek|MVCC)/64$'
ITER_REGEX='^BenchmarkVersionIteration/(Physical|MVCC)/keys=64/depth=32/reverse=false$'

if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "RUNS must be a positive integer" >&2
  exit 2
fi
if ((RUNS % 2 != 0)); then
  echo "RUNS must be even to balance AB/BA sample order" >&2
  exit 2
fi
for value in "$BENCHTIME" "$PROFILE_BENCHTIME"; do
  if ! [[ "$value" =~ ^[1-9][0-9]*(ms|s|x)$ ]]; then
    echo "benchmark durations must be positive Go durations or iteration counts" >&2
    exit 2
  fi
done
for command in git go lscpu ps python3 realpath taskset sha256sum; do
  command -v "$command" >/dev/null 2>&1 || { echo "missing required command: $command" >&2; exit 2; }
done

OUT_DIR=$(realpath -m "$OUT_DIR")
if [[ -z "$OUT_DIR" || "$OUT_DIR" == "/" || "$OUT_DIR" == "$ROOT" ]]; then
  echo "refusing unsafe OUT_DIR: $OUT_DIR" >&2
  exit 2
fi
git cat-file -e "${BASELINE_HASH}^{commit}" >/dev/null 2>&1 || git fetch --no-tags origin "$BASELINE_HASH"
BASELINE_SHA=$(git rev-parse "${BASELINE_HASH}^{commit}")
CANDIDATE_SHA=$(git rev-parse "${CANDIDATE_HASH}^{commit}")
"$ROOT/scripts/mvcc_candidate_checkout_guard.sh" "$ROOT" "$CANDIDATE_SHA"
if [[ "$BASELINE_SHA" == "$CANDIDATE_SHA" ]]; then
  echo "baseline and candidate resolve to the same commit" >&2
  exit 2
fi

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR/profiles"
TMP_ROOT=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/mvcc-adapter-gate.XXXXXX")
BASELINE_WT="$TMP_ROOT/baseline"
cleanup() {
  git worktree remove --force "$BASELINE_WT" >/dev/null 2>&1 || true
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT
git worktree add --detach "$BASELINE_WT" "$BASELINE_SHA" >/dev/null

BASELINE_BIN="$TMP_ROOT/baseline-mvcc.test"
CANDIDATE_BIN="$TMP_ROOT/candidate-mvcc.test"
env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false -o "$CANDIDATE_BIN" ./TreeDB/mvcc
(
  cd "$BASELINE_WT"
  env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false -o "$BASELINE_BIN" ./TreeDB/mvcc
)
sha256sum "$BASELINE_BIN" "$CANDIDATE_BIN" >"$OUT_DIR/binary-sha256.txt"

{
  echo "baseline_sha=$BASELINE_SHA"
  echo "candidate_sha=$CANDIDATE_SHA"
  echo "runs=$RUNS"
  echo "benchtime=$BENCHTIME"
  echo "profile_benchtime=$PROFILE_BENCHTIME"
  echo "cpuset=$CPUSET"
  echo "gomaxprocs=1"
  go version
  go env GOOS GOARCH GOAMD64 GOROOT GOPATH
  uname -a
  lscpu
} >"$OUT_DIR/environment.txt"
: >"$OUT_DIR/baseline.txt"
: >"$OUT_DIR/candidate.txt"
: >"$OUT_DIR/processes.txt"

run_group() {
  local revision="$1" sample="$2" binary="$3" regex="$4" output="$5"
  echo "--- revision=$revision sample=$sample regex=$regex ---" >>"$output"
  {
    echo "--- before revision=$revision sample=$sample regex=$regex at $(date -Iseconds) ---"
    ps -eo pid,ppid,etime,%cpu,cmd --sort=-%cpu | head -20 || true
  } >>"$OUT_DIR/processes.txt"
  taskset -c "$CPUSET" env GOMAXPROCS=1 "$binary" -test.run '^$' \
    -test.bench "$regex" -test.benchmem -test.benchtime="$BENCHTIME" -test.count=1 >>"$output"
}
run_pair() {
  local sample="$1" baseline_binary="$2" candidate_binary="$3" regex="$4"
  if ((sample % 2 == 1)); then
    run_group baseline "$sample" "$baseline_binary" "$regex" "$OUT_DIR/baseline.txt"
    run_group candidate "$sample" "$candidate_binary" "$regex" "$OUT_DIR/candidate.txt"
  else
    run_group candidate "$sample" "$candidate_binary" "$regex" "$OUT_DIR/candidate.txt"
    run_group baseline "$sample" "$baseline_binary" "$regex" "$OUT_DIR/baseline.txt"
  fi
}
for ((sample = 1; sample <= RUNS; sample++)); do
  run_pair "$sample" "$BASELINE_BIN" "$CANDIDATE_BIN" "$COMMIT_REGEX"
  run_pair "$sample" "$BASELINE_BIN" "$CANDIDATE_BIN" "$GET_REGEX"
  run_pair "$sample" "$BASELINE_BIN" "$CANDIDATE_BIN" "$ITER_REGEX"
done

profile_mvcc() {
  local label="$1" regex="$2"
  taskset -c "$CPUSET" env GOMAXPROCS=1 "$CANDIDATE_BIN" -test.run '^$' \
    -test.bench "$regex" -test.benchtime="$PROFILE_BENCHTIME" -test.count=1 \
    -test.cpuprofile="$OUT_DIR/profiles/$label.pprof" >/dev/null
  go tool pprof -top -nodecount=30 "$CANDIDATE_BIN" "$OUT_DIR/profiles/$label.pprof" \
    >"$OUT_DIR/profiles/${label}_top.txt"
}
# Profiles are intentionally captured before evaluation so a >2x failure always
# includes actionable evidence rather than only a threshold message.
profile_mvcc commit '^BenchmarkCommitAt/MVCC/1$'
profile_mvcc get '^BenchmarkGetAt/MVCC/64$'
profile_mvcc iteration '^BenchmarkVersionIteration/MVCC/keys=64/depth=32/reverse=false$'

python3 .github/scripts/check_mvcc_adapter_overhead_gate.py \
  --baseline "$OUT_DIR/baseline.txt" --candidate "$OUT_DIR/candidate.txt" \
  --baseline-sha "$BASELINE_SHA" --candidate-sha "$CANDIDATE_SHA" \
  --expected-samples "$RUNS" --max-regression-percent "$MAX_REGRESSION_PERCENT" \
  --max-bytes-regression-percent "$MAX_BYTES_REGRESSION_PERCENT" \
  --max-bytes-regression-absolute "$MAX_BYTES_REGRESSION_ABSOLUTE" \
  --max-adapter-ratio "$MAX_ADAPTER_RATIO" \
  --json-output "$OUT_DIR/summary.json" --markdown-output "$OUT_DIR/summary.md"

echo "mvcc adapter-overhead gate artifacts: $OUT_DIR"
