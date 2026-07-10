#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BASELINE_HASH="${BASELINE_HASH:?BASELINE_HASH is required}"
CANDIDATE_HASH="${CANDIDATE_HASH:-HEAD}"
RUNS="${RUNS:-7}"
BENCHTIME="${BENCHTIME:-2s}"
CPUSET="${CPUSET:-0}"
MAX_REGRESSION_PERCENT="${MAX_REGRESSION_PERCENT:-5}"
SCRIPT_GOWORK="${SCRIPT_GOWORK:-off}"
OUT_DIR="${OUT_DIR:-$ROOT/artifacts/mvcc_raw_path_gate}"
BENCH_REGEX='^(BenchmarkGetVersioned|BenchmarkConditionalTxnBaselineBatchWrite)$'

if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "RUNS must be a positive integer" >&2
  exit 2
fi
if ! [[ "$BENCHTIME" =~ ^[1-9][0-9]*(ms|s|x)$ ]]; then
  echo "BENCHTIME must be a positive Go benchmark duration or iteration count" >&2
  exit 2
fi
for command in git go lscpu ps python3 realpath taskset sha256sum; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 2
  fi
done

OUT_DIR=$(realpath -m "$OUT_DIR")
if [[ -z "$OUT_DIR" || "$OUT_DIR" == "/" || "$OUT_DIR" == "$ROOT" ]]; then
  echo "refusing unsafe OUT_DIR: $OUT_DIR" >&2
  exit 2
fi

if ! git cat-file -e "${BASELINE_HASH}^{commit}" >/dev/null 2>&1; then
  git fetch --no-tags origin "$BASELINE_HASH"
fi
BASELINE_SHA=$(git rev-parse "${BASELINE_HASH}^{commit}")
CANDIDATE_SHA=$(git rev-parse "${CANDIDATE_HASH}^{commit}")
CHECKED_OUT_SHA=$(git rev-parse "HEAD^{commit}")
if [[ "$CHECKED_OUT_SHA" != "$CANDIDATE_SHA" ]]; then
  echo "candidate mismatch: checkout=$CHECKED_OUT_SHA requested=$CANDIDATE_SHA" >&2
  exit 2
fi
if [[ "$BASELINE_SHA" == "$CANDIDATE_SHA" ]]; then
  echo "baseline and candidate resolve to the same commit" >&2
  exit 2
fi

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
TMP_ROOT=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/mvcc-raw-path-gate.XXXXXX")
BASELINE_WT="$TMP_ROOT/baseline"
cleanup() {
  git worktree remove --force "$BASELINE_WT" >/dev/null 2>&1 || true
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT
git worktree add --detach "$BASELINE_WT" "$BASELINE_SHA" >/dev/null

ENVIRONMENT="$OUT_DIR/environment.txt"
PROCESSES="$OUT_DIR/processes.txt"
BASELINE_LOG="$OUT_DIR/baseline.txt"
CANDIDATE_LOG="$OUT_DIR/candidate.txt"
SUMMARY_JSON="$OUT_DIR/summary.json"
SUMMARY_MD="$OUT_DIR/summary.md"
BASELINE_BIN="$TMP_ROOT/baseline.test"
CANDIDATE_BIN="$TMP_ROOT/candidate.test"

{
  echo "baseline_sha=$BASELINE_SHA"
  echo "candidate_sha=$CANDIDATE_SHA"
  echo "runs=$RUNS"
  echo "benchtime=$BENCHTIME"
  echo "cpuset=$CPUSET"
  echo "gomaxprocs=1"
  echo "max_regression_percent=$MAX_REGRESSION_PERCENT"
  echo "github_runner_image=${ImageOS:-unknown} ${ImageVersion:-unknown}"
  echo "runner_arch=${RUNNER_ARCH:-unknown}"
  echo "runner_os=${RUNNER_OS:-unknown}"
  go version
  uname -a
  lscpu
} >"$ENVIRONMENT"

env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false \
  -o "$CANDIDATE_BIN" ./TreeDB/db
(
  cd "$BASELINE_WT"
  env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false \
    -o "$BASELINE_BIN" ./TreeDB/db
)
(
  cd "$TMP_ROOT"
  sha256sum baseline.test candidate.test
) | tee "$OUT_DIR/binary-sha256.txt"

: >"$BASELINE_LOG"
: >"$CANDIDATE_LOG"
: >"$PROCESSES"
run_sample() {
  local revision="$1"
  local sample="$2"
  local binary="$3"
  local output="$4"
  {
    echo "--- before revision=$revision sample=$sample at $(date -Iseconds) ---"
    ps -eo pid,ppid,etime,%cpu,cmd --sort=-%cpu | head -20 || true
  } >>"$PROCESSES"
  taskset -c "$CPUSET" env GOMAXPROCS=1 "$binary" \
    -test.run '^$' \
    -test.bench "$BENCH_REGEX" \
    -test.benchtime="$BENCHTIME" \
    -test.count=1 >>"$output"
}

for ((sample = 1; sample <= RUNS; sample++)); do
  if ((sample % 2 == 1)); then
    run_sample baseline "$sample" "$BASELINE_BIN" "$BASELINE_LOG"
    run_sample candidate "$sample" "$CANDIDATE_BIN" "$CANDIDATE_LOG"
  else
    run_sample candidate "$sample" "$CANDIDATE_BIN" "$CANDIDATE_LOG"
    run_sample baseline "$sample" "$BASELINE_BIN" "$BASELINE_LOG"
  fi
done

python3 .github/scripts/check_mvcc_raw_path_gate.py \
  --baseline "$BASELINE_LOG" \
  --candidate "$CANDIDATE_LOG" \
  --baseline-sha "$BASELINE_SHA" \
  --candidate-sha "$CANDIDATE_SHA" \
  --expected-samples "$RUNS" \
  --max-regression-percent "$MAX_REGRESSION_PERCENT" \
  --json-output "$SUMMARY_JSON" \
  --markdown-output "$SUMMARY_MD"

echo "mvcc raw-path gate artifacts: $OUT_DIR"
