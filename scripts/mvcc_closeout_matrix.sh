#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

CANDIDATE_HASH="${CANDIDATE_HASH:-HEAD}"
RUNS="${RUNS:-5}"
BENCHTIME="${BENCHTIME:-750ms}"
CPUSET="${CPUSET:-0}"
SCRIPT_GOWORK="${SCRIPT_GOWORK:-off}"
OUT_DIR="${OUT_DIR:-$ROOT/artifacts/mvcc_closeout_matrix}"

if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "RUNS must be a positive integer" >&2
  exit 2
fi
if ! [[ "$BENCHTIME" =~ ^[1-9][0-9]*(ms|s|x)$ ]]; then
  echo "BENCHTIME must be a positive Go benchmark duration or iteration count" >&2
  exit 2
fi
for command in git go lscpu python3 realpath taskset sha256sum; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 2
  fi
done
if [[ ! -x /usr/bin/time ]]; then
  echo "missing required command: /usr/bin/time" >&2
  exit 2
fi

OUT_DIR=$(realpath -m "$OUT_DIR")
if [[ -z "$OUT_DIR" || "$OUT_DIR" == "/" || "$OUT_DIR" == "$ROOT" ]]; then
  echo "refusing unsafe OUT_DIR: $OUT_DIR" >&2
  exit 2
fi
CANDIDATE_SHA=$(git rev-parse "${CANDIDATE_HASH}^{commit}")
"$ROOT/scripts/mvcc_candidate_checkout_guard.sh" "$ROOT" "$CANDIDATE_SHA"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
TMP_ROOT=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/mvcc-closeout.XXXXXX")
cleanup() {
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT
BENCH_BIN="$TMP_ROOT/mvcc.test"

{
  echo "candidate_sha=$CANDIDATE_SHA"
  echo "runs=$RUNS"
  echo "benchtime=$BENCHTIME"
  echo "cpuset=$CPUSET"
  echo "gomaxprocs=1"
  echo "github_runner_image=${ImageOS:-unknown} ${ImageVersion:-unknown}"
  echo "runner_arch=${RUNNER_ARCH:-unknown}"
  echo "runner_os=${RUNNER_OS:-unknown}"
  go version
  go env GOOS GOARCH GOAMD64 GOROOT GOPATH
  uname -a
  lscpu
} >"$OUT_DIR/environment.txt"

env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false -o "$BENCH_BIN" ./TreeDB/mvcc
sha256sum "$BENCH_BIN" >"$OUT_DIR/binary-sha256.txt"
: >"$OUT_DIR/bench.txt"
: >"$OUT_DIR/resources.txt"

run_group() {
  local sample="$1"
  local group="$2"
  local regex="$3"
  local benchtime="$4"
  echo "--- sample=$sample group=$group benchtime=$benchtime ---" >>"$OUT_DIR/bench.txt"
  echo "--- sample=$sample group=$group benchtime=$benchtime ---" >>"$OUT_DIR/resources.txt"
  /usr/bin/time -v -a -o "$OUT_DIR/resources.txt" \
    taskset -c "$CPUSET" env GOMAXPROCS=1 "$BENCH_BIN" \
      -test.run '^$' \
      -test.bench "$regex" \
      -test.benchmem \
      -test.benchtime="$benchtime" \
      -test.count=1 >>"$OUT_DIR/bench.txt"
}

for ((sample = 1; sample <= RUNS; sample++)); do
  run_group "$sample" regular '^BenchmarkDgraphMVCCCloseout/(CommitAt|GetAt|AllVersions)/' "$BENCHTIME"
  # Prune requires a fresh populated database per operation. Keep exactly one
  # fixture per external sample instead of allowing Go's duration calibration
  # to multiply setup databases and distort process RSS.
  run_group "$sample" prune '^BenchmarkDgraphMVCCCloseout/Prune/' '1x'
done

taskset -c "$CPUSET" env GOMAXPROCS=1 "$BENCH_BIN" \
  -test.run '^$' \
  -test.bench '^BenchmarkDgraphMVCCCloseout$' \
  -test.benchtime=1x \
  -test.cpuprofile="$OUT_DIR/cpu.pprof" >/dev/null
go tool pprof -top -nodecount=30 "$BENCH_BIN" "$OUT_DIR/cpu.pprof" >"$OUT_DIR/cpu_top.txt"

python3 .github/scripts/summarize_mvcc_closeout.py \
  --bench "$OUT_DIR/bench.txt" \
  --resources "$OUT_DIR/resources.txt" \
  --candidate-sha "$CANDIDATE_SHA" \
  --expected-samples "$RUNS" \
  --json-output "$OUT_DIR/summary.json" \
  --markdown-output "$OUT_DIR/summary.md"

echo "MVCC closeout matrix artifacts: $OUT_DIR"
