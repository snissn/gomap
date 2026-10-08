#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BASELINE_HASH="${BASELINE_HASH:?BASELINE_HASH is required}"
CANDIDATE_HASH="${CANDIDATE_HASH:-HEAD}"
RUNS="${RUNS:-8}"
BENCHTIME="${BENCHTIME:-2s}"
BATCH_WRITE_BENCHTIME="${BATCH_WRITE_BENCHTIME:-1000x}"
CPUSET="${CPUSET:-0}"
MAX_REGRESSION_PERCENT="${MAX_REGRESSION_PERCENT:-5}"
MAX_BYTES_REGRESSION_PERCENT="${MAX_BYTES_REGRESSION_PERCENT:-1}"
MAX_BYTES_REGRESSION_ABSOLUTE="${MAX_BYTES_REGRESSION_ABSOLUTE:-64}"
SCRIPT_GOWORK="${SCRIPT_GOWORK:-off}"
OUT_DIR="${OUT_DIR:-$ROOT/artifacts/mvcc_raw_path_gate}"
GET_VERSIONED_BENCH_REGEX='^BenchmarkGetVersioned$'
BATCH_WRITE_BENCH_REGEX='^BenchmarkConditionalTxnBaselineBatchWrite$'
SNAPSHOT_BENCH_REGEX='^BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek$'
CACHING_BENCH_REGEX='^BenchmarkRepeatedIterator$'
DURABILITY_BENCH_REGEX='^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1$'

if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "RUNS must be a positive integer" >&2
  exit 2
fi
if ((RUNS % 2 != 0)); then
  echo "RUNS must be even to balance AB/BA sample order" >&2
  exit 2
fi
if ! [[ "$BENCHTIME" =~ ^[1-9][0-9]*(ms|s|x)$ ]]; then
  echo "BENCHTIME must be a positive Go benchmark duration or iteration count" >&2
  exit 2
fi
if ! [[ "$BATCH_WRITE_BENCHTIME" =~ ^[1-9][0-9]*x$ ]]; then
  echo "BATCH_WRITE_BENCHTIME must be a positive Go benchmark iteration count" >&2
  exit 2
fi
for command in git go lscpu ps python3 realpath taskset sha256sum timeout; do
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
"$ROOT/scripts/mvcc_candidate_checkout_guard.sh" "$ROOT" "$CANDIDATE_SHA"
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
BASELINE_DB_BIN="$TMP_ROOT/baseline-db.test"
BASELINE_CACHING_BIN="$TMP_ROOT/baseline-caching.test"
BASELINE_TREEDB_BIN="$TMP_ROOT/baseline-treedb.test"
CANDIDATE_DB_BIN="$TMP_ROOT/candidate-db.test"
CANDIDATE_CACHING_BIN="$TMP_ROOT/candidate-caching.test"
CANDIDATE_TREEDB_BIN="$TMP_ROOT/candidate-treedb.test"

{
  echo "baseline_sha=$BASELINE_SHA"
  echo "candidate_sha=$CANDIDATE_SHA"
  echo "runs=$RUNS"
  echo "benchtime=$BENCHTIME"
  echo "batch_write_benchtime=$BATCH_WRITE_BENCHTIME"
  echo "cpuset=$CPUSET"
  echo "gomaxprocs=1"
  echo "max_regression_percent=$MAX_REGRESSION_PERCENT"
  echo "max_bytes_regression_percent=$MAX_BYTES_REGRESSION_PERCENT"
  echo "max_bytes_regression_absolute=$MAX_BYTES_REGRESSION_ABSOLUTE"
  echo "github_runner_image=${ImageOS:-unknown} ${ImageVersion:-unknown}"
  echo "runner_arch=${RUNNER_ARCH:-unknown}"
  echo "runner_os=${RUNNER_OS:-unknown}"
  go version
  uname -a
  lscpu
} >"$ENVIRONMENT"

env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false \
  -o "$CANDIDATE_DB_BIN" ./TreeDB/db
env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false \
  -o "$CANDIDATE_CACHING_BIN" ./TreeDB/caching
env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false \
  -o "$CANDIDATE_TREEDB_BIN" ./TreeDB
(
  cd "$BASELINE_WT"
  env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false \
    -o "$BASELINE_DB_BIN" ./TreeDB/db
  env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false \
    -o "$BASELINE_CACHING_BIN" ./TreeDB/caching
  env GOWORK="$SCRIPT_GOWORK" go test -c -trimpath -buildvcs=false \
    -o "$BASELINE_TREEDB_BIN" ./TreeDB
)
(
  cd "$TMP_ROOT"
  sha256sum baseline-*.test candidate-*.test
) | tee "$OUT_DIR/binary-sha256.txt"

: >"$BASELINE_LOG"
: >"$CANDIDATE_LOG"
: >"$PROCESSES"
run_sample() {
  local revision="$1"
  local sample="$2"
  local group="$3"
  local binary="$4"
  local regex="$5"
  local output="$6"
  local benchtime="$7"
  {
    echo "--- before revision=$revision sample=$sample group=$group at $(date -Iseconds) ---"
    ps -eo pid,ppid,etime,%cpu,cmd --sort=-%cpu | head -20 || true
  } >>"$PROCESSES"
  taskset -c "$CPUSET" env GOMAXPROCS=1 "$binary" \
    -test.run '^$' \
    -test.bench "$regex" \
    -test.benchmem \
    -test.benchtime="$benchtime" \
    -test.count=1 >>"$output"
}

run_pair() {
  local sample="$1"
  local group="$2"
  local baseline_binary="$3"
  local candidate_binary="$4"
  local regex="$5"
  local benchtime="$6"
  if ((sample % 2 == 1)); then
    run_sample baseline "$sample" "$group" "$baseline_binary" "$regex" "$BASELINE_LOG" "$benchtime"
    run_sample candidate "$sample" "$group" "$candidate_binary" "$regex" "$CANDIDATE_LOG" "$benchtime"
  else
    run_sample candidate "$sample" "$group" "$candidate_binary" "$regex" "$CANDIDATE_LOG" "$benchtime"
    run_sample baseline "$sample" "$group" "$baseline_binary" "$regex" "$BASELINE_LOG" "$benchtime"
  fi
}

for ((sample = 1; sample <= RUNS; sample++)); do
  run_pair "$sample" get_versioned "$BASELINE_DB_BIN" "$CANDIDATE_DB_BIN" "$GET_VERSIONED_BENCH_REGEX" "$BENCHTIME"
  run_pair "$sample" batch_write "$BASELINE_DB_BIN" "$CANDIDATE_DB_BIN" "$BATCH_WRITE_BENCH_REGEX" "$BATCH_WRITE_BENCHTIME"
  run_pair "$sample" snapshot_seek "$BASELINE_TREEDB_BIN" "$CANDIDATE_TREEDB_BIN" "$SNAPSHOT_BENCH_REGEX" "$BENCHTIME"
  run_pair "$sample" repeated_iterator "$BASELINE_CACHING_BIN" "$CANDIDATE_CACHING_BIN" "$CACHING_BENCH_REGEX" "$BENCHTIME"
  run_pair "$sample" durable_sync "$BASELINE_TREEDB_BIN" "$CANDIDATE_TREEDB_BIN" "$DURABILITY_BENCH_REGEX" "$BENCHTIME"
done

# Hash the six actual benchmark executables while TMP_ROOT is still live. The
# EXIT trap removes them only after the checker has emitted a verdict or error.
if python3 .github/scripts/check_mvcc_raw_path_gate.py \
  --baseline "$BASELINE_LOG" \
  --candidate "$CANDIDATE_LOG" \
  --baseline-sha "$BASELINE_SHA" \
  --candidate-sha "$CANDIDATE_SHA" \
  --baseline-db-binary "$BASELINE_DB_BIN" \
  --candidate-db-binary "$CANDIDATE_DB_BIN" \
  --baseline-caching-binary "$BASELINE_CACHING_BIN" \
  --candidate-caching-binary "$CANDIDATE_CACHING_BIN" \
  --baseline-treedb-binary "$BASELINE_TREEDB_BIN" \
  --candidate-treedb-binary "$CANDIDATE_TREEDB_BIN" \
  --expected-samples "$RUNS" \
  --max-regression-percent "$MAX_REGRESSION_PERCENT" \
  --max-bytes-regression-percent "$MAX_BYTES_REGRESSION_PERCENT" \
  --max-bytes-regression-absolute "$MAX_BYTES_REGRESSION_ABSOLUTE" \
  --json-output "$SUMMARY_JSON" \
  --markdown-output "$SUMMARY_MD"; then
  gate_status=0
else
  gate_status=$?
fi

# Diagnose a changed-binary Iterator timing failure without replacing its samples
# or verdict. Keep original executables before cleanup for CI-matched analysis.
if ((gate_status != 0)) && python3 - "$SUMMARY_JSON" <<'PY_CHECK'
import json, sys
with open(sys.argv[1]) as source:
    rows = json.load(source)["results"]
sys.exit(0 if any(row["benchmark"] == "BenchmarkRepeatedIterator"
                  and not row["timing_pass"] and not row["binary_equivalent"]
                  for row in rows) else 1)
PY_CHECK
then
  (
    diagnostics="$OUT_DIR/iterator-diagnostics"
    mkdir -p "$diagnostics" || exit 1
    for revision in baseline candidate; do
      binary="$TMP_ROOT/$revision-caching.test"
      cp "$binary" "$diagnostics/$revision-caching.test" || exit 1
      timeout --kill-after=5s 30s env GOWORK="$SCRIPT_GOWORK" go tool objdump -s \
        'github.com/snissn/gomap/TreeDB/caching\.\(.*\)\.(Iterator|Close|beginForegroundRead|endForegroundRead|retainMemtableView|releaseMemtableView|ensureBackendRange|computeBackendRange)$|github.com/snissn/gomap/TreeDB/caching\.BenchmarkRepeatedIterator' \
        "$binary" >"$diagnostics/$revision-hot-objdump.txt" 2>&1 || true
    done
    for sample in 1 2; do
      if ((sample == 1)); then revisions='baseline candidate'; else revisions='candidate baseline'; fi
      for revision in $revisions; do
        binary="$diagnostics/$revision-caching.test"
        profile="$diagnostics/$revision-$sample.cpu.pprof"
        if taskset -c "$CPUSET" timeout --kill-after=5s 30s env GOMAXPROCS=1 "$binary" \
          -test.run '^$' -test.bench "$CACHING_BENCH_REGEX" \
          -test.benchmem -test.benchtime=2s -test.count=1 \
          -test.cpuprofile="$profile" >"$diagnostics/$revision-$sample.txt" 2>&1; then
          timeout --kill-after=5s 30s env GOWORK="$SCRIPT_GOWORK" go tool pprof -top -nodecount=40 \
            "$binary" "$profile" >"$diagnostics/$revision-$sample-cpu-top.txt" 2>&1 || true
        fi
      done
    done
    (cd "$diagnostics" && sha256sum baseline-caching.test candidate-caching.test) \
      >"$diagnostics/binary-sha256.txt"
  ) || echo "Iterator diagnostics incomplete; original gate status preserved" >&2
fi

# Retain exact Seek executables and compare the existing public iterator only
# after a changed-binary snapshot timing failure. Diagnostic samples never feed
# the checker or replace the original gate status.
if ((gate_status != 0)) && python3 - "$SUMMARY_JSON" <<'PY_CHECK'
import json, sys
with open(sys.argv[1]) as source:
    rows = json.load(source)["results"]
sys.exit(0 if any(row["benchmark"] ==
                  "BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek"
                  and not row["timing_pass"] and not row["binary_equivalent"]
                  for row in rows) else 1)
PY_CHECK
then
  (
    diagnostics="$OUT_DIR/snapshot-seek-diagnostics"
    mkdir -p "$diagnostics" || exit 1
    for revision in baseline candidate; do
      cp "$TMP_ROOT/$revision-treedb.test" "$diagnostics/$revision-treedb.test" || exit 1
    done
    (cd "$diagnostics" && sha256sum baseline-treedb.test candidate-treedb.test) \
      >"$diagnostics/binary-sha256.txt" || exit 1
    # Bind both copied executables before disassembly or diagnostic execution.
    if python3 - "$SUMMARY_JSON" "$diagnostics" >"$diagnostics/binary-verification.txt" 2>&1 <<'PY_HASH'
import hashlib, json, pathlib, sys
with open(sys.argv[1]) as source:
    expected = json.load(source)["binary_digests"]["treedb"]
for revision in ("baseline", "candidate"):
    binary = pathlib.Path(sys.argv[2]) / (revision + "-treedb.test")
    actual = hashlib.sha256(binary.read_bytes()).hexdigest()
    if actual != expected[revision]:
        raise SystemExit("retained binary mismatch: " + revision)
    print(revision + " sha256=" + actual + " verified")
PY_HASH
    then
      :
    else
      exit 1
    fi
    public_regex='^BenchmarkSnapshotIteratorSeekNext/keys=1024/public_seek_baseline$'
    hot_symbols='github.com/snissn/gomap/TreeDB\.BenchmarkSnapshotIteratorSeekNext|github.com/snissn/gomap/TreeDB/(db|caching|tree|node|pager)\.\(\*[^)]*\)\.(begin|endOperation|Seek|seek|Valid|Key|UnsafeKey|resetStack|resetPointerPrefetch|loadCurrent|getLeafKeyFlags|loadNodeRef|loadChildRefView|loadChildRefViewInto|loadNodeViewWithLoadKind|loadNodeViewWithLoadKindInto|Get|IsVerified|MarkVerified|Type|Count|InternalFenceBounds|SearchInternal|GetInternalChildRef|SearchLeaf|GetLeafKeyFlagsView)$|github.com/snissn/gomap/TreeDB/(tree|node)\.(compareTreeKey|InitFreshNodeView)'
    for revision in baseline candidate; do
      diagnostic_status=0
      timeout --kill-after=5s 30s env GOWORK="$SCRIPT_GOWORK" \
        go tool objdump -s "$hot_symbols" "$diagnostics/$revision-treedb.test" \
        >"$diagnostics/$revision-hot-objdump.txt" 2>&1 || diagnostic_status=$?
      empty_output=false
      [[ -s "$diagnostics/$revision-hot-objdump.txt" ]] || empty_output=true
      printf 'exit=%s\nempty_output=%s\n' "$diagnostic_status" "$empty_output" \
        >"$diagnostics/$revision-objdump-status.txt"
    done
    for ((sample = 1; sample <= RUNS; sample++)); do
      if ((sample % 2 == 1)); then revisions='baseline candidate'; else revisions='candidate baseline'; fi
      for revision in $revisions; do
        {
          printf 'revision=%s sample=%s group=public_seek_baseline\n' "$revision" "$sample"
          date -Iseconds
          ps -eo pid,ppid,etime,%cpu,cmd --sort=-%cpu | head -20 || true
        } >>"$diagnostics/processes.txt"
        diagnostic_status=0
        taskset -c "$CPUSET" timeout --kill-after=5s 30s env GOMAXPROCS=1 \
          "$diagnostics/$revision-treedb.test" -test.run '^$' \
          -test.bench "$public_regex" -test.benchmem \
          -test.benchtime="$BENCHTIME" -test.count=1 \
          >"$diagnostics/$revision-$sample-public-seek.txt" 2>&1 || diagnostic_status=$?
        printf 'exit=%s\n' "$diagnostic_status" \
          >"$diagnostics/$revision-$sample-public-seek-status.txt"
      done
    done
  ) || echo "Snapshot Seek diagnostics incomplete; original gate status preserved" >&2
fi

echo "mvcc raw-path gate artifacts: $OUT_DIR"
exit "$gate_status"
