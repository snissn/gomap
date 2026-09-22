#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$ROOT"

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "$TMP_BASE/gomap_treedb_raw_smoke_XXXXXX")}"
COUNT="${COUNT:-5}"
GOWORK_MODE="${GOWORK:-off}"

usage() {
  cat <<'EOF'
Usage: scripts/treedb_raw_smoke_gate.sh [options]

Runs the #1242 raw TreeDB smoke gate for PRs that touch DB publish, zipper,
leaf-log, value-log, cache, prepared output, or root apply behavior.

Options:
  --out DIR    Output directory for benchmark logs. Default: mktemp.
  --count N    Go benchmark count. Default: 5.
  --help       Show this help.

Environment:
  OUT_DIR      Same as --out.
  COUNT        Same as --count.
  GOWORK       Go workspace mode. Default: off.

Outputs:
  raw_tests.log
  raw_tests.json
  raw_db_root_apply.bench
  raw_db_ops.bench
  commands.sh
  go_version.txt
EOF
}

require_option_value() {
  local opt=$1
  local value=${2-}
  if [[ -z "$value" || "$value" == --* ]]; then
    echo "missing value for $opt" >&2
    usage >&2
    exit 2
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      require_option_value "$1" "${2-}"
      OUT_DIR="$2"
      shift 2
      ;;
    --count)
      require_option_value "$1" "${2-}"
      COUNT="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "invalid COUNT=$COUNT (want positive integer)" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd -P)
if [[ -n "$(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "output directory must be empty: $OUT_DIR" >&2
  exit 2
fi

RAW_TEST_RE='Test(PublishOrderedRoot|ApplyOrderedRoot|Zipper|Coalesce|MergeLeaf|ShortestSeparator|MaintenanceRoots|Vacuum.*Collection|ValueLogGC|ValueLogRewrite|LeafGeneration|Close|ClosedDB|Reopen)'
ROOT_APPLY_BENCH_RE='BenchmarkPublishSystemRootIterator_Warm(SparseDelta|DenseDelta)$'
RAW_OPS_BENCH_RE='Benchmark(Batch|WriteParallel|Stress|ReadUnderWrite|LargeVal)$'

require_raw_test_results() {
  local json_log=$1
  python3 - "$json_log" <<'PY'
import json
import sys

count = 0
with open(sys.argv[1], encoding="utf-8") as fh:
    for line in fh:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("Action") == "run" and event.get("Test"):
            count += 1
if count == 0:
    print("raw test regex matched no tests", file=sys.stderr)
    sys.exit(1)
PY
}

require_benchmark_results() {
  local log=$1
  local label=$2
  if ! grep -Eq '^Benchmark[^[:space:]]+[[:space:]]+[0-9]+' "$log"; then
    echo "$label benchmark regex matched no benchmarks" >&2
    exit 1
  fi
}

{
  echo '#!/usr/bin/env bash'
  echo 'set -euo pipefail'
  printf 'GOWORK_MODE=%q\n' "$GOWORK_MODE"
  printf 'COUNT=%q\n' "$COUNT"
  printf 'RAW_TEST_RE=%q\n' "$RAW_TEST_RE"
  printf 'ROOT_APPLY_BENCH_RE=%q\n' "$ROOT_APPLY_BENCH_RE"
  printf 'RAW_OPS_BENCH_RE=%q\n' "$RAW_OPS_BENCH_RE"
  echo 'GOWORK="$GOWORK_MODE" go test -json ./TreeDB/db ./TreeDB/zipper ./TreeDB/caching -run "$RAW_TEST_RE" -count=1 | tee raw_tests.json'
  echo 'python3 - "$PWD/raw_tests.json" <<'"'"'PY'"'"''
  echo 'import json, sys'
  echo 'count = 0'
  echo 'for line in open(sys.argv[1], encoding="utf-8"):'
  echo '    try: event = json.loads(line)'
  echo '    except json.JSONDecodeError: continue'
  echo '    if event.get("Action") == "run" and event.get("Test"): count += 1'
  echo 'if count == 0: raise SystemExit("raw test regex matched no tests")'
  echo 'PY'
  echo 'GOWORK="$GOWORK_MODE" go test ./TreeDB/db -run "^$" -bench "$ROOT_APPLY_BENCH_RE" -benchmem -count="$COUNT" | tee raw_db_root_apply.bench'
  echo 'grep -Eq '"'"'^Benchmark[^[:space:]]+[[:space:]]+[0-9]+'"'"' raw_db_root_apply.bench || { echo "root-apply benchmark regex matched no benchmarks" >&2; exit 1; }'
  echo 'GOWORK="$GOWORK_MODE" go test ./TreeDB/db -run "^$" -bench "$RAW_OPS_BENCH_RE" -benchmem -count="$COUNT" | tee raw_db_ops.bench'
  echo 'grep -Eq '"'"'^Benchmark[^[:space:]]+[[:space:]]+[0-9]+'"'"' raw_db_ops.bench || { echo "raw DB operation benchmark regex matched no benchmarks" >&2; exit 1; }'
} >"$OUT_DIR/commands.sh"
chmod +x "$OUT_DIR/commands.sh"

GOWORK="$GOWORK_MODE" go version | tee "$OUT_DIR/go_version.txt"

echo "running raw TreeDB tests"
GOWORK="$GOWORK_MODE" go test -json ./TreeDB/db ./TreeDB/zipper ./TreeDB/caching \
  -run "$RAW_TEST_RE" -count=1 2>&1 | tee "$OUT_DIR/raw_tests.json" "$OUT_DIR/raw_tests.log"
require_raw_test_results "$OUT_DIR/raw_tests.json"

echo "running raw root-apply benchmarks"
GOWORK="$GOWORK_MODE" go test ./TreeDB/db -run '^$' \
  -bench "$ROOT_APPLY_BENCH_RE" \
  -benchmem -count="$COUNT" 2>&1 | tee "$OUT_DIR/raw_db_root_apply.bench"
require_benchmark_results "$OUT_DIR/raw_db_root_apply.bench" "root-apply"

echo "running raw DB operation benchmarks"
GOWORK="$GOWORK_MODE" go test ./TreeDB/db -run '^$' \
  -bench "$RAW_OPS_BENCH_RE" \
  -benchmem -count="$COUNT" 2>&1 | tee "$OUT_DIR/raw_db_ops.bench"
require_benchmark_results "$OUT_DIR/raw_db_ops.bench" "raw DB operation"

echo "raw TreeDB smoke artifacts: $OUT_DIR"
