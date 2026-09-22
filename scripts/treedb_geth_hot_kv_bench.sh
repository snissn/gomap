#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-}"
KEYS="${KEYS:-30000}"
VALSIZE="${VALSIZE:-128}"
BATCHSIZE="${BATCHSIZE:-8000}"
VAL_PATTERN="${VAL_PATTERN:-random}"
DBS="${DBS:-treedb_public_command_wal,pebble,leveldb}"
PROFILE="${PROFILE:-wal_on_fast}"
PROFILE_DIR="${PROFILE_DIR:-}"
PATH_LABEL="${PATH_LABEL:-native-fastpath}"
PROGRESS="${PROGRESS:-false}"
KEEP="${KEEP:-false}"
SEED="${SEED:-1}"
SKIP_BUILD="${SKIP_BUILD:-false}"
BATCH_DELETE_RANGE_WIDTH="${BATCH_DELETE_RANGE_WIDTH:-100}"
BATCH_DELETE_RANGES_PER_BATCH="${BATCH_DELETE_RANGES_PER_BATCH:-100}"

usage() {
  cat <<'EOF'
Usage: scripts/treedb_geth_hot_kv_bench.sh [--out DIR] [--profile-dir DIR] [--keep]

Runs the unified_bench geth_hot_kv suite: a small raw-KV proxy for geth/Nitro
hot database behavior (sequential point write, random read, full iteration,
DeleteRange).
Defaults match the historical #2392 30k-row harness shape.

Environment overrides:
  RUN_DIR                         Output dir. Default: /tmp/treedb_geth_hot_kv_<timestamp>
  KEYS                            Default: 30000
  VALSIZE                         Default: 128
  BATCHSIZE                       Default: 8000
  VAL_PATTERN                     Default: random
  DBS                             Default: treedb_public_command_wal,pebble,leveldb
  PROFILE                         unified_bench profile. Default: wal_on_fast
  PROFILE_DIR                     Enable pprof/benchprof artifact capture in this dir
  PATH_LABEL                      benchprof path label. Default: native-fastpath
  KEEP=true                       Preserve DB temp dirs reported by unified_bench
  SEED                            Default: 1
  SKIP_BUILD=true                 Reuse existing ./bin/unified-bench instead of rebuilding
  BATCH_DELETE_RANGE_WIDTH        Default: 100
  BATCH_DELETE_RANGES_PER_BATCH   Default: 100

Examples:
  scripts/treedb_geth_hot_kv_bench.sh

  # TreeDB-only profiling run:
  DBS=treedb_public_command_wal \
  PROFILE_DIR=/tmp/treedb_hotkv_profiles \
  scripts/treedb_geth_hot_kv_bench.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out|--run-dir)
      [[ $# -ge 2 ]] || { echo "missing value for $1" >&2; exit 2; }
      RUN_DIR="$2"
      shift 2
      ;;
    --profile-dir)
      [[ $# -ge 2 ]] || { echo "missing value for $1" >&2; exit 2; }
      PROFILE_DIR="$2"
      shift 2
      ;;
    --keep)
      KEEP=true
      shift
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

if [[ -z "$RUN_DIR" ]]; then
  RUN_DIR="${TMPDIR:-/tmp}/treedb_geth_hot_kv_$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "$RUN_DIR"

if [[ "$SKIP_BUILD" != "true" || ! -x ./bin/unified-bench ]]; then
  make unified-bench
fi

args=(
  -suite geth_hot_kv
  -format markdown
  -profile "$PROFILE"
  -dbs "$DBS"
  -keys "$KEYS"
  -valsize "$VALSIZE"
  -batchsize "$BATCHSIZE"
  -val-pattern "$VAL_PATTERN"
  -batch-delete-range-width "$BATCH_DELETE_RANGE_WIDTH"
  -batch-delete-ranges-per-batch "$BATCH_DELETE_RANGES_PER_BATCH"
  -seed "$SEED"
  -progress="$PROGRESS"
)

if [[ "$KEEP" == "true" ]]; then
  args+=( -keep )
fi

if [[ -n "$PROFILE_DIR" ]]; then
  mkdir -p "$PROFILE_DIR"
  args+=( -profile-dir "$PROFILE_DIR" -path-label "$PATH_LABEL" )
fi

report="$RUN_DIR/geth_hot_kv_report.md"
stderr_log="$RUN_DIR/geth_hot_kv.stderr.log"

./bin/unified-bench "${args[@]}" > "$report" 2> "$stderr_log"

cat <<EOF
geth_hot_kv benchmark complete
  report:      $report
  stderr log:  $stderr_log
EOF
if [[ -n "$PROFILE_DIR" ]]; then
  cat <<EOF
  profiles:    $PROFILE_DIR
  benchprof:   $PROFILE_DIR/benchprof_results.md
EOF
fi
