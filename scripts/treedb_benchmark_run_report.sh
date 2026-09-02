#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$ROOT"

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
RUN_ROOT="${RUN_ROOT:-}"
TIER="${TIER:-pr}"
INDEXES_LIST="${INDEXES_LIST:-0 1 2}"
RAW_KEYS="${RAW_KEYS:-}"
COLLECTION_DOCS="${COLLECTION_DOCS:-}"
MONGO_DOCS="${MONGO_DOCS:-}"
COLLECTION_BATCH_SIZE="${COLLECTION_BATCH_SIZE:-16000}"
COLLECTION_PROFILES="${COLLECTION_PROFILES:-true}"
COLLECTION_PROFILE_BENCHTIME="${COLLECTION_PROFILE_BENCHTIME:-}"
COLLECTION_PROFILE_COUNT="${COLLECTION_PROFILE_COUNT:-1}"
MONGO_BATCH_SIZE="${MONGO_BATCH_SIZE:-10000}"
INSERT_PRODUCERS="${INSERT_PRODUCERS:-8}"
MONGO_LOAD_SCALING_DOCS="${MONGO_LOAD_SCALING_DOCS:-}"
MONGO_LOAD_SCALING_BATCH_SIZE="${MONGO_LOAD_SCALING_BATCH_SIZE:-1000}"
MONGO_LOAD_PRODUCERS="${MONGO_LOAD_PRODUCERS:-1,2,4,8,16,32}"
MONGO_MODE="${MONGO_MODE:-docker}"
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
MONGO_IMAGE="${MONGO_IMAGE:-mongo:8}"
MONGO_COMPACT="${MONGO_COMPACT:-}"
MONGO_MAX_POOL_SIZE="${MONGO_MAX_POOL_SIZE:-128}"
MONGO_MAX_CONNECTING="${MONGO_MAX_CONNECTING:-32}"
MONGO_READERS="${MONGO_READERS:-1,2,4,8,16,32}"
MONGO_WRITERS="${MONGO_WRITERS:-1,2,4,8,16,32}"
TIMEOUT="${TIMEOUT:-120m}"
TITLE="${TITLE:-TreeDB Benchmark Run Report}"
SKIP_RAW="${SKIP_RAW:-false}"
SKIP_COLLECTIONS="${SKIP_COLLECTIONS:-false}"
SKIP_MONGO="${SKIP_MONGO:-false}"
SKIP_LOAD_MODES="${SKIP_LOAD_MODES:-false}"
SKIP_LOAD_SCALING="${SKIP_LOAD_SCALING:-false}"
SKIP_SCALING="${SKIP_SCALING:-false}"
ORIGINAL_ARGS=("$@")
FAILURES=0

usage() {
  cat <<'EOF'
Usage: scripts/treedb_benchmark_run_report.sh [options]

Runs the full TreeDB Benchmark Run Report bundle and renders deep_report.html.
The output layout intentionally matches cmd/benchmark_run_report:

  <run-root>/raw_engine_full_matrix/
  <run-root>/collections_sqlite_canonical_1m/
  <run-root>/mongo_gateway_full_sweep_1m_expanded/
  <run-root>/mongo_client_mode_load_matrix_1m/
  <run-root>/mongo_gateway_load_scaling_1m/
  <run-root>/mongo_gateway_reader_writer_scaling_1m/
  <run-root>/deep_report.html

Options:
  --out DIR              Output run root. Default: /tmp/gomap_full_benchmark_report_<timestamp>.
  --run-root DIR         Alias for --out.
  --tier smoke|pr|large  Size preset. Default: pr.
  --indexes LIST         Space- or comma-separated secondary index counts. Default: "0 1 2".
  --raw-keys N           Override raw engine key count.
  --collection-docs N    Override collection/SQLite document count.
  --mongo-docs N         Override Mongo-compatible document count.
  --mongo-load-docs N    Override Mongo load-scaling document count. Default: --mongo-docs.
  --mongo-load-batch-size N
                         Override Mongo load-scaling InsertMany batch size. Default: 1000.
  --mongo-load-producers LIST
                         Producer counts for Mongo load scaling. Default: "1,2,4,8,16,32".
  --mongo-mode MODE      Mongo mode for full/load comparison: docker or external. Default: docker.
  --mongo-uri URI        MongoDB URI for external/scaling runs. Default: mongodb://127.0.0.1:27017.
  --mongo-image IMAGE    Docker image for --mongo-mode docker. Default: mongo:8.
  --mongo-compact BOOL   Compact the MongoDB collection before final stats collection.
                         Set to true/false, 1/0, or yes/no.
                         Default: true for docker mode; false for external mode unless explicitly set.
  --timeout DURATION     Per-cell timeout for Mongo gateway commands. Default: 120m.
  --title TITLE          HTML report title.
  --skip-raw             Skip raw TreeDB engine profile matrix.
  --skip-collections     Skip TreeDB collections vs SQLite.
  --skip-collection-profiles
                          Skip pprof capture for TreeDB collections vs SQLite.
  --skip-mongo           Skip all Mongo-compatible sections.
  --skip-load-modes      Skip Mongo client-mode load matrix.
  --include-load-scaling Include Mongo InsertMany producer-scaling sweep.
                         Default: included.
  --skip-load-scaling    Skip Mongo InsertMany producer-scaling sweep.
  --skip-scaling         Skip Mongo reader/writer scaling.
  --help                 Show this help.

Environment overrides use the uppercase variable names in the script, including
RUN_ROOT, TIER, INDEXES_LIST, RAW_KEYS, COLLECTION_DOCS,
COLLECTION_PROFILES, COLLECTION_PROFILE_BENCHTIME, COLLECTION_PROFILE_COUNT, MONGO_DOCS,
MONGO_LOAD_SCALING_DOCS, MONGO_LOAD_SCALING_BATCH_SIZE, MONGO_LOAD_PRODUCERS,
MONGO_MODE, MONGO_URI, MONGO_IMAGE (default: mongo:8), MONGO_COMPACT, MONGO_READERS, MONGO_WRITERS, TIMEOUT, and TITLE.
EOF
}

require_value() {
  local opt=$1
  local value=${2-}
  if [[ -z "$value" || "$value" == --* ]]; then
    echo "missing value for $opt" >&2
    usage >&2
    exit 2
  fi
}

normalize_list() {
  printf '%s' "$1" | tr ',' ' '
}

bool_true() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes) return 0 ;;
    *) return 1 ;;
  esac
}

normalize_bool_text() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes) printf 'true' ;;
    0|false|no) printf 'false' ;;
    *) return 1 ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      require_value "$1" "${2-}"
      RUN_ROOT="$2"
      shift 2
      ;;
    --run-root)
      require_value "$1" "${2-}"
      RUN_ROOT="$2"
      shift 2
      ;;
    --tier)
      require_value "$1" "${2-}"
      TIER="$2"
      shift 2
      ;;
    --indexes)
      require_value "$1" "${2-}"
      INDEXES_LIST="$2"
      shift 2
      ;;
    --raw-keys)
      require_value "$1" "${2-}"
      RAW_KEYS="$2"
      shift 2
      ;;
    --collection-docs)
      require_value "$1" "${2-}"
      COLLECTION_DOCS="$2"
      shift 2
      ;;
    --mongo-docs)
      require_value "$1" "${2-}"
      MONGO_DOCS="$2"
      shift 2
      ;;
    --mongo-load-docs)
      require_value "$1" "${2-}"
      MONGO_LOAD_SCALING_DOCS="$2"
      shift 2
      ;;
    --mongo-load-batch-size)
      require_value "$1" "${2-}"
      MONGO_LOAD_SCALING_BATCH_SIZE="$2"
      shift 2
      ;;
    --mongo-load-producers)
      require_value "$1" "${2-}"
      MONGO_LOAD_PRODUCERS="$2"
      shift 2
      ;;
    --mongo-mode)
      require_value "$1" "${2-}"
      MONGO_MODE="$2"
      shift 2
      ;;
    --mongo-uri)
      require_value "$1" "${2-}"
      MONGO_URI="$2"
      shift 2
      ;;
    --mongo-image)
      require_value "$1" "${2-}"
      MONGO_IMAGE="$2"
      shift 2
      ;;
    --mongo-compact=*)
      MONGO_COMPACT="${1#*=}"
      shift
      ;;
    --mongo-compact)
      require_value "$1" "${2-}"
      MONGO_COMPACT="$2"
      shift 2
      ;;
    --timeout)
      require_value "$1" "${2-}"
      TIMEOUT="$2"
      shift 2
      ;;
    --title)
      require_value "$1" "${2-}"
      TITLE="$2"
      shift 2
      ;;
    --skip-raw)
      SKIP_RAW=true
      shift
      ;;
    --skip-collections)
      SKIP_COLLECTIONS=true
      shift
      ;;
    --skip-collection-profiles)
      COLLECTION_PROFILES=false
      shift
      ;;
    --skip-mongo)
      SKIP_MONGO=true
      shift
      ;;
    --skip-load-modes)
      SKIP_LOAD_MODES=true
      shift
      ;;
    --include-load-scaling)
      SKIP_LOAD_SCALING=false
      shift
      ;;
    --skip-load-scaling)
      SKIP_LOAD_SCALING=true
      shift
      ;;
    --skip-scaling)
      SKIP_SCALING=true
      shift
      ;;
    --help)
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

case "$TIER" in
  smoke)
    RAW_KEYS="${RAW_KEYS:-50000}"
    COLLECTION_DOCS="${COLLECTION_DOCS:-10000}"
    MONGO_DOCS="${MONGO_DOCS:-10000}"
    MONGO_READS="${MONGO_READS:-5000}"
    MONGO_RANGE_READS="${MONGO_RANGE_READS:-1000}"
    MONGO_UPDATES="${MONGO_UPDATES:-1000}"
    MONGO_CONCURRENT_READS="${MONGO_CONCURRENT_READS:-8000}"
    MONGO_CONCURRENT_WRITES="${MONGO_CONCURRENT_WRITES:-4000}"
    ;;
  pr)
    RAW_KEYS="${RAW_KEYS:-200000}"
    COLLECTION_DOCS="${COLLECTION_DOCS:-100000}"
    MONGO_DOCS="${MONGO_DOCS:-100000}"
    MONGO_READS="${MONGO_READS:-50000}"
    MONGO_RANGE_READS="${MONGO_RANGE_READS:-5000}"
    MONGO_UPDATES="${MONGO_UPDATES:-5000}"
    MONGO_CONCURRENT_READS="${MONGO_CONCURRENT_READS:-80000}"
    MONGO_CONCURRENT_WRITES="${MONGO_CONCURRENT_WRITES:-80000}"
    ;;
  large)
    RAW_KEYS="${RAW_KEYS:-800000}"
    COLLECTION_DOCS="${COLLECTION_DOCS:-1000000}"
    MONGO_DOCS="${MONGO_DOCS:-1000000}"
    MONGO_READS="${MONGO_READS:-100000}"
    MONGO_RANGE_READS="${MONGO_RANGE_READS:-10000}"
    MONGO_UPDATES="${MONGO_UPDATES:-10000}"
    MONGO_CONCURRENT_READS="${MONGO_CONCURRENT_READS:-100000}"
    MONGO_CONCURRENT_WRITES="${MONGO_CONCURRENT_WRITES:-100000}"
    ;;
  *)
    echo "invalid --tier $TIER (want smoke, pr, or large)" >&2
    exit 2
    ;;
esac
MONGO_LOAD_SCALING_DOCS="${MONGO_LOAD_SCALING_DOCS:-$MONGO_DOCS}"
raw_skip_mongo=$SKIP_MONGO
SKIP_MONGO=$(normalize_bool_text "$SKIP_MONGO") || {
  echo "invalid SKIP_MONGO=$raw_skip_mongo (want true/false, 1/0, or yes/no)" >&2
  exit 2
}
if ! bool_true "$SKIP_MONGO"; then
  case "$MONGO_MODE" in
    docker|external) ;;
    *)
      echo "invalid --mongo-mode $MONGO_MODE (want docker or external)" >&2
      exit 2
      ;;
  esac
  if [[ -z "$MONGO_COMPACT" ]]; then
    if [[ "$MONGO_MODE" == "external" ]]; then
      MONGO_COMPACT=false
    else
      MONGO_COMPACT=true
    fi
  else
    raw_mongo_compact=$MONGO_COMPACT
    MONGO_COMPACT=$(normalize_bool_text "$MONGO_COMPACT") || {
      echo "invalid MONGO_COMPACT=$raw_mongo_compact (want true/false, 1/0, or yes/no)" >&2
      exit 2
    }
  fi
  if [[ "$MONGO_MODE" == "docker" ]] && ! command -v docker >/dev/null 2>&1; then
    echo "MONGO_MODE=docker requires docker; use --mongo-mode external --mongo-uri URI" >&2
    exit 2
  fi
fi

INDEXES_LIST=$(normalize_list "$INDEXES_LIST")
MONGO_LOAD_PRODUCERS=$(normalize_list "$MONGO_LOAD_PRODUCERS")
MONGO_READERS=$(normalize_list "$MONGO_READERS")
MONGO_WRITERS=$(normalize_list "$MONGO_WRITERS")

if [[ -z "$RUN_ROOT" ]]; then
  RUN_ROOT="$TMP_BASE/gomap_full_benchmark_report_$(date +%Y%m%d_%H%M%S)"
fi
RUN_ROOT=$(mkdir -p "$(dirname "$RUN_ROOT")" && cd "$(dirname "$RUN_ROOT")" && pwd -P)/$(basename "$RUN_ROOT")
LOG_DIR="$RUN_ROOT/logs"
mkdir -p "$LOG_DIR"

run_logged() {
  local name=$1
  shift
  local start
  start=$(date +%s)
  echo
  echo "==> $name"
  printf 'command:' | tee -a "$RUN_ROOT/commands.log"
  printf ' %q' "$@" | tee -a "$RUN_ROOT/commands.log"
  printf '\n' | tee -a "$RUN_ROOT/commands.log"
  set +e
  "$@" 2>&1 | tee "$LOG_DIR/${name}.log"
  local status=${PIPESTATUS[0]}
  set -e
  local end
  end=$(date +%s)
  printf 'exit_status: %s duration_sec: %s\n' "$status" "$((end - start))" | tee -a "$RUN_ROOT/commands.log"
  if [[ "$status" -ne 0 ]]; then
    FAILURES=$((FAILURES + 1))
    echo "warning: $name failed with exit status $status; continuing so the final report can render" | tee -a "$RUN_ROOT/commands.log" >&2
  fi
  return 0
}

write_metadata() {
  {
    echo "HEAD=$(git rev-parse HEAD)"
    if git rev-parse --verify origin/main >/dev/null 2>&1; then
      echo "origin/main=$(git rev-parse origin/main)"
    fi
    echo "branch=$(git branch --show-current || true)"
  } > "$RUN_ROOT/HEAD.txt"
  {
    echo "# Full TreeDB Benchmark Report Run"
    echo
    echo "- generated_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- worktree: $ROOT"
    echo "- run_root: $RUN_ROOT"
    echo "- tier: $TIER"
    echo "- indexes: $INDEXES_LIST"
    echo "- raw_keys: $RAW_KEYS"
    echo "- collection_docs: $COLLECTION_DOCS"
    echo "- collection_profiles: $COLLECTION_PROFILES"
    echo "- collection_profile_benchtime: ${COLLECTION_PROFILE_BENCHTIME:-${COLLECTION_BENCHTIME:-${COLLECTION_DOCS}x}}"
    echo "- collection_profile_count: $COLLECTION_PROFILE_COUNT"
    echo "- mongo_docs: $MONGO_DOCS"
    echo "- mongo_load_scaling_docs: $MONGO_LOAD_SCALING_DOCS"
    echo "- mongo_load_scaling_batch_size: $MONGO_LOAD_SCALING_BATCH_SIZE"
    echo "- mongo_load_producers: $MONGO_LOAD_PRODUCERS"
    echo "- mongo_mode: $MONGO_MODE"
    echo "- mongo_image: $MONGO_IMAGE"
    echo "- mongo_compact: $MONGO_COMPACT"
    echo "- mongo_uri: $MONGO_URI"
    echo "- timeout: $TIMEOUT"
    echo "- mongo_readers: $MONGO_READERS"
    echo "- mongo_writers: $MONGO_WRITERS"
    echo "- mongo_max_pool_size: $MONGO_MAX_POOL_SIZE"
    echo "- mongo_max_connecting: $MONGO_MAX_CONNECTING"
    echo "- skip_raw: $SKIP_RAW"
    echo "- skip_collections: $SKIP_COLLECTIONS"
    echo "- skip_mongo: $SKIP_MONGO"
    echo "- skip_load_modes: $SKIP_LOAD_MODES"
    echo "- skip_load_scaling: $SKIP_LOAD_SCALING"
    echo "- skip_scaling: $SKIP_SCALING"
    echo "- go: $(go version)"
    echo "- uname: $(uname -a)"
    echo
    echo "## Exact Invocation"
    echo
    printf '```sh\n'
    printf '%q' "scripts/treedb_benchmark_run_report.sh"
    [[ ${#ORIGINAL_ARGS[@]} -gt 0 ]] && printf ' %q' "${ORIGINAL_ARGS[@]}"
    printf '\n```\n'
    echo
    echo "## One Command"
    echo
    printf '```sh\n'
    printf 'scripts/treedb_benchmark_run_report.sh'
    printf ' --out %q' "$RUN_ROOT"
    printf ' --tier %q' "$TIER"
    printf ' --indexes %q' "$INDEXES_LIST"
    printf ' --mongo-mode %q' "$MONGO_MODE"
    printf ' --mongo-image %q' "$MONGO_IMAGE"
    printf ' --mongo-compact %q' "$MONGO_COMPACT"
    printf ' --mongo-uri %q' "$MONGO_URI"
    printf '\n```\n'
  } > "$RUN_ROOT/RUNBOOK.md"
}

run_raw_engine() {
  bool_true "$SKIP_RAW" && return 0
  run_logged build_raw_tools make unified-bench benchprof
  local root="$RUN_ROOT/raw_engine_full_matrix"
  mkdir -p "$root"
  # unified-bench uses cross-adapter benchmark presets here, not the public
  # TreeDB server profile vocabulary. #2148 owns command-WAL/bench migration for
  # this raw-engine report matrix.
  for profile in wal_on_fast fast; do
    for checkpoint_mode in checkpoint_between_tests no_checkpoint_between_tests; do
      local out="$root/${profile}_${checkpoint_mode}"
      local log_name="raw_${profile}_${checkpoint_mode}"
      local args=(
        ./bin/unified-bench
        -dbs treedb
        -keys "$RAW_KEYS"
        -profile "$profile"
        -path-label native-fastpath
        -profile-dir "$out"
        -progress=false
      )
      if [[ "$checkpoint_mode" == "checkpoint_between_tests" ]]; then
        args+=(-checkpoint-between-tests)
      fi
      run_logged "$log_name" "${args[@]}"
      if [[ -f "$LOG_DIR/${log_name}.log" && -d "$out" ]]; then
        cp "$LOG_DIR/${log_name}.log" "$out/unified-bench.log"
      fi
      run_logged "benchprof_${profile}_${checkpoint_mode}" ./bin/benchprof -profiles-dir "$out"
    done
  done
}

run_collections() {
  bool_true "$SKIP_COLLECTIONS" && return 0
  run_logged build_collection_tools make collection-canonical-bench-bin collection-load-fixture treemap-bin
  local root="$RUN_ROOT/collections_sqlite_canonical_1m"
  mkdir -p "$root"
  for indexes in $INDEXES_LIST; do
    local profile_args=()
    if bool_true "$COLLECTION_PROFILES"; then
      profile_args=(
        -profile-timed-matrix
        -profile-benchtime "${COLLECTION_PROFILE_BENCHTIME:-${COLLECTION_BENCHTIME:-${COLLECTION_DOCS}x}}"
        -profile-count "$COLLECTION_PROFILE_COUNT"
      )
    fi
    run_logged "collections_indexes_${indexes}" env USE_BUILT_BIN=1 ./scripts/bench_collections_canonical.sh \
      -out-dir "$root/indexes_${indexes}" \
      -docs "$COLLECTION_DOCS" \
      -batch-size "$COLLECTION_BATCH_SIZE" \
      -indexes "$indexes" \
      -formats template-v1,bson,json \
      -benchtime "${COLLECTION_BENCHTIME:-${COLLECTION_DOCS}x}" \
      -count "${COLLECTION_COUNT:-1}" \
      "${profile_args[@]:-}"
  done
}

run_mongo_full_sweep() {
  bool_true "$SKIP_MONGO" && return 0
  local root="$RUN_ROOT/mongo_gateway_full_sweep_1m_expanded"
  run_logged mongo_full_sweep env \
    TREEDB_DOCUMENT_FORMATS=bson \
    TREEDB_CLIENT_MODES=driver-command-raw \
    MONGO_CLIENT_MODES=driver-command-raw \
    BATCH_SIZE="$MONGO_BATCH_SIZE" \
    INSERT_PRODUCERS="$INSERT_PRODUCERS" \
    MONGO_MAX_POOL_SIZE="$MONGO_MAX_POOL_SIZE" \
    MONGO_MAX_CONNECTING="$MONGO_MAX_CONNECTING" \
    PREBUILD_DOCUMENTS=true \
    PROFILE_TREEDB=true \
    ./scripts/mongo_gateway_compare.sh \
      --out "$root" \
      --docs "$MONGO_DOCS" \
      --indexes "$INDEXES_LIST" \
      --reads "$MONGO_READS" \
      --range-reads "$MONGO_RANGE_READS" \
      --updates "$MONGO_UPDATES" \
      --range-index \
      --concurrent-read-kinds "id,email,range" \
      --concurrent-reader-sweep "$MONGO_READERS" \
      --concurrent-reads "$MONGO_CONCURRENT_READS" \
      --concurrent-writer-sweep "$MONGO_WRITERS" \
      --concurrent-writes "$MONGO_CONCURRENT_WRITES" \
      --mongo-mode "$MONGO_MODE" \
      --mongo-image "$MONGO_IMAGE" \
      --mongo-compact "$MONGO_COMPACT" \
      --mongo-uri "$MONGO_URI" \
      --timeout "$TIMEOUT" \
      --title "Mongo API Full Sweep"
}

run_mongo_load_modes() {
  bool_true "$SKIP_MONGO" && return 0
  bool_true "$SKIP_LOAD_MODES" && return 0
  local root="$RUN_ROOT/mongo_client_mode_load_matrix_1m"
  run_logged mongo_load_modes env \
    TREEDB_DOCUMENT_FORMATS=bson \
    TREEDB_CLIENT_MODES="driver driver-command driver-command-raw raw-wire-tcp raw-wire" \
    MONGO_CLIENT_MODES="driver driver-command driver-command-raw driver-unack" \
    BATCH_SIZE="$MONGO_BATCH_SIZE" \
    INSERT_PRODUCERS="$INSERT_PRODUCERS" \
    MONGO_MAX_POOL_SIZE="$MONGO_MAX_POOL_SIZE" \
    MONGO_MAX_CONNECTING="$MONGO_MAX_CONNECTING" \
    PREBUILD_DOCUMENTS=true \
    READS=0 \
    RANGE_READS=0 \
    UPDATES=0 \
    DELETES=0 \
    CONCURRENT_READ_KINDS=id \
    CONCURRENT_READERS=0 \
    CONCURRENT_READER_SWEEP= \
    CONCURRENT_READS=0 \
    CONCURRENT_RANGE_READERS=0 \
    CONCURRENT_RANGE_READER_SWEEP= \
    CONCURRENT_RANGE_READS=0 \
    CONCURRENT_WRITERS=0 \
    CONCURRENT_WRITER_SWEEP= \
    CONCURRENT_WRITES=0 \
    PROFILE_TREEDB=true \
    ./scripts/mongo_gateway_compare.sh \
      --out "$root" \
      --docs "$MONGO_DOCS" \
      --indexes "$INDEXES_LIST" \
      --mongo-mode "$MONGO_MODE" \
      --mongo-image "$MONGO_IMAGE" \
      --mongo-compact "$MONGO_COMPACT" \
      --mongo-uri "$MONGO_URI" \
      --timeout "$TIMEOUT" \
      --title "Mongo API Client-Mode Load Matrix"
}

run_mongo_load_scaling() {
  bool_true "$SKIP_MONGO" && return 0
  bool_true "$SKIP_LOAD_SCALING" && return 0
  local root="$RUN_ROOT/mongo_gateway_load_scaling_1m"
  run_logged mongo_load_scaling env \
    MONGO_MAX_POOL_SIZE="$MONGO_MAX_POOL_SIZE" \
    MONGO_MAX_CONNECTING="$MONGO_MAX_CONNECTING" \
    ./scripts/mongo_gateway_load_scaling_bench.sh \
      --out "$root" \
      --docs "$MONGO_LOAD_SCALING_DOCS" \
      --indexes "$INDEXES_LIST" \
      --batch-size "$MONGO_LOAD_SCALING_BATCH_SIZE" \
      --producers "$MONGO_LOAD_PRODUCERS" \
      --mongo-mode "$MONGO_MODE" \
      --mongo-image "$MONGO_IMAGE" \
      --mongo-compact "$MONGO_COMPACT" \
      --mongo-uri "$MONGO_URI" \
      --timeout "$TIMEOUT" \
      --title "Mongo API InsertMany Producer Scaling"
}

run_mongo_scaling() {
  bool_true "$SKIP_MONGO" && return 0
  bool_true "$SKIP_SCALING" && return 0
  local root="$RUN_ROOT/mongo_gateway_reader_writer_scaling_1m"
  mkdir -p "$root"
  for indexes in $INDEXES_LIST; do
    run_logged "mongo_scaling_indexes_${indexes}" env \
      MONGO_MAX_POOL_SIZE="$MONGO_MAX_POOL_SIZE" \
      MONGO_MAX_CONNECTING="$MONGO_MAX_CONNECTING" \
      ./scripts/mongo_gateway_scaling_bench.sh \
      --out "$root/indexes_${indexes}" \
      --docs "$MONGO_DOCS" \
      --indexes "$indexes" \
      --batch-size "$MONGO_BATCH_SIZE" \
      --insert-producers "$INSERT_PRODUCERS" \
      --writers "$MONGO_WRITERS" \
      --readers "$MONGO_READERS" \
      --concurrent-writes "$MONGO_CONCURRENT_WRITES" \
      --concurrent-reads "$MONGO_CONCURRENT_READS" \
      --include-mongo \
      --mongo-mode "$MONGO_MODE" \
      --mongo-image "$MONGO_IMAGE" \
      --mongo-compact "$MONGO_COMPACT" \
      --mongo-uri "$MONGO_URI" \
      --timeout "$TIMEOUT" \
      --title "Mongo API Reader/Writer Scaling, ${indexes} indexes"
  done
}

render_report() {
  run_logged render_deep_report go run ./cmd/benchmark_run_report \
    -run-root "$RUN_ROOT" \
    -out "$RUN_ROOT/deep_report.html" \
    -title "$TITLE"
}

mkdir -p "$RUN_ROOT"
write_metadata
run_raw_engine
run_collections
run_mongo_full_sweep
run_mongo_load_modes
run_mongo_load_scaling
run_mongo_scaling
render_report

echo
echo "full benchmark report: $RUN_ROOT/deep_report.html"
if [[ "$FAILURES" -ne 0 ]]; then
  echo "completed with $FAILURES failed command(s); inspect $RUN_ROOT/commands.log and $LOG_DIR" >&2
  exit 1
fi
