#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$ROOT"

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
RUN_ROOT="${RUN_ROOT:-}"
TIER="${TIER:-pr}"
DURABILITY_TIER="${DURABILITY_TIER:-}"
DURABILITY_NOTE="${DURABILITY_NOTE:-}"
COLLECTION_TREEDB_PROFILE="${COLLECTION_TREEDB_PROFILE:-}"
SQLITE_JOURNAL_MODE="${SQLITE_JOURNAL_MODE:-}"
SQLITE_SYNCHRONOUS="${SQLITE_SYNCHRONOUS:-}"
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
MONGO_WRITE_CONCERN="${MONGO_WRITE_CONCERN:-}"
MONGO_MAX_POOL_SIZE="${MONGO_MAX_POOL_SIZE:-128}"
MONGO_MAX_CONNECTING="${MONGO_MAX_CONNECTING:-32}"
MONGO_READERS="${MONGO_READERS:-1,2,4,8,16,32}"
MONGO_WRITERS="${MONGO_WRITERS:-1,2,4,8,16,32}"
MONGO_TREEDB_PROFILE="${MONGO_TREEDB_PROFILE:-}"
MONGO_TREEDB_OPEN_MODE="${MONGO_TREEDB_OPEN_MODE:-legacy}"
MONGO_TREEDB_MEMTABLE_MODE="${MONGO_TREEDB_MEMTABLE_MODE:-}"
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
  --durability-tier fast|relaxed|durable
                         Apples-to-apples durability preset across TreeDB, MongoDB, and SQLite
                         (sets defaults for collection/mongo TreeDB profiles, Mongo write concern,
                         and SQLite journal/sync; individually overridable).
  --collection-treedb-profile NAME
                         TreeDB profile for Collections vs SQLite: no_wal_fast, command_wal_relaxed,
                         command_wal_durable, or bench_unsafe. Default: command_wal_relaxed.
  --sqlite-journal-mode MODE
                         SQLite journal_mode for Collections vs SQLite: WAL, OFF, MEMORY, or DELETE.
                         Default: WAL.
  --sqlite-synchronous MODE
                         SQLite synchronous mode for Collections vs SQLite: OFF, NORMAL, FULL, or EXTRA.
                         Default: NORMAL.
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
  --mongo-treedb-profile NAME
                         TreeDB profile for Mongo suites (default: script default; use command_wal_relaxed with standalone).
  --mongo-treedb-open-mode MODE
                         TreeDB open mode for Mongo suites: legacy or standalone (COW). Default: legacy.
  --mongo-treedb-memtable-mode MODE
                         Optional TreeDB memtable mode override for Mongo suites (for example: cow_btree).
  --mongo-image IMAGE    Docker image for --mongo-mode docker. Default: mongo:8.
  --mongo-compact BOOL   Compact the MongoDB collection before final stats collection.
                         Set to true/false, 1/0, or yes/no.
                         Default: true for docker mode; false for external mode unless explicitly set.
  --mongo-write-concern MODE
                         MongoDB write concern for -target mongo: default, w1, w1_j (or journal), or majority_j.
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
RUN_ROOT, TIER, DURABILITY_TIER, COLLECTION_TREEDB_PROFILE, SQLITE_JOURNAL_MODE, SQLITE_SYNCHRONOUS,
INDEXES_LIST, RAW_KEYS, COLLECTION_DOCS,
COLLECTION_PROFILES, COLLECTION_PROFILE_BENCHTIME, COLLECTION_PROFILE_COUNT, MONGO_DOCS,
MONGO_LOAD_SCALING_DOCS, MONGO_LOAD_SCALING_BATCH_SIZE, MONGO_LOAD_PRODUCERS,
MONGO_MODE, MONGO_URI, MONGO_IMAGE (default: mongo:8), MONGO_COMPACT, MONGO_WRITE_CONCERN,
MONGO_TREEDB_PROFILE, MONGO_TREEDB_OPEN_MODE, MONGO_TREEDB_MEMTABLE_MODE, MONGO_READERS, MONGO_WRITERS, TIMEOUT, and TITLE.
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
    --durability-tier)
      require_value "$1" "${2-}"
      DURABILITY_TIER="$2"
      shift 2
      ;;
    --collection-treedb-profile)
      require_value "$1" "${2-}"
      COLLECTION_TREEDB_PROFILE="$2"
      shift 2
      ;;
    --sqlite-journal-mode)
      require_value "$1" "${2-}"
      SQLITE_JOURNAL_MODE="$2"
      shift 2
      ;;
    --sqlite-synchronous)
      require_value "$1" "${2-}"
      SQLITE_SYNCHRONOUS="$2"
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
    --mongo-treedb-profile)
      require_value "$1" "${2-}"
      MONGO_TREEDB_PROFILE="$2"
      shift 2
      ;;
    --mongo-treedb-open-mode)
      require_value "$1" "${2-}"
      MONGO_TREEDB_OPEN_MODE="$2"
      shift 2
      ;;
    --mongo-treedb-memtable-mode)
      require_value "$1" "${2-}"
      MONGO_TREEDB_MEMTABLE_MODE="$2"
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
    --mongo-write-concern)
      require_value "$1" "${2-}"
      MONGO_WRITE_CONCERN="$2"
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

if [[ -n "$DURABILITY_TIER" ]]; then
  DURABILITY_TIER=$(printf '%s' "$DURABILITY_TIER" | tr '[:upper:]' '[:lower:]')
  case "$DURABILITY_TIER" in
    fast)
      COLLECTION_TREEDB_PROFILE="${COLLECTION_TREEDB_PROFILE:-no_wal_fast}"
      MONGO_TREEDB_PROFILE="${MONGO_TREEDB_PROFILE:-no_wal_fast}"
      MONGO_WRITE_CONCERN="${MONGO_WRITE_CONCERN:-w1}"
      SQLITE_JOURNAL_MODE="${SQLITE_JOURNAL_MODE:-WAL}"
      SQLITE_SYNCHRONOUS="${SQLITE_SYNCHRONOUS:-OFF}"
      if [[ -z "$DURABILITY_NOTE" ]]; then
        DURABILITY_NOTE="fast tier: TreeDB no_wal_fast, SQLite WAL+OFF; MongoDB 8 removed --nojournal, so w:1 (j:false) is MongoDB 8 fastest acknowledged write mode"
      fi
      ;;
    relaxed)
      COLLECTION_TREEDB_PROFILE="${COLLECTION_TREEDB_PROFILE:-command_wal_relaxed}"
      MONGO_TREEDB_PROFILE="${MONGO_TREEDB_PROFILE:-command_wal_relaxed}"
      MONGO_WRITE_CONCERN="${MONGO_WRITE_CONCERN:-w1}"
      SQLITE_JOURNAL_MODE="${SQLITE_JOURNAL_MODE:-WAL}"
      SQLITE_SYNCHRONOUS="${SQLITE_SYNCHRONOUS:-NORMAL}"
      DURABILITY_NOTE="${DURABILITY_NOTE:-relaxed tier: TreeDB command_wal_relaxed, MongoDB w:1 (j:false, default WiredTiger journaling), SQLite WAL+NORMAL}"
      ;;
    durable)
      COLLECTION_TREEDB_PROFILE="${COLLECTION_TREEDB_PROFILE:-command_wal_durable}"
      MONGO_TREEDB_PROFILE="${MONGO_TREEDB_PROFILE:-command_wal_durable}"
      MONGO_WRITE_CONCERN="${MONGO_WRITE_CONCERN:-w1_j}"
      SQLITE_JOURNAL_MODE="${SQLITE_JOURNAL_MODE:-WAL}"
      SQLITE_SYNCHRONOUS="${SQLITE_SYNCHRONOUS:-FULL}"
      DURABILITY_NOTE="${DURABILITY_NOTE:-durable tier: TreeDB command_wal_durable, MongoDB w:1+j:true (fsync WiredTiger journal before every write ack), SQLite WAL+FULL}"
      ;;
    *)
      echo "invalid --durability-tier $DURABILITY_TIER (want fast, relaxed, or durable)" >&2
      exit 2
      ;;
  esac
fi

COLLECTION_TREEDB_PROFILE="${COLLECTION_TREEDB_PROFILE:-command_wal_relaxed}"
SQLITE_JOURNAL_MODE=$(printf '%s' "${SQLITE_JOURNAL_MODE:-WAL}" | tr '[:lower:]' '[:upper:]')
SQLITE_SYNCHRONOUS=$(printf '%s' "${SQLITE_SYNCHRONOUS:-NORMAL}" | tr '[:lower:]' '[:upper:]')

case "$COLLECTION_TREEDB_PROFILE" in
  no_wal_fast|command_wal_relaxed|command_wal_durable|bench_unsafe) ;;
  *)
    echo "invalid --collection-treedb-profile $COLLECTION_TREEDB_PROFILE (want no_wal_fast, command_wal_relaxed, command_wal_durable, or bench_unsafe)" >&2
    exit 2
    ;;
esac
case "$SQLITE_JOURNAL_MODE" in
  WAL|OFF|MEMORY|DELETE) ;;
  *)
    echo "invalid --sqlite-journal-mode $SQLITE_JOURNAL_MODE (want WAL, OFF, MEMORY, or DELETE)" >&2
    exit 2
    ;;
esac
case "$SQLITE_SYNCHRONOUS" in
  OFF|NORMAL|FULL|EXTRA) ;;
  *)
    echo "invalid --sqlite-synchronous $SQLITE_SYNCHRONOUS (want OFF, NORMAL, FULL, or EXTRA)" >&2
    exit 2
    ;;
esac
if [[ -n "$MONGO_WRITE_CONCERN" ]]; then
  case "$(printf '%s' "$MONGO_WRITE_CONCERN" | tr '[:upper:]' '[:lower:]')" in
    default|w1|w1_j|journal|majority_j) ;;
    *)
      echo "invalid --mongo-write-concern $MONGO_WRITE_CONCERN (want default, w1, w1_j, journal, or majority_j)" >&2
      exit 2
      ;;
  esac
fi
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
    echo "- durability_tier: ${DURABILITY_TIER:-default}"
    echo "- collection_treedb_profile: $COLLECTION_TREEDB_PROFILE"
    echo "- sqlite_journal_mode: $SQLITE_JOURNAL_MODE"
    echo "- sqlite_synchronous: $SQLITE_SYNCHRONOUS"
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
    echo "- mongo_write_concern: ${MONGO_WRITE_CONCERN:-default}"
    echo "- mongo_uri: $MONGO_URI"
    echo "- mongo_treedb_open_mode: $MONGO_TREEDB_OPEN_MODE"
    echo "- mongo_treedb_profile: ${MONGO_TREEDB_PROFILE:-default}"
    echo "- mongo_treedb_memtable_mode: ${MONGO_TREEDB_MEMTABLE_MODE:-auto}"
    if [[ -n "$DURABILITY_NOTE" ]]; then
      echo "- durability_note: $DURABILITY_NOTE"
    fi
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
    if [[ -n "$DURABILITY_TIER" ]]; then
      printf ' --durability-tier %q' "$DURABILITY_TIER"
    fi
    printf ' --collection-treedb-profile %q' "$COLLECTION_TREEDB_PROFILE"
    printf ' --sqlite-journal-mode %q' "$SQLITE_JOURNAL_MODE"
    printf ' --sqlite-synchronous %q' "$SQLITE_SYNCHRONOUS"
    if [[ -n "$MONGO_TREEDB_PROFILE" ]]; then
      printf ' --mongo-treedb-profile %q' "$MONGO_TREEDB_PROFILE"
    fi
    if [[ -n "$MONGO_WRITE_CONCERN" ]]; then
      printf ' --mongo-write-concern %q' "$MONGO_WRITE_CONCERN"
    fi
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
      -profile "$COLLECTION_TREEDB_PROFILE" \
      -sqlite-journal-mode "$SQLITE_JOURNAL_MODE" \
      -sqlite-synchronous "$SQLITE_SYNCHRONOUS" \
      -formats template-v1,bson,json \
      -benchtime "${COLLECTION_BENCHTIME:-${COLLECTION_DOCS}x}" \
      -count "${COLLECTION_COUNT:-1}" \
      ${profile_args[@]+"${profile_args[@]}"}
  done
}

mongo_treedb_env_args() {
  MONGO_TREEDB_ENV_ARGS=(
    TREEDB_OPEN_MODE="$MONGO_TREEDB_OPEN_MODE"
    TREEDB_MEMTABLE_MODE="$MONGO_TREEDB_MEMTABLE_MODE"
  )
  if [[ -n "$MONGO_TREEDB_PROFILE" ]]; then
    MONGO_TREEDB_ENV_ARGS+=(TREEDB_PROFILE="$MONGO_TREEDB_PROFILE")
  fi
  if [[ -n "$MONGO_WRITE_CONCERN" ]]; then
    MONGO_TREEDB_ENV_ARGS+=(MONGO_WRITE_CONCERN="$MONGO_WRITE_CONCERN")
  fi
}

run_mongo_full_sweep() {
  bool_true "$SKIP_MONGO" && return 0
  local root="$RUN_ROOT/mongo_gateway_full_sweep_1m_expanded"
  mongo_treedb_env_args
  run_logged mongo_full_sweep env \
    "${MONGO_TREEDB_ENV_ARGS[@]}" \
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
  mongo_treedb_env_args
  run_logged mongo_load_modes env \
    "${MONGO_TREEDB_ENV_ARGS[@]}" \
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
  mongo_treedb_env_args
  run_logged mongo_load_scaling env \
    "${MONGO_TREEDB_ENV_ARGS[@]}" \
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
  mongo_treedb_env_args
  for indexes in $INDEXES_LIST; do
    run_logged "mongo_scaling_indexes_${indexes}" env \
      "${MONGO_TREEDB_ENV_ARGS[@]}" \
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
