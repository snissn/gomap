#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "$TMP_BASE/gomap_mongo_gateway_compare_XXXXXX")}"
DOCS_LIST="${DOCS_LIST:-1000 10000}"
INDEXES_LIST="${INDEXES_LIST:-0 2}"
BATCH_SIZE="${BATCH_SIZE:-500}"
INSERT_PRODUCERS="${INSERT_PRODUCERS:-1}"
MONGO_MAX_POOL_SIZE="${MONGO_MAX_POOL_SIZE:-0}"
MONGO_MIN_POOL_SIZE="${MONGO_MIN_POOL_SIZE:-0}"
MONGO_MAX_CONNECTING="${MONGO_MAX_CONNECTING:-0}"
PREBUILD_DOCUMENTS="${PREBUILD_DOCUMENTS:-false}"
MONGO_COMPACT="${MONGO_COMPACT:-}"
RANGE_INDEX="${RANGE_INDEX:-false}"
PROFILE_TREEDB="${PROFILE_TREEDB:-false}"
READS="${READS:-}"
READS_DIVISOR="${READS_DIVISOR:-1}"
RANGE_READS="${RANGE_READS:-}"
UPDATES="${UPDATES:-}"
DELETES="${DELETES:-0}"
RANGE_READS_DIVISOR="${RANGE_READS_DIVISOR:-10}"
UPDATES_DIVISOR="${UPDATES_DIVISOR:-10}"
CONCURRENT_READERS="${CONCURRENT_READERS:-0}"
CONCURRENT_READ_KINDS="${CONCURRENT_READ_KINDS:-id}"
CONCURRENT_READER_SWEEP="${CONCURRENT_READER_SWEEP:-}"
CONCURRENT_READS="${CONCURRENT_READS:-}"
CONCURRENT_READS_DIVISOR="${CONCURRENT_READS_DIVISOR:-10}"
CONCURRENT_RANGE_READERS="${CONCURRENT_RANGE_READERS:-0}"
CONCURRENT_RANGE_READER_SWEEP="${CONCURRENT_RANGE_READER_SWEEP:-}"
CONCURRENT_RANGE_READS="${CONCURRENT_RANGE_READS:-}"
CONCURRENT_RANGE_READS_DIVISOR="${CONCURRENT_RANGE_READS_DIVISOR:-10}"
CONCURRENT_WRITERS="${CONCURRENT_WRITERS:-0}"
CONCURRENT_WRITER_SWEEP="${CONCURRENT_WRITER_SWEEP:-}"
CONCURRENT_WRITES="${CONCURRENT_WRITES:-}"
CONCURRENT_WRITES_DIVISOR="${CONCURRENT_WRITES_DIVISOR:-10}"
MONGO_MODE="${MONGO_MODE:-docker}"
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
MONGO_IMAGE="${MONGO_IMAGE:-mongo:8}"
MONGO_CLIENT_MODE="${MONGO_CLIENT_MODE:-driver}"
MONGO_CLIENT_MODES="${MONGO_CLIENT_MODES:-$MONGO_CLIENT_MODE}"
DATABASE_PREFIX="${DATABASE_PREFIX:-mongo_gateway_compare}"
COLLECTION="${COLLECTION:-docs}"
TIMEOUT="${TIMEOUT:-20m}"
TITLE="${TITLE:-Mongo Gateway Benchmark Comparison}"
# Indexed benchmark baseline: default INDEXES_LIST includes secondary indexes.
# Use command_wal_relaxed for command-WAL coverage once indexed catalog command
# support is enabled for this workload; bench_unsafe is the explicit no-WAL ceiling.
TREEDB_PROFILE="${TREEDB_PROFILE:-bench_unsafe}"
TREEDB_DOCUMENT_FORMAT="${TREEDB_DOCUMENT_FORMAT:-template-v1}"
TREEDB_DOCUMENT_FORMATS="${TREEDB_DOCUMENT_FORMATS:-$TREEDB_DOCUMENT_FORMAT}"
TREEDB_CLIENT_MODE="${TREEDB_CLIENT_MODE:-driver}"
TREEDB_CLIENT_MODES="${TREEDB_CLIENT_MODES:-$TREEDB_CLIENT_MODE}"
TREEDB_DATA_ROOT_STORAGE="${TREEDB_DATA_ROOT_STORAGE:-compressed}"
TREEDB_INDEX_STATE_ROOT_STORAGE="${TREEDB_INDEX_STATE_ROOT_STORAGE:-compressed}"
TREEDB_INDEX_ROOT_STORAGE="${TREEDB_INDEX_ROOT_STORAGE:-compressed}"
TREEDB_MAINTENANCE="${TREEDB_MAINTENANCE:-full}"
TREEDB_READ_STATE="${TREEDB_READ_STATE:-settled}"

usage() {
  cat <<'EOF'
Usage: scripts/mongo_gateway_compare.sh [options]

Runs the same cmd/mongo_gateway_bench workload against TreeDB and MongoDB,
then writes a reusable report bundle.

Options:
  --out DIR             Output directory. Default: mktemp under $TMPDIR or /tmp.
  --docs LIST           Space-separated document counts. Default: "1000 10000".
  --indexes LIST        Space-separated secondary-index counts. Default: "0 2".
  --reads COUNT         Point reads per target/cell. Default: documents.
  --range-reads COUNT   Range reads per target/cell. Default: documents / 10.
  --updates COUNT       Updates per target/cell. Default: documents / 10.
  --deletes COUNT       Deletes per target/cell. Default: 0.
  --insert-producers N  Producer goroutines for the insert load phase. Default: 1.
  --mongo-max-pool-size N
                        MongoDB Go driver maxPoolSize. Default: 0, use driver default.
  --mongo-min-pool-size N
                        MongoDB Go driver minPoolSize. Default: 0, use driver default.
  --mongo-max-connecting N
                        MongoDB Go driver maxConnecting. Default: 0, use driver default.
  --mongo-compact BOOL  Compact the MongoDB collection before final stats collection.
                        Set to true/false, 1/0, or yes/no.
                        Default: true for docker mode; false for external mode unless explicitly set.
  --prebuild-documents  Prebuild documents before timed load phases.
  --range-index         Create age_1 for the range-read phase.
  --profile-treedb      Capture per-phase TreeDB pprof artifacts in profiles/.
  --concurrent-readers N
                        Reader goroutines for concurrent read phases.
  --concurrent-read-kinds LIST
                        Concurrent read kinds: id, email, range, or all.
                        Default: id.
  --concurrent-reader-sweep LIST
                        Space- or comma-separated reader counts for concurrent
                        read throughput sweep phases. Uses
                        --concurrent-reads when set, otherwise derives from
                        documents / CONCURRENT_READS_DIVISOR. Cannot be
                        combined with --concurrent-readers.
  --concurrent-reads COUNT
                        Concurrent point reads per target/cell.
  --concurrent-range-readers N
                        Reader goroutines for the concurrent age range-read phase.
  --concurrent-range-reader-sweep LIST
                        Space- or comma-separated reader counts for concurrent
                        age range-read throughput sweep phases. Uses
                        --concurrent-range-reads when set, otherwise derives
                        from documents / CONCURRENT_RANGE_READS_DIVISOR. Cannot
                        be combined with --concurrent-range-readers.
  --concurrent-range-reads COUNT
                        Concurrent age range reads per target/cell.
  --concurrent-writers N
                        Writer goroutines for the concurrent update phase.
  --concurrent-writer-sweep LIST
                        Space- or comma-separated writer counts for concurrent
                        update throughput sweep phases. Uses
                        --concurrent-writes when set, otherwise derives from
                        documents / CONCURRENT_WRITES_DIVISOR. Cannot be
                        combined with --concurrent-writers.
  --concurrent-writes COUNT
                        Concurrent updates per target/cell.
  --mongo-mode MODE     docker or external. Default: docker.
  --mongo-uri URI       MongoDB URI for --mongo-mode external.
  --mongo-image IMAGE   Docker image for --mongo-mode docker. Default: mongo:8.
  --mongo-client-mode MODE
                        Single MongoDB client mode: driver, driver-find-raw,
                        driver-command, driver-command-raw, or driver-unack.
  --mongo-client-modes LIST
                        Space-separated MongoDB client modes. Example:
                        "driver driver-find-raw driver-command driver-command-raw driver-unack".
  --timeout DURATION    Per-run benchmark timeout. Default: 20m.
  --treedb-profile NAME TreeDB profile. Default: bench_unsafe.
  --treedb-document-format FORMAT
                        Single TreeDB document format.
  --treedb-document-formats LIST
                        Space-separated TreeDB formats. Example: "json template-v1 bson".
  --treedb-client-mode MODE
                        Single TreeDB client mode: driver, driver-find-raw, driver-command, driver-command-raw, direct, raw-wire-tcp, raw-wire-tcp-pipeline, raw-wire, native-wire-tcp, or native-wire-inproc. driver-unack is rejected because TreeDB rejects w:0 before mutation.
  --treedb-client-modes LIST
                        Space-separated TreeDB client modes. Example: "driver driver-find-raw driver-command driver-command-raw direct raw-wire-tcp raw-wire-tcp-pipeline raw-wire native-wire-tcp native-wire-inproc".
  --treedb-maintenance MODE
                        TreeDB final maintenance: full, checkpoint, or none.
  --treedb-read-state STATE
                        TreeDB read state before read phases: settled or unsettled.
  --title TITLE         Markdown report title.
  --help                Show this help.

Environment overrides:
  OUT_DIR, DOCS_LIST, INDEXES_LIST, BATCH_SIZE, INSERT_PRODUCERS,
  MONGO_MAX_POOL_SIZE, MONGO_MIN_POOL_SIZE, MONGO_MAX_CONNECTING, PREBUILD_DOCUMENTS,
  RANGE_INDEX, PROFILE_TREEDB,
  READS, READS_DIVISOR,
  RANGE_READS, UPDATES, DELETES, RANGE_READS_DIVISOR, UPDATES_DIVISOR,
  CONCURRENT_READERS, CONCURRENT_READ_KINDS, CONCURRENT_READS, CONCURRENT_READS_DIVISOR,
  CONCURRENT_READER_SWEEP,
  CONCURRENT_RANGE_READERS, CONCURRENT_RANGE_READS, CONCURRENT_RANGE_READS_DIVISOR,
  CONCURRENT_RANGE_READER_SWEEP,
  CONCURRENT_WRITERS, CONCURRENT_WRITER_SWEEP, CONCURRENT_WRITES, CONCURRENT_WRITES_DIVISOR,
  MONGO_MODE, MONGO_URI, MONGO_IMAGE, MONGO_COMPACT, DATABASE_PREFIX, COLLECTION, TIMEOUT,
  MONGO_CLIENT_MODE, MONGO_CLIENT_MODES,
  TREEDB_PROFILE, TREEDB_DOCUMENT_FORMAT, TREEDB_DOCUMENT_FORMATS,
  TREEDB_CLIENT_MODE, TREEDB_CLIENT_MODES,
  TREEDB_DATA_ROOT_STORAGE, TREEDB_INDEX_STATE_ROOT_STORAGE, TREEDB_INDEX_ROOT_STORAGE,
  TREEDB_MAINTENANCE, TREEDB_READ_STATE, TITLE.
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
    --docs)
      require_option_value "$1" "${2-}"
      DOCS_LIST="$2"
      shift 2
      ;;
    --indexes)
      require_option_value "$1" "${2-}"
      INDEXES_LIST="$2"
      shift 2
      ;;
    --reads)
      require_option_value "$1" "${2-}"
      READS="$2"
      shift 2
      ;;
    --range-reads)
      require_option_value "$1" "${2-}"
      RANGE_READS="$2"
      shift 2
      ;;
    --updates)
      require_option_value "$1" "${2-}"
      UPDATES="$2"
      shift 2
      ;;
    --deletes)
      require_option_value "$1" "${2-}"
      DELETES="$2"
      shift 2
      ;;
    --insert-producers)
      require_option_value "$1" "${2-}"
      INSERT_PRODUCERS="$2"
      shift 2
      ;;
    --mongo-max-pool-size)
      require_option_value "$1" "${2-}"
      MONGO_MAX_POOL_SIZE="$2"
      shift 2
      ;;
    --mongo-min-pool-size)
      require_option_value "$1" "${2-}"
      MONGO_MIN_POOL_SIZE="$2"
      shift 2
      ;;
    --mongo-max-connecting)
      require_option_value "$1" "${2-}"
      MONGO_MAX_CONNECTING="$2"
      shift 2
      ;;
    --prebuild-documents)
      PREBUILD_DOCUMENTS=true
      shift
      ;;
    --range-index)
      RANGE_INDEX=true
      shift
      ;;
    --profile-treedb)
      PROFILE_TREEDB=true
      shift
      ;;
    --concurrent-readers)
      require_option_value "$1" "${2-}"
      CONCURRENT_READERS="$2"
      shift 2
      ;;
    --concurrent-read-kinds)
      require_option_value "$1" "${2-}"
      CONCURRENT_READ_KINDS="$2"
      shift 2
      ;;
    --concurrent-reader-sweep)
      require_option_value "$1" "${2-}"
      CONCURRENT_READER_SWEEP="$2"
      shift 2
      ;;
    --concurrent-reads)
      require_option_value "$1" "${2-}"
      CONCURRENT_READS="$2"
      shift 2
      ;;
    --concurrent-range-readers)
      require_option_value "$1" "${2-}"
      CONCURRENT_RANGE_READERS="$2"
      shift 2
      ;;
    --concurrent-range-reader-sweep)
      require_option_value "$1" "${2-}"
      CONCURRENT_RANGE_READER_SWEEP="$2"
      shift 2
      ;;
    --concurrent-range-reads)
      require_option_value "$1" "${2-}"
      CONCURRENT_RANGE_READS="$2"
      shift 2
      ;;
    --concurrent-writers)
      require_option_value "$1" "${2-}"
      CONCURRENT_WRITERS="$2"
      shift 2
      ;;
    --concurrent-writer-sweep)
      require_option_value "$1" "${2-}"
      CONCURRENT_WRITER_SWEEP="$2"
      shift 2
      ;;
    --concurrent-writes)
      require_option_value "$1" "${2-}"
      CONCURRENT_WRITES="$2"
      shift 2
      ;;
    --mongo-mode)
      require_option_value "$1" "${2-}"
      MONGO_MODE="$2"
      shift 2
      ;;
    --mongo-uri)
      require_option_value "$1" "${2-}"
      MONGO_URI="$2"
      shift 2
      ;;
    --mongo-image)
      require_option_value "$1" "${2-}"
      MONGO_IMAGE="$2"
      shift 2
      ;;
    --mongo-compact=* )
      MONGO_COMPACT="${1#*=}"
      shift
      ;;
    --mongo-compact)
      require_option_value "$1" "${2-}"
      MONGO_COMPACT="$2"
      shift 2
      ;;
    --mongo-client-mode)
      require_option_value "$1" "${2-}"
      MONGO_CLIENT_MODE="$2"
      MONGO_CLIENT_MODES="$2"
      shift 2
      ;;
    --mongo-client-modes)
      require_option_value "$1" "${2-}"
      MONGO_CLIENT_MODES="$2"
      shift 2
      ;;
    --timeout)
      require_option_value "$1" "${2-}"
      TIMEOUT="$2"
      shift 2
      ;;
    --treedb-profile)
      require_option_value "$1" "${2-}"
      TREEDB_PROFILE="$2"
      shift 2
      ;;
    --treedb-document-format)
      require_option_value "$1" "${2-}"
      TREEDB_DOCUMENT_FORMAT="$2"
      TREEDB_DOCUMENT_FORMATS="$2"
      shift 2
      ;;
    --treedb-document-formats)
      require_option_value "$1" "${2-}"
      TREEDB_DOCUMENT_FORMATS="$2"
      shift 2
      ;;
    --treedb-client-mode)
      require_option_value "$1" "${2-}"
      TREEDB_CLIENT_MODE="$2"
      TREEDB_CLIENT_MODES="$2"
      shift 2
      ;;
    --treedb-client-modes)
      require_option_value "$1" "${2-}"
      TREEDB_CLIENT_MODES="$2"
      shift 2
      ;;
    --treedb-maintenance)
      require_option_value "$1" "${2-}"
      TREEDB_MAINTENANCE="$2"
      shift 2
      ;;
    --treedb-read-state)
      require_option_value "$1" "${2-}"
      TREEDB_READ_STATE="$2"
      shift 2
      ;;
    --title)
      require_option_value "$1" "${2-}"
      TITLE="$2"
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

mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd -P)
RAW_DIR="$OUT_DIR/raw"
TREE_DIR="$OUT_DIR/treedb_data"
MONGO_DIR="$OUT_DIR/mongodb_data"
BIN_DIR="$OUT_DIR/bin"
MATRIX="$OUT_DIR/matrix.tsv"
REPORT="$OUT_DIR/report.md"
SUMMARY="$OUT_DIR/summary.tsv"
PROFILE_DIR="$OUT_DIR/profiles"
STDOUT_LOG="$OUT_DIR/harness_stdout.log"
README="$OUT_DIR/README.md"

mkdir -p "$RAW_DIR" "$TREE_DIR" "$MONGO_DIR" "$BIN_DIR"

BENCH_BIN="$BIN_DIR/mongo_gateway_bench"
REPORT_BIN="$BIN_DIR/mongo_gateway_compare_report"
ACTIVE_CONTAINERS=()
STARTED_MONGO_CONTAINER=""
STARTED_MONGO_URI=""

cleanup() {
  local container
  for container in "${ACTIVE_CONTAINERS[@]:-}"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

is_positive_int() {
  [[ "$1" =~ ^[0-9]+$ ]] && [[ "$1" -gt 0 ]]
}

is_nonnegative_int() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

is_positive_decimal_string() {
  [[ "$1" =~ ^[0-9]+$ ]] || return 1
  local value=$1
  while [[ "$value" == 0* && ${#value} -gt 1 ]]; do
    value=${value#0}
  done
  [[ "$value" != "0" ]]
}

trim_spaces() {
  local value=$1
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

derived_count() {
  local docs=$1
  local explicit=$2
  local divisor=$3
  if [[ -n "$explicit" ]]; then
    if ! is_nonnegative_int "$explicit"; then
      echo "invalid explicit operation count: $explicit" >&2
      exit 2
    fi
    echo "$explicit"
    return
  fi
  if ! is_positive_int "$divisor"; then
    echo "invalid divisor: $divisor" >&2
    exit 2
  fi
  local value=$((docs / divisor))
  if [[ "$value" -lt 1 ]]; then
    value=1
  fi
  echo "$value"
}

du_bytes() {
  local dir=$1
  local kib
  kib=$(du -sk "$dir" | awk '{print $1}')
  echo $((kib * 1024))
}

docker_du_bytes() {
  local dir=$1
  local kib
  kib=$(docker run --rm -v "$dir:/data/db:ro" "$MONGO_IMAGE" sh -c 'du -sk /data/db' | awk '{print $1}')
  echo $((kib * 1024))
}

reset_mongo_data_dir() {
  local dir=$1
  mkdir -p "$dir"
  docker run --rm -v "$dir:/data/db" "$MONGO_IMAGE" sh -c 'find /data/db -mindepth 1 -maxdepth 1 -exec rm -rf {} +' >/dev/null
}

safe_label() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -c '[:alnum:]_.-' '_'
}

lower_word() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

normalized_label() {
  local value
  value=$(lower_word "$1")
  value="${value//-/_}"
  safe_label "$value"
}

normalize_unique_word_list() {
  local name=$1
  local values=$2
  local seen=""
  local out=""
  local count=0
  local item normalized label
  for item in $values; do
    normalized=$(lower_word "$item")
    label=$(normalized_label "$normalized")
    if [[ " $seen " == *" $label "* ]]; then
      echo "duplicate $name value after normalization: $item (label=$label)" >&2
      exit 2
    fi
    seen="$seen $label"
    if [[ -z "$out" ]]; then
      out=$normalized
    else
      out="$out $normalized"
    fi
    count=$((count + 1))
  done
  if [[ "$count" -eq 0 ]]; then
    echo "$name must contain at least one value" >&2
    exit 2
  fi
  printf '%s' "$out"
}

validate_mongo_client_modes() {
  local mode
  for mode in $1; do
    case "$mode" in
      driver|driver-find-raw|driver-command|driver-command-raw|driver-unack)
        ;;
      *)
        echo "invalid MONGO_CLIENT_MODES value: $mode (want driver, driver-find-raw, driver-command, driver-command-raw, or driver-unack)" >&2
        exit 2
        ;;
    esac
  done
}

validate_treedb_client_modes() {
  local mode
  for mode in $1; do
    case "$mode" in
      driver|driver-find-raw|driver-command|driver-command-raw|direct|raw-wire|raw-wire-tcp|raw-wire-tcp-pipeline|native-wire-inproc|native-wire-tcp)
        ;;
      driver-unack)
        echo "invalid TREEDB_CLIENT_MODES value: driver-unack (TreeDB rejects w:0 before mutation)" >&2
        exit 2
        ;;
      *)
        echo "invalid TREEDB_CLIENT_MODES value: $mode" >&2
        exit 2
        ;;
    esac
  done
}

list_word_count() {
  local count=0
  local item
  for item in $1; do
    count=$((count + 1))
  done
  echo "$count"
}

concurrent_read_kinds_include_range() {
  local kind
  for kind in ${CONCURRENT_READ_KINDS//,/ }; do
    case "$(lower_word "$kind")" in
      range|all)
        return 0
        ;;
    esac
  done
  return 1
}

mongo_config_name() {
  local client_mode
  client_mode=$(lower_word "$1")
  local client_label=$2
  local config
  if [[ "$client_mode" == "driver" ]] && [[ "$(list_word_count "$MONGO_CLIENT_MODES")" -eq 1 ]]; then
    config="mongo"
  else
    config="mongo_${client_label}"
  fi
  if [[ "$RANGE_INDEX" == "true" ]]; then
    config="${config}_range_index"
  fi
  printf '%s' "$config"
}

start_mongo_container() {
  local cell=$1
  local data_dir=$2
  local container="gomap-mongo-$(safe_label "$cell")-$$"
  STARTED_MONGO_CONTAINER=""
  STARTED_MONGO_URI=""
  reset_mongo_data_dir "$data_dir"
  echo "starting MongoDB container $container with data dir $data_dir" >&2
  docker run -d --rm \
    --name "$container" \
    -p 127.0.0.1::27017 \
    -v "$data_dir:/data/db" \
    "$MONGO_IMAGE" --quiet >/dev/null
  ACTIVE_CONTAINERS+=("$container")
  local port=""
  for _ in $(seq 1 60); do
    port=$(docker port "$container" 27017/tcp 2>/dev/null | awk -F: 'END {print $NF}')
    if [[ -n "$port" ]]; then
      break
    fi
    sleep 1
  done
  if [[ -z "$port" ]]; then
    echo "MongoDB container did not expose a port" >&2
    exit 1
  fi
  STARTED_MONGO_CONTAINER="$container"
  STARTED_MONGO_URI="mongodb://127.0.0.1:$port"
}

stop_mongo_container() {
  local container=$1
  if [[ -z "$container" ]]; then
    return
  fi
  docker rm -f "$container" >/dev/null 2>&1 || true
  local next_containers=()
  local active
  for active in "${ACTIVE_CONTAINERS[@]:-}"; do
    if [[ "$active" != "$container" ]]; then
      next_containers+=("$active")
    fi
  done
  ACTIVE_CONTAINERS=("${next_containers[@]:-}")
}

wait_for_mongo() {
  local uri=$1
  local database=$2
  for _ in $(seq 1 90); do
    if "$BENCH_BIN" \
      -target mongo \
      -mongo-uri "$uri" \
      -database "$database" \
      -collection "$COLLECTION" \
      -documents 1 \
      -reads 0 \
      -range-reads 0 \
      -updates 0 \
      -deletes 0 \
      -secondary-indexes 0 \
      -timeout 20s \
      -format json >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

run_target() {
  local target=$1
  local docs=$2
  local indexes=$3
  local raw_json=$4
  local database=$5
  local reads=$6
  local range_reads=$7
  local updates=$8
  local deletes=$9
  local concurrent_readers=${10}
  local concurrent_reads=${11}
  local concurrent_range_readers=${12}
  local concurrent_range_reads=${13}
  local concurrent_writers=${14}
  local concurrent_writes=${15}
  shift 15

  local prebuild_arg=""
  if [[ "$PREBUILD_DOCUMENTS" == "true" ]]; then
    prebuild_arg="-prebuild-documents"
  fi
  local range_index_arg=""
  if [[ "$RANGE_INDEX" == "true" ]]; then
    range_index_arg="-range-index"
  fi

  "$BENCH_BIN" \
    -target "$target" \
    -database "$database" \
    -collection "$COLLECTION" \
    -documents "$docs" \
    -batch-size "$BATCH_SIZE" \
    -insert-producers "$INSERT_PRODUCERS" \
    -mongo-max-pool-size "$MONGO_MAX_POOL_SIZE" \
    -mongo-min-pool-size "$MONGO_MIN_POOL_SIZE" \
    -mongo-max-connecting "$MONGO_MAX_CONNECTING" \
    -reads "$reads" \
    -range-reads "$range_reads" \
    -updates "$updates" \
    -deletes "$deletes" \
    -concurrent-read-kinds "$CONCURRENT_READ_KINDS" \
    -concurrent-readers "$concurrent_readers" \
    -concurrent-reader-sweep "$CONCURRENT_READER_SWEEP" \
    -concurrent-reads "$concurrent_reads" \
    -concurrent-range-readers "$concurrent_range_readers" \
    -concurrent-range-reader-sweep "$CONCURRENT_RANGE_READER_SWEEP" \
    -concurrent-range-reads "$concurrent_range_reads" \
    -concurrent-writers "$concurrent_writers" \
    -concurrent-writer-sweep "$CONCURRENT_WRITER_SWEEP" \
    -concurrent-writes "$concurrent_writes" \
    -secondary-indexes "$indexes" \
    -timeout "$TIMEOUT" \
    -format json \
    ${prebuild_arg:+"$prebuild_arg"} \
    ${range_index_arg:+"$range_index_arg"} \
    "$@" >"$raw_json"
}

if [[ "$MONGO_MODE" != "docker" && "$MONGO_MODE" != "external" ]]; then
  echo "unknown MONGO_MODE=$MONGO_MODE (want docker or external)" >&2
  exit 2
fi
if [[ "$TREEDB_READ_STATE" == "flushed" ]]; then
  TREEDB_READ_STATE=settled
fi
if [[ "$TREEDB_READ_STATE" != "settled" && "$TREEDB_READ_STATE" != "unsettled" ]]; then
  echo "unknown TREEDB_READ_STATE=$TREEDB_READ_STATE (want settled or unsettled)" >&2
  exit 2
fi
if [[ "$MONGO_MODE" == "docker" ]] && ! command -v docker >/dev/null 2>&1; then
  echo "MONGO_MODE=docker requires docker; use --mongo-mode external --mongo-uri URI to use an existing server" >&2
  exit 2
fi
if [[ -z "$MONGO_COMPACT" ]]; then
  if [[ "$MONGO_MODE" == "external" ]]; then
    MONGO_COMPACT=false
  else
    MONGO_COMPACT=true
  fi
fi
if ! is_positive_int "$INSERT_PRODUCERS"; then
  echo "invalid INSERT_PRODUCERS=$INSERT_PRODUCERS (want positive integer)" >&2
  exit 2
fi
for value_name in DELETES CONCURRENT_READERS CONCURRENT_RANGE_READERS CONCURRENT_WRITERS MONGO_MAX_POOL_SIZE MONGO_MIN_POOL_SIZE MONGO_MAX_CONNECTING; do
  value=${!value_name}
  if ! is_nonnegative_int "$value"; then
    echo "invalid $value_name=$value (want non-negative integer)" >&2
    exit 2
  fi
done
effective_mongo_max_pool_size="$MONGO_MAX_POOL_SIZE"
if [[ "$effective_mongo_max_pool_size" -eq 0 ]]; then
  effective_mongo_max_pool_size=100
fi
if [[ "$MONGO_MIN_POOL_SIZE" -gt "$effective_mongo_max_pool_size" ]]; then
  echo "invalid MONGO_MIN_POOL_SIZE=$MONGO_MIN_POOL_SIZE (must be <= effective maxPoolSize $effective_mongo_max_pool_size)" >&2
  exit 2
fi
if [[ "$PREBUILD_DOCUMENTS" != "true" && "$PREBUILD_DOCUMENTS" != "false" ]]; then
  echo "invalid PREBUILD_DOCUMENTS=$PREBUILD_DOCUMENTS (want true or false)" >&2
  exit 2
fi
if [[ "$RANGE_INDEX" != "true" && "$RANGE_INDEX" != "false" ]]; then
  echo "invalid RANGE_INDEX=$RANGE_INDEX (want true or false)" >&2
  exit 2
fi
if [[ "$PROFILE_TREEDB" != "true" && "$PROFILE_TREEDB" != "false" ]]; then
  echo "invalid PROFILE_TREEDB=$PROFILE_TREEDB (want true or false)" >&2
  exit 2
fi
case "$MONGO_COMPACT" in
  true|false)
    ;;
  1|yes|YES|Yes|TRUE|True)
    MONGO_COMPACT=true
    ;;
  0|no|NO|No|FALSE|False)
    MONGO_COMPACT=false
    ;;
  *)
    echo "invalid MONGO_COMPACT=$MONGO_COMPACT (want true/false, 1/0, or yes/no)" >&2
    exit 2
    ;;
esac
MONGO_CLIENT_MODES=$(normalize_unique_word_list MONGO_CLIENT_MODES "$MONGO_CLIENT_MODES")
validate_mongo_client_modes "$MONGO_CLIENT_MODES"
TREEDB_CLIENT_MODES=$(normalize_unique_word_list TREEDB_CLIENT_MODES "$TREEDB_CLIENT_MODES")
validate_treedb_client_modes "$TREEDB_CLIENT_MODES"
TREEDB_DOCUMENT_FORMATS=$(normalize_unique_word_list TREEDB_DOCUMENT_FORMATS "$TREEDB_DOCUMENT_FORMATS")
raw_concurrent_reader_sweep=$CONCURRENT_READER_SWEEP
CONCURRENT_READER_SWEEP=$(trim_spaces "$CONCURRENT_READER_SWEEP")
if [[ -n "$raw_concurrent_reader_sweep" && -z "$CONCURRENT_READER_SWEEP" ]]; then
  echo "CONCURRENT_READER_SWEEP must contain at least one positive integer" >&2
  exit 2
fi
if [[ -n "$CONCURRENT_READER_SWEEP" && "$CONCURRENT_READERS" -gt 0 ]]; then
  echo "CONCURRENT_READER_SWEEP cannot be combined with CONCURRENT_READERS" >&2
  exit 2
fi
if [[ -n "$CONCURRENT_READER_SWEEP" ]]; then
  seen_reader_counts=""
  normalized_reader_sweep=""
  validated_reader_counts=0
  for reader_count in ${CONCURRENT_READER_SWEEP//,/ }; do
    if ! is_positive_decimal_string "$reader_count"; then
      echo "invalid CONCURRENT_READER_SWEEP value: $reader_count" >&2
      exit 2
    fi
    normalized_reader_count=$reader_count
    while [[ "$normalized_reader_count" == 0* && ${#normalized_reader_count} -gt 1 ]]; do
      normalized_reader_count=${normalized_reader_count#0}
    done
    if [[ " $seen_reader_counts " == *" $normalized_reader_count "* ]]; then
      echo "duplicate CONCURRENT_READER_SWEEP value: $reader_count" >&2
      exit 2
    fi
    seen_reader_counts="$seen_reader_counts $normalized_reader_count"
    if [[ -z "$normalized_reader_sweep" ]]; then
      normalized_reader_sweep=$normalized_reader_count
    else
      normalized_reader_sweep="$normalized_reader_sweep,$normalized_reader_count"
    fi
    validated_reader_counts=$((validated_reader_counts + 1))
  done
  if [[ "$validated_reader_counts" -eq 0 ]]; then
    echo "CONCURRENT_READER_SWEEP must contain at least one positive integer" >&2
    exit 2
  fi
  if [[ -n "$CONCURRENT_READS" && "$CONCURRENT_READS" == "0" ]]; then
    echo "CONCURRENT_READER_SWEEP requires CONCURRENT_READS > 0 when CONCURRENT_READS is set" >&2
    exit 2
  fi
  if [[ -n "$CONCURRENT_READS" ]]; then
    if ! is_nonnegative_int "$CONCURRENT_READS"; then
      echo "invalid CONCURRENT_READS=$CONCURRENT_READS (want non-negative integer)" >&2
      exit 2
    fi
  fi
  CONCURRENT_READER_SWEEP=$normalized_reader_sweep
elif [[ "$CONCURRENT_READERS" -eq 0 && -n "$CONCURRENT_READS" && "$CONCURRENT_READS" != "0" ]]; then
  echo "CONCURRENT_READERS or CONCURRENT_READER_SWEEP must be set when CONCURRENT_READS is set" >&2
  exit 2
fi

raw_concurrent_range_reader_sweep=$CONCURRENT_RANGE_READER_SWEEP
CONCURRENT_RANGE_READER_SWEEP=$(trim_spaces "$CONCURRENT_RANGE_READER_SWEEP")
if [[ -n "$raw_concurrent_range_reader_sweep" && -z "$CONCURRENT_RANGE_READER_SWEEP" ]]; then
  echo "CONCURRENT_RANGE_READER_SWEEP must contain at least one positive integer" >&2
  exit 2
fi
if [[ -n "$CONCURRENT_RANGE_READER_SWEEP" && "$CONCURRENT_RANGE_READERS" -gt 0 ]]; then
  echo "CONCURRENT_RANGE_READER_SWEEP cannot be combined with CONCURRENT_RANGE_READERS" >&2
  exit 2
fi
if [[ -n "$CONCURRENT_RANGE_READER_SWEEP" ]]; then
  seen_range_reader_counts=""
  normalized_range_reader_sweep=""
  validated_range_reader_counts=0
  for reader_count in ${CONCURRENT_RANGE_READER_SWEEP//,/ }; do
    if ! is_positive_decimal_string "$reader_count"; then
      echo "invalid CONCURRENT_RANGE_READER_SWEEP value: $reader_count" >&2
      exit 2
    fi
    normalized_reader_count=$reader_count
    while [[ "$normalized_reader_count" == 0* && ${#normalized_reader_count} -gt 1 ]]; do
      normalized_reader_count=${normalized_reader_count#0}
    done
    if [[ " $seen_range_reader_counts " == *" $normalized_reader_count "* ]]; then
      echo "duplicate CONCURRENT_RANGE_READER_SWEEP value: $reader_count" >&2
      exit 2
    fi
    seen_range_reader_counts="$seen_range_reader_counts $normalized_reader_count"
    if [[ -z "$normalized_range_reader_sweep" ]]; then
      normalized_range_reader_sweep=$normalized_reader_count
    else
      normalized_range_reader_sweep="$normalized_range_reader_sweep,$normalized_reader_count"
    fi
    validated_range_reader_counts=$((validated_range_reader_counts + 1))
  done
  if [[ "$validated_range_reader_counts" -eq 0 ]]; then
    echo "CONCURRENT_RANGE_READER_SWEEP must contain at least one positive integer" >&2
    exit 2
  fi
  if [[ -n "$CONCURRENT_RANGE_READS" && "$CONCURRENT_RANGE_READS" == "0" ]]; then
    echo "CONCURRENT_RANGE_READER_SWEEP requires CONCURRENT_RANGE_READS > 0 when CONCURRENT_RANGE_READS is set" >&2
    exit 2
  fi
  if [[ -n "$CONCURRENT_RANGE_READS" ]]; then
    if ! is_nonnegative_int "$CONCURRENT_RANGE_READS"; then
      echo "invalid CONCURRENT_RANGE_READS=$CONCURRENT_RANGE_READS (want non-negative integer)" >&2
      exit 2
    fi
  fi
  CONCURRENT_RANGE_READER_SWEEP=$normalized_range_reader_sweep
elif [[ "$CONCURRENT_RANGE_READERS" -eq 0 && -n "$CONCURRENT_RANGE_READS" && "$CONCURRENT_RANGE_READS" != "0" ]]; then
  echo "CONCURRENT_RANGE_READERS or CONCURRENT_RANGE_READER_SWEEP must be set when CONCURRENT_RANGE_READS is set" >&2
  exit 2
fi
if concurrent_read_kinds_include_range &&
  [[ "$CONCURRENT_READERS" -gt 0 || -n "$CONCURRENT_READER_SWEEP" ]] &&
  [[ "$CONCURRENT_RANGE_READERS" -gt 0 || -n "$CONCURRENT_RANGE_READER_SWEEP" ]]; then
  echo "CONCURRENT_READ_KINDS=range/all cannot be combined with CONCURRENT_RANGE_READERS or CONCURRENT_RANGE_READER_SWEEP when concurrent readers are enabled" >&2
  exit 2
fi

raw_concurrent_writer_sweep=$CONCURRENT_WRITER_SWEEP
CONCURRENT_WRITER_SWEEP=$(trim_spaces "$CONCURRENT_WRITER_SWEEP")
if [[ -n "$raw_concurrent_writer_sweep" && -z "$CONCURRENT_WRITER_SWEEP" ]]; then
  echo "CONCURRENT_WRITER_SWEEP must contain at least one positive integer" >&2
  exit 2
fi
if [[ -n "$CONCURRENT_WRITER_SWEEP" && "$CONCURRENT_WRITERS" -gt 0 ]]; then
  echo "CONCURRENT_WRITER_SWEEP cannot be combined with CONCURRENT_WRITERS" >&2
  exit 2
fi
if [[ -n "$CONCURRENT_WRITER_SWEEP" ]]; then
  seen_writer_counts=""
  normalized_writer_sweep=""
  validated_writer_counts=0
  for writer_count in ${CONCURRENT_WRITER_SWEEP//,/ }; do
    if ! is_positive_decimal_string "$writer_count"; then
      echo "invalid CONCURRENT_WRITER_SWEEP value: $writer_count" >&2
      exit 2
    fi
    normalized_writer_count=$writer_count
    while [[ "$normalized_writer_count" == 0* && ${#normalized_writer_count} -gt 1 ]]; do
      normalized_writer_count=${normalized_writer_count#0}
    done
    if [[ " $seen_writer_counts " == *" $normalized_writer_count "* ]]; then
      echo "duplicate CONCURRENT_WRITER_SWEEP value: $writer_count" >&2
      exit 2
    fi
    seen_writer_counts="$seen_writer_counts $normalized_writer_count"
    if [[ -z "$normalized_writer_sweep" ]]; then
      normalized_writer_sweep=$normalized_writer_count
    else
      normalized_writer_sweep="$normalized_writer_sweep,$normalized_writer_count"
    fi
    validated_writer_counts=$((validated_writer_counts + 1))
  done
  if [[ "$validated_writer_counts" -eq 0 ]]; then
    echo "CONCURRENT_WRITER_SWEEP must contain at least one positive integer" >&2
    exit 2
  fi
  if [[ -n "$CONCURRENT_WRITES" && "$CONCURRENT_WRITES" == "0" ]]; then
    echo "CONCURRENT_WRITER_SWEEP requires CONCURRENT_WRITES > 0 when CONCURRENT_WRITES is set" >&2
    exit 2
  fi
  if [[ -n "$CONCURRENT_WRITES" ]]; then
    if ! is_nonnegative_int "$CONCURRENT_WRITES"; then
      echo "invalid CONCURRENT_WRITES=$CONCURRENT_WRITES (want non-negative integer)" >&2
      exit 2
    fi
  fi
  CONCURRENT_WRITER_SWEEP=$normalized_writer_sweep
elif [[ "$CONCURRENT_WRITERS" -eq 0 && -n "$CONCURRENT_WRITES" && "$CONCURRENT_WRITES" != "0" ]]; then
  echo "CONCURRENT_WRITERS or CONCURRENT_WRITER_SWEEP must be set when CONCURRENT_WRITES is set" >&2
  exit 2
fi

{
  echo "running Mongo gateway comparison into: $OUT_DIR"
  echo "docs list: $DOCS_LIST"
  echo "secondary-index list: $INDEXES_LIST"
  echo "batch size: $BATCH_SIZE"
  echo "insert producers: $INSERT_PRODUCERS"
  echo "mongo pool options: maxPoolSize=$MONGO_MAX_POOL_SIZE minPoolSize=$MONGO_MIN_POOL_SIZE maxConnecting=$MONGO_MAX_CONNECTING"
  echo "prebuild documents: $PREBUILD_DOCUMENTS"
  echo "mongo compact before final stats: $MONGO_COMPACT"
  echo "range index: $RANGE_INDEX"
  echo "profile TreeDB: $PROFILE_TREEDB"
  echo "reads: ${READS:-documents / $READS_DIVISOR}"
  echo "range reads: ${RANGE_READS:-documents / $RANGE_READS_DIVISOR}"
  echo "updates: ${UPDATES:-documents / $UPDATES_DIVISOR}"
  echo "deletes: $DELETES"
  echo "concurrent read kinds: $CONCURRENT_READ_KINDS"
  echo "concurrent readers: $CONCURRENT_READERS"
  echo "concurrent reader sweep: ${CONCURRENT_READER_SWEEP:-none}"
  echo "concurrent reads: ${CONCURRENT_READS:-documents / $CONCURRENT_READS_DIVISOR when readers or reader sweep is set}"
  echo "concurrent range readers: $CONCURRENT_RANGE_READERS"
  echo "concurrent range reader sweep: ${CONCURRENT_RANGE_READER_SWEEP:-none}"
  echo "concurrent range reads: ${CONCURRENT_RANGE_READS:-documents / $CONCURRENT_RANGE_READS_DIVISOR when range readers or range reader sweep is set}"
  echo "concurrent writers: $CONCURRENT_WRITERS"
  echo "concurrent writer sweep: ${CONCURRENT_WRITER_SWEEP:-none}"
  echo "concurrent writes: ${CONCURRENT_WRITES:-documents / $CONCURRENT_WRITES_DIVISOR when writers or writer sweep is set}"
  echo "mongo mode: $MONGO_MODE"
  echo "mongo client modes: $MONGO_CLIENT_MODES"
  echo "treedb profile: $TREEDB_PROFILE"
  echo "treedb document formats: $TREEDB_DOCUMENT_FORMATS"
  echo "treedb client modes: $TREEDB_CLIENT_MODES"
  echo "treedb root storage: data=$TREEDB_DATA_ROOT_STORAGE index_state=$TREEDB_INDEX_STATE_ROOT_STORAGE index=$TREEDB_INDEX_ROOT_STORAGE"
  echo "treedb maintenance: $TREEDB_MAINTENANCE"
  echo "treedb read state: $TREEDB_READ_STATE"
  if [[ "$MONGO_MODE" == "docker" ]]; then
    echo "mongo image: $MONGO_IMAGE"
  else
    echo "mongo uri: $MONGO_URI"
  fi
  echo "benchmark timeout: $TIMEOUT"
} | tee "$STDOUT_LOG"

GOWORK=off go build -o "$BENCH_BIN" ./cmd/mongo_gateway_bench
GOWORK=off go build -o "$REPORT_BIN" ./cmd/mongo_gateway_compare_report

printf "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n" >"$MATRIX"

for docs in $DOCS_LIST; do
  if ! is_positive_int "$docs"; then
    echo "invalid document count: $docs" >&2
    exit 2
  fi
  reads=$(derived_count "$docs" "$READS" "$READS_DIVISOR")
  range_reads=$(derived_count "$docs" "$RANGE_READS" "$RANGE_READS_DIVISOR")
  updates=$(derived_count "$docs" "$UPDATES" "$UPDATES_DIVISOR")
  concurrent_reads=0
  if [[ "$CONCURRENT_READERS" -gt 0 || -n "$CONCURRENT_READER_SWEEP" ]]; then
    concurrent_reads=$(derived_count "$docs" "$CONCURRENT_READS" "$CONCURRENT_READS_DIVISOR")
    if [[ "$concurrent_reads" -eq 0 ]]; then
      echo "concurrent reads must be > 0 when concurrent readers or reader sweep is set" >&2
      exit 2
    fi
  fi
  concurrent_range_reads=0
  if [[ "$CONCURRENT_RANGE_READERS" -gt 0 || -n "$CONCURRENT_RANGE_READER_SWEEP" ]]; then
    concurrent_range_reads=$(derived_count "$docs" "$CONCURRENT_RANGE_READS" "$CONCURRENT_RANGE_READS_DIVISOR")
    if [[ "$concurrent_range_reads" -eq 0 ]]; then
      echo "concurrent range reads must be > 0 when concurrent range readers or range reader sweep is set" >&2
      exit 2
    fi
  fi
  concurrent_writes=0
  if [[ "$CONCURRENT_WRITERS" -gt 0 || -n "$CONCURRENT_WRITER_SWEEP" ]]; then
    concurrent_writes=$(derived_count "$docs" "$CONCURRENT_WRITES" "$CONCURRENT_WRITES_DIVISOR")
    if [[ "$concurrent_writes" -eq 0 ]]; then
      echo "concurrent writes must be > 0 when concurrent writers or writer sweep is set" >&2
      exit 2
    fi
  fi
  for indexes in $INDEXES_LIST; do
    if [[ ! "$indexes" =~ ^[0-9]+$ ]]; then
      echo "invalid index count: $indexes" >&2
      exit 2
    fi
    cell="docs_${docs}_idx_${indexes}"
    database="${DATABASE_PREFIX}_${cell}"
    mongo_data="$MONGO_DIR/$cell"

    for tree_format in $TREEDB_DOCUMENT_FORMATS; do
      for tree_client_mode in $TREEDB_CLIENT_MODES; do
        format_label=$(normalized_label "$tree_format")
        client_label=$(normalized_label "$tree_client_mode")
        tree_config="treedb_${format_label}_${client_label}"
        if [[ "$RANGE_INDEX" == "true" ]]; then
          tree_config="${tree_config}_range_index"
        fi
        tree_raw_rel="raw/${tree_config}_${cell}.json"
        tree_raw="$OUT_DIR/$tree_raw_rel"
        tree_data="$TREE_DIR/${tree_config}_${cell}"
        tree_profile_args=()
        if [[ "$PROFILE_TREEDB" == "true" ]]; then
          tree_profile_dir="$PROFILE_DIR/${tree_config}_${cell}"
          rm -rf "$tree_profile_dir"
          mkdir -p "$tree_profile_dir"
          tree_profile_args=(-profile-dir "$tree_profile_dir")
        fi

        echo
        echo "==> $cell TreeDB ($tree_format, client=$tree_client_mode)"
        run_target treedb "$docs" "$indexes" "$tree_raw" "$database" "$reads" "$range_reads" "$updates" "$DELETES" \
          "$CONCURRENT_READERS" "$concurrent_reads" "$CONCURRENT_RANGE_READERS" "$concurrent_range_reads" "$CONCURRENT_WRITERS" "$concurrent_writes" \
          -treedb-dir "$tree_data" \
          -keep-treedb-dir \
          -treedb-profile "$TREEDB_PROFILE" \
          -treedb-document-format "$tree_format" \
          -client-mode "$tree_client_mode" \
          -treedb-data-root-storage "$TREEDB_DATA_ROOT_STORAGE" \
          -treedb-index-state-root-storage "$TREEDB_INDEX_STATE_ROOT_STORAGE" \
          -treedb-index-root-storage "$TREEDB_INDEX_ROOT_STORAGE" \
          -treedb-maintenance "$TREEDB_MAINTENANCE" \
          -treedb-read-state "$TREEDB_READ_STATE" \
          "${tree_profile_args[@]:-}"
        tree_physical=$(du_bytes "$tree_data")
        printf "treedb\t%s\t%s\t%s\t%s\t%s\n" "$tree_config" "$docs" "$indexes" "$tree_raw_rel" "$tree_physical" >>"$MATRIX"
      done
    done

    for mongo_client_mode in $MONGO_CLIENT_MODES; do
      mongo_client_label=$(normalized_label "$mongo_client_mode")
      mongo_config=$(mongo_config_name "$mongo_client_mode" "$mongo_client_label")
      mongo_raw_rel="raw/${mongo_config}_${cell}.json"
      mongo_raw="$OUT_DIR/$mongo_raw_rel"
      mongo_cell_data="$mongo_data/$mongo_client_label"

      echo "==> $cell MongoDB (client=$mongo_client_mode)"
      mongo_uri="$MONGO_URI"
      mongo_container=""
      if [[ "$MONGO_MODE" == "docker" ]]; then
        start_mongo_container "${cell}_${mongo_client_label}" "$mongo_cell_data"
        mongo_uri="$STARTED_MONGO_URI"
        mongo_container="$STARTED_MONGO_CONTAINER"
        if ! wait_for_mongo "$mongo_uri" "$database"; then
          echo "MongoDB container did not become ready for $cell client=$mongo_client_mode" >&2
          exit 1
        fi
      fi
    run_target mongo "$docs" "$indexes" "$mongo_raw" "$database" "$reads" "$range_reads" "$updates" "$DELETES" \
        "$CONCURRENT_READERS" "$concurrent_reads" "$CONCURRENT_RANGE_READERS" "$concurrent_range_reads" "$CONCURRENT_WRITERS" "$concurrent_writes" \
        -mongo-uri "$mongo_uri" \
        -mongo-compact="$MONGO_COMPACT" \
      -client-mode "$mongo_client_mode"
      if [[ "$MONGO_MODE" == "docker" ]]; then
        stop_mongo_container "$mongo_container"
        mongo_physical=$(docker_du_bytes "$mongo_cell_data")
      else
        mongo_physical=0
      fi
      printf "mongo\t%s\t%s\t%s\t%s\t%s\n" "$mongo_config" "$docs" "$indexes" "$mongo_raw_rel" "$mongo_physical" >>"$MATRIX"
    done
  done
done

"$REPORT_BIN" \
  -matrix "$MATRIX" \
  -report "$REPORT" \
  -summary "$SUMMARY" \
  -title "$TITLE"

cat >"$README" <<EOF
# Mongo Gateway Comparison Bundle

- output directory: \`$OUT_DIR\`
- report: \`$REPORT\`
- summary TSV: \`$SUMMARY\`
- matrix TSV: \`$MATRIX\`
- raw JSON directory: \`$RAW_DIR\`
- TreeDB data directory: \`$TREE_DIR\`
- MongoDB data directory: \`$MONGO_DIR\`
- docs list: \`$DOCS_LIST\`
- secondary-index list: \`$INDEXES_LIST\`
- batch size: \`$BATCH_SIZE\`
- insert producers: \`$INSERT_PRODUCERS\`
- MongoDB Go driver pool options: \`maxPoolSize=$MONGO_MAX_POOL_SIZE minPoolSize=$MONGO_MIN_POOL_SIZE maxConnecting=$MONGO_MAX_CONNECTING\`
- prebuild documents: \`$PREBUILD_DOCUMENTS\`
- range index: \`$RANGE_INDEX\`
- TreeDB pprof profiles: \`$PROFILE_TREEDB\`
- profile directory: \`$PROFILE_DIR\`
- reads: \`${READS:-documents / $READS_DIVISOR}\`
- range reads: \`${RANGE_READS:-documents / $RANGE_READS_DIVISOR}\`
- updates: \`${UPDATES:-documents / $UPDATES_DIVISOR}\`
- deletes: \`$DELETES\`
- concurrent read kinds: \`$CONCURRENT_READ_KINDS\`
- concurrent readers: \`$CONCURRENT_READERS\`
- concurrent reader sweep: \`${CONCURRENT_READER_SWEEP:-none}\`
- concurrent reads: \`${CONCURRENT_READS:-documents / $CONCURRENT_READS_DIVISOR when readers or reader sweep is set}\`
- concurrent range readers: \`$CONCURRENT_RANGE_READERS\`
- concurrent range reader sweep: \`${CONCURRENT_RANGE_READER_SWEEP:-none}\`
- concurrent range reads: \`${CONCURRENT_RANGE_READS:-documents / $CONCURRENT_RANGE_READS_DIVISOR when range readers or range reader sweep is set}\`
- concurrent writers: \`$CONCURRENT_WRITERS\`
- concurrent writer sweep: \`${CONCURRENT_WRITER_SWEEP:-none}\`
- concurrent writes: \`${CONCURRENT_WRITES:-documents / $CONCURRENT_WRITES_DIVISOR when writers or writer sweep is set}\`
- MongoDB mode: \`$MONGO_MODE\`
- MongoDB compact before final stats: \`$MONGO_COMPACT\`
- MongoDB image: \`$MONGO_IMAGE\`
- MongoDB client modes: \`$MONGO_CLIENT_MODES\`
- benchmark timeout: \`$TIMEOUT\`
- TreeDB profile: \`$TREEDB_PROFILE\`
- TreeDB document formats: \`$TREEDB_DOCUMENT_FORMATS\`
- TreeDB client modes: \`$TREEDB_CLIENT_MODES\`
- TreeDB root storage: \`data=$TREEDB_DATA_ROOT_STORAGE index_state=$TREEDB_INDEX_STATE_ROOT_STORAGE index=$TREEDB_INDEX_ROOT_STORAGE\`
- TreeDB maintenance: \`$TREEDB_MAINTENANCE\`
- TreeDB read state: \`$TREEDB_READ_STATE\`

Regenerate from the raw run index:

\`\`\`sh
GOWORK=off go run ./cmd/mongo_gateway_compare_report \\
  -matrix "$MATRIX" \\
  -report "$REPORT" \\
  -summary "$SUMMARY" \\
  -title "$TITLE"
\`\`\`
EOF

echo
echo "comparison report: $REPORT"
echo "summary TSV: $SUMMARY"
echo "bundle README: $README"
