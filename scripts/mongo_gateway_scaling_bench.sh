#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$ROOT"

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "$TMP_BASE/gomap_mongo_gateway_scaling_XXXXXX")}"
DOCS="${DOCS:-100000}"
INDEXES="${INDEXES:-2}"
BATCH_SIZE="${BATCH_SIZE:-10000}"
INSERT_PRODUCERS="${INSERT_PRODUCERS:-8}"
WRITERS_LIST="${WRITERS_LIST:-1 2 4 8 16 32}"
READERS_LIST="${READERS_LIST:-1 2 4 8 16}"
RUN_READER_SWEEP="${RUN_READER_SWEEP:-true}"
CONCURRENT_WRITES="${CONCURRENT_WRITES:-80000}"
CONCURRENT_READS="${CONCURRENT_READS:-80000}"
READS="${READS:-0}"
RANGE_READS="${RANGE_READS:-0}"
UPDATES="${UPDATES:-0}"
DELETES="${DELETES:-0}"
UPDATE_INDEXED_FIELD="${UPDATE_INDEXED_FIELD:-false}"
# Indexed benchmark baseline: the default shape uses secondary indexes. Use
# command_wal_relaxed for command-WAL coverage once indexed catalog command
# support is enabled for this workload; bench_unsafe is the explicit no-WAL ceiling.
TREEDB_PROFILE="${TREEDB_PROFILE:-bench_unsafe}"
TREEDB_DOCUMENT_FORMAT="${TREEDB_DOCUMENT_FORMAT:-bson}"
TREEDB_CLIENT_MODE="${TREEDB_CLIENT_MODE:-driver-command-raw}"
TREEDB_MAINTENANCE="${TREEDB_MAINTENANCE:-none}"
TREEDB_DATA_ROOT_STORAGE="${TREEDB_DATA_ROOT_STORAGE:-compressed}"
TREEDB_INDEX_STATE_ROOT_STORAGE="${TREEDB_INDEX_STATE_ROOT_STORAGE:-compressed}"
TREEDB_INDEX_ROOT_STORAGE="${TREEDB_INDEX_ROOT_STORAGE:-compressed}"
MONGO_MAX_POOL_SIZE="${MONGO_MAX_POOL_SIZE:-64}"
MONGO_MIN_POOL_SIZE="${MONGO_MIN_POOL_SIZE:-0}"
MONGO_MAX_CONNECTING="${MONGO_MAX_CONNECTING:-16}"
PREBUILD_DOCUMENTS="${PREBUILD_DOCUMENTS:-true}"
INCLUDE_MONGO="${INCLUDE_MONGO:-0}"
MONGO_MODE="${MONGO_MODE:-docker}"
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
MONGO_IMAGE="${MONGO_IMAGE:-mongo:8}"
MONGO_COMPACT="${MONGO_COMPACT:-}"
DATABASE_PREFIX="${DATABASE_PREFIX:-}"
COLLECTION="${COLLECTION:-docs}"
TIMEOUT="${TIMEOUT:-60m}"
TITLE="${TITLE:-Mongo Gateway Reader/Writer Scaling}"

usage() {
  cat <<'EOF'
Usage: scripts/mongo_gateway_scaling_bench.sh [options]

Runs a TreeDB Mongo gateway scaling matrix over concurrent reader and writer
counts, writes raw mongo_gateway_bench JSON for every cell, and generates the
standard mongo_gateway_compare_report Markdown/TSV output.

Options:
  --out DIR              Output directory; if it exists, it must be empty. Default: mktemp under $TMPDIR or /tmp.
  --docs N               Document count. Default: 100000.
  --indexes N            Secondary index count. Default: 2.
  --batch-size N         Insert batch size. Default: 10000.
  --insert-producers N   Insert load producers. Default: 8.
  --writers LIST         Quoted space-separated concurrent writer counts (for example: "1 2 4"). Default: "1 2 4 8 16 32".
  --readers LIST         Quoted space-separated concurrent reader counts (for example: "1 2 4"). Default: "1 2 4 8 16".
  --no-reader-sweep      Run writer-scaling cells only.
  --concurrent-writes N  Total updates per writer-scaling cell. Default: 80000.
  --concurrent-reads N   Total reads per reader-scaling cell. Default: 80000.
  --include-mongo        Also run each cell against MongoDB (docker or external based on --mongo-mode).
  --mongo-uri URI        MongoDB URI for --mongo-mode external. Default: mongodb://127.0.0.1:27017.
  --mongo-mode MODE      MongoDB mode for mongo runs: docker or external. Default: docker.
  --mongo-image IMAGE    Docker image for --mongo-mode docker. Default: mongo:8.
  --mongo-compact BOOL  Compact the MongoDB collection before final stats collection.
                        Set to true/false, 1/0, or yes/no.
                        Default: true for docker mode; false for external mode unless explicitly set.
  --database-prefix NAME MongoDB database prefix. Default: mongo_gateway_scaling_<run_id>.
  --treedb-format NAME   TreeDB document format. Default: bson.
  --client-mode NAME     TreeDB client mode. Default: driver-command-raw.
  --profile NAME         TreeDB profile. Default: bench_unsafe.
  --maintenance MODE     TreeDB maintenance mode. Default: none.
  --update-indexed-field Include the city field in update phases to exercise
                         secondary-index maintenance; requires INDEXES=2.
  --timeout DURATION     Per-cell timeout. Default: 60m.
  --title TITLE          Report title.
  --help                 Show this help.

Environment overrides use the uppercase variable names shown in the script:
OUT_DIR, DOCS, INDEXES, BATCH_SIZE, INSERT_PRODUCERS, WRITERS_LIST,
READERS_LIST, CONCURRENT_WRITES, CONCURRENT_READS, INCLUDE_MONGO (0/1, true/false, or yes/no), MONGO_MODE, MONGO_URI, MONGO_IMAGE, MONGO_COMPACT, DATABASE_PREFIX,
TREEDB_DOCUMENT_FORMAT, TREEDB_CLIENT_MODE, TREEDB_PROFILE,
TREEDB_MAINTENANCE, UPDATE_INDEXED_FIELD (0/1, true/false, or yes/no), RUN_READER_SWEEP (0/1, true/false, or yes/no), GOWORK, TIMEOUT, TITLE,
and related storage/pool settings.
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

normalize_bool_01() {
  local name=$1
  local raw=$2
  local value
  value=$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')
  case "$value" in
    1|true|yes)
      printf '1'
      ;;
    0|false|no)
      printf '0'
      ;;
    *)
      echo "$name must be 0/1, true/false, or yes/no; got: $raw" >&2
      usage >&2
      exit 2
      ;;
  esac
}

bool_01_text() {
  case "$1" in
    1)
      printf 'true'
      ;;
    *)
      printf 'false'
      ;;
  esac
}

safe_label() {
  printf '%s' "$1" | tr -c '[:alnum:]_-' '_'
}

mongo_database_prefix_label() {
  local value
  value=$(safe_label "$1")
  if [[ -z "$value" ]]; then
    value="mongo_gateway_scaling"
  fi
  printf '%.48s' "$value"
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
      DOCS="$2"
      shift 2
      ;;
    --indexes)
      require_option_value "$1" "${2-}"
      INDEXES="$2"
      shift 2
      ;;
    --batch-size)
      require_option_value "$1" "${2-}"
      BATCH_SIZE="$2"
      shift 2
      ;;
    --insert-producers)
      require_option_value "$1" "${2-}"
      INSERT_PRODUCERS="$2"
      shift 2
      ;;
    --writers)
      require_option_value "$1" "${2-}"
      WRITERS_LIST="$2"
      shift 2
      ;;
    --readers)
      require_option_value "$1" "${2-}"
      READERS_LIST="$2"
      shift 2
      ;;
    --no-reader-sweep)
      RUN_READER_SWEEP=false
      shift
      ;;
    --concurrent-writes)
      require_option_value "$1" "${2-}"
      CONCURRENT_WRITES="$2"
      shift 2
      ;;
    --concurrent-reads)
      require_option_value "$1" "${2-}"
      CONCURRENT_READS="$2"
      shift 2
      ;;
    --include-mongo)
      INCLUDE_MONGO=1
      shift
      ;;
    --mongo-uri)
      require_option_value "$1" "${2-}"
      MONGO_URI="$2"
      shift 2
      ;;
    --mongo-mode)
      require_option_value "$1" "${2-}"
      MONGO_MODE="$2"
      shift 2
      ;;
    --mongo-image)
      require_option_value "$1" "${2-}"
      MONGO_IMAGE="$2"
      shift 2
      ;;
    --mongo-compact=*)
      MONGO_COMPACT="${1#*=}"
      shift
      ;;
    --mongo-compact)
      require_option_value "$1" "${2-}"
      MONGO_COMPACT="$2"
      shift 2
      ;;
    --database-prefix)
      require_option_value "$1" "${2-}"
      DATABASE_PREFIX="$2"
      shift 2
      ;;
    --treedb-format)
      require_option_value "$1" "${2-}"
      TREEDB_DOCUMENT_FORMAT="$2"
      shift 2
      ;;
    --client-mode)
      require_option_value "$1" "${2-}"
      TREEDB_CLIENT_MODE="$2"
      shift 2
      ;;
    --profile)
      require_option_value "$1" "${2-}"
      TREEDB_PROFILE="$2"
      shift 2
      ;;
    --maintenance)
      require_option_value "$1" "${2-}"
      TREEDB_MAINTENANCE="$2"
      shift 2
      ;;
    --update-indexed-field)
      UPDATE_INDEXED_FIELD=true
      shift
      ;;
    --timeout)
      require_option_value "$1" "${2-}"
      TIMEOUT="$2"
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

is_nonnegative_int() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

is_positive_int() {
  [[ "$1" =~ ^[0-9]+$ ]] && [[ "$1" -gt 0 ]]
}

for value_name in DOCS BATCH_SIZE INSERT_PRODUCERS CONCURRENT_WRITES CONCURRENT_READS; do
  value=${!value_name}
  if ! is_positive_int "$value"; then
    echo "invalid $value_name=$value (want positive integer)" >&2
    exit 2
  fi
done
for value_name in MONGO_MAX_POOL_SIZE MONGO_MIN_POOL_SIZE MONGO_MAX_CONNECTING; do
  value=${!value_name}
  if ! is_nonnegative_int "$value"; then
    echo "invalid $value_name=$value (want non-negative integer)" >&2
    exit 2
  fi
done
for value_name in INDEXES READS RANGE_READS UPDATES DELETES; do
  value=${!value_name}
  if ! is_nonnegative_int "$value"; then
    echo "invalid $value_name=$value (want non-negative integer)" >&2
    exit 2
  fi
done
for writers in $WRITERS_LIST; do
  if ! is_positive_int "$writers"; then
    echo "invalid writer count: $writers" >&2
    exit 2
  fi
done
for readers in $READERS_LIST; do
  if ! is_positive_int "$readers"; then
    echo "invalid reader count: $readers" >&2
    exit 2
  fi
done
if [[ "$PREBUILD_DOCUMENTS" != "true" && "$PREBUILD_DOCUMENTS" != "false" ]]; then
  echo "invalid PREBUILD_DOCUMENTS=$PREBUILD_DOCUMENTS (want true or false)" >&2
  exit 2
fi
UPDATE_INDEXED_FIELD=$(normalize_bool_01 UPDATE_INDEXED_FIELD "$UPDATE_INDEXED_FIELD")
case "$UPDATE_INDEXED_FIELD" in
  1)
    if (( 10#$INDEXES != 2 )); then
      echo "UPDATE_INDEXED_FIELD=1 requires INDEXES=2 so the city index exists" >&2
      exit 2
    fi
    ;;
  0)
  ;;
esac
UPDATE_INDEXED_FIELD_TEXT=$(bool_01_text "$UPDATE_INDEXED_FIELD")
INCLUDE_MONGO=$(normalize_bool_01 INCLUDE_MONGO "$INCLUDE_MONGO")
RUN_READER_SWEEP=$(normalize_bool_01 RUN_READER_SWEEP "$RUN_READER_SWEEP")
if [[ -z "$MONGO_COMPACT" ]]; then
  if [[ "$MONGO_MODE" == "external" ]]; then
    MONGO_COMPACT=0
  else
    MONGO_COMPACT=1
  fi
else
  MONGO_COMPACT=$(normalize_bool_01 MONGO_COMPACT "$MONGO_COMPACT")
fi
MONGO_COMPACT_TEXT=$(bool_01_text "$MONGO_COMPACT")
if [[ "$MONGO_MODE" != "docker" && "$MONGO_MODE" != "external" ]]; then
  echo "unknown MONGO_MODE=$MONGO_MODE (want docker or external)" >&2
  exit 2
fi
if [[ "$INCLUDE_MONGO" == "1" && "$MONGO_MODE" == "docker" ]] && ! command -v docker >/dev/null 2>&1; then
  echo "MONGO_MODE=docker requires docker; use --mongo-mode external --mongo-uri URI to use an existing server" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd -P)
if [[ -n "$(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "output directory must be empty: $OUT_DIR" >&2
  exit 2
fi
RUN_ID=$(safe_label "$(basename "$OUT_DIR")")
if [[ -z "$DATABASE_PREFIX" ]]; then
  DATABASE_PREFIX="mongo_gateway_scaling_${RUN_ID}"
fi
DATABASE_PREFIX=$(mongo_database_prefix_label "$DATABASE_PREFIX")
RAW_DIR="$OUT_DIR/raw"
BIN_DIR="$OUT_DIR/bin"
MATRIX="$OUT_DIR/matrix.tsv"
WRITER_METRICS="$OUT_DIR/writer_metrics.tsv"
REPORT="$OUT_DIR/report.md"
SUMMARY="$OUT_DIR/summary.tsv"
README="$OUT_DIR/README.md"
TREE_METADATA="$OUT_DIR/treedb-location.txt"
MONGO_DIR="$OUT_DIR/mongodb_data"

path_is_within() {
  local child=${1%/}
  local parent=${2%/}
  if [[ "$child/" == "$parent/"* ]]; then
    return 0
  fi
  return 1
}

TREE_DIR_IS_TEMP=false
if path_is_within "$OUT_DIR" "$ROOT"; then
  TREE_DIR=$(mktemp -d "$TMP_BASE/gomap_mongo_gateway_scaling_treedb_XXXXXX")
  TREE_DIR_IS_TEMP=true
else
  TREE_DIR="$OUT_DIR/treedb_data"
fi

cat >"$TREE_METADATA" <<EOF
TreeDB directory: $TREE_DIR
This file is written early so the TreeDB path can be recovered if the script exits before bundle documentation is generated.
EOF

report_treedb_location_on_exit() {
  local status=$1
  if [[ "$status" -ne 0 && "$TREE_DIR_IS_TEMP" == "true" ]]; then
    echo "mongo_gateway_scaling_bench.sh exited before completion." >&2
    echo "TreeDB data directory: $TREE_DIR" >&2
    echo "Recorded in: $TREE_METADATA" >&2
  fi
}

ACTIVE_CONTAINERS=()
STARTED_MONGO_CONTAINER=""
STARTED_MONGO_URI=""

cleanup() {
  local container
  for container in "${ACTIVE_CONTAINERS[@]:-}"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
}

cleanup_all() {
  local status=$1
  cleanup
  report_treedb_location_on_exit "$status"
}

trap 'cleanup_all "$?"' EXIT

mkdir -p "$RAW_DIR" "$TREE_DIR" "$BIN_DIR" "$MONGO_DIR"

PYTHON3_BIN=""
if command -v python3 >/dev/null 2>&1; then
  PYTHON3_BIN=$(command -v python3)
else
  echo "python3 not found; writer_metrics.tsv will be skipped" >&2
fi

BENCH_BIN="$BIN_DIR/mongo_gateway_bench"
REPORT_BIN="$BIN_DIR/mongo_gateway_compare_report"
GO_WORK_MODE="${GOWORK:-off}"
GOWORK="$GO_WORK_MODE" go build -o "$BENCH_BIN" ./cmd/mongo_gateway_bench
GOWORK="$GO_WORK_MODE" go build -o "$REPORT_BIN" ./cmd/mongo_gateway_compare_report

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

start_mongo_container() {
  local cell=$1
  local data_dir=$2
  local container="gomap-mongo-scaling-$(safe_label "$cell")-$$"
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

run_bench() {
  local target=$1
  local raw_json=$2
  local database=$3
  local concurrent_readers=$4
  local concurrent_reads=$5
  local concurrent_writers=$6
  local concurrent_writes=$7
  shift 7

  if [[ "$PREBUILD_DOCUMENTS" == "true" ]]; then
    set -- -prebuild-documents "$@"
  fi
  if [[ "$UPDATE_INDEXED_FIELD" == "1" ]]; then
    set -- -update-indexed-field "$@"
  fi

  "$BENCH_BIN" \
    -target "$target" \
    -database "$database" \
    -collection "$COLLECTION" \
    -documents "$DOCS" \
    -batch-size "$BATCH_SIZE" \
    -insert-producers "$INSERT_PRODUCERS" \
    -mongo-max-pool-size "$MONGO_MAX_POOL_SIZE" \
    -mongo-min-pool-size "$MONGO_MIN_POOL_SIZE" \
    -mongo-max-connecting "$MONGO_MAX_CONNECTING" \
    -reads "$READS" \
    -range-reads "$RANGE_READS" \
    -updates "$UPDATES" \
    -deletes "$DELETES" \
    -concurrent-readers "$concurrent_readers" \
    -concurrent-reads "$concurrent_reads" \
    -concurrent-writers "$concurrent_writers" \
    -concurrent-writes "$concurrent_writes" \
    -secondary-indexes "$INDEXES" \
    -timeout "$TIMEOUT" \
    -format json \
    "$@" >"$raw_json"
}

printf "target\tconfig\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n" >"$MATRIX"

echo "running Mongo gateway scaling matrix into: $OUT_DIR"
echo "docs=$DOCS indexes=$INDEXES batch_size=$BATCH_SIZE insert_producers=$INSERT_PRODUCERS"
echo "writers=$WRITERS_LIST readers=$READERS_LIST run_reader_sweep=$RUN_READER_SWEEP include_mongo=$INCLUDE_MONGO update_indexed_field=$UPDATE_INDEXED_FIELD_TEXT"
echo "mongo mode: $MONGO_MODE"
if [[ "$MONGO_MODE" == "docker" ]]; then
  echo "mongo image: $MONGO_IMAGE"
else
  echo "mongo uri: $MONGO_URI"
fi
echo "mongo compact before final stats: $MONGO_COMPACT_TEXT"

run_cell() {
  local scenario=$1
  local readers=$2
  local reads=$3
  local writers=$4
  local writes=$5
  local config_suffix
  config_suffix=$(safe_label "$scenario")

  local tree_config="treedb_${TREEDB_DOCUMENT_FORMAT}_${TREEDB_CLIENT_MODE}_${config_suffix}"
  tree_config=$(safe_label "$tree_config")
  local tree_raw_rel="raw/${tree_config}.json"
  local tree_raw="$OUT_DIR/$tree_raw_rel"
  local tree_data="$TREE_DIR/$tree_config"
  local database="${DATABASE_PREFIX}_${config_suffix}"

  echo "==> TreeDB $scenario readers=$readers writers=$writers"
  run_bench treedb "$tree_raw" "$database" "$readers" "$reads" "$writers" "$writes" \
    -treedb-dir "$tree_data" \
    -keep-treedb-dir \
    -treedb-profile "$TREEDB_PROFILE" \
    -treedb-document-format "$TREEDB_DOCUMENT_FORMAT" \
    -client-mode "$TREEDB_CLIENT_MODE" \
    -treedb-data-root-storage "$TREEDB_DATA_ROOT_STORAGE" \
    -treedb-index-state-root-storage "$TREEDB_INDEX_STATE_ROOT_STORAGE" \
    -treedb-index-root-storage "$TREEDB_INDEX_ROOT_STORAGE" \
    -treedb-maintenance "$TREEDB_MAINTENANCE"
  local tree_physical
  tree_physical=$(du_bytes "$tree_data")
  printf "treedb\t%s\t%s\t%s\t%s\t%s\n" "$tree_config" "$DOCS" "$INDEXES" "$tree_raw_rel" "$tree_physical" >>"$MATRIX"

  if [[ "$INCLUDE_MONGO" == "1" ]]; then
    local mongo_config="mongo_${config_suffix}"
    local mongo_raw_rel="raw/${mongo_config}.json"
    local mongo_raw="$OUT_DIR/$mongo_raw_rel"
    local mongo_data="$MONGO_DIR/$config_suffix"
    local mongo_uri="$MONGO_URI"
    local mongo_container=""
    local mongo_physical=0

    if [[ "$MONGO_MODE" == "docker" ]]; then
      start_mongo_container "$config_suffix" "$mongo_data"
      mongo_uri="$STARTED_MONGO_URI"
      mongo_container="$STARTED_MONGO_CONTAINER"
      if ! wait_for_mongo "$mongo_uri" "$database"; then
        echo "MongoDB container did not become ready for $scenario readers=$readers writers=$writers" >&2
        exit 1
      fi
    fi
    echo "==> MongoDB $scenario readers=$readers writers=$writers"
    run_bench mongo "$mongo_raw" "$database" "$readers" "$reads" "$writers" "$writes" \
      -mongo-uri "$mongo_uri" \
      -mongo-compact="$MONGO_COMPACT_TEXT"
    if [[ "$MONGO_MODE" == "docker" ]]; then
      stop_mongo_container "$mongo_container"
      mongo_physical=$(docker_du_bytes "$mongo_data")
    fi
    printf "mongo\t%s\t%s\t%s\t%s\t%s\n" "$mongo_config" "$DOCS" "$INDEXES" "$mongo_raw_rel" "$mongo_physical" >>"$MATRIX"
  fi
}

for writers in $WRITERS_LIST; do
  run_cell "writers_${writers}" 0 0 "$writers" "$CONCURRENT_WRITES"
done

if [[ "$RUN_READER_SWEEP" == "1" ]]; then
  for readers in $READERS_LIST; do
    run_cell "readers_${readers}" "$readers" "$CONCURRENT_READS" 0 0
  done
fi

report_extra=()
if [[ "$INCLUDE_MONGO" != "1" ]]; then
  report_extra=(-allow-incomplete)
fi

"$REPORT_BIN" \
  -matrix "$MATRIX" \
  -report "$REPORT" \
  -summary "$SUMMARY" \
  -title "$TITLE" \
  "${report_extra[@]:-}"

if [[ -n "$PYTHON3_BIN" ]]; then
  "$PYTHON3_BIN" "$ROOT/scripts/mongo_gateway_writer_metrics.py" "$OUT_DIR" "$MATRIX" "$WRITER_METRICS"
fi
WRITER_METRICS_STATUS="$WRITER_METRICS"
if [[ -z "$PYTHON3_BIN" ]]; then
  WRITER_METRICS_STATUS="skipped (python3 not found)"
fi

report_extra_text=""
if (( ${#report_extra[@]} > 0 )); then
  report_extra_text=" \\
  ${report_extra[*]}"
fi

cat >"$README" <<EOF
# Mongo Gateway Scaling Bundle

- output directory: \`$OUT_DIR\`
- report: \`$REPORT\`
- summary TSV: \`$SUMMARY\`
- matrix TSV: \`$MATRIX\`
- writer metrics TSV: \`$WRITER_METRICS_STATUS\`
- raw JSON directory: \`$RAW_DIR\`
- TreeDB data directory: \`$TREE_DIR\`
- TreeDB location metadata: \`$TREE_METADATA\`
- docs: \`$DOCS\`
- secondary indexes: \`$INDEXES\`
- batch size: \`$BATCH_SIZE\`
- insert producers: \`$INSERT_PRODUCERS\`
- writer sweep: \`$WRITERS_LIST\`
- reader sweep: \`$READERS_LIST\`
- run reader sweep: \`$RUN_READER_SWEEP\`
- concurrent writes per writer cell: \`$CONCURRENT_WRITES\`
- concurrent reads per reader cell: \`$CONCURRENT_READS\`
- TreeDB profile: \`$TREEDB_PROFILE\`
- TreeDB document format: \`$TREEDB_DOCUMENT_FORMAT\`
- TreeDB client mode: \`$TREEDB_CLIENT_MODE\`
- TreeDB maintenance: \`$TREEDB_MAINTENANCE\`
- update indexed field: \`$UPDATE_INDEXED_FIELD_TEXT\`
- include MongoDB: \`$INCLUDE_MONGO\`
- MongoDB mode: \`$MONGO_MODE\`
- MongoDB URI: \`$MONGO_URI\`
- MongoDB image: \`$MONGO_IMAGE\`
- MongoDB compact before final stats: \`$MONGO_COMPACT_TEXT\`
- MongoDB database prefix: \`$DATABASE_PREFIX\`
- GOWORK: \`$GO_WORK_MODE\`
- timeout: \`$TIMEOUT\`

Regenerate the report:

\`\`\`sh
GOWORK=$GO_WORK_MODE go run ./cmd/mongo_gateway_compare_report \\
  -matrix "$MATRIX" \\
  -report "$REPORT" \\
  -summary "$SUMMARY" \\
  -title "$TITLE"$report_extra_text
\`\`\`
EOF

echo "scaling report: $REPORT"
echo "summary TSV: $SUMMARY"
echo "writer metrics TSV: $WRITER_METRICS_STATUS"
echo "bundle README: $README"
