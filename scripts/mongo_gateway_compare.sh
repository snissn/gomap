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
READS="${READS:-}"
READS_DIVISOR="${READS_DIVISOR:-1}"
RANGE_READS="${RANGE_READS:-}"
UPDATES="${UPDATES:-}"
DELETES="${DELETES:-0}"
RANGE_READS_DIVISOR="${RANGE_READS_DIVISOR:-10}"
UPDATES_DIVISOR="${UPDATES_DIVISOR:-10}"
CONCURRENT_READERS="${CONCURRENT_READERS:-0}"
CONCURRENT_READS="${CONCURRENT_READS:-}"
CONCURRENT_READS_DIVISOR="${CONCURRENT_READS_DIVISOR:-10}"
CONCURRENT_WRITERS="${CONCURRENT_WRITERS:-0}"
CONCURRENT_WRITES="${CONCURRENT_WRITES:-}"
CONCURRENT_WRITES_DIVISOR="${CONCURRENT_WRITES_DIVISOR:-10}"
MONGO_MODE="${MONGO_MODE:-docker}"
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
MONGO_IMAGE="${MONGO_IMAGE:-mongo:7}"
DATABASE_PREFIX="${DATABASE_PREFIX:-mongo_gateway_compare}"
COLLECTION="${COLLECTION:-docs}"
TIMEOUT="${TIMEOUT:-20m}"
TITLE="${TITLE:-Mongo Gateway Benchmark Comparison}"
TREEDB_PROFILE="${TREEDB_PROFILE:-wal_on_fast}"
TREEDB_DOCUMENT_FORMAT="${TREEDB_DOCUMENT_FORMAT:-template-v1}"
TREEDB_DOCUMENT_FORMATS="${TREEDB_DOCUMENT_FORMATS:-$TREEDB_DOCUMENT_FORMAT}"
TREEDB_CLIENT_MODE="${TREEDB_CLIENT_MODE:-driver}"
TREEDB_CLIENT_MODES="${TREEDB_CLIENT_MODES:-$TREEDB_CLIENT_MODE}"
TREEDB_DATA_ROOT_STORAGE="${TREEDB_DATA_ROOT_STORAGE:-compressed}"
TREEDB_INDEX_STATE_ROOT_STORAGE="${TREEDB_INDEX_STATE_ROOT_STORAGE:-compressed}"
TREEDB_INDEX_ROOT_STORAGE="${TREEDB_INDEX_ROOT_STORAGE:-compressed}"
TREEDB_MAINTENANCE="${TREEDB_MAINTENANCE:-full}"

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
  --prebuild-documents  Prebuild documents before timed load phases.
  --concurrent-readers N
                        Reader goroutines for the concurrent _id read phase.
  --concurrent-reads COUNT
                        Concurrent point reads per target/cell.
  --concurrent-writers N
                        Writer goroutines for the concurrent update phase.
  --concurrent-writes COUNT
                        Concurrent updates per target/cell.
  --mongo-mode MODE     docker or external. Default: docker.
  --mongo-uri URI       MongoDB URI for --mongo-mode external.
  --mongo-image IMAGE   Docker image for --mongo-mode docker. Default: mongo:7.
  --timeout DURATION    Per-run benchmark timeout. Default: 20m.
  --treedb-profile NAME TreeDB profile. Default: wal_on_fast.
  --treedb-document-format FORMAT
                        Single TreeDB document format.
  --treedb-document-formats LIST
                        Space-separated TreeDB formats. Example: "json template-v1 bson".
  --treedb-client-mode MODE
                        Single TreeDB client mode: driver, driver-command, driver-command-raw, driver-unack, raw-wire-tcp, or raw-wire.
  --treedb-client-modes LIST
                        Space-separated TreeDB client modes. Example: "driver driver-command driver-command-raw driver-unack raw-wire-tcp raw-wire".
  --treedb-maintenance MODE
                        TreeDB final maintenance: full, checkpoint, or none.
  --title TITLE         Markdown report title.
  --help                Show this help.

Environment overrides:
  OUT_DIR, DOCS_LIST, INDEXES_LIST, BATCH_SIZE, INSERT_PRODUCERS,
  MONGO_MAX_POOL_SIZE, MONGO_MIN_POOL_SIZE, MONGO_MAX_CONNECTING, PREBUILD_DOCUMENTS,
  READS, READS_DIVISOR,
  RANGE_READS, UPDATES, DELETES, RANGE_READS_DIVISOR, UPDATES_DIVISOR,
  CONCURRENT_READERS, CONCURRENT_READS, CONCURRENT_READS_DIVISOR,
  CONCURRENT_WRITERS, CONCURRENT_WRITES, CONCURRENT_WRITES_DIVISOR,
  MONGO_MODE, MONGO_URI, MONGO_IMAGE, DATABASE_PREFIX, COLLECTION, TIMEOUT,
  TREEDB_PROFILE, TREEDB_DOCUMENT_FORMAT, TREEDB_DOCUMENT_FORMATS,
  TREEDB_CLIENT_MODE, TREEDB_CLIENT_MODES,
  TREEDB_DATA_ROOT_STORAGE, TREEDB_INDEX_STATE_ROOT_STORAGE, TREEDB_INDEX_ROOT_STORAGE,
  TREEDB_MAINTENANCE, TITLE.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      OUT_DIR="$2"
      shift 2
      ;;
    --docs)
      DOCS_LIST="$2"
      shift 2
      ;;
    --indexes)
      INDEXES_LIST="$2"
      shift 2
      ;;
    --reads)
      READS="$2"
      shift 2
      ;;
    --range-reads)
      RANGE_READS="$2"
      shift 2
      ;;
    --updates)
      UPDATES="$2"
      shift 2
      ;;
    --deletes)
      DELETES="$2"
      shift 2
      ;;
    --insert-producers)
      INSERT_PRODUCERS="$2"
      shift 2
      ;;
    --mongo-max-pool-size)
      MONGO_MAX_POOL_SIZE="$2"
      shift 2
      ;;
    --mongo-min-pool-size)
      MONGO_MIN_POOL_SIZE="$2"
      shift 2
      ;;
    --mongo-max-connecting)
      MONGO_MAX_CONNECTING="$2"
      shift 2
      ;;
    --prebuild-documents)
      PREBUILD_DOCUMENTS=true
      shift
      ;;
    --concurrent-readers)
      CONCURRENT_READERS="$2"
      shift 2
      ;;
    --concurrent-reads)
      CONCURRENT_READS="$2"
      shift 2
      ;;
    --concurrent-writers)
      CONCURRENT_WRITERS="$2"
      shift 2
      ;;
    --concurrent-writes)
      CONCURRENT_WRITES="$2"
      shift 2
      ;;
    --mongo-mode)
      MONGO_MODE="$2"
      shift 2
      ;;
    --mongo-uri)
      MONGO_URI="$2"
      shift 2
      ;;
    --mongo-image)
      MONGO_IMAGE="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    --treedb-profile)
      TREEDB_PROFILE="$2"
      shift 2
      ;;
    --treedb-document-format)
      TREEDB_DOCUMENT_FORMAT="$2"
      TREEDB_DOCUMENT_FORMATS="$2"
      shift 2
      ;;
    --treedb-document-formats)
      TREEDB_DOCUMENT_FORMATS="$2"
      shift 2
      ;;
    --treedb-client-mode)
      TREEDB_CLIENT_MODE="$2"
      TREEDB_CLIENT_MODES="$2"
      shift 2
      ;;
    --treedb-client-modes)
      TREEDB_CLIENT_MODES="$2"
      shift 2
      ;;
    --treedb-maintenance)
      TREEDB_MAINTENANCE="$2"
      shift 2
      ;;
    --title)
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
  printf '%s' "$1" | tr -c '[:alnum:]_.-' '_'
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
  local keep=()
  local active
  for active in "${ACTIVE_CONTAINERS[@]:-}"; do
    if [[ "$active" != "$container" ]]; then
      keep+=("$active")
    fi
  done
  ACTIVE_CONTAINERS=("${keep[@]}")
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
  local concurrent_writers=${12}
  local concurrent_writes=${13}
  shift 13

  local prebuild_args=()
  if [[ "$PREBUILD_DOCUMENTS" == "true" ]]; then
    prebuild_args=(-prebuild-documents)
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
    -concurrent-readers "$concurrent_readers" \
    -concurrent-reads "$concurrent_reads" \
    -concurrent-writers "$concurrent_writers" \
    -concurrent-writes "$concurrent_writes" \
    -secondary-indexes "$indexes" \
    -timeout "$TIMEOUT" \
    -format json \
    "${prebuild_args[@]}" \
    "$@" >"$raw_json"
}

if [[ "$MONGO_MODE" != "docker" && "$MONGO_MODE" != "external" ]]; then
  echo "unknown MONGO_MODE=$MONGO_MODE (want docker or external)" >&2
  exit 2
fi
if [[ "$MONGO_MODE" == "docker" ]] && ! command -v docker >/dev/null 2>&1; then
  echo "MONGO_MODE=docker requires docker; use --mongo-mode external --mongo-uri URI to use an existing server" >&2
  exit 2
fi
if ! is_positive_int "$INSERT_PRODUCERS"; then
  echo "invalid INSERT_PRODUCERS=$INSERT_PRODUCERS (want positive integer)" >&2
  exit 2
fi
for value_name in DELETES CONCURRENT_READERS CONCURRENT_WRITERS MONGO_MAX_POOL_SIZE MONGO_MIN_POOL_SIZE MONGO_MAX_CONNECTING; do
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
if [[ "$CONCURRENT_READERS" -eq 0 && -n "$CONCURRENT_READS" && "$CONCURRENT_READS" != "0" ]]; then
  echo "CONCURRENT_READERS must be > 0 when CONCURRENT_READS is set" >&2
  exit 2
fi
if [[ "$CONCURRENT_WRITERS" -eq 0 && -n "$CONCURRENT_WRITES" && "$CONCURRENT_WRITES" != "0" ]]; then
  echo "CONCURRENT_WRITERS must be > 0 when CONCURRENT_WRITES is set" >&2
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
  echo "reads: ${READS:-documents / $READS_DIVISOR}"
  echo "range reads: ${RANGE_READS:-documents / $RANGE_READS_DIVISOR}"
  echo "updates: ${UPDATES:-documents / $UPDATES_DIVISOR}"
  echo "deletes: $DELETES"
  echo "concurrent readers: $CONCURRENT_READERS"
  echo "concurrent reads: ${CONCURRENT_READS:-documents / $CONCURRENT_READS_DIVISOR when readers > 0}"
  echo "concurrent writers: $CONCURRENT_WRITERS"
  echo "concurrent writes: ${CONCURRENT_WRITES:-documents / $CONCURRENT_WRITES_DIVISOR when writers > 0}"
  echo "mongo mode: $MONGO_MODE"
  echo "treedb profile: $TREEDB_PROFILE"
  echo "treedb document formats: $TREEDB_DOCUMENT_FORMATS"
  echo "treedb client modes: $TREEDB_CLIENT_MODES"
  echo "treedb root storage: data=$TREEDB_DATA_ROOT_STORAGE index_state=$TREEDB_INDEX_STATE_ROOT_STORAGE index=$TREEDB_INDEX_ROOT_STORAGE"
  echo "treedb maintenance: $TREEDB_MAINTENANCE"
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
  if [[ "$CONCURRENT_READERS" -gt 0 ]]; then
    concurrent_reads=$(derived_count "$docs" "$CONCURRENT_READS" "$CONCURRENT_READS_DIVISOR")
    if [[ "$concurrent_reads" -eq 0 ]]; then
      echo "concurrent reads must be > 0 when CONCURRENT_READERS is > 0" >&2
      exit 2
    fi
  fi
  concurrent_writes=0
  if [[ "$CONCURRENT_WRITERS" -gt 0 ]]; then
    concurrent_writes=$(derived_count "$docs" "$CONCURRENT_WRITES" "$CONCURRENT_WRITES_DIVISOR")
    if [[ "$concurrent_writes" -eq 0 ]]; then
      echo "concurrent writes must be > 0 when CONCURRENT_WRITERS is > 0" >&2
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
    mongo_raw_rel="raw/mongo_${cell}.json"
    mongo_raw="$OUT_DIR/$mongo_raw_rel"
    mongo_data="$MONGO_DIR/$cell"

    for tree_format in $TREEDB_DOCUMENT_FORMATS; do
      for tree_client_mode in $TREEDB_CLIENT_MODES; do
        format_label=$(safe_label "${tree_format//-/_}")
        client_label=$(safe_label "${tree_client_mode//-/_}")
        tree_config="treedb_${format_label}_${client_label}"
        tree_raw_rel="raw/${tree_config}_${cell}.json"
        tree_raw="$OUT_DIR/$tree_raw_rel"
        tree_data="$TREE_DIR/${tree_config}_${cell}"

        echo
        echo "==> $cell TreeDB ($tree_format, client=$tree_client_mode)"
        run_target treedb "$docs" "$indexes" "$tree_raw" "$database" "$reads" "$range_reads" "$updates" "$DELETES" \
          "$CONCURRENT_READERS" "$concurrent_reads" "$CONCURRENT_WRITERS" "$concurrent_writes" \
          -treedb-dir "$tree_data" \
          -keep-treedb-dir \
          -treedb-profile "$TREEDB_PROFILE" \
          -treedb-document-format "$tree_format" \
          -client-mode "$tree_client_mode" \
          -treedb-data-root-storage "$TREEDB_DATA_ROOT_STORAGE" \
          -treedb-index-state-root-storage "$TREEDB_INDEX_STATE_ROOT_STORAGE" \
          -treedb-index-root-storage "$TREEDB_INDEX_ROOT_STORAGE" \
          -treedb-maintenance "$TREEDB_MAINTENANCE"
        tree_physical=$(du_bytes "$tree_data")
        printf "treedb\t%s\t%s\t%s\t%s\t%s\n" "$tree_config" "$docs" "$indexes" "$tree_raw_rel" "$tree_physical" >>"$MATRIX"
      done
    done

    echo "==> $cell MongoDB"
    mongo_uri="$MONGO_URI"
    mongo_container=""
    if [[ "$MONGO_MODE" == "docker" ]]; then
      start_mongo_container "$cell" "$mongo_data"
      mongo_uri="$STARTED_MONGO_URI"
      mongo_container="$STARTED_MONGO_CONTAINER"
      if ! wait_for_mongo "$mongo_uri" "$database"; then
        echo "MongoDB container did not become ready for $cell" >&2
        exit 1
      fi
    fi
    run_target mongo "$docs" "$indexes" "$mongo_raw" "$database" "$reads" "$range_reads" "$updates" "$DELETES" \
      "$CONCURRENT_READERS" "$concurrent_reads" "$CONCURRENT_WRITERS" "$concurrent_writes" \
      -mongo-uri "$mongo_uri"
    if [[ "$MONGO_MODE" == "docker" ]]; then
      stop_mongo_container "$mongo_container"
      mongo_physical=$(docker_du_bytes "$mongo_data")
    else
      mongo_physical=0
    fi
    printf "mongo\tmongo\t%s\t%s\t%s\t%s\n" "$docs" "$indexes" "$mongo_raw_rel" "$mongo_physical" >>"$MATRIX"
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
- reads: \`${READS:-documents / $READS_DIVISOR}\`
- range reads: \`${RANGE_READS:-documents / $RANGE_READS_DIVISOR}\`
- updates: \`${UPDATES:-documents / $UPDATES_DIVISOR}\`
- deletes: \`$DELETES\`
- concurrent readers: \`$CONCURRENT_READERS\`
- concurrent reads: \`${CONCURRENT_READS:-documents / $CONCURRENT_READS_DIVISOR when readers > 0}\`
- concurrent writers: \`$CONCURRENT_WRITERS\`
- concurrent writes: \`${CONCURRENT_WRITES:-documents / $CONCURRENT_WRITES_DIVISOR when writers > 0}\`
- MongoDB mode: \`$MONGO_MODE\`
- MongoDB image: \`$MONGO_IMAGE\`
- benchmark timeout: \`$TIMEOUT\`
- TreeDB profile: \`$TREEDB_PROFILE\`
- TreeDB document formats: \`$TREEDB_DOCUMENT_FORMATS\`
- TreeDB client modes: \`$TREEDB_CLIENT_MODES\`
- TreeDB root storage: \`data=$TREEDB_DATA_ROOT_STORAGE index_state=$TREEDB_INDEX_STATE_ROOT_STORAGE index=$TREEDB_INDEX_ROOT_STORAGE\`
- TreeDB maintenance: \`$TREEDB_MAINTENANCE\`

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
