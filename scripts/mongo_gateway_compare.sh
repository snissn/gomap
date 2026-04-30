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
READS="${READS:-}"
RANGE_READS="${RANGE_READS:-}"
UPDATES="${UPDATES:-}"
DELETES="${DELETES:-0}"
RANGE_READS_DIVISOR="${RANGE_READS_DIVISOR:-10}"
UPDATES_DIVISOR="${UPDATES_DIVISOR:-10}"
MONGO_MODE="${MONGO_MODE:-docker}"
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
MONGO_IMAGE="${MONGO_IMAGE:-mongo:7}"
DATABASE_PREFIX="${DATABASE_PREFIX:-mongo_gateway_compare}"
COLLECTION="${COLLECTION:-docs}"
TIMEOUT="${TIMEOUT:-20m}"
TITLE="${TITLE:-Mongo Gateway Benchmark Comparison}"

usage() {
  cat <<'EOF'
Usage: scripts/mongo_gateway_compare.sh [options]

Runs the same cmd/mongo_gateway_bench workload against TreeDB and MongoDB,
then writes a reusable report bundle.

Options:
  --out DIR             Output directory. Default: mktemp under $TMPDIR or /tmp.
  --docs LIST           Space-separated document counts. Default: "1000 10000".
  --indexes LIST        Space-separated secondary-index counts. Default: "0 2".
  --mongo-mode MODE     docker or external. Default: docker.
  --mongo-uri URI       MongoDB URI for --mongo-mode external.
  --mongo-image IMAGE   Docker image for --mongo-mode docker. Default: mongo:7.
  --timeout DURATION    Per-run benchmark timeout. Default: 20m.
  --title TITLE         Markdown report title.
  --help                Show this help.

Environment overrides:
  OUT_DIR, DOCS_LIST, INDEXES_LIST, BATCH_SIZE, READS, RANGE_READS, UPDATES,
  DELETES, RANGE_READS_DIVISOR, UPDATES_DIVISOR, MONGO_MODE, MONGO_URI,
  MONGO_IMAGE, DATABASE_PREFIX, COLLECTION, TIMEOUT, TITLE.
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

derived_count() {
  local docs=$1
  local explicit=$2
  local divisor=$3
  if [[ -n "$explicit" ]]; then
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
  shift 9

  "$BENCH_BIN" \
    -target "$target" \
    -database "$database" \
    -collection "$COLLECTION" \
    -documents "$docs" \
    -batch-size "$BATCH_SIZE" \
    -reads "$reads" \
    -range-reads "$range_reads" \
    -updates "$updates" \
    -deletes "$deletes" \
    -secondary-indexes "$indexes" \
    -timeout "$TIMEOUT" \
    -format json \
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

{
  echo "running Mongo gateway comparison into: $OUT_DIR"
  echo "docs list: $DOCS_LIST"
  echo "secondary-index list: $INDEXES_LIST"
  echo "batch size: $BATCH_SIZE"
  echo "mongo mode: $MONGO_MODE"
  if [[ "$MONGO_MODE" == "docker" ]]; then
    echo "mongo image: $MONGO_IMAGE"
  else
    echo "mongo uri: $MONGO_URI"
  fi
  echo "benchmark timeout: $TIMEOUT"
} | tee "$STDOUT_LOG"

GOWORK=off go build -o "$BENCH_BIN" ./cmd/mongo_gateway_bench
GOWORK=off go build -o "$REPORT_BIN" ./cmd/mongo_gateway_compare_report

printf "target\tdocuments\tsecondary_indexes\traw_json\tphysical_bytes\n" >"$MATRIX"

for docs in $DOCS_LIST; do
  if ! is_positive_int "$docs"; then
    echo "invalid document count: $docs" >&2
    exit 2
  fi
  reads="${READS:-$docs}"
  range_reads=$(derived_count "$docs" "$RANGE_READS" "$RANGE_READS_DIVISOR")
  updates=$(derived_count "$docs" "$UPDATES" "$UPDATES_DIVISOR")
  for indexes in $INDEXES_LIST; do
    if [[ ! "$indexes" =~ ^[0-9]+$ ]]; then
      echo "invalid index count: $indexes" >&2
      exit 2
    fi
    cell="docs_${docs}_idx_${indexes}"
    database="${DATABASE_PREFIX}_${cell}"
    tree_raw_rel="raw/treedb_${cell}.json"
    mongo_raw_rel="raw/mongo_${cell}.json"
    tree_raw="$OUT_DIR/$tree_raw_rel"
    mongo_raw="$OUT_DIR/$mongo_raw_rel"
    tree_data="$TREE_DIR/$cell"
    mongo_data="$MONGO_DIR/$cell"

    echo
    echo "==> $cell TreeDB"
    run_target treedb "$docs" "$indexes" "$tree_raw" "$database" "$reads" "$range_reads" "$updates" "$DELETES" \
      -treedb-dir "$tree_data" \
      -keep-treedb-dir
    tree_physical=$(du_bytes "$tree_data")
    printf "treedb\t%s\t%s\t%s\t%s\n" "$docs" "$indexes" "$tree_raw_rel" "$tree_physical" >>"$MATRIX"

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
      -mongo-uri "$mongo_uri"
    if [[ "$MONGO_MODE" == "docker" ]]; then
      stop_mongo_container "$mongo_container"
      mongo_physical=$(docker_du_bytes "$mongo_data")
    else
      mongo_physical=0
    fi
    printf "mongo\t%s\t%s\t%s\t%s\n" "$docs" "$indexes" "$mongo_raw_rel" "$mongo_physical" >>"$MATRIX"
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
- reads: \`per-cell ${READS:-documents}\`
- range reads: \`${RANGE_READS:-documents / $RANGE_READS_DIVISOR}\`
- updates: \`${UPDATES:-documents / $UPDATES_DIVISOR}\`
- deletes: \`$DELETES\`
- MongoDB mode: \`$MONGO_MODE\`
- MongoDB image: \`$MONGO_IMAGE\`
- benchmark timeout: \`$TIMEOUT\`

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
