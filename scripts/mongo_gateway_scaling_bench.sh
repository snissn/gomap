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
WRITERS_LIST="${WRITERS_LIST:-1 2 4 8 16}"
READERS_LIST="${READERS_LIST:-1 2 4 8 16}"
CONCURRENT_WRITES="${CONCURRENT_WRITES:-80000}"
CONCURRENT_READS="${CONCURRENT_READS:-80000}"
READS="${READS:-0}"
RANGE_READS="${RANGE_READS:-0}"
UPDATES="${UPDATES:-0}"
DELETES="${DELETES:-0}"
UPDATE_INDEXED_FIELD="${UPDATE_INDEXED_FIELD:-false}"
TREEDB_PROFILE="${TREEDB_PROFILE:-wal_on_fast}"
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
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
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
  --writers LIST         Quoted space-separated concurrent writer counts (for example: "1 2 4"). Default: "1 2 4 8 16".
  --readers LIST         Quoted space-separated concurrent reader counts (for example: "1 2 4"). Default: "1 2 4 8 16".
  --concurrent-writes N  Total updates per writer-scaling cell. Default: 80000.
  --concurrent-reads N   Total reads per reader-scaling cell. Default: 80000.
  --include-mongo        Also run each cell against an external MongoDB URI.
  --mongo-uri URI        MongoDB URI for --include-mongo. Default: mongodb://127.0.0.1:27017.
  --database-prefix NAME MongoDB database prefix. Default: mongo_gateway_scaling_<run_id>.
  --treedb-format NAME   TreeDB document format. Default: bson.
  --client-mode NAME     TreeDB client mode. Default: driver-command-raw.
  --profile NAME         TreeDB profile. Default: wal_on_fast.
  --maintenance MODE     TreeDB maintenance mode. Default: none.
  --update-indexed-field Include the city field in update phases to exercise
                         secondary-index maintenance; requires INDEXES=2.
  --timeout DURATION     Per-cell timeout. Default: 60m.
  --title TITLE          Report title.
  --help                 Show this help.

Environment overrides use the uppercase variable names shown in the script:
OUT_DIR, DOCS, INDEXES, BATCH_SIZE, INSERT_PRODUCERS, WRITERS_LIST,
READERS_LIST, CONCURRENT_WRITES, CONCURRENT_READS, INCLUDE_MONGO (0/1, true/false, or yes/no), MONGO_URI, DATABASE_PREFIX,
TREEDB_DOCUMENT_FORMAT, TREEDB_CLIENT_MODE, TREEDB_PROFILE,
TREEDB_MAINTENANCE, UPDATE_INDEXED_FIELD (0/1, true/false, or yes/no), GOWORK, TIMEOUT, TITLE,
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

safe_label() {
  printf '%s' "$1" | tr -c '[:alnum:]_.-' '_'
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
INCLUDE_MONGO=$(normalize_bool_01 INCLUDE_MONGO "$INCLUDE_MONGO")

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
DATABASE_PREFIX=$(safe_label "$DATABASE_PREFIX")
RAW_DIR="$OUT_DIR/raw"
BIN_DIR="$OUT_DIR/bin"
MATRIX="$OUT_DIR/matrix.tsv"
REPORT="$OUT_DIR/report.md"
SUMMARY="$OUT_DIR/summary.tsv"
README="$OUT_DIR/README.md"
TREE_METADATA="$OUT_DIR/treedb-location.txt"

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

trap 'report_treedb_location_on_exit "$?"' EXIT

mkdir -p "$RAW_DIR" "$TREE_DIR" "$BIN_DIR"

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
echo "writers=$WRITERS_LIST readers=$READERS_LIST include_mongo=$INCLUDE_MONGO update_indexed_field=$UPDATE_INDEXED_FIELD"

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
    echo "==> MongoDB $scenario readers=$readers writers=$writers"
    run_bench mongo "$mongo_raw" "$database" "$readers" "$reads" "$writers" "$writes" \
      -mongo-uri "$MONGO_URI"
    printf "mongo\t%s\t%s\t%s\t%s\t0\n" "$mongo_config" "$DOCS" "$INDEXES" "$mongo_raw_rel" >>"$MATRIX"
  fi
}

for writers in $WRITERS_LIST; do
  run_cell "writers_${writers}" 0 0 "$writers" "$CONCURRENT_WRITES"
done

for readers in $READERS_LIST; do
  run_cell "readers_${readers}" "$readers" "$CONCURRENT_READS" 0 0
done

report_extra=()
if [[ "$INCLUDE_MONGO" != "1" ]]; then
  report_extra=(-allow-incomplete)
fi

"$REPORT_BIN" \
  -matrix "$MATRIX" \
  -report "$REPORT" \
  -summary "$SUMMARY" \
  -title "$TITLE" \
  "${report_extra[@]}"

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
- raw JSON directory: \`$RAW_DIR\`
- TreeDB data directory: \`$TREE_DIR\`
- TreeDB location metadata: \`$TREE_METADATA\`
- docs: \`$DOCS\`
- secondary indexes: \`$INDEXES\`
- batch size: \`$BATCH_SIZE\`
- insert producers: \`$INSERT_PRODUCERS\`
- writer sweep: \`$WRITERS_LIST\`
- reader sweep: \`$READERS_LIST\`
- concurrent writes per writer cell: \`$CONCURRENT_WRITES\`
- concurrent reads per reader cell: \`$CONCURRENT_READS\`
- TreeDB profile: \`$TREEDB_PROFILE\`
- TreeDB document format: \`$TREEDB_DOCUMENT_FORMAT\`
- TreeDB client mode: \`$TREEDB_CLIENT_MODE\`
- TreeDB maintenance: \`$TREEDB_MAINTENANCE\`
- update indexed field: \`$UPDATE_INDEXED_FIELD\`
- include MongoDB: \`$INCLUDE_MONGO\`
- MongoDB URI: \`$MONGO_URI\`
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
echo "bundle README: $README"
