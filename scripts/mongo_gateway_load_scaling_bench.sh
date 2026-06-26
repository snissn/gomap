#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "$TMP_BASE/gomap_mongo_gateway_load_scaling_XXXXXX")}"
DOCS="${DOCS:-100000}"
INDEXES_LIST="${INDEXES_LIST:-0 1 2}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
PRODUCERS_LIST="${PRODUCERS_LIST:-1 2 4 8 16 32}"
MONGO_MODE="${MONGO_MODE:-docker}"
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
MONGO_IMAGE="${MONGO_IMAGE:-mongo:8}"
MONGO_COMPACT="${MONGO_COMPACT:-}"
MONGO_MAX_POOL_SIZE="${MONGO_MAX_POOL_SIZE:-128}"
MONGO_MAX_CONNECTING="${MONGO_MAX_CONNECTING:-32}"
TIMEOUT="${TIMEOUT:-120m}"
TITLE="${TITLE:-Mongo API InsertMany Producer Scaling}"

usage() {
  cat <<'EOF'
Usage: scripts/mongo_gateway_load_scaling_bench.sh [options]

Runs load-only TreeDB-vs-MongoDB InsertMany comparisons across insert producer
counts. Each producer count gets a normal mongo_gateway_compare.sh bundle under:

  <out>/producers_<N>/

The wrapper also writes an aggregate summary TSV at:

  <out>/summary.tsv

Options:
  --out DIR              Output directory. Default: mktemp under $TMPDIR or /tmp.
  --docs N               Documents per producer-count run. Default: 100000.
  --indexes LIST         Space- or comma-separated secondary index counts. Default: "0 1 2".
  --batch-size N         InsertMany batch size. Default: 1000.
  --producers LIST       Space- or comma-separated producer counts. Default: "1 2 4 8 16 32".
  --mongo-mode MODE      docker or external. Default: docker.
  --mongo-uri URI        MongoDB URI for external runs. Default: mongodb://127.0.0.1:27017.
  --mongo-image IMAGE    Docker image for docker mode. Default: mongo:8.
  --mongo-compact BOOL   Compact MongoDB before final stats collection.
  --timeout DURATION     Per-cell timeout. Default: 120m.
  --title TITLE          Bundle title.
  --help                 Show this help.

Environment overrides use the uppercase variable names in the script.
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

is_positive_int() {
  [[ "$1" =~ ^[0-9]+$ ]] && [[ "$1" -gt 0 ]]
}

is_nonnegative_int() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

max_int_in_list() {
  local max=0
  local value
  for value in $1; do
    if [[ "$value" -gt "$max" ]]; then
      max=$value
    fi
  done
  printf '%s' "$max"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      require_value "$1" "${2-}"
      OUT_DIR="$2"
      shift 2
      ;;
    --docs)
      require_value "$1" "${2-}"
      DOCS="$2"
      shift 2
      ;;
    --indexes)
      require_value "$1" "${2-}"
      INDEXES_LIST="$2"
      shift 2
      ;;
    --batch-size)
      require_value "$1" "${2-}"
      BATCH_SIZE="$2"
      shift 2
      ;;
    --producers)
      require_value "$1" "${2-}"
      PRODUCERS_LIST="$2"
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

INDEXES_LIST=$(normalize_list "$INDEXES_LIST")
PRODUCERS_LIST=$(normalize_list "$PRODUCERS_LIST")

if ! is_positive_int "$DOCS"; then
  echo "invalid --docs $DOCS (want positive integer)" >&2
  exit 2
fi
if ! is_positive_int "$BATCH_SIZE"; then
  echo "invalid --batch-size $BATCH_SIZE (want positive integer)" >&2
  exit 2
fi
for indexes in $INDEXES_LIST; do
  if ! is_nonnegative_int "$indexes"; then
    echo "invalid --indexes value $indexes (want non-negative integers)" >&2
    exit 2
  fi
done
for producers in $PRODUCERS_LIST; do
  if ! is_positive_int "$producers"; then
    echo "invalid --producers value $producers (want positive integers)" >&2
    exit 2
  fi
done
case "$MONGO_MODE" in
  docker|external) ;;
  *)
    echo "invalid --mongo-mode $MONGO_MODE (want docker or external)" >&2
    exit 2
    ;;
esac

OUT_DIR=$(mkdir -p "$(dirname "$OUT_DIR")" && cd "$(dirname "$OUT_DIR")" && pwd -P)/$(basename "$OUT_DIR")
mkdir -p "$OUT_DIR"

load_batches=$(((DOCS + BATCH_SIZE - 1) / BATCH_SIZE))
max_producers=$(max_int_in_list "$PRODUCERS_LIST")
if [[ "$load_batches" -lt "$max_producers" ]]; then
  echo "warning: docs=$DOCS batch_size=$BATCH_SIZE yields only $load_batches load batches; requested producers up to $max_producers will be capped" >&2
fi

RUN_INDEX="$OUT_DIR/producer_runs.tsv"
printf 'producer\trun_dir\tsummary_tsv\tmatrix_tsv\treport_md\n' > "$RUN_INDEX"

for producers in $PRODUCERS_LIST; do
  run_dir="$OUT_DIR/producers_${producers}"
  mongo_compact_args=()
  if [[ -n "$MONGO_COMPACT" ]]; then
    mongo_compact_args=(--mongo-compact "$MONGO_COMPACT")
  fi
  env \
    TREEDB_DOCUMENT_FORMATS=bson \
    TREEDB_CLIENT_MODES=driver-command-raw \
    MONGO_CLIENT_MODES=driver-command-raw \
    BATCH_SIZE="$BATCH_SIZE" \
    INSERT_PRODUCERS="$producers" \
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
    PROFILE_TREEDB=false \
    ./scripts/mongo_gateway_compare.sh \
      --out "$run_dir" \
      --docs "$DOCS" \
      --indexes "$INDEXES_LIST" \
      --mongo-mode "$MONGO_MODE" \
      --mongo-image "$MONGO_IMAGE" \
      "${mongo_compact_args[@]}" \
      --mongo-uri "$MONGO_URI" \
      --timeout "$TIMEOUT" \
      --title "$TITLE, producers=${producers}"
  printf '%s\t%s\t%s\t%s\t%s\n' "$producers" "producers_${producers}" "producers_${producers}/summary.tsv" "producers_${producers}/matrix.tsv" "producers_${producers}/report.md" >> "$RUN_INDEX"
done

AGG_SUMMARY="$OUT_DIR/summary.tsv"
tmp_summary="$OUT_DIR/.summary.tsv.tmp"
rm -f "$tmp_summary"
wrote_header=false
for producers in $PRODUCERS_LIST; do
  summary="$OUT_DIR/producers_${producers}/summary.tsv"
  if [[ ! -s "$summary" ]]; then
    echo "missing summary for producers=$producers: $summary" >&2
    exit 1
  fi
  include_header=false
  if [[ "$wrote_header" == false ]]; then
    include_header=true
    wrote_header=true
  fi
  if ! awk -v include_header="$include_header" -F '\t' '
    BEGIN {
      OFS = FS
    }
    NR == 1 {
      for (i = 1; i <= NF; i++) {
        if ($i == "phase") {
          phase_col = i
        }
      }
      if (include_header == "true") {
        print
      }
      next
    }
    phase_col > 0 && $phase_col == "load_insert_many" {
      rows++
      print
    }
    END {
      if (phase_col == 0) {
        exit 2
      }
      if (rows == 0) {
        exit 3
      }
    }
  ' "$summary" >> "$tmp_summary"; then
    echo "failed to append load_insert_many rows for producers=$producers from $summary" >&2
    exit 1
  fi
done
mv "$tmp_summary" "$AGG_SUMMARY"

cat > "$OUT_DIR/README.md" <<EOF
# Mongo Gateway InsertMany Producer Scaling Bundle

- output directory: \`$OUT_DIR\`
- aggregate summary TSV: \`$AGG_SUMMARY\`
- producer run index: \`$RUN_INDEX\`
- docs: \`$DOCS\`
- secondary indexes: \`$INDEXES_LIST\`
- batch size: \`$BATCH_SIZE\`
- load batches: \`$load_batches\`
- producer counts: \`$PRODUCERS_LIST\`
- MongoDB mode: \`$MONGO_MODE\`
- MongoDB image: \`$MONGO_IMAGE\`
- MongoDB URI: \`$MONGO_URI\`
- MongoDB compact: \`${MONGO_COMPACT:-mode default}\`
- MongoDB Go driver pool options: \`maxPoolSize=$MONGO_MAX_POOL_SIZE maxConnecting=$MONGO_MAX_CONNECTING\`
- timeout: \`$TIMEOUT\`

Each \`producers_<N>/\` directory is a normal load-only \`mongo_gateway_compare.sh\`
bundle with raw JSON, \`matrix.tsv\`, \`summary.tsv\`, and \`report.md\`.

The aggregate \`summary.tsv\` keeps one header and only the \`load_insert_many\`
rows from each producer run summary.
Use the \`insert_producers\`, \`effective_producers\`, and \`load_batch_count\`
columns to distinguish requested producer counts from capped effective counts.
EOF

echo "Mongo gateway load-scaling bundle: $OUT_DIR"
echo "Aggregate summary: $AGG_SUMMARY"
