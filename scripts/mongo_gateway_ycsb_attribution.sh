#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "$TMP_BASE/treedb_mongo_ycsb_attribution_XXXXXX")}"
DOCUMENTS="${DOCUMENTS:-100000}"
BATCH_SIZE="${BATCH_SIZE:-1}"
INSERT_PRODUCERS="${INSERT_PRODUCERS:-16}"
MONGO_MAX_POOL_SIZE="${MONGO_MAX_POOL_SIZE:-16}"
MONGO_MIN_POOL_SIZE="${MONGO_MIN_POOL_SIZE:-0}"
MONGO_MAX_CONNECTING="${MONGO_MAX_CONNECTING:-16}"
DOCUMENT_SHAPE="${DOCUMENT_SHAPE:-gateway}"
POINT_READ_PROJECTION="${POINT_READ_PROJECTION:-full}"
if [[ "$DOCUMENT_SHAPE" == "ycsb" ]]; then
  INDEXES_LIST="${INDEXES_LIST:-0}"
else
  INDEXES_LIST="${INDEXES_LIST:-0 1}"
fi
CLIENT_MODES="${CLIENT_MODES:-driver raw-wire-tcp native-wire-tcp direct}"
DATABASE_PREFIX="${DATABASE_PREFIX:-ycsb_attr}"
COLLECTION="${COLLECTION:-usertable}"
TIMEOUT="${TIMEOUT:-10m}"
# Mixed-shape benchmark baseline: gateway-shape runs include secondary indexes.
# Use command_wal_relaxed for command-WAL coverage once indexed catalog command
# support is enabled for this workload; bench_unsafe is the explicit no-WAL ceiling.
TREEDB_PROFILE="${TREEDB_PROFILE:-bench_unsafe}"
TREEDB_DOCUMENT_FORMAT="${TREEDB_DOCUMENT_FORMAT:-bson}"
TREEDB_MAINTENANCE="${TREEDB_MAINTENANCE:-none}"
TREEDB_READ_STATE="${TREEDB_READ_STATE:-unsettled}"
PREBUILD_DOCUMENTS="${PREBUILD_DOCUMENTS:-true}"

READS="${READS:-0}"
RANGE_READS="${RANGE_READS:-0}"
UPDATES="${UPDATES:-0}"
DELETES="${DELETES:-0}"
CONCURRENT_READERS="${CONCURRENT_READERS:-0}"
CONCURRENT_READ_KINDS="${CONCURRENT_READ_KINDS:-id}"
CONCURRENT_READS="${CONCURRENT_READS:-0}"
CONCURRENT_RANGE_READERS="${CONCURRENT_RANGE_READERS:-0}"
CONCURRENT_RANGE_READS="${CONCURRENT_RANGE_READS:-0}"
CONCURRENT_WRITERS="${CONCURRENT_WRITERS:-0}"
CONCURRENT_WRITES="${CONCURRENT_WRITES:-0}"

PROFILE="${PROFILE:-false}"
PROFILE_MODES="${PROFILE_MODES:-driver raw-wire-tcp}"
PROFILE_INDEXES="${PROFILE_INDEXES:-$INDEXES_LIST}"
PROFILE_BLOCK_RATE="${PROFILE_BLOCK_RATE:-1}"
PROFILE_MUTEX_FRACTION="${PROFILE_MUTEX_FRACTION:-5}"
PROFILE_TRACE="${PROFILE_TRACE:-false}"
PROFILE_HEAP_GC="${PROFILE_HEAP_GC:-false}"

BENCH_BIN="${BENCH_BIN:-bin/mongo_gateway_bench}"

usage() {
  cat <<'EOF'
Usage: scripts/mongo_gateway_ycsb_attribution.sh

Runs a TreeDB Mongo gateway attribution matrix for the YCSB load shape:
BSON documents, batch size 1, 16 insert producers, and 0/1 secondary indexes.

Environment overrides:
  OUT_DIR, DOCUMENTS, BATCH_SIZE, INSERT_PRODUCERS,
  INDEXES_LIST, CLIENT_MODES, DATABASE_PREFIX, COLLECTION, TIMEOUT,
  TREEDB_PROFILE, TREEDB_DOCUMENT_FORMAT, TREEDB_MAINTENANCE, TREEDB_READ_STATE,
  DOCUMENT_SHAPE, POINT_READ_PROJECTION,
  MONGO_MAX_POOL_SIZE, MONGO_MIN_POOL_SIZE, MONGO_MAX_CONNECTING,
  PREBUILD_DOCUMENTS,
  READS, RANGE_READS, UPDATES, DELETES,
  CONCURRENT_READERS, CONCURRENT_READ_KINDS, CONCURRENT_READS,
  CONCURRENT_RANGE_READERS, CONCURRENT_RANGE_READS,
  CONCURRENT_WRITERS, CONCURRENT_WRITES,
  PROFILE, PROFILE_MODES, PROFILE_INDEXES, PROFILE_BLOCK_RATE,
  PROFILE_MUTEX_FRACTION, PROFILE_TRACE, PROFILE_HEAP_GC.

Examples:
  PROFILE=true scripts/mongo_gateway_ycsb_attribution.sh
  DOCUMENTS=1000 CLIENT_MODES="driver direct" INDEXES_LIST=0 scripts/mongo_gateway_ycsb_attribution.sh

Outputs:
  $OUT_DIR/summary.tsv
  $OUT_DIR/summary.md
  $OUT_DIR/index_<n>/<client_mode>/result.json
  $OUT_DIR/index_<n>/<client_mode>/profiles/* when PROFILE=true for that cell
EOF
}

bool_true() {
  case "${1,,}" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

list_contains() {
  local needle=$1
  shift
  local item
  for item in "$@"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

sanitize_name() {
  local value=$1
  value=${value//[^A-Za-z0-9_.-]/_}
  value=${value//-/_}
  printf '%s' "$value"
}

run_cpu_top_summaries() {
  local profile_dir=$1
  local cpu
  shopt -s nullglob
  for cpu in "$profile_dir"/*.cpu.pprof; do
    go tool pprof -top "$cpu" > "${cpu%.pprof}_top.txt"
  done
  shopt -u nullglob
}

if [[ "${1-}" == "--help" || "${1-}" == "-h" ]]; then
  usage
  exit 0
fi

mkdir -p "$OUT_DIR"

echo "building $BENCH_BIN"
GOWORK=off go build -o "$BENCH_BIN" ./cmd/mongo_gateway_bench

read -r -a client_modes <<< "$CLIENT_MODES"
read -r -a indexes_values <<< "$INDEXES_LIST"
read -r -a profile_modes <<< "$PROFILE_MODES"
read -r -a profile_indexes <<< "$PROFILE_INDEXES"

cat > "$OUT_DIR/config.txt" <<EOF
documents=$DOCUMENTS
batch_size=$BATCH_SIZE
insert_producers=$INSERT_PRODUCERS
mongo_max_pool_size=$MONGO_MAX_POOL_SIZE
mongo_min_pool_size=$MONGO_MIN_POOL_SIZE
mongo_max_connecting=$MONGO_MAX_CONNECTING
document_shape=$DOCUMENT_SHAPE
point_read_projection=$POINT_READ_PROJECTION
indexes_list=$INDEXES_LIST
client_modes=$CLIENT_MODES
treedb_profile=$TREEDB_PROFILE
treedb_document_format=$TREEDB_DOCUMENT_FORMAT
treedb_maintenance=$TREEDB_MAINTENANCE
treedb_read_state=$TREEDB_READ_STATE
prebuild_documents=$PREBUILD_DOCUMENTS
reads=$READS
range_reads=$RANGE_READS
updates=$UPDATES
deletes=$DELETES
concurrent_readers=$CONCURRENT_READERS
concurrent_read_kinds=$CONCURRENT_READ_KINDS
concurrent_reads=$CONCURRENT_READS
concurrent_range_readers=$CONCURRENT_RANGE_READERS
concurrent_range_reads=$CONCURRENT_RANGE_READS
concurrent_writers=$CONCURRENT_WRITERS
concurrent_writes=$CONCURRENT_WRITES
profile=$PROFILE
profile_modes=$PROFILE_MODES
profile_indexes=$PROFILE_INDEXES
EOF

for indexes in "${indexes_values[@]}"; do
  for mode in "${client_modes[@]}"; do
    mode_dir=$(sanitize_name "$mode")
    cell_dir="$OUT_DIR/index_${indexes}/${mode_dir}"
    mkdir -p "$cell_dir"

    args=(
      "$BENCH_BIN"
      -target treedb
      -treedb-dir "$cell_dir/db"
      -database "${DATABASE_PREFIX}_${indexes}_${mode_dir}"
      -collection "$COLLECTION"
      -documents "$DOCUMENTS"
      -batch-size "$BATCH_SIZE"
      -insert-producers "$INSERT_PRODUCERS"
      -mongo-max-pool-size "$MONGO_MAX_POOL_SIZE"
      -mongo-min-pool-size "$MONGO_MIN_POOL_SIZE"
      -mongo-max-connecting "$MONGO_MAX_CONNECTING"
      -document-shape "$DOCUMENT_SHAPE"
      -point-read-projection "$POINT_READ_PROJECTION"
      -secondary-indexes "$indexes"
      -client-mode "$mode"
      -treedb-profile "$TREEDB_PROFILE"
      -treedb-document-format "$TREEDB_DOCUMENT_FORMAT"
      -treedb-maintenance "$TREEDB_MAINTENANCE"
      -treedb-read-state "$TREEDB_READ_STATE"
      -reads "$READS"
      -range-reads "$RANGE_READS"
      -updates "$UPDATES"
      -deletes "$DELETES"
      -concurrent-readers "$CONCURRENT_READERS"
      -concurrent-read-kinds "$CONCURRENT_READ_KINDS"
      -concurrent-reads "$CONCURRENT_READS"
      -concurrent-range-readers "$CONCURRENT_RANGE_READERS"
      -concurrent-range-reads "$CONCURRENT_RANGE_READS"
      -concurrent-writers "$CONCURRENT_WRITERS"
      -concurrent-writes "$CONCURRENT_WRITES"
      -timeout "$TIMEOUT"
      -format json
    )
    if bool_true "$PREBUILD_DOCUMENTS"; then
      args+=(-prebuild-documents)
    fi

    profile_dir=""
    if bool_true "$PROFILE" && list_contains "$mode" "${profile_modes[@]}" && list_contains "$indexes" "${profile_indexes[@]}"; then
      profile_dir="$cell_dir/profiles"
      args+=(
        -profile-dir "$profile_dir"
        -profile-block-rate "$PROFILE_BLOCK_RATE"
        -profile-mutex-fraction "$PROFILE_MUTEX_FRACTION"
      )
      if bool_true "$PROFILE_TRACE"; then
        args+=(-profile-trace)
      fi
      if bool_true "$PROFILE_HEAP_GC"; then
        args+=(-profile-heap-gc)
      fi
    fi

    printf 'running indexes=%s mode=%s\n' "$indexes" "$mode"
    printf '%q ' "${args[@]}" > "$cell_dir/command.txt"
    printf '\n' >> "$cell_dir/command.txt"
    "${args[@]}" > "$cell_dir/result.json" 2> "$cell_dir/stderr.log"
    if [[ -n "$profile_dir" ]]; then
      run_cpu_top_summaries "$profile_dir"
    fi
  done
done

python - "$OUT_DIR" <<'PY'
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
rows = []
for result_path in sorted(out.glob("index_*/*/result.json")):
    data = json.loads(result_path.read_text())
    for phase in data.get("phases", []):
        if phase.get("operations", 0) <= 0:
            continue
        latency = phase.get("latency_micros") or {}
        rows.append({
            "indexes": data.get("secondary_indexes"),
            "mode": data.get("client_mode"),
            "phase": phase.get("name", ""),
            "ops": phase.get("ops_per_sec", 0.0),
            "sampled_ns": phase.get("sampled_ns_per_op", 0.0),
            "driver_mean_us": phase.get("driver_mean_latency_us", 0.0),
            "p50": latency.get("p50", 0.0),
            "p95": latency.get("p95", 0.0),
            "p99": latency.get("p99", 0.0),
            "producers": phase.get("effective_producers", ""),
            "profile_dir": data.get("profile_dir", ""),
            "result": str(result_path.relative_to(out)),
        })

headers = [
    "secondary_indexes",
    "client_mode",
    "phase",
    "ops_per_sec",
    "sampled_ns_per_op",
    "driver_mean_us",
    "p50_us",
    "p95_us",
    "p99_us",
    "effective_producers",
    "profile_dir",
    "result_json",
]

def fmt(value):
    if isinstance(value, float):
        return f"{value:.1f}"
    return "" if value is None else str(value)

with (out / "summary.tsv").open("w", encoding="utf-8") as f:
    f.write("\t".join(headers) + "\n")
    for row in rows:
        values = [
            row["indexes"],
            row["mode"],
            row["phase"],
            row["ops"],
            row["sampled_ns"],
            row["driver_mean_us"],
            row["p50"],
            row["p95"],
            row["p99"],
            row["producers"],
            row["profile_dir"],
            row["result"],
        ]
        f.write("\t".join(fmt(v) for v in values) + "\n")

with (out / "summary.md").open("w", encoding="utf-8") as f:
    f.write("# Mongo Gateway YCSB Attribution\n\n")
    config = (out / "config.txt").read_text(encoding="utf-8")
    f.write("```text\n")
    f.write(config)
    f.write("```\n\n")
    f.write("| secondary indexes | client mode | phase | ops/sec | sampled ns/op | driver mean us | p50 us | p95 us | p99 us | producers | result |\n")
    f.write("| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
    for row in rows:
        f.write(
            "| {indexes} | {mode} | {phase} | {ops:.1f} | {sampled_ns:.1f} | {driver_mean_us:.1f} | {p50:.1f} | {p95:.1f} | {p99:.1f} | {producers} | `{result}` |\n".format(**row)
        )
    profile_tops = sorted(out.glob("index_*/*/profiles/*_top.txt"))
    if profile_tops:
        f.write("\n## CPU Top Artifacts\n\n")
        for top in profile_tops:
            f.write(f"- `{top.relative_to(out)}`\n")
PY

echo "wrote $OUT_DIR/summary.md"
echo "wrote $OUT_DIR/summary.tsv"
