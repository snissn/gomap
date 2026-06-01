#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$(mktemp -d /tmp/gomap_collections_harness_XXXXXX)}"
COUNT="${COUNT:-1}"
BENCHTIME="${BENCHTIME:-1s}"
BATCH_SIZE="${TREEDB_COLLECTION_BENCH_BATCH_SIZE:-8000}"
BENCH_ENGINE="${TREEDB_COLLECTION_BENCH_ENGINE:-command_wal_relaxed}"
PATH_LABEL="${TREEDB_COLLECTION_PATH_LABEL:-native-fastpath}"
DATA_OUTER="${TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG:-true}"
INDEX_OUTER="${TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG:-false}"
CHUNK_SIZE="$(printf '%s' "${TREEDB_COLLECTION_CHUNK_SIZE:-}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
CHUNK_SIZE_LABEL="${CHUNK_SIZE:-profile/default}"
PAGER_SYNC_CONCURRENCY="$(printf '%s' "${TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY:-}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
if [[ "$PAGER_SYNC_CONCURRENCY" == "0" ]]; then
  PAGER_SYNC_CONCURRENCY=""
fi
PAGER_SYNC_CONCURRENCY_LABEL="${PAGER_SYNC_CONCURRENCY:-profile/default}"
INCLUDE_SQLITE="${TREEDB_COLLECTION_HARNESS_INCLUDE_SQLITE:-false}"
INCLUDE_UNIFIED="${TREEDB_COLLECTION_HARNESS_INCLUDE_UNIFIED:-false}"
PROFILE_BENCHES="${TREEDB_COLLECTION_HARNESS_PROFILE_BENCHES:-false}"
TIMED_PROFILE_BENCHES="${TREEDB_COLLECTION_HARNESS_TIMED_PROFILE_BENCHES:-false}"
REPORT_VLOG_REWRITE="${TREEDB_COLLECTION_HARNESS_REPORT_VLOG_REWRITE:-false}"
REPORT_SQLITE_VACUUM="${TREEDB_COLLECTION_HARNESS_REPORT_SQLITE_VACUUM:-false}"
SQLITE_CGO_ENABLED="${TREEDB_COLLECTION_SQLITE_CGO_ENABLED:-1}"
UNIFIED_KEYS="${TREEDB_COLLECTION_HARNESS_UNIFIED_KEYS:-100000}"
UNIFIED_VALSIZE="${TREEDB_COLLECTION_HARNESS_UNIFIED_VALSIZE:-128}"
UNIFIED_TESTS="${TREEDB_COLLECTION_HARNESS_UNIFIED_TESTS:-sequential_write,random_write,batch_write,batch_random,random_read,random_read_parallel_acquire_snapshot,full_scan,prefix_scan}"
COLLECTION_JSON_REGEX="${TREEDB_COLLECTION_HARNESS_JSON_REGEX:-Benchmark(CollectionShapeInsertBatch|CollectionShapeInsertBatchCheckpoint|CollectionShapeInsertBatchSingleStringJSON|CollectionShapeInsertBatchCheckpointSingleStringJSON|CollectionShapeReadPrimary|CollectionShapeReadPrimaryParallel|CollectionShapeReadPrimaryInto|CollectionShapeReadPrimaryIntoParallel|CollectionMixedReadWritePrimary|CollectionMixedReadWriteSecondaryUnique|SecondaryLookupUnique|SecondaryLookupNonUnique|CollectionOverheadPlanNoIndex|CollectionOverheadPlanIndexed|CollectionOverheadPlanIndexedTemplateV1|CollectionOverheadIndexStateJSONExtraction|CollectionOverheadIndexStateTemplateV1Extraction|CollectionOverheadPlanIndexedPrecomputedState)$}"
COLLECTION_TEMPLATE_REGEX="${TREEDB_COLLECTION_HARNESS_TEMPLATE_REGEX:-Benchmark(CollectionShapeInsertBatch|CollectionShapeInsertBatchCheckpoint|CollectionShapeReadPrimary|CollectionShapeReadPrimaryParallel|CollectionShapeReadPrimaryInto|CollectionShapeReadPrimaryIntoParallel|CollectionMixedReadWritePrimary|CollectionMixedReadWriteSecondaryUnique|SecondaryLookupUnique|SecondaryLookupNonUnique|CollectionOverheadPlanNoIndex|CollectionOverheadPlanIndexed|CollectionOverheadPlanIndexedTemplateV1|CollectionOverheadIndexStateJSONExtraction|CollectionOverheadIndexStateTemplateV1Extraction|CollectionOverheadPlanIndexedPrecomputedState)$}"
COLLECTION_INDEX_VLOG_REGEX="${TREEDB_COLLECTION_HARNESS_INDEX_VLOG_REGEX:-^BenchmarkCollectionShapeInsertBatch$/^indexes_2$}"
SQLITE_REGEX="${TREEDB_COLLECTION_HARNESS_SQLITE_REGEX:-BenchmarkSQLite(ShapeInsertBatchJSON|ShapeInsertBatchCheckpointJSON|ShapeInsertBatchNativeColumns|ShapeInsertBatchCheckpointNativeColumns|ShapeReadPrimaryJSON|ShapeReadPrimaryNativeColumns|ShapeSecondaryLookupJSON|ShapeSecondaryLookupNativeColumns|InsertBatchWithSecondaryIndexes|InsertBatchCheckpointWithSecondaryIndexes|NativeColumnsInsertBatchWithSecondaryIndexes|NativeColumnsInsertBatchCheckpointWithSecondaryIndexes)$}"
PROFILE_BENCH_LIST="${TREEDB_COLLECTION_HARNESS_PROFILE_BENCH_LIST:-BenchmarkCollectionShapeInsertBatch/indexes_2 BenchmarkCollectionShapeInsertBatchCheckpoint/indexes_2 BenchmarkCollectionMixedReadWritePrimary}"
TIMED_PROFILE_BENCH_LIST="${TREEDB_COLLECTION_HARNESS_TIMED_PROFILE_BENCH_LIST:-BenchmarkCollectionTimedProfileInsertBatchWithSecondaryIndexes BenchmarkCollectionTimedProfileInsertBatchCheckpointWithSecondaryIndexes}"
PROFILE_COUNT="${TREEDB_COLLECTION_HARNESS_PROFILE_COUNT:-1}"
PROFILE_BENCHTIME="${TREEDB_COLLECTION_HARNESS_PROFILE_BENCHTIME:-}"
TIMED_PROFILE_DOCS="${TREEDB_COLLECTION_HARNESS_TIMED_PROFILE_DOCS:-240000}"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
COMMIT="$(git rev-parse --short HEAD)"
WORKTREE="$ROOT"

usage() {
  cat <<'EOF'
usage: scripts/bench_collections_harness.sh [options]

Options:
  --out DIR              Output directory.
  --count N              go test benchmark count.
  --benchtime VALUE      go test -benchtime value.
  --batch-size N         Collection and unified bench batch size.
  --engine NAME          Collection TreeDB profile/engine label.
  --include-sqlite       Include SQLite JSON/native-column parity benches.
  --include-unified      Include raw TreeDB unified_bench profile-dir anchors.
  --profile-benches      Add focused whole-process pprof bundles.
  --timed-profiles       Add timed-window collection CPU pprof bundles.
  --unified-keys N       Key count for unified_bench anchors.
  --help                 Show this help.

Environment variables with the TREEDB_COLLECTION_HARNESS_* prefix can override
the benchmark regexes, unified bench tests, profile lists, and profile sizes.
EOF
}

is_true() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      OUT_DIR="$2"
      shift 2
      ;;
    --count)
      COUNT="$2"
      shift 2
      ;;
    --benchtime)
      BENCHTIME="$2"
      shift 2
      ;;
    --batch-size)
      BATCH_SIZE="$2"
      shift 2
      ;;
    --engine)
      BENCH_ENGINE="$2"
      shift 2
      ;;
    --include-sqlite)
      INCLUDE_SQLITE=true
      shift
      ;;
    --include-unified)
      INCLUDE_UNIFIED=true
      shift
      ;;
    --profile-benches)
      PROFILE_BENCHES=true
      shift
      ;;
    --timed-profiles)
      TIMED_PROFILE_BENCHES=true
      shift
      ;;
    --unified-keys)
      UNIFIED_KEYS="$2"
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

if [[ -z "$PROFILE_BENCHTIME" ]]; then
  PROFILE_BENCHTIME="$BENCHTIME"
fi

mkdir -p "$OUT_DIR"
INDEX_TSV="$OUT_DIR/harness_matrix_index.tsv"
PROFILE_INDEX_TSV="$OUT_DIR/harness_profile_index.tsv"
UNIFIED_INDEX_TSV="$OUT_DIR/harness_unified_index.tsv"

printf "cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json\tcpu_profile\tmem_profile\n" >"$INDEX_TSV"
printf "cell\tengine\tdocument_format\tbenchmark\tcpu_profile_mode\treport_md\treport_json\tcpu_profile\tmem_profile\tcpu_top\tmem_top\n" >"$PROFILE_INDEX_TSV"
printf "cell\tprofile\tkeys\tvalsize\tbatchsize\ttests\tprofile_dir\tbenchprof_md\tinsights_md\tinsights_html\n" >"$UNIFIED_INDEX_TSV"

regex_escape_segment() {
  printf '%s' "$1" | sed -e 's/[][\\.^$*+?{}()|]/\\&/g'
}

bench_regex_for_go_test() {
  local bench=$1
  local out=""
  local parts=()
  local part
  local escaped
  IFS='/' read -r -a parts <<<"$bench"
  for part in "${parts[@]}"; do
    escaped=$(regex_escape_segment "$part")
    if [[ -n "$out" ]]; then
      out+="/"
    fi
    out+="^${escaped}$"
  done
  printf '%s' "$out"
}

echo "running collections harness into: $OUT_DIR"
echo "worktree: $WORKTREE"
echo "branch: $BRANCH"
echo "commit: $COMMIT"
echo "engine: $BENCH_ENGINE"
echo "count: $COUNT"
echo "benchtime: $BENCHTIME"
echo "batch size: $BATCH_SIZE"
echo "pager chunk size: $CHUNK_SIZE_LABEL"
echo "pager sync concurrency: $PAGER_SYNC_CONCURRENCY_LABEL"
echo "include sqlite: $INCLUDE_SQLITE"
echo "include unified: $INCLUDE_UNIFIED"
echo "report value-log rewrite: $REPORT_VLOG_REWRITE"
echo "report sqlite vacuum: $REPORT_SQLITE_VACUUM"

run_collection_cell() {
  local cell=$1
  local document_format=$2
  local regex=$3
  local data_outer=${4:-$DATA_OUTER}
  local index_outer=${5:-$INDEX_OUTER}
  local cell_dir="$OUT_DIR/$cell"

  echo
  echo "==> collection cell $cell"
  OUT_DIR="$cell_dir" \
    BENCH_REGEX="$regex" \
    COUNT="$COUNT" \
    BENCHTIME="$BENCHTIME" \
    TREEDB_COLLECTION_PATH_LABEL="$PATH_LABEL" \
    TREEDB_COLLECTION_BENCH_ENGINE="$BENCH_ENGINE" \
    TREEDB_COLLECTION_BENCH_BATCH_SIZE="$BATCH_SIZE" \
    TREEDB_COLLECTION_DOCUMENT_FORMAT="$document_format" \
    TREEDB_COLLECTION_CHUNK_SIZE="$CHUNK_SIZE" \
    TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="$data_outer" \
    TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="$index_outer" \
    TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY="$PAGER_SYNC_CONCURRENCY" \
    TREEDB_COLLECTION_REPORT_VLOG_REWRITE="$REPORT_VLOG_REWRITE" \
    scripts/bench_collections_report.sh

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$cell" "$BENCH_ENGINE" "$document_format" "$data_outer" "$index_outer" \
    "$CHUNK_SIZE_LABEL" "$PAGER_SYNC_CONCURRENCY_LABEL" \
    "$cell_dir/collections_report.md" \
    "$cell_dir/collections_report.json" \
    "$cell_dir/collections_cpu.pprof" \
    "$cell_dir/collections_mem.pprof" >>"$INDEX_TSV"
}

run_profile_cell_benches() {
  local cell=$1
  local document_format=$2
  local benches=()
  read -r -a benches <<<"$PROFILE_BENCH_LIST"
  for bench in "${benches[@]}"; do
    local profile_dir="$OUT_DIR/$cell/profiles/$(printf '%s' "$bench" | tr '/ ' '__')"
    local bench_regex
    bench_regex=$(bench_regex_for_go_test "$bench")
    echo
    echo "==> profile $cell $bench"
    OUT_DIR="$profile_dir" \
      BENCH_REGEX="$bench_regex" \
      COUNT="$PROFILE_COUNT" \
      BENCHTIME="$PROFILE_BENCHTIME" \
      TREEDB_COLLECTION_PATH_LABEL="$PATH_LABEL" \
      TREEDB_COLLECTION_BENCH_ENGINE="$BENCH_ENGINE" \
      TREEDB_COLLECTION_BENCH_BATCH_SIZE="$BATCH_SIZE" \
      TREEDB_COLLECTION_DOCUMENT_FORMAT="$document_format" \
      TREEDB_COLLECTION_CHUNK_SIZE="$CHUNK_SIZE" \
      TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="$DATA_OUTER" \
      TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="$INDEX_OUTER" \
      TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY="$PAGER_SYNC_CONCURRENCY" \
      scripts/bench_collections_report.sh
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$cell" "$BENCH_ENGINE" "$document_format" "$bench" "whole_go_test_process" \
      "$profile_dir/collections_report.md" \
      "$profile_dir/collections_report.json" \
      "$profile_dir/collections_cpu.pprof" \
      "$profile_dir/collections_mem.pprof" \
      "$profile_dir/collections_cpu_top.txt" \
      "$profile_dir/collections_mem_top.txt" >>"$PROFILE_INDEX_TSV"
  done
}

run_timed_profile_cell_benches() {
  local cell=$1
  local document_format=$2
  local benches=()
  read -r -a benches <<<"$TIMED_PROFILE_BENCH_LIST"
  for bench in "${benches[@]}"; do
    local profile_dir="$OUT_DIR/$cell/timed_profiles/$bench"
    local bench_regex
    bench_regex=$(bench_regex_for_go_test "$bench")
    echo
    echo "==> timed profile $cell $bench"
    OUT_DIR="$profile_dir" \
      BENCH_REGEX="$bench_regex" \
      COUNT=1 \
      BENCHTIME="${TIMED_PROFILE_DOCS}x" \
      TREEDB_COLLECTION_TIMED_CPU_PROFILE=true \
      TREEDB_COLLECTION_PATH_LABEL="$PATH_LABEL" \
      TREEDB_COLLECTION_BENCH_ENGINE="$BENCH_ENGINE" \
      TREEDB_COLLECTION_BENCH_BATCH_SIZE="$BATCH_SIZE" \
      TREEDB_COLLECTION_DOCUMENT_FORMAT="$document_format" \
      TREEDB_COLLECTION_CHUNK_SIZE="$CHUNK_SIZE" \
      TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="$DATA_OUTER" \
      TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="$INDEX_OUTER" \
      TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY="$PAGER_SYNC_CONCURRENCY" \
      scripts/bench_collections_report.sh
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$cell" "$BENCH_ENGINE" "$document_format" "$bench" "timed_benchmark_window" \
      "$profile_dir/collections_report.md" \
      "$profile_dir/collections_report.json" \
      "$profile_dir/collections_cpu.pprof" \
      "$profile_dir/collections_mem.pprof" \
      "$profile_dir/collections_cpu_top.txt" \
      "$profile_dir/collections_mem_top.txt" >>"$PROFILE_INDEX_TSV"
  done
}

run_sqlite_cell() {
  if ! is_true "$SQLITE_CGO_ENABLED"; then
    echo "SQLite harness requires CGO; set TREEDB_COLLECTION_SQLITE_CGO_ENABLED=1 or omit --include-sqlite" >&2
    exit 2
  fi
  if ! command -v cc >/dev/null 2>&1 && ! command -v gcc >/dev/null 2>&1 && ! command -v clang >/dev/null 2>&1; then
    echo "SQLite harness requires a C compiler for github.com/mattn/go-sqlite3" >&2
    exit 2
  fi

  local cell="sqlite_wal_normal_shapes"
  local cell_dir="$OUT_DIR/$cell"
  echo
  echo "==> sqlite cell $cell"
  OUT_DIR="$cell_dir" \
    BENCH_REGEX="$SQLITE_REGEX" \
    COUNT="$COUNT" \
    BENCHTIME="$BENCHTIME" \
    TREEDB_COLLECTION_PATH_LABEL="sqlite" \
    TREEDB_COLLECTION_BENCH_ENGINE="sqlite_wal_normal" \
    TREEDB_COLLECTION_BENCH_BATCH_SIZE="$BATCH_SIZE" \
    TREEDB_COLLECTION_DOCUMENT_FORMAT="json" \
    TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="-" \
    TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="-" \
    TREEDB_COLLECTION_REPORT_SQLITE_VACUUM="$REPORT_SQLITE_VACUUM" \
    GO_TEST_TAGS="sqlite_bench" \
    CGO_ENABLED=1 \
    scripts/bench_collections_report.sh

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$cell" "sqlite_wal_normal" "json" "-" "-" "-" "-" \
    "$cell_dir/collections_report.md" \
    "$cell_dir/collections_report.json" \
    "$cell_dir/collections_cpu.pprof" \
    "$cell_dir/collections_mem.pprof" >>"$INDEX_TSV"
}

run_unified_anchor() {
  local profile=$1
  local cell="unified_${profile}"
  local profile_dir="$OUT_DIR/$cell"
  mkdir -p "$profile_dir"
  echo
  echo "==> unified bench $profile"
  GOWORK=off go run ./cmd/unified_bench \
    -dbs treedb \
    -profile "$profile" \
    -path-label "$PATH_LABEL" \
    -keys "$UNIFIED_KEYS" \
    -valsize "$UNIFIED_VALSIZE" \
    -batchsize "$BATCH_SIZE" \
    -test "$UNIFIED_TESTS" \
    -checkpoint-between-tests \
    -read-require-hit \
    -profile-dir "$profile_dir" \
    -format markdown \
    -progress=false | tee "$profile_dir/unified_bench_stdout.md"

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$cell" "$profile" "$UNIFIED_KEYS" "$UNIFIED_VALSIZE" "$BATCH_SIZE" "$UNIFIED_TESTS" \
    "$profile_dir" \
    "$profile_dir/benchprof_results.md" \
    "$profile_dir/insights.md" \
    "$profile_dir/insights.html" >>"$UNIFIED_INDEX_TSV"
}

run_collection_cell "collections_json_shapes" "json" "$COLLECTION_JSON_REGEX"
run_collection_cell "collections_template_v1_shapes" "template-v1" "$COLLECTION_TEMPLATE_REGEX"
run_collection_cell "collections_json_index_vlog_insert2" "json" "$COLLECTION_INDEX_VLOG_REGEX" "$DATA_OUTER" true
run_collection_cell "collections_template_v1_index_vlog_insert2" "template-v1" "$COLLECTION_INDEX_VLOG_REGEX" "$DATA_OUTER" true

if is_true "$PROFILE_BENCHES"; then
  run_profile_cell_benches "collections_json_shapes" "json"
  run_profile_cell_benches "collections_template_v1_shapes" "template-v1"
fi

if is_true "$TIMED_PROFILE_BENCHES"; then
  run_timed_profile_cell_benches "collections_json_shapes" "json"
  run_timed_profile_cell_benches "collections_template_v1_shapes" "template-v1"
fi

if is_true "$INCLUDE_SQLITE"; then
  run_sqlite_cell
fi

GOWORK=off go run ./cmd/collection_bench_matrix_summary \
  -matrix-index "$INDEX_TSV" \
  -out-dir "$OUT_DIR" \
  -available-benchmarks

if is_true "$INCLUDE_UNIFIED"; then
  run_unified_anchor "fast"
  run_unified_anchor "wal_on_fast"
fi

cat >"$OUT_DIR/README.md" <<EOF
# Collections Benchmark Harness

- output directory: \`$OUT_DIR\`
- worktree: \`$WORKTREE\`
- branch: \`$BRANCH\`
- commit: \`$COMMIT\`
- benchmark engine: \`$BENCH_ENGINE\`
- benchmark count: \`$COUNT\`
- benchmark time: \`$BENCHTIME\`
- collection batch size: \`$BATCH_SIZE\`
- pager chunk size: \`$CHUNK_SIZE_LABEL\`
- pager sync concurrency: \`$PAGER_SYNC_CONCURRENCY_LABEL\`
- include sqlite: \`$INCLUDE_SQLITE\`
- include unified bench: \`$INCLUDE_UNIFIED\`
- profile benches: \`$PROFILE_BENCHES\`
- timed profile benches: \`$TIMED_PROFILE_BENCHES\`
- report value-log rewrite: \`$REPORT_VLOG_REWRITE\`
- report sqlite vacuum: \`$REPORT_SQLITE_VACUUM\`

## Summary Artifacts

- collection matrix index: \`$INDEX_TSV\`
- collection matrix summary: \`$OUT_DIR/collections_matrix_summary.md\`
- collection matrix summary html: \`$OUT_DIR/collections_matrix_summary.html\`
- collection matrix summary tsv: \`$OUT_DIR/collections_matrix_summary.tsv\`
- user-story throughput tsv: \`$OUT_DIR/collections_user_story_summary.tsv\`
- disk-usage summary tsv: \`$OUT_DIR/collections_disk_usage_summary.tsv\`
- maintenance compaction tsv: \`$OUT_DIR/collections_maintenance_summary.tsv\`
- profile index: \`$PROFILE_INDEX_TSV\`
- unified index: \`$UNIFIED_INDEX_TSV\`

Latency columns in the generated reports include adjacent throughput columns, for example \`ns/op\` with \`ops/sec\` and \`insert ns/doc\` with \`insert docs/sec\`.
Insert benchmark rows may also include untimed end-of-run disk metrics, and the disk summary compares total, collection, and index bytes using per-doc deltas against matching zero-index baselines.

## Cells

- JSON collection shapes: \`$OUT_DIR/collections_json_shapes/collections_report.md\`
- template-v1 collection shapes: \`$OUT_DIR/collections_template_v1_shapes/collections_report.md\`
- JSON two-index insert with index outer leaves in value log: \`$OUT_DIR/collections_json_index_vlog_insert2/collections_report.md\`
- template-v1 two-index insert with index outer leaves in value log: \`$OUT_DIR/collections_template_v1_index_vlog_insert2/collections_report.md\`
EOF

if is_true "$INCLUDE_SQLITE"; then
  echo "- SQLite shape parity: \`$OUT_DIR/sqlite_wal_normal_shapes/collections_report.md\`" >>"$OUT_DIR/README.md"
fi
if is_true "$INCLUDE_UNIFIED"; then
  cat >>"$OUT_DIR/README.md" <<EOF
- unified fast benchprof: \`$OUT_DIR/unified_fast/benchprof_results.md\`
- unified fast insights: \`$OUT_DIR/unified_fast/insights.md\`
- unified WAL-on-fast benchprof: \`$OUT_DIR/unified_wal_on_fast/benchprof_results.md\`
- unified WAL-on-fast insights: \`$OUT_DIR/unified_wal_on_fast/insights.md\`
EOF
fi

echo
echo "harness readme: $OUT_DIR/README.md"
echo "matrix summary: $OUT_DIR/collections_matrix_summary.md"
echo "matrix html:    $OUT_DIR/collections_matrix_summary.html"
