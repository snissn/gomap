#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$(mktemp -d /tmp/gomap_collections_matrix_XXXXXX)}"
MATRIX="${TREEDB_COLLECTION_MATRIX:-production}"
PATH_LABEL_ENV="${TREEDB_COLLECTION_PATH_LABEL:-}"
PATH_LABEL="${PATH_LABEL_ENV:-native-fastpath}"
COUNT="${COUNT:-1}"
BENCHTIME="${BENCHTIME:-1s}"
BATCH_SIZE="${TREEDB_COLLECTION_BENCH_BATCH_SIZE:-8000}"
DOCUMENT_FORMAT="${TREEDB_COLLECTION_DOCUMENT_FORMAT:-json}"
CHUNK_SIZE="$(printf '%s' "${TREEDB_COLLECTION_CHUNK_SIZE:-}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
CHUNK_SIZE_LABEL="${CHUNK_SIZE:-profile/default}"
PAGER_SYNC_CONCURRENCY="$(printf '%s' "${TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY:-}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
if [[ "$PAGER_SYNC_CONCURRENCY" == "0" ]]; then
  PAGER_SYNC_CONCURRENCY=""
fi
PAGER_SYNC_CONCURRENCY_LABEL="${PAGER_SYNC_CONCURRENCY:-profile/default}"
BENCH_REGEX="${BENCH_REGEX:-Benchmark(CollectionInsertBatchWithSecondaryIndexes|CollectionInsertBatchCheckpointWithSecondaryIndexes|CollectionOverheadIndexStateJSONExtraction|CollectionOverheadIndexStateTemplateV1Extraction|CollectionOverheadPlanIndexedTemplateV1|CollectionOverheadPlanIndexedPrecomputedState)$}"
INCLUDE_SQLITE="${TREEDB_COLLECTION_INCLUDE_SQLITE:-false}"
SQLITE_ENGINE="${TREEDB_COLLECTION_SQLITE_ENGINE:-sqlite_wal_normal}"
SQLITE_CELL_LABEL="${TREEDB_COLLECTION_SQLITE_CELL:-$SQLITE_ENGINE}"
SQLITE_CELL_LABEL="$(printf '%s' "$SQLITE_CELL_LABEL" | tr -c '[:alnum:]_.-' '_')"
if [[ -z "$SQLITE_CELL_LABEL" ]]; then
  SQLITE_CELL_LABEL="sqlite"
fi
if [[ "$SQLITE_CELL_LABEL" == sqlite_* ]]; then
  SQLITE_CELL="$SQLITE_CELL_LABEL"
else
  SQLITE_CELL="sqlite_$SQLITE_CELL_LABEL"
fi
SQLITE_BENCH_REGEX="${TREEDB_COLLECTION_SQLITE_BENCH_REGEX:-BenchmarkSQLite(InsertBatchWithSecondaryIndexes|InsertBatchCheckpointWithSecondaryIndexes|NativeColumnsInsertBatchWithSecondaryIndexes|NativeColumnsInsertBatchCheckpointWithSecondaryIndexes)$}"
SQLITE_CGO_ENABLED="${TREEDB_COLLECTION_SQLITE_CGO_ENABLED:-1}"
PROFILE_BENCHES="${TREEDB_COLLECTION_PROFILE_BENCHES:-false}"
PROFILE_BENCH_LIST="${TREEDB_COLLECTION_PROFILE_BENCH_LIST:-BenchmarkCollectionInsertBatchWithSecondaryIndexes BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes}"
SQLITE_PROFILE_BENCH_LIST="${TREEDB_COLLECTION_SQLITE_PROFILE_BENCH_LIST:-BenchmarkSQLiteInsertBatchWithSecondaryIndexes BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes BenchmarkSQLiteNativeColumnsInsertBatchWithSecondaryIndexes BenchmarkSQLiteNativeColumnsInsertBatchCheckpointWithSecondaryIndexes}"
PROFILE_COUNT="${TREEDB_COLLECTION_PROFILE_COUNT:-1}"
PROFILE_BENCHTIME="${TREEDB_COLLECTION_PROFILE_BENCHTIME:-$BENCHTIME}"
TIMED_PROFILE_BENCHES="${TREEDB_COLLECTION_TIMED_PROFILE_BENCHES:-false}"
TIMED_PROFILE_BENCH_LIST="${TREEDB_COLLECTION_TIMED_PROFILE_BENCH_LIST:-BenchmarkCollectionTimedProfileInsertBatchWithSecondaryIndexes BenchmarkCollectionTimedProfileInsertBatchCheckpointWithSecondaryIndexes}"
TIMED_PROFILE_COUNT="${TREEDB_COLLECTION_TIMED_PROFILE_COUNT:-1}"
TIMED_PROFILE_DOCS="${TREEDB_COLLECTION_TIMED_PROFILE_DOCS:-240000}"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
COMMIT="$(git rev-parse --short HEAD)"
WORKTREE="$ROOT"

is_true() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

case "$MATRIX" in
  production)
    MATRIX_ROWS=(
      "command_wal_relaxed_data_vlog_index_leaf command_wal_relaxed true false"
      "command_wal_relaxed_data_vlog_index_vlog command_wal_relaxed true true"
      "bench_unsafe_data_vlog_index_leaf bench_unsafe true false"
      "bench_unsafe_data_vlog_index_vlog bench_unsafe true true"
    )
    ;;
  full)
    MATRIX_ROWS=(
      "command_wal_relaxed_data_leaf_index_leaf command_wal_relaxed false false"
      "command_wal_relaxed_data_leaf_index_vlog command_wal_relaxed false true"
      "command_wal_relaxed_data_vlog_index_leaf command_wal_relaxed true false"
      "command_wal_relaxed_data_vlog_index_vlog command_wal_relaxed true true"
      "bench_unsafe_data_leaf_index_leaf bench_unsafe false false"
      "bench_unsafe_data_leaf_index_vlog bench_unsafe false true"
      "bench_unsafe_data_vlog_index_leaf bench_unsafe true false"
      "bench_unsafe_data_vlog_index_vlog bench_unsafe true true"
    )
    ;;
  quick)
    MATRIX_ROWS=(
      "command_wal_relaxed_data_vlog_index_leaf command_wal_relaxed true false"
      "command_wal_relaxed_data_vlog_index_vlog command_wal_relaxed true true"
    )
    ;;
  sqlite)
    MATRIX_ROWS=()
    INCLUDE_SQLITE=true
    if [[ -z "$PATH_LABEL_ENV" ]]; then
      PATH_LABEL="sqlite"
    fi
    ;;
  *)
    echo "unknown TREEDB_COLLECTION_MATRIX=$MATRIX (want production, full, quick, or sqlite)" >&2
    exit 2
    ;;
esac

mkdir -p "$OUT_DIR"
INDEX_TSV="$OUT_DIR/matrix_index.tsv"
PROFILE_INDEX_TSV="$OUT_DIR/profile_index.tsv"
SUMMARY_MD="$OUT_DIR/README.md"

printf "cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json\tcpu_profile\tmem_profile\n" >"$INDEX_TSV"
printf "cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\tbenchmark\tcpu_profile_mode\treport_md\treport_json\tcpu_profile\tmem_profile\tcpu_top\tmem_top\n" >"$PROFILE_INDEX_TSV"

echo "running collections benchmark matrix into: $OUT_DIR"
echo "matrix: $MATRIX"
echo "execution path: $PATH_LABEL"
echo "benchmark regex: $BENCH_REGEX"
echo "benchmark count: $COUNT"
echo "benchmark time: $BENCHTIME"
echo "batch size: $BATCH_SIZE"
echo "document format: $DOCUMENT_FORMAT"
echo "pager chunk size: $CHUNK_SIZE_LABEL"
echo "pager sync concurrency: $PAGER_SYNC_CONCURRENCY_LABEL"
echo "profile bench captures: $PROFILE_BENCHES"
if is_true "$PROFILE_BENCHES"; then
  echo "profile benchmark list: $PROFILE_BENCH_LIST"
  echo "profile count: $PROFILE_COUNT"
  echo "profile benchmark time: $PROFILE_BENCHTIME"
fi
echo "timed CPU profile captures: $TIMED_PROFILE_BENCHES"
if is_true "$TIMED_PROFILE_BENCHES"; then
  echo "timed CPU profile benchmark list: $TIMED_PROFILE_BENCH_LIST"
  echo "timed CPU profile count: $TIMED_PROFILE_COUNT"
  echo "timed CPU profile docs: $TIMED_PROFILE_DOCS"
fi
echo "include sqlite: $INCLUDE_SQLITE"
if is_true "$INCLUDE_SQLITE"; then
  echo "sqlite engine: $SQLITE_ENGINE"
  echo "sqlite cell: $SQLITE_CELL"
  echo "sqlite benchmark regex: $SQLITE_BENCH_REGEX"
  echo "sqlite CGO_ENABLED: $SQLITE_CGO_ENABLED"
  if ! is_true "$SQLITE_CGO_ENABLED"; then
    echo "SQLite comparison requires CGO because sqlite benchmarks use the sqlite_bench && cgo build tags; set TREEDB_COLLECTION_SQLITE_CGO_ENABLED=1 or TREEDB_COLLECTION_INCLUDE_SQLITE=false" >&2
    exit 2
  fi
  if ! command -v cc >/dev/null 2>&1 && ! command -v gcc >/dev/null 2>&1 && ! command -v clang >/dev/null 2>&1; then
    echo "SQLite comparison requires a C compiler for github.com/mattn/go-sqlite3; install cc/gcc/clang or set TREEDB_COLLECTION_INCLUDE_SQLITE=false" >&2
    exit 2
  fi
fi

run_profile_benches() {
  local cell=$1
  local engine=$2
  local data_outer=$3
  local index_outer=$4
  local path_label=$5
  local bench_list=$6
  local go_tags=${7:-}
  local cgo_enabled=${8:-}
  local pager_sync_concurrency_label=${9:-$PAGER_SYNC_CONCURRENCY_LABEL}
  local document_format=${10:-$DOCUMENT_FORMAT}
  local pager_chunk_size_label="$CHUNK_SIZE_LABEL"
  if [[ "$pager_sync_concurrency_label" == "-" ]]; then
    pager_chunk_size_label="-"
  fi
  local benches=()

  read -r -a benches <<<"$bench_list"
  for bench in "${benches[@]}"; do
    if [[ -z "$bench" ]]; then
      continue
    fi
    local profile_dir="$OUT_DIR/$cell/profiles/$bench"
    local env_args=(
      "OUT_DIR=$profile_dir"
      "BENCH_REGEX=^${bench}$"
      "COUNT=$PROFILE_COUNT"
      "BENCHTIME=$PROFILE_BENCHTIME"
      "TREEDB_COLLECTION_PATH_LABEL=$path_label"
      "TREEDB_COLLECTION_BENCH_ENGINE=$engine"
      "TREEDB_COLLECTION_BENCH_BATCH_SIZE=$BATCH_SIZE"
      "TREEDB_COLLECTION_DOCUMENT_FORMAT=$document_format"
      "TREEDB_COLLECTION_CHUNK_SIZE=$CHUNK_SIZE"
      "TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=$data_outer"
      "TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=$index_outer"
      "TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY=$PAGER_SYNC_CONCURRENCY"
    )
    if [[ -n "$go_tags" ]]; then
      env_args+=("GO_TEST_TAGS=$go_tags")
    fi
    if [[ -n "$cgo_enabled" ]]; then
      env_args+=("CGO_ENABLED=$cgo_enabled")
    fi
    echo
    echo "==> $cell profile $bench"
    env "${env_args[@]}" scripts/bench_collections_report.sh

    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$cell" \
      "$engine" \
      "$document_format" \
      "$data_outer" \
      "$index_outer" \
      "$pager_chunk_size_label" \
      "$pager_sync_concurrency_label" \
      "$bench" \
      "whole_go_test_process" \
      "$profile_dir/collections_report.md" \
      "$profile_dir/collections_report.json" \
      "$profile_dir/collections_cpu.pprof" \
      "$profile_dir/collections_mem.pprof" \
      "$profile_dir/collections_cpu_top.txt" \
      "$profile_dir/collections_mem_top.txt" >>"$PROFILE_INDEX_TSV"
  done
}

run_timed_profile_benches() {
  local cell=$1
  local engine=$2
  local data_outer=$3
  local index_outer=$4
  local path_label=$5
  local bench_list=$6
  local benches=()

  read -r -a benches <<<"$bench_list"
  for bench in "${benches[@]}"; do
    if [[ -z "$bench" ]]; then
      continue
    fi
    local profile_dir="$OUT_DIR/$cell/timed_profiles/$bench"
    local env_args=(
      "OUT_DIR=$profile_dir"
      "BENCH_REGEX=^${bench}$"
      "COUNT=$TIMED_PROFILE_COUNT"
      "BENCHTIME=${TIMED_PROFILE_DOCS}x"
      "TREEDB_COLLECTION_TIMED_CPU_PROFILE=true"
      "TREEDB_COLLECTION_PATH_LABEL=$path_label"
      "TREEDB_COLLECTION_BENCH_ENGINE=$engine"
      "TREEDB_COLLECTION_BENCH_BATCH_SIZE=$BATCH_SIZE"
      "TREEDB_COLLECTION_DOCUMENT_FORMAT=$DOCUMENT_FORMAT"
      "TREEDB_COLLECTION_CHUNK_SIZE=$CHUNK_SIZE"
      "TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=$data_outer"
      "TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=$index_outer"
      "TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY=$PAGER_SYNC_CONCURRENCY"
    )
    echo
    echo "==> $cell timed CPU profile $bench"
    env "${env_args[@]}" scripts/bench_collections_report.sh

    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$cell" \
      "$engine" \
      "$DOCUMENT_FORMAT" \
      "$data_outer" \
      "$index_outer" \
      "$CHUNK_SIZE_LABEL" \
      "$PAGER_SYNC_CONCURRENCY_LABEL" \
      "$bench" \
      "timed_benchmark_window" \
      "$profile_dir/collections_report.md" \
      "$profile_dir/collections_report.json" \
      "$profile_dir/collections_cpu.pprof" \
      "$profile_dir/collections_mem.pprof" \
      "$profile_dir/collections_cpu_top.txt" \
      "$profile_dir/collections_mem_top.txt" >>"$PROFILE_INDEX_TSV"
  done
}

for row in ${MATRIX_ROWS[@]+"${MATRIX_ROWS[@]}"}; do
  read -r cell engine data_outer index_outer <<<"$row"
  cell_dir="$OUT_DIR/$cell"
  echo
  echo "==> $cell"
  OUT_DIR="$cell_dir" \
    BENCH_REGEX="$BENCH_REGEX" \
    COUNT="$COUNT" \
    BENCHTIME="$BENCHTIME" \
    TREEDB_COLLECTION_PATH_LABEL="$PATH_LABEL" \
    TREEDB_COLLECTION_BENCH_ENGINE="$engine" \
    TREEDB_COLLECTION_BENCH_BATCH_SIZE="$BATCH_SIZE" \
    TREEDB_COLLECTION_DOCUMENT_FORMAT="$DOCUMENT_FORMAT" \
    TREEDB_COLLECTION_CHUNK_SIZE="$CHUNK_SIZE" \
    TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="$data_outer" \
    TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="$index_outer" \
    TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY="$PAGER_SYNC_CONCURRENCY" \
    scripts/bench_collections_report.sh

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$cell" \
    "$engine" \
    "$DOCUMENT_FORMAT" \
    "$data_outer" \
    "$index_outer" \
    "$CHUNK_SIZE_LABEL" \
    "$PAGER_SYNC_CONCURRENCY_LABEL" \
    "$cell_dir/collections_report.md" \
    "$cell_dir/collections_report.json" \
    "$cell_dir/collections_cpu.pprof" \
    "$cell_dir/collections_mem.pprof" >>"$INDEX_TSV"

  if is_true "$PROFILE_BENCHES"; then
    run_profile_benches "$cell" "$engine" "$data_outer" "$index_outer" "$PATH_LABEL" "$PROFILE_BENCH_LIST"
  fi
  if is_true "$TIMED_PROFILE_BENCHES"; then
    run_timed_profile_benches "$cell" "$engine" "$data_outer" "$index_outer" "$PATH_LABEL" "$TIMED_PROFILE_BENCH_LIST"
  fi
done

if is_true "$INCLUDE_SQLITE"; then
  cell="$SQLITE_CELL"
  cell_dir="$OUT_DIR/$cell"
  echo
  echo "==> $cell"
  OUT_DIR="$cell_dir" \
    BENCH_REGEX="$SQLITE_BENCH_REGEX" \
    COUNT="$COUNT" \
    BENCHTIME="$BENCHTIME" \
    TREEDB_COLLECTION_PATH_LABEL="sqlite" \
    TREEDB_COLLECTION_BENCH_ENGINE="$SQLITE_ENGINE" \
    TREEDB_COLLECTION_BENCH_BATCH_SIZE="$BATCH_SIZE" \
    TREEDB_COLLECTION_DOCUMENT_FORMAT="json" \
    TREEDB_COLLECTION_CHUNK_SIZE="" \
    TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="-" \
    TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="-" \
    TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY="" \
    GO_TEST_TAGS="sqlite_bench" \
    CGO_ENABLED="$SQLITE_CGO_ENABLED" \
    scripts/bench_collections_report.sh

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$cell" \
    "$SQLITE_ENGINE" \
    "json" \
    "-" \
    "-" \
    "-" \
    "-" \
    "$cell_dir/collections_report.md" \
    "$cell_dir/collections_report.json" \
    "$cell_dir/collections_cpu.pprof" \
    "$cell_dir/collections_mem.pprof" >>"$INDEX_TSV"

  if is_true "$PROFILE_BENCHES"; then
    run_profile_benches "$cell" "$SQLITE_ENGINE" "-" "-" "sqlite" "$SQLITE_PROFILE_BENCH_LIST" "sqlite_bench" "$SQLITE_CGO_ENABLED" "-" "json"
  fi
fi

GOWORK=off go run ./cmd/collection_bench_matrix_summary \
  -matrix-index "$INDEX_TSV" \
  -out-dir "$OUT_DIR"

cat >"$SUMMARY_MD" <<EOF
# Collections Benchmark Matrix

- output directory: \`$OUT_DIR\`
- worktree: \`$WORKTREE\`
- branch: \`$BRANCH\`
- commit: \`$COMMIT\`
- execution path: \`$PATH_LABEL\`
- matrix: \`$MATRIX\`
- benchmark regex: \`$BENCH_REGEX\`
- benchmark count: \`$COUNT\`
- benchmark time: \`$BENCHTIME\`
- collection batch size: \`$BATCH_SIZE\`
- document format: \`$DOCUMENT_FORMAT\`
- pager chunk size: \`$CHUNK_SIZE_LABEL\`
- pager sync concurrency: \`$PAGER_SYNC_CONCURRENCY_LABEL\`
- focused profile captures: \`$PROFILE_BENCHES\`
- focused profile count: \`$PROFILE_COUNT\`
- focused profile benchmark time: \`$PROFILE_BENCHTIME\`
- timed CPU profile captures: \`$TIMED_PROFILE_BENCHES\`
- timed CPU profile count: \`$TIMED_PROFILE_COUNT\`
- timed CPU profile docs: \`$TIMED_PROFILE_DOCS\`
- include sqlite: \`$INCLUDE_SQLITE\`
- sqlite benchmark regex: \`$SQLITE_BENCH_REGEX\`
- matrix index: \`$INDEX_TSV\`
- focused profile index: \`$PROFILE_INDEX_TSV\`
- matrix summary markdown: \`$OUT_DIR/collections_matrix_summary.md\`
- matrix summary tsv: \`$OUT_DIR/collections_matrix_summary.tsv\`
- user-story throughput tsv: \`$OUT_DIR/collections_user_story_summary.tsv\`

## Cells

| Cell | Engine | Document format | Data outer leaves in vlog | Index outer leaves in vlog | Report |
| --- | --- | --- | --- | --- | --- |
EOF

for row in ${MATRIX_ROWS[@]+"${MATRIX_ROWS[@]}"}; do
  read -r cell engine data_outer index_outer <<<"$row"
  printf "| \`%s\` | \`%s\` | \`%s\` | \`%s\` | \`%s\` | [%s](%s) |\n" \
    "$cell" \
    "$engine" \
    "$DOCUMENT_FORMAT" \
    "$data_outer" \
    "$index_outer" \
    "$cell" \
    "$cell/collections_report.md" >>"$SUMMARY_MD"
done

if is_true "$INCLUDE_SQLITE"; then
  printf "| \`%s\` | \`%s\` | \`%s\` | \`%s\` | \`%s\` | [%s](%s) |\n" \
    "$SQLITE_CELL" \
    "$SQLITE_ENGINE" \
    "json" \
    "-" \
    "-" \
    "$SQLITE_CELL" \
    "$SQLITE_CELL/collections_report.md" >>"$SUMMARY_MD"
fi

cat >>"$SUMMARY_MD" <<'EOF'

## Intended 768 Use

The production matrix keeps collection data roots in value-log outer-leaf mode and varies index roots between inline outer leaves and value-log outer leaves. The `command_wal_relaxed` cells exercise the current command-WAL collection profile, while `bench_unsafe` cells provide the explicit benchmark-only no-WAL ceiling without changing the collection storage policy.

The focused default benchmark set keeps JSON extraction overhead, non-JSON indexed planning overhead, indexed batch apply, and indexed checkpoint apply in the same artifact so regressions can be separated into JSON cost, planner cost, root publish cost, and durability-boundary cost. Checkpointed rows report `insert_ns/doc` and `sync_ns/doc`, and the user-story TSV renders those as insert/sync milliseconds per batch.

Set `TREEDB_COLLECTION_DOCUMENT_FORMAT=template-v1` to run the collection ingest rows with the template-v1 document format instead of JSON. Matrix reports include a `document_format` column, and the default diagnostic rows include both JSON extraction and template-v1 extraction/planning probes so JSON overhead stays quarantined from TreeDB publish/index maintenance.

Set `TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY=N` to compare pager sync parallelism on checkpoint-heavy rows. Leaving it unset uses the selected TreeDB profile default.

Set `TREEDB_COLLECTION_CHUNK_SIZE=N` to compare pager mmap chunk-size granularity on checkpoint-heavy rows. Leaving it unset uses the selected TreeDB profile/default chunk size. This is useful when the checkpoint split points at dirty-page `msync` cost rather than JSON extraction or collection planning.

Set `TREEDB_COLLECTION_PROFILE_BENCHES=true` to add per-benchmark profile bundles for indexed ingest and checkpointed indexed ingest. `profile_index.tsv` lists the report, CPU profile mode, CPU profile, allocation profile, and pprof top files for each focused capture. These `go test` profiles cover the whole benchmark process; timed benchmark rows remain the source of truth, and pprof top output should be read as coarse attribution because setup or off-timer document generation can appear.

Set `TREEDB_COLLECTION_TIMED_PROFILE_BENCHES=true` to add timed-window CPU profile bundles for indexed ingest and checkpointed indexed ingest. Those captures run fixed-document `BenchmarkCollectionTimedProfile...` variants with prebuilt document batches and `TREEDB_COLLECTION_TIMED_CPU_PROFILE=true`, so `collections_cpu.pprof` excludes benchmark setup and off-timer document generation. Use these captures when deciding whether the next optimization target is TreeDB publish/index maintenance, JSON extraction, or expected storage-engine work.

Set `TREEDB_COLLECTION_INCLUDE_SQLITE=true` to append a SQLite comparison cell. The SQLite cell uses the CGO-backed `github.com/mattn/go-sqlite3` driver, WAL mode, `synchronous=NORMAL`, memory temp store, a large page cache, and disabled WAL autocheckpoint. It includes both a JSON-document table with stored generated columns for `email` and `city` plus native indexes, and a native-column table with `id`, `name`, `email`, `city`, and `pad` stored directly plus the same unique/non-unique secondary indexes. That keeps document or field generation outside the timed section while showing both SQLite-with-JSON-extraction and best-case SQLite native-column throughput.
EOF

echo
echo "matrix index: $INDEX_TSV"
echo "matrix readme: $SUMMARY_MD"
