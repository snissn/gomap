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
BENCH_REGEX="${BENCH_REGEX:-Benchmark(CollectionInsertBatchWithSecondaryIndexes|CollectionInsertBatchCheckpointWithSecondaryIndexes|CollectionOverheadIndexStateJSONExtraction|CollectionOverheadPlanIndexedPrecomputedState)$}"
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
SQLITE_BENCH_REGEX="${TREEDB_COLLECTION_SQLITE_BENCH_REGEX:-BenchmarkSQLite(InsertBatchWithSecondaryIndexes|InsertBatchCheckpointWithSecondaryIndexes)$}"
SQLITE_CGO_ENABLED="${TREEDB_COLLECTION_SQLITE_CGO_ENABLED:-1}"
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
      "production_fast_data_vlog_index_leaf production_fast true false"
      "production_fast_data_vlog_index_vlog production_fast true true"
      "production_wal_on_fast_data_vlog_index_leaf production_wal_on_fast true false"
      "production_wal_on_fast_data_vlog_index_vlog production_wal_on_fast true true"
    )
    ;;
  full)
    MATRIX_ROWS=(
      "production_fast_data_leaf_index_leaf production_fast false false"
      "production_fast_data_leaf_index_vlog production_fast false true"
      "production_fast_data_vlog_index_leaf production_fast true false"
      "production_fast_data_vlog_index_vlog production_fast true true"
      "production_wal_on_fast_data_leaf_index_leaf production_wal_on_fast false false"
      "production_wal_on_fast_data_leaf_index_vlog production_wal_on_fast false true"
      "production_wal_on_fast_data_vlog_index_leaf production_wal_on_fast true false"
      "production_wal_on_fast_data_vlog_index_vlog production_wal_on_fast true true"
    )
    ;;
  quick)
    MATRIX_ROWS=(
      "production_fast_data_vlog_index_leaf production_fast true false"
      "production_fast_data_vlog_index_vlog production_fast true true"
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
SUMMARY_MD="$OUT_DIR/README.md"

printf "cell\tengine\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\treport_md\treport_json\tcpu_profile\tmem_profile\n" >"$INDEX_TSV"

echo "running collections benchmark matrix into: $OUT_DIR"
echo "matrix: $MATRIX"
echo "execution path: $PATH_LABEL"
echo "benchmark regex: $BENCH_REGEX"
echo "benchmark count: $COUNT"
echo "benchmark time: $BENCHTIME"
echo "batch size: $BATCH_SIZE"
echo "include sqlite: $INCLUDE_SQLITE"
if is_true "$INCLUDE_SQLITE"; then
  echo "sqlite engine: $SQLITE_ENGINE"
  echo "sqlite cell: $SQLITE_CELL"
  echo "sqlite benchmark regex: $SQLITE_BENCH_REGEX"
  echo "sqlite CGO_ENABLED: $SQLITE_CGO_ENABLED"
  if ! command -v cc >/dev/null 2>&1 && ! command -v gcc >/dev/null 2>&1 && ! command -v clang >/dev/null 2>&1; then
    echo "SQLite comparison requires a C compiler for github.com/mattn/go-sqlite3; install cc/gcc/clang or set TREEDB_COLLECTION_INCLUDE_SQLITE=false" >&2
    exit 2
  fi
fi

for row in "${MATRIX_ROWS[@]}"; do
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
    TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="$data_outer" \
    TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="$index_outer" \
    scripts/bench_collections_report.sh

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$cell" \
    "$engine" \
    "$data_outer" \
    "$index_outer" \
    "$cell_dir/collections_report.md" \
    "$cell_dir/collections_report.json" \
    "$cell_dir/collections_cpu.pprof" \
    "$cell_dir/collections_mem.pprof" >>"$INDEX_TSV"
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
    TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="-" \
    TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="-" \
    GO_TEST_TAGS="sqlite_bench" \
    CGO_ENABLED="$SQLITE_CGO_ENABLED" \
    scripts/bench_collections_report.sh

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$cell" \
    "$SQLITE_ENGINE" \
    "-" \
    "-" \
    "$cell_dir/collections_report.md" \
    "$cell_dir/collections_report.json" \
    "$cell_dir/collections_cpu.pprof" \
    "$cell_dir/collections_mem.pprof" >>"$INDEX_TSV"
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
- include sqlite: \`$INCLUDE_SQLITE\`
- sqlite benchmark regex: \`$SQLITE_BENCH_REGEX\`
- matrix index: \`$INDEX_TSV\`
- matrix summary markdown: \`$OUT_DIR/collections_matrix_summary.md\`
- matrix summary tsv: \`$OUT_DIR/collections_matrix_summary.tsv\`
- user-story throughput tsv: \`$OUT_DIR/collections_user_story_summary.tsv\`

## Cells

| Cell | Engine | Data outer leaves in vlog | Index outer leaves in vlog | Report |
| --- | --- | --- | --- | --- |
EOF

for row in "${MATRIX_ROWS[@]}"; do
  read -r cell engine data_outer index_outer <<<"$row"
  printf "| \`%s\` | \`%s\` | \`%s\` | \`%s\` | [%s](%s) |\n" \
    "$cell" \
    "$engine" \
    "$data_outer" \
    "$index_outer" \
    "$cell" \
    "$cell/collections_report.md" >>"$SUMMARY_MD"
done

if is_true "$INCLUDE_SQLITE"; then
  printf "| \`%s\` | \`%s\` | \`%s\` | \`%s\` | [%s](%s) |\n" \
    "$SQLITE_CELL" \
    "$SQLITE_ENGINE" \
    "-" \
    "-" \
    "$SQLITE_CELL" \
    "$SQLITE_CELL/collections_report.md" >>"$SUMMARY_MD"
fi

cat >>"$SUMMARY_MD" <<'EOF'

## Intended 768 Use

The production matrix keeps collection data roots in value-log outer-leaf mode and varies index roots between inline outer leaves and value-log outer leaves. The `production_fast` and `production_wal_on_fast` engines keep the cached no-WAL and WAL-on fast paths visible without changing the collection storage policy.

The focused default benchmark set keeps JSON extraction overhead, non-JSON indexed planning overhead, indexed batch apply, and indexed checkpoint apply in the same artifact so regressions can be separated into JSON cost, planner cost, root publish cost, and durability-boundary cost.

Set `TREEDB_COLLECTION_INCLUDE_SQLITE=true` to append a SQLite comparison cell. The SQLite cell uses the CGO-backed `github.com/mattn/go-sqlite3` driver, WAL mode, `synchronous=NORMAL`, memory temp store, a large page cache, disabled WAL autocheckpoint, generated JSON columns for `email` and `city`, and unique/non-unique indexes on those generated columns. That keeps document generation outside the timed section while SQLite still pays JSON extraction and secondary-index maintenance during insert.
EOF

echo
echo "matrix index: $INDEX_TSV"
echo "matrix readme: $SUMMARY_MD"
