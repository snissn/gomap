#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$(mktemp -d /tmp/gomap_collections_report_XXXXXX)}"
BENCH_REGEX="${BENCH_REGEX:-Benchmark(CollectionInsertProvidedID|CollectionInsertBatchProvidedID|CollectionGetByID|CollectionGetByIDParallel|CollectionDeleteByID|CollectionInsertWithSecondaryIndexes|CollectionInsertBatchWithSecondaryIndexes|CollectionInsertBatchCheckpointWithSecondaryIndexes|CollectionDeleteWithSecondaryIndexes|SecondaryLookupUnique|SecondaryLookupNonUnique|SecondaryUpsertFieldChange|CollectionCreateIndexBackfillExistingDocs|CollectionOverheadPlanNoIndex|CollectionOverheadPlanIndexed|CollectionOverheadIndexStateJSONExtraction|CollectionOverheadPlanIndexedPrecomputedState)}"
COUNT="${COUNT:-3}"
BENCHTIME="${BENCHTIME:-}"
BENCH_ENGINE="${TREEDB_COLLECTION_BENCH_ENGINE:-production_fast}"
BATCH_SIZE="${TREEDB_COLLECTION_BENCH_BATCH_SIZE:-8000}"
DATA_OUTER="${TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG:-true}"
INDEX_OUTER="${TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG:-false}"
STORAGE_POLICY_LABEL="data_outer=${DATA_OUTER},index_outer=${INDEX_OUTER}"
PATH_LABEL="${TREEDB_COLLECTION_PATH_LABEL:-}"

RAW_JSON="$OUT_DIR/collections_bench.json"
CPU_PROFILE="$OUT_DIR/collections_cpu.pprof"
MEM_PROFILE="$OUT_DIR/collections_mem.pprof"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
COMMIT="$(git rev-parse --short HEAD)"
WORKTREE="$ROOT"

mkdir -p "$OUT_DIR"
HARNESS_UNAVAILABLE=0

cmd=(
  go test ./TreeDB/collections
  -run '^$'
  -bench "$BENCH_REGEX"
  -benchmem
  -count "$COUNT"
  -json
  -cpuprofile "$CPU_PROFILE"
  -memprofile "$MEM_PROFILE"
)

if [[ -n "$BENCHTIME" ]]; then
  cmd+=(-benchtime "$BENCHTIME")
fi

echo "running focused collections benchmarks into: $OUT_DIR"
echo "benchmark engine: $BENCH_ENGINE"
echo "collection batch size: $BATCH_SIZE"
echo "storage policy: $STORAGE_POLICY_LABEL"
echo "execution path: $PATH_LABEL"

if [[ -z "$PATH_LABEL" ]]; then
  echo "TREEDB_COLLECTION_PATH_LABEL is required (oracle|native-fastpath)" >&2
  exit 2
fi

if [[ ! -d "$ROOT/TreeDB/collections" ]]; then
  HARNESS_UNAVAILABLE=1
  GOWORK=off go run ./cmd/collection_bench_report \
    -out-dir "$OUT_DIR" \
    -branch "$BRANCH" \
    -commit "$COMMIT" \
    -worktree "$WORKTREE" \
    -execution-path "$PATH_LABEL" \
    -benchmark-engine "$BENCH_ENGINE" \
    -storage-policy "$STORAGE_POLICY_LABEL" \
    -collection-batch-size "$BATCH_SIZE" \
    -bench-pattern "$BENCH_REGEX" \
    -count "$COUNT" \
    -unavailable-reason "N/A before R0 harness bring-up"
else
  TREEDB_COLLECTION_BENCH_ENGINE="$BENCH_ENGINE" TREEDB_COLLECTION_BENCH_BATCH_SIZE="$BATCH_SIZE" TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="$DATA_OUTER" TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="$INDEX_OUTER" GOWORK=off "${cmd[@]}" | tee "$RAW_JSON"

  GOWORK=off go run ./cmd/collection_bench_report \
    -in "$RAW_JSON" \
    -out-dir "$OUT_DIR" \
    -branch "$BRANCH" \
    -commit "$COMMIT" \
    -worktree "$WORKTREE" \
    -execution-path "$PATH_LABEL" \
    -benchmark-engine "$BENCH_ENGINE" \
    -storage-policy "$STORAGE_POLICY_LABEL" \
    -collection-batch-size "$BATCH_SIZE" \
    -bench-pattern "$BENCH_REGEX" \
    -count "$COUNT"
fi

if [[ "$HARNESS_UNAVAILABLE" == "1" ]]; then
  ARTIFACT_LINES="- benchmark harness: unavailable (N/A before R0 harness bring-up)
- raw benchmark json: unavailable
- cpu profile: unavailable
- memory profile: unavailable
- markdown report: \`$OUT_DIR/collections_report.md\`
- html report: \`$OUT_DIR/collections_report.html\`
- json report: \`$OUT_DIR/collections_report.json\`"
else
  ARTIFACT_LINES="- raw benchmark json: \`$RAW_JSON\`
- cpu profile: \`$CPU_PROFILE\`
- memory profile: \`$MEM_PROFILE\`
- markdown report: \`$OUT_DIR/collections_report.md\`
- html report: \`$OUT_DIR/collections_report.html\`
- json report: \`$OUT_DIR/collections_report.json\`"
fi

cat >"$OUT_DIR/README.md" <<EOF
# Focused Collections Benchmark Bundle

- output directory: \`$OUT_DIR\`
- worktree: \`$WORKTREE\`
- branch: \`$BRANCH\`
- commit: \`$COMMIT\`
- execution path: \`$PATH_LABEL\`
- benchmark engine: \`$BENCH_ENGINE\`
- storage policy: \`$STORAGE_POLICY_LABEL\`
- collection batch size: \`$BATCH_SIZE\`
- benchmark regex: \`$BENCH_REGEX\`
- benchmark count: \`$COUNT\`
$ARTIFACT_LINES
- backend-direct override: \`TREEDB_COLLECTION_BENCH_ENGINE=backend_direct scripts/bench_collections_report.sh\`
- backend-direct fast override: \`TREEDB_COLLECTION_BENCH_ENGINE=backend_direct_fast scripts/bench_collections_report.sh\`
- backend-direct WAL-on-fast override: \`TREEDB_COLLECTION_BENCH_ENGINE=backend_direct_wal_on_fast scripts/bench_collections_report.sh\`
- production fast override: \`TREEDB_COLLECTION_BENCH_ENGINE=production_fast scripts/bench_collections_report.sh\`
- production WAL-on-fast override: \`TREEDB_COLLECTION_BENCH_ENGINE=production_wal_on_fast scripts/bench_collections_report.sh\`
- backend-direct fast/control override: \`TREEDB_COLLECTION_BENCH_ENGINE=backend_direct_fast TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=false TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=false scripts/bench_collections_report.sh\`
- data-root pager-leaf override: \`TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=false scripts/bench_collections_report.sh\`
- index-root outer-leaf override: \`TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=true scripts/bench_collections_report.sh\`
- batch-size override: \`TREEDB_COLLECTION_BENCH_BATCH_SIZE=8000 scripts/bench_collections_report.sh\`
- oracle-path override: \`TREEDB_COLLECTION_PATH_LABEL=oracle scripts/bench_collections_report.sh\`
- production matrix runner: \`TREEDB_COLLECTION_PATH_LABEL=native-fastpath scripts/bench_collections_matrix.sh\`
EOF

echo "markdown report: $OUT_DIR/collections_report.md"
echo "html report:     $OUT_DIR/collections_report.html"
echo "bundle index:    $OUT_DIR/README.md"
