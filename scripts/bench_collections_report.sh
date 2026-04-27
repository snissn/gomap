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
GO_TEST_TAGS="${GO_TEST_TAGS:-}"
STORAGE_POLICY_LABEL="data_outer=${DATA_OUTER},index_outer=${INDEX_OUTER}"
PATH_LABEL="${TREEDB_COLLECTION_PATH_LABEL:-}"

RAW_JSON="$OUT_DIR/collections_bench.json"
CPU_PROFILE="$OUT_DIR/collections_cpu.pprof"
MEM_PROFILE="$OUT_DIR/collections_mem.pprof"
CPU_TOP="$OUT_DIR/collections_cpu_top.txt"
MEM_TOP="$OUT_DIR/collections_mem_top.txt"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
COMMIT="$(git rev-parse --short HEAD)"
WORKTREE="$ROOT"

mkdir -p "$OUT_DIR"

write_pprof_top() {
  local profile=$1
  local dest=$2
  shift 2
  if [[ ! -s "$profile" ]]; then
    return 0
  fi
  if ! go tool pprof "$@" "$profile" >"$dest.tmp" 2>"$dest.err"; then
    {
      echo "go tool pprof failed for $profile"
      cat "$dest.err"
    } >"$dest"
  else
    mv "$dest.tmp" "$dest"
  fi
  rm -f "$dest.tmp" "$dest.err"
}

cmd=(
  go test
)
if [[ -n "$GO_TEST_TAGS" ]]; then
  cmd+=(-tags "$GO_TEST_TAGS")
fi
cmd+=(
  ./TreeDB/collections
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
if [[ -n "$GO_TEST_TAGS" ]]; then
  echo "go test tags: $GO_TEST_TAGS"
fi

if [[ -z "$PATH_LABEL" ]]; then
  echo "TREEDB_COLLECTION_PATH_LABEL is required (oracle|native-fastpath|sqlite)" >&2
  exit 2
fi

if [[ ! -d "$ROOT/TreeDB/collections" ]]; then
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

  write_pprof_top "$CPU_PROFILE" "$CPU_TOP" -top
  write_pprof_top "$MEM_PROFILE" "$MEM_TOP" -top -sample_index=alloc_space

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
- go test tags: \`${GO_TEST_TAGS:-none}\`
- raw benchmark json: \`$RAW_JSON\`
- cpu profile: \`$CPU_PROFILE\`
- memory profile: \`$MEM_PROFILE\`
- cpu profile top: \`$CPU_TOP\`
- allocation profile top: \`$MEM_TOP\`
- markdown report: \`$OUT_DIR/collections_report.md\`
- html report: \`$OUT_DIR/collections_report.html\`
- json report: \`$OUT_DIR/collections_report.json\`
- production fast override: \`TREEDB_COLLECTION_BENCH_ENGINE=production_fast scripts/bench_collections_report.sh\`
- production WAL-on-fast override: \`TREEDB_COLLECTION_BENCH_ENGINE=production_wal_on_fast scripts/bench_collections_report.sh\`
- backend-direct fast/control override: \`TREEDB_COLLECTION_BENCH_ENGINE=backend_direct_fast TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=false TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=false scripts/bench_collections_report.sh\`
- data-root outer-leaf override: \`TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=true scripts/bench_collections_report.sh\`
- index-root outer-leaf override: \`TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=false scripts/bench_collections_report.sh\`
- batch-size override: \`TREEDB_COLLECTION_BENCH_BATCH_SIZE=8000 scripts/bench_collections_report.sh\`
- oracle-path override: \`TREEDB_COLLECTION_PATH_LABEL=oracle scripts/bench_collections_report.sh\`
- production matrix runner: \`TREEDB_COLLECTION_PATH_LABEL=native-fastpath scripts/bench_collections_matrix.sh\`
- sqlite comparison override: \`TREEDB_COLLECTION_PATH_LABEL=sqlite TREEDB_COLLECTION_BENCH_ENGINE=sqlite_wal_normal GO_TEST_TAGS=sqlite_bench BENCH_REGEX='BenchmarkSQLite(InsertBatchWithSecondaryIndexes|InsertBatchCheckpointWithSecondaryIndexes)$' scripts/bench_collections_report.sh\`

## Profile Caveat

The \`go test\` CPU and allocation profiles cover the whole benchmark process. Timed benchmark rows remain the source of truth for \`ns/op\`, \`B/op\`, and \`allocs/op\`; the pprof top files are coarse attribution aids and can include setup or off-timer document generation work.
EOF

echo "markdown report: $OUT_DIR/collections_report.md"
echo "html report:     $OUT_DIR/collections_report.html"
echo "bundle index:    $OUT_DIR/README.md"
if [[ -s "$CPU_TOP" ]]; then
  echo "cpu top:         $CPU_TOP"
fi
if [[ -s "$MEM_TOP" ]]; then
  echo "allocation top:  $MEM_TOP"
fi
