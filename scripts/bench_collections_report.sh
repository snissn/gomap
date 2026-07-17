#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$(mktemp -d /tmp/gomap_collections_report_XXXXXX)}"
BENCH_REGEX="${BENCH_REGEX:-Benchmark(CollectionInsertProvidedID|CollectionInsertBatchProvidedID|CollectionGetByID|CollectionGetByIDParallel|CollectionDeleteByID|CollectionInsertWithSecondaryIndexes|CollectionInsertBatchWithSecondaryIndexes|CollectionInsertBatchCheckpointWithSecondaryIndexes|CollectionDeleteWithSecondaryIndexes|SecondaryLookupUnique|SecondaryLookupNonUnique|SecondaryUpsertFieldChange|CollectionCreateIndexBackfillExistingDocs|CollectionOverheadPlanNoIndex|CollectionOverheadPlanIndexed|CollectionOverheadPlanIndexedTemplateV1|CollectionOverheadIndexStateJSONExtraction|CollectionOverheadIndexStateTemplateV1Extraction|CollectionOverheadPlanIndexedPrecomputedState)}"
COUNT="${COUNT:-3}"
BENCHTIME="${BENCHTIME:-}"
# The default regex includes create-index backfill DDL coverage, which the
# public command-WAL collection path does not support yet.
BENCH_ENGINE="${TREEDB_COLLECTION_BENCH_ENGINE:-bench_unsafe}"
BATCH_SIZE="${TREEDB_COLLECTION_BENCH_BATCH_SIZE:-8000}"
DOCUMENT_FORMAT="${TREEDB_COLLECTION_DOCUMENT_FORMAT:-json}"
DATA_OUTER="${TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG:-true}"
INDEX_OUTER="${TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG:-false}"
CHUNK_SIZE="$(printf '%s' "${TREEDB_COLLECTION_CHUNK_SIZE:-}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
CHUNK_SIZE_LABEL="${CHUNK_SIZE:-profile/default}"
PAGER_SYNC_CONCURRENCY="$(printf '%s' "${TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY:-}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
if [[ "$PAGER_SYNC_CONCURRENCY" == "0" ]]; then
  PAGER_SYNC_CONCURRENCY=""
fi
PAGER_SYNC_CONCURRENCY_LABEL="${PAGER_SYNC_CONCURRENCY:-profile/default}"
if [[ "$DATA_OUTER" == "-" && "$INDEX_OUTER" == "-" ]]; then
  CHUNK_SIZE_LABEL="-"
  PAGER_SYNC_CONCURRENCY_LABEL="-"
fi
GO_TEST_TAGS="${GO_TEST_TAGS:-}"
TIMED_CPU_PROFILE="${TREEDB_COLLECTION_TIMED_CPU_PROFILE:-false}"
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
HARNESS_UNAVAILABLE=0

is_true() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

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
  -memprofile "$MEM_PROFILE"
)
if is_true "$TIMED_CPU_PROFILE"; then
  rm -f "$CPU_PROFILE" "$CPU_TOP"
  export TREEDB_COLLECTION_TIMED_CPU_PROFILE_PATH="$CPU_PROFILE"
else
  unset TREEDB_COLLECTION_TIMED_CPU_PROFILE_PATH
  cmd+=(-cpuprofile "$CPU_PROFILE")
fi

if [[ -n "$BENCHTIME" ]]; then
  cmd+=(-benchtime "$BENCHTIME")
fi

echo "running focused collections benchmarks into: $OUT_DIR"
echo "benchmark engine: $BENCH_ENGINE"
echo "collection batch size: $BATCH_SIZE"
echo "document format: $DOCUMENT_FORMAT"
echo "storage policy: $STORAGE_POLICY_LABEL"
echo "pager chunk size: $CHUNK_SIZE_LABEL"
echo "pager sync concurrency: $PAGER_SYNC_CONCURRENCY_LABEL"
echo "execution path: $PATH_LABEL"
echo "cpu profile mode: $(is_true "$TIMED_CPU_PROFILE" && echo "timed benchmark window" || echo "whole go test process")"
if [[ -n "$GO_TEST_TAGS" ]]; then
  echo "go test tags: $GO_TEST_TAGS"
fi

if [[ -z "$PATH_LABEL" ]]; then
  echo "TREEDB_COLLECTION_PATH_LABEL is required (oracle|native-fastpath|sqlite)" >&2
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
    -document-format "$DOCUMENT_FORMAT" \
    -storage-policy "$STORAGE_POLICY_LABEL" \
    -pager-chunk-size "$CHUNK_SIZE_LABEL" \
    -pager-sync-concurrency "$PAGER_SYNC_CONCURRENCY_LABEL" \
    -collection-batch-size "$BATCH_SIZE" \
    -bench-pattern "$BENCH_REGEX" \
    -count "$COUNT" \
    -unavailable-reason "N/A before R0 harness bring-up"
else
  TREEDB_COLLECTION_BENCH_ENGINE="$BENCH_ENGINE" TREEDB_COLLECTION_BENCH_BATCH_SIZE="$BATCH_SIZE" TREEDB_COLLECTION_DOCUMENT_FORMAT="$DOCUMENT_FORMAT" TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG="$DATA_OUTER" TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG="$INDEX_OUTER" TREEDB_COLLECTION_CHUNK_SIZE="$CHUNK_SIZE" TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY="$PAGER_SYNC_CONCURRENCY" GOWORK=off "${cmd[@]}" | tee "$RAW_JSON"

  if is_true "$TIMED_CPU_PROFILE" && [[ ! -s "$CPU_PROFILE" ]]; then
    echo "timed CPU profile was requested but no profile was written to $CPU_PROFILE; use a timed-profile benchmark" >&2
    exit 1
  fi

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
    -document-format "$DOCUMENT_FORMAT" \
    -storage-policy "$STORAGE_POLICY_LABEL" \
    -pager-chunk-size "$CHUNK_SIZE_LABEL" \
    -pager-sync-concurrency "$PAGER_SYNC_CONCURRENCY_LABEL" \
    -collection-batch-size "$BATCH_SIZE" \
    -bench-pattern "$BENCH_REGEX" \
    -count "$COUNT"
fi

if [[ "$HARNESS_UNAVAILABLE" == "1" ]]; then
  ARTIFACT_LINES="- benchmark harness: unavailable (N/A before R0 harness bring-up)
- raw benchmark json: unavailable
- cpu profile: unavailable
- cpu profile mode: unavailable
- memory profile: unavailable
- cpu profile top: unavailable
- allocation profile top: unavailable
- markdown report: \`$OUT_DIR/collections_report.md\`
- html report: \`$OUT_DIR/collections_report.html\`
- json report: \`$OUT_DIR/collections_report.json\`"
else
  ARTIFACT_LINES="- raw benchmark json: \`$RAW_JSON\`
- cpu profile: \`$CPU_PROFILE\`
- cpu profile mode: \`$(is_true "$TIMED_CPU_PROFILE" && echo "timed benchmark window" || echo "whole go test process")\`
- memory profile: \`$MEM_PROFILE\`
- cpu profile top: \`$CPU_TOP\`
- allocation profile top: \`$MEM_TOP\`
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
- document format: \`$DOCUMENT_FORMAT\`
- storage policy: \`$STORAGE_POLICY_LABEL\`
- pager chunk size: \`$CHUNK_SIZE_LABEL\`
- pager sync concurrency: \`$PAGER_SYNC_CONCURRENCY_LABEL\`
- collection batch size: \`$BATCH_SIZE\`
- benchmark regex: \`$BENCH_REGEX\`
- benchmark count: \`$COUNT\`
- go test tags: \`${GO_TEST_TAGS:-none}\`
$ARTIFACT_LINES
- benchmark-only no-WAL default: \`TREEDB_COLLECTION_BENCH_ENGINE=bench_unsafe scripts/bench_collections_report.sh\`
- command-WAL relaxed override: \`TREEDB_COLLECTION_BENCH_ENGINE=command_wal_relaxed BENCH_REGEX='Benchmark(CollectionInsertProvidedID|CollectionInsertBatchProvidedID|CollectionGetByID|CollectionGetByIDParallel|CollectionDeleteByID|CollectionInsertWithSecondaryIndexes|CollectionInsertBatchWithSecondaryIndexes|CollectionInsertBatchCheckpointWithSecondaryIndexes|CollectionDeleteWithSecondaryIndexes|SecondaryLookupUnique|SecondaryLookupNonUnique|SecondaryUpsertFieldChange|CollectionOverheadPlanNoIndex|CollectionOverheadPlanIndexed|CollectionOverheadPlanIndexedTemplateV1|CollectionOverheadIndexStateJSONExtraction|CollectionOverheadIndexStateTemplateV1Extraction|CollectionOverheadPlanIndexedPrecomputedState)' scripts/bench_collections_report.sh\`
- command-WAL durable override: \`TREEDB_COLLECTION_BENCH_ENGINE=command_wal_durable BENCH_REGEX='Benchmark(CollectionInsertProvidedID|CollectionInsertBatchProvidedID|CollectionGetByID|CollectionGetByIDParallel|CollectionDeleteByID|CollectionInsertWithSecondaryIndexes|CollectionInsertBatchWithSecondaryIndexes|CollectionInsertBatchCheckpointWithSecondaryIndexes|CollectionDeleteWithSecondaryIndexes|SecondaryLookupUnique|SecondaryLookupNonUnique|SecondaryUpsertFieldChange|CollectionOverheadPlanNoIndex|CollectionOverheadPlanIndexed|CollectionOverheadPlanIndexedTemplateV1|CollectionOverheadIndexStateJSONExtraction|CollectionOverheadIndexStateTemplateV1Extraction|CollectionOverheadPlanIndexedPrecomputedState)' scripts/bench_collections_report.sh\`
- benchmark-only no-WAL storage-control override: \`TREEDB_COLLECTION_BENCH_ENGINE=bench_unsafe TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=false TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=false scripts/bench_collections_report.sh\`
- data-root pager-leaf override: \`TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=false scripts/bench_collections_report.sh\`
- index-root outer-leaf override: \`TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=true scripts/bench_collections_report.sh\`
- pager chunk size override: \`TREEDB_COLLECTION_CHUNK_SIZE=65536 scripts/bench_collections_report.sh\`
- pager sync concurrency override: \`TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY=4 scripts/bench_collections_report.sh\`
- batch-size override: \`TREEDB_COLLECTION_BENCH_BATCH_SIZE=8000 scripts/bench_collections_report.sh\`
- template-v1 document-format override: \`TREEDB_COLLECTION_DOCUMENT_FORMAT=template-v1 scripts/bench_collections_report.sh\`
- oracle-path override: \`TREEDB_COLLECTION_PATH_LABEL=oracle scripts/bench_collections_report.sh\`
- production matrix runner: \`TREEDB_COLLECTION_PATH_LABEL=native-fastpath scripts/bench_collections_matrix.sh\`
- sqlite comparison override: \`CGO_ENABLED=1 TREEDB_COLLECTION_PATH_LABEL=sqlite TREEDB_COLLECTION_BENCH_ENGINE=sqlite_wal_normal GO_TEST_TAGS=sqlite_bench BENCH_REGEX='BenchmarkSQLite(InsertBatchWithSecondaryIndexes|InsertBatchCheckpointWithSecondaryIndexes|NativeColumnsInsertBatchWithSecondaryIndexes|NativeColumnsInsertBatchCheckpointWithSecondaryIndexes)$' scripts/bench_collections_report.sh\`
- timed CPU profile override: \`TREEDB_COLLECTION_TIMED_CPU_PROFILE=true BENCH_REGEX='^BenchmarkCollectionTimedProfileInsertBatchWithSecondaryIndexes$' BENCHTIME=240000x COUNT=1 scripts/bench_collections_report.sh\`

## Profile Caveat

The \`go test\` CPU and allocation profiles cover the whole benchmark process. Timed benchmark rows remain the source of truth for \`ns/op\`, \`B/op\`, and \`allocs/op\`; the pprof top files are coarse attribution aids and can include setup or off-timer document generation work.

When \`TREEDB_COLLECTION_TIMED_CPU_PROFILE=true\` is used with one of the \`BenchmarkCollectionTimedProfile...\` benchmarks, \`collections_cpu.pprof\` is started after benchmark setup/document-batch generation and stopped before post-benchmark reporting. The script passes the concrete path through \`TREEDB_COLLECTION_TIMED_CPU_PROFILE_PATH\`. That mode is intended for CPU attribution only; allocation accounting still comes from the timed benchmark row.
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
