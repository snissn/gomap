#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/gomap_text_hybrid_scoreboard_$(date +%Y%m%d_%H%M%S)}"
GO_BIN="${GO_BIN:-go}"
PYTHON="${PYTHON:-python3}"
BENCHTIME="${BENCHTIME:-3x}"
COUNT="${COUNT:-1}"
DOCS_10K="${DOCS_10K:-10000}"
DIMS="${DIMS:-16}"
M="${M:-8}"
RUN_100K="${RUN_100K:-false}"
LEXICAL_REPETITIONS="${LEXICAL_REPETITIONS:-3}"
LEXICAL_TIMEOUT_SECONDS="${LEXICAL_TIMEOUT_SECONDS:-900}"
RUN_LEXICAL_COMPARISON="${RUN_LEXICAL_COMPARISON:-true}"
LEXICAL_ALLOW_DIRTY="${LEXICAL_ALLOW_DIRTY:-false}"
LEXICAL_RAN=false
TEXT_100K_BENCHTIME="${TEXT_100K_BENCHTIME:-1x}"
TEXT_100K_COUNT="${TEXT_100K_COUNT:-1}"

if [[ "$RUN_LEXICAL_COMPARISON" == "true" || "$RUN_LEXICAL_COMPARISON" == "1" || "$RUN_LEXICAL_COMPARISON" == "yes" ]]; then
  echo "==> pinned same-corpus TreeDB/Lucene/Bleve/SQLite lexical comparison"
  LEXICAL_ARGS=(
    --manifest benchmarks/text_hybrid_scoreboard/lexical_manifest.json
    --out-dir "$RUN_DIR/lexical"
    --repetitions "$LEXICAL_REPETITIONS"
    --timeout-seconds "$LEXICAL_TIMEOUT_SECONDS"
    --go-bin "$GO_BIN"
  )
  if [[ "$LEXICAL_ALLOW_DIRTY" == "true" || "$LEXICAL_ALLOW_DIRTY" == "1" || "$LEXICAL_ALLOW_DIRTY" == "yes" ]]; then
    LEXICAL_ARGS+=(--allow-dirty)
  fi
  "$PYTHON" benchmarks/text_hybrid_scoreboard/run_lexical_comparison.py "${LEXICAL_ARGS[@]}"
  LEXICAL_RAN=true
fi

mkdir -p "$RUN_DIR"
{
  echo "timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "repo=$ROOT"
  echo "branch=$(git branch --show-current 2>/dev/null || true)"
  echo "commit=$(git rev-parse HEAD 2>/dev/null || true)"
  echo "go=$("$GO_BIN" version 2>/dev/null || true)"
  echo "python=$("$PYTHON" --version 2>&1 || true)"
  echo "uname=$(uname -a 2>/dev/null || true)"
  echo "cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
  echo "ncpu=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || true)"
  echo "uptime=$(uptime 2>/dev/null || true)"
} | tee "$RUN_DIR/context.txt"

run_go_bench() {
  local label="$1"
  local outfile="$2"
  shift 2
  echo "==> $label"
  set -o pipefail
  "$@" | tee "$outfile"
}

scoreboard_doc_label() {
  local docs="$1"
  case "$docs" in
    10000) printf '10k' ;;
    100000) printf '100k' ;;
    1000000) printf '1m' ;;
    *) printf 'docs_%s' "$docs" ;;
  esac
}

INDEX_10K="$RUN_DIR/treedb_index_insert_search_10k.txt"
run_go_bench "TreeDB $DOCS_10K-doc text-v2/vector/hybrid+scalar candidates" "$INDEX_10K" \
  env GOWORK=off \
    TREEDB_INDEX_BENCH_DOCS="$DOCS_10K" \
    TREEDB_INDEX_BENCH_DIMS="$DIMS" \
    TREEDB_INDEX_BENCH_M="$M" \
    "$GO_BIN" test ./TreeDB/collections \
      -run '^$' \
      -bench '^BenchmarkIndexInsertSearch2564/(search_text_v2_candidates_no_docs|search_vector_candidates_no_docs|search_hybrid_v2_no_docs_scalar_filter|indexed_insert_batch_flush_vector_rebuild)$' \
      -benchmem \
      -benchtime="$BENCHTIME" \
      -count="$COUNT"

HYBRID_10K_LABEL="treedb_hybrid_closeout_$(scoreboard_doc_label "$DOCS_10K")"
HYBRID_10K="$RUN_DIR/$HYBRID_10K_LABEL.txt"
run_go_bench "TreeDB $DOCS_10K-doc text-only/vector-only/hybrid/hybrid+scalar executor rows" "$HYBRID_10K" \
  env GOWORK=off \
    TREEDB_HYBRID_BENCH_DOCS="$DOCS_10K" \
    TREEDB_HYBRID_BENCH_DIMS="$DIMS" \
    TREEDB_HYBRID_BENCH_M="$M" \
    "$GO_BIN" test ./TreeDB/collections \
      -run '^$' \
      -bench '^BenchmarkSearchHybridCloseout2506/mode_(text_only_no_docs|vector_only_no_docs|hybrid_no_docs)/topK_10/candidates_64/filter_(none_100pct|rare_06pct)$' \
      -benchmem \
      -benchtime="$BENCHTIME" \
      -count="$COUNT"

TEXT_BLOCKMAX_10K="$RUN_DIR/treedb_text_blockmax_10k.txt"
run_go_bench "TreeDB $DOCS_10K-doc text-v2 blockmax/exhaustive common-term rows" "$TEXT_BLOCKMAX_10K" \
  env GOWORK=off \
    TREEDB_TEXT_V2_BLOCKMAX_DOCS="$DOCS_10K" \
    "$GO_BIN" test ./TreeDB/collections \
      -run '^$' \
      -bench '^BenchmarkTextV2BlockMaxCommonTerm2628/(blockmax_common_topk|exhaustive_common_topk)$' \
      -benchmem \
      -benchtime="$BENCHTIME" \
      -count="$COUNT"

SCOREBOARD_ARGS=(
  -out-dir "$RUN_DIR"
  -context "$RUN_DIR/context.txt"
  -base-ref "origin/main"
  -base-sha "$(git merge-base HEAD origin/main 2>/dev/null || true)"
  -go-bench "treedb_index_insert_search_10k=$INDEX_10K"
  -go-bench "$HYBRID_10K_LABEL=$HYBRID_10K"
  -go-bench "treedb_text_blockmax_10k=$TEXT_BLOCKMAX_10K"
  -command "treedb_index_insert_search_10k=GOWORK=off TREEDB_INDEX_BENCH_DOCS=$DOCS_10K TREEDB_INDEX_BENCH_DIMS=$DIMS TREEDB_INDEX_BENCH_M=$M $GO_BIN test ./TreeDB/collections -run '^$' -bench '^BenchmarkIndexInsertSearch2564/(search_text_v2_candidates_no_docs|search_vector_candidates_no_docs|search_hybrid_v2_no_docs_scalar_filter|indexed_insert_batch_flush_vector_rebuild)$' -benchmem -benchtime=$BENCHTIME -count=$COUNT"
  -command "$HYBRID_10K_LABEL=GOWORK=off TREEDB_HYBRID_BENCH_DOCS=$DOCS_10K TREEDB_HYBRID_BENCH_DIMS=$DIMS TREEDB_HYBRID_BENCH_M=$M $GO_BIN test ./TreeDB/collections -run '^$' -bench '^BenchmarkSearchHybridCloseout2506/mode_(text_only_no_docs|vector_only_no_docs|hybrid_no_docs)/topK_10/candidates_64/filter_(none_100pct|rare_06pct)$' -benchmem -benchtime=$BENCHTIME -count=$COUNT"
  -command "treedb_text_blockmax_10k=GOWORK=off TREEDB_TEXT_V2_BLOCKMAX_DOCS=$DOCS_10K $GO_BIN test ./TreeDB/collections -run '^$' -bench '^BenchmarkTextV2BlockMaxCommonTerm2628/(blockmax_common_topk|exhaustive_common_topk)$' -benchmem -benchtime=$BENCHTIME -count=$COUNT"
)


if [[ "$RUN_100K" == "true" || "$RUN_100K" == "1" || "$RUN_100K" == "yes" ]]; then
  TEXT_BLOCKMAX_100K="$RUN_DIR/treedb_text_blockmax_100k.txt"
  run_go_bench "TreeDB 100k text-v2 blockmax/exhaustive common-term rows" "$TEXT_BLOCKMAX_100K" \
    env GOWORK=off \
      TREEDB_TEXT_V2_BLOCKMAX_DOCS=100000 \
      "$GO_BIN" test ./TreeDB/collections \
        -run '^$' \
        -bench '^BenchmarkTextV2BlockMaxCommonTerm2628/(blockmax_common_topk|exhaustive_common_topk)$' \
        -benchmem \
        -benchtime="$TEXT_100K_BENCHTIME" \
        -count="$TEXT_100K_COUNT"
  SCOREBOARD_ARGS+=(
    -go-bench "treedb_text_blockmax_100k=$TEXT_BLOCKMAX_100K"
    -command "treedb_text_blockmax_100k=GOWORK=off TREEDB_TEXT_V2_BLOCKMAX_DOCS=100000 $GO_BIN test ./TreeDB/collections -run '^$' -bench '^BenchmarkTextV2BlockMaxCommonTerm2628/(blockmax_common_topk|exhaustive_common_topk)$' -benchmem -benchtime=$TEXT_100K_BENCHTIME -count=$TEXT_100K_COUNT"
  )
else
  SCOREBOARD_ARGS+=(
    -caveat "100k text-v2 blockmax rows are available with RUN_100K=true; the default smoke matrix keeps them off to avoid surprise long local runs."
  )
fi

SCOREBOARD_ARGS+=(
  -unavailable "Tantivy lexical=not measured by this harness; no Tantivy adapter or retained evidence is included"
  -unavailable "Qdrant/Weaviate/Milvus/OpenSearch hybrid=not run by the default smoke harness; use service-specific durable deployments or documented local proxies before citing industry hybrid parity"
)
if [[ "$LEXICAL_RAN" == "true" ]]; then
  SCOREBOARD_ARGS+=(
    -caveat "Pinned lexical engine evidence is reported separately in $RUN_DIR/lexical/lexical_comparison.json and .md; unsupported rows never enter that report's headline matrix."
  )
fi

"$GO_BIN" run ./cmd/treedb_text_hybrid_scoreboard "${SCOREBOARD_ARGS[@]}"

cat > "$RUN_DIR/README.md" <<EOF_README
# TreeDB text/hybrid scoreboard run

Primary artifacts:

- scoreboard: \`$RUN_DIR/scoreboard.md\`
- scoreboard JSON: \`$RUN_DIR/scoreboard.json\`
- context: \`$RUN_DIR/context.txt\`
- TreeDB $DOCS_10K-doc index/search raw: \`$INDEX_10K\`
- TreeDB $DOCS_10K-doc hybrid executor raw: \`$HYBRID_10K\`
- TreeDB $DOCS_10K-doc text blockmax raw: \`$TEXT_BLOCKMAX_10K\`
EOF_README
if [[ "$LEXICAL_RAN" == "true" ]]; then
  cat >> "$RUN_DIR/README.md" <<EOF_LEXICAL
- lexical comparison: \`$RUN_DIR/lexical/lexical_comparison.md\`
- lexical comparison JSON: \`$RUN_DIR/lexical/lexical_comparison.json\`
EOF_LEXICAL
fi
cat >> "$RUN_DIR/README.md" <<EOF_README

Set \`RUN_100K=true\` for the heavier 100k text blockmax rows.
Set \`RUN_LEXICAL_COMPARISON=false\` only when reproducing the legacy hybrid-only matrix.
Set \`LEXICAL_ALLOW_DIRTY=true\` only for development smoke; dirty lexical reports are ineligible for retained evidence.
EOF_README

echo "scoreboard: $RUN_DIR/scoreboard.md"
