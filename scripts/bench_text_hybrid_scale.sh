#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

RUN_DIR="${RUN_DIR:-/tmp/gomap_text_hybrid_scale_$(date +%Y%m%d_%H%M%S)}"
GO_BIN="${GO_BIN:-go}"
SMOKE_ROWS="${SMOKE_ROWS:-10000}"
SMOKE_QUERIES="${SMOKE_QUERIES:-20}"
SMOKE_BATCH_SIZE="${SMOKE_BATCH_SIZE:-4096}"
DIMS="${DIMS:-16}"
M="${M:-8}"
EF_CONSTRUCTION="${EF_CONSTRUCTION:-128}"
EF_SEARCH="${EF_SEARCH:-128}"
TOP_K="${TOP_K:-10}"
CANDIDATE_LIMIT="${CANDIDATE_LIMIT:-64}"
SMOKE_CANDIDATE_LIMIT="${SMOKE_CANDIDATE_LIMIT:-$CANDIDATE_LIMIT}"
ONE_M_CANDIDATE_LIMIT="${ONE_M_CANDIDATE_LIMIT:-65536}"
TEN_M_CANDIDATE_LIMIT="${TEN_M_CANDIDATE_LIMIT:-655360}"
READERS="${READERS:-4}"
RUN_SMOKE="${RUN_SMOKE:-true}"
RUN_1M="${RUN_1M:-false}"
ONE_M_ROWS="${ONE_M_ROWS:-1000000}"
ONE_M_QUERIES="${ONE_M_QUERIES:-25}"
ONE_M_BATCH_SIZE="${ONE_M_BATCH_SIZE:-16384}"
ONE_M_BACKFILL_ROWS="${ONE_M_BACKFILL_ROWS:-100000}"
ONE_M_MAINTENANCE_UPDATES="${ONE_M_MAINTENANCE_UPDATES:-10000}"
ONE_M_MAINTENANCE_DELETES="${ONE_M_MAINTENANCE_DELETES:-5000}"
RUN_10M="${RUN_10M:-false}"
APPROVE_10M="${APPROVE_10M:-false}"
TEN_M_ROWS="${TEN_M_ROWS:-10000000}"
TEN_M_QUERIES="${TEN_M_QUERIES:-10}"
TEN_M_BATCH_SIZE="${TEN_M_BATCH_SIZE:-32768}"
TEN_M_BACKFILL_ROWS="${TEN_M_BACKFILL_ROWS:-1000000}"
TEN_M_MAINTENANCE_UPDATES="${TEN_M_MAINTENANCE_UPDATES:-10000}"
TEN_M_MAINTENANCE_DELETES="${TEN_M_MAINTENANCE_DELETES:-5000}"
KEEP_DB="${KEEP_DB:-false}"
RUN_GO_BENCH="${RUN_GO_BENCH:-false}"
GO_BENCH_ROWS="${GO_BENCH_ROWS:-$SMOKE_ROWS}"
GO_BENCHTIME="${GO_BENCHTIME:-1x}"
GO_COUNT="${GO_COUNT:-1}"
# `retrieval` is the bounded #4327 qualification: load, query matrix, and
# close/reopen parity only. The default remains the full historical campaign.
PHASES="${PHASES:-all}"
RETRIEVAL_REPETITIONS="${RETRIEVAL_REPETITIONS:-1}"

if [[ ! "$RETRIEVAL_REPETITIONS" =~ ^[1-9][0-9]*$ ]]; then
  echo "RETRIEVAL_REPETITIONS must be a positive integer; got $RETRIEVAL_REPETITIONS" >&2
  exit 2
fi
if (( RETRIEVAL_REPETITIONS > 1 )) && [[ "$PHASES" != "retrieval" ]]; then
  echo "RETRIEVAL_REPETITIONS>1 requires PHASES=retrieval; refusing to repeat a non-retrieval campaign" >&2
  exit 2
fi

mkdir -p "$RUN_DIR"

{
  echo "timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "repo=$ROOT"
  echo "branch=$(git branch --show-current 2>/dev/null || true)"
  echo "commit=$(git rev-parse HEAD 2>/dev/null || true)"
  echo "base_ref=origin/main"
  echo "base_sha=$(git merge-base HEAD origin/main 2>/dev/null || true)"
  echo "go=$($GO_BIN version 2>/dev/null || true)"
  echo "uname=$(uname -a 2>/dev/null || true)"
  echo "cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
  echo "ncpu=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || true)"
  echo "uptime=$(uptime 2>/dev/null || true)"
  echo "disk_free=$(df -h "$RUN_DIR" 2>/dev/null | tail -1 || true)"
} | tee "$RUN_DIR/context.txt"

write_10m_plan() {
  cat > "$RUN_DIR/10m_selected_matrix_commands.md" <<EOF_PLAN
# TreeDB text-v2/hybrid 10M selected-matrix commands

Full 10M runs are intentionally not started by default. They can take multiple
hours and tens of GB depending on storage profile, vector rebuild, backfill, and
rewrite settings. Run only after explicit coordinator approval. This command keeps
strict guardrails; if current bounded text/hybrid candidate generation fails
closed on common-term rows, treat that as scale evidence and a follow-up input,
not as a passing latency row.

Primary selected 10M text/hybrid/load/reopen/concurrent row:

\`\`\`sh
RUN_DIR=$RUN_DIR RUN_10M=true APPROVE_10M=true PHASES=$PHASES \\
TEN_M_ROWS=$TEN_M_ROWS TEN_M_QUERIES=$TEN_M_QUERIES TEN_M_BATCH_SIZE=$TEN_M_BATCH_SIZE \\
TEN_M_CANDIDATE_LIMIT=$TEN_M_CANDIDATE_LIMIT \\
TEN_M_BACKFILL_ROWS=$TEN_M_BACKFILL_ROWS TEN_M_MAINTENANCE_UPDATES=$TEN_M_MAINTENANCE_UPDATES TEN_M_MAINTENANCE_DELETES=$TEN_M_MAINTENANCE_DELETES \\
scripts/bench_text_hybrid_scale.sh
\`\`\`

Direct command equivalent:

\`\`\`sh
GOWORK=off $GO_BIN run ./cmd/treedb_text_hybrid_scale \\
  -out-dir "$RUN_DIR/scale_10m_selected" \\
  -rows $TEN_M_ROWS -batch-size $TEN_M_BATCH_SIZE -dims $DIMS -m $M \\
  -ef-construction $EF_CONSTRUCTION -ef-search $EF_SEARCH \\
  -top-k $TOP_K -candidate-limit $TEN_M_CANDIDATE_LIMIT -queries $TEN_M_QUERIES \\
  -readers $READERS -backfill-rows $TEN_M_BACKFILL_ROWS \\
  -maintenance-updates $TEN_M_MAINTENANCE_UPDATES -maintenance-deletes $TEN_M_MAINTENANCE_DELETES \\
  -keep-db=$KEEP_DB -phases "$PHASES" -base-ref origin/main -base-sha "$(git merge-base HEAD origin/main 2>/dev/null || true)"
\`\`\`

For allocation evidence on selected Go benchmark rows, run with an explicit row
count and be prepared for long setup time:

\`\`\`sh
RUN_GO_BENCH=true GO_BENCH_ROWS=$TEN_M_ROWS GO_BENCHTIME=1x GO_COUNT=1 \\
RUN_SMOKE=false RUN_10M=false RUN_DIR=$RUN_DIR scripts/bench_text_hybrid_scale.sh
\`\`\`
EOF_PLAN
}

run_scale() {
  local label="$1"
  local rows="$2"
  local queries="$3"
  local batch_size="$4"
  local backfill_rows="$5"
  local maintenance_updates="$6"
  local maintenance_deletes="$7"
  local candidate_limit="$8"
  local out="$RUN_DIR/$label"
  mkdir -p "$out"
  local cmd=(env GOWORK=off "$GO_BIN" run ./cmd/treedb_text_hybrid_scale
    -out-dir "$out"
    -rows "$rows"
    -batch-size "$batch_size"
    -dims "$DIMS"
    -m "$M"
    -ef-construction "$EF_CONSTRUCTION"
    -ef-search "$EF_SEARCH"
    -top-k "$TOP_K"
    -candidate-limit "$candidate_limit"
    -queries "$queries"
    -readers "$READERS"
    -backfill-rows "$backfill_rows"
    -maintenance-updates "$maintenance_updates"
    -maintenance-deletes "$maintenance_deletes"
    -keep-db="$KEEP_DB"
    -phases "$PHASES"
    -base-ref origin/main
    -base-sha "$(git merge-base HEAD origin/main 2>/dev/null || true)")
  printf '%q ' "${cmd[@]}" > "$out/command.txt"
  echo >> "$out/command.txt"
  echo "==> scale $label rows=$rows queries=$queries candidate_limit=$candidate_limit"
  "${cmd[@]}" 2>&1 | tee "$out/run.log"
}

run_go_benchmarks() {
  local rows="$GO_BENCH_ROWS"
  local out="$RUN_DIR/go_bench_${rows}"
  mkdir -p "$out"
  echo "==> Go benchmark allocation rows docs=$rows"
  env GOWORK=off TREEDB_TEXT_V2_SEARCH_DOCS="$rows" "$GO_BIN" test ./TreeDB/collections \
    -run '^$' \
    -bench "^BenchmarkTextV2ScoreSearchScale2627/docs_${rows}/(score_only_common_no_docs|rare_no_docs|multi_term_and_no_docs)$" \
    -benchmem -benchtime="$GO_BENCHTIME" -count="$GO_COUNT" 2>&1 | tee "$out/text_score_search.txt"
  env GOWORK=off TREEDB_TEXT_V2_BLOCKMAX_DOCS="$rows" "$GO_BIN" test ./TreeDB/collections \
    -run '^$' \
    -bench '^BenchmarkTextV2BlockMaxCommonTerm2628/blockmax_common_topk$' \
    -benchmem -benchtime="$GO_BENCHTIME" -count="$GO_COUNT" 2>&1 | tee "$out/text_blockmax.txt"
  env GOWORK=off TREEDB_HYBRID_BENCH_DOCS="$rows" TREEDB_HYBRID_BENCH_DIMS="$DIMS" TREEDB_HYBRID_BENCH_M="$M" "$GO_BIN" test ./TreeDB/collections \
    -run '^$' \
    -bench '^BenchmarkSearchHybridCloseout2506/mode_(text_only_no_docs|vector_only_no_docs|hybrid_no_docs)/topK_10/candidates_64/filter_(none_100pct|rare_06pct)$' \
    -benchmem -benchtime="$GO_BENCHTIME" -count="$GO_COUNT" 2>&1 | tee "$out/hybrid_closeout.txt"
}

write_10m_plan

if [[ "$RUN_SMOKE" == "true" || "$RUN_SMOKE" == "1" || "$RUN_SMOKE" == "yes" ]]; then
  run_scale "scale_smoke_${SMOKE_ROWS}" "$SMOKE_ROWS" "$SMOKE_QUERIES" "$SMOKE_BATCH_SIZE" "$SMOKE_ROWS" "$(( SMOKE_ROWS / 100 > 0 ? SMOKE_ROWS / 100 : 1 ))" "$(( SMOKE_ROWS / 200 > 0 ? SMOKE_ROWS / 200 : 1 ))" "$SMOKE_CANDIDATE_LIMIT"
fi

if [[ "$RUN_1M" == "true" || "$RUN_1M" == "1" || "$RUN_1M" == "yes" ]]; then
  for ((rep = 1; rep <= RETRIEVAL_REPETITIONS; rep++)); do
    run_scale "scale_1m_rep${rep}" "$ONE_M_ROWS" "$ONE_M_QUERIES" "$ONE_M_BATCH_SIZE" "$ONE_M_BACKFILL_ROWS" "$ONE_M_MAINTENANCE_UPDATES" "$ONE_M_MAINTENANCE_DELETES" "$ONE_M_CANDIDATE_LIMIT"
  done
fi

if [[ "$RUN_10M" == "true" || "$RUN_10M" == "1" || "$RUN_10M" == "yes" ]]; then
  if [[ "$APPROVE_10M" != "true" && "$APPROVE_10M" != "1" && "$APPROVE_10M" != "yes" ]]; then
    cat > "$RUN_DIR/10m_not_run.md" <<EOF_SKIP
# 10M run not started

RUN_10M was requested, but APPROVE_10M was not true. This protects against
surprise multi-hour/tens-of-GB local jobs. See 10m_selected_matrix_commands.md.
EOF_SKIP
    echo "10M run skipped: set APPROVE_10M=true only after coordinator approval."
  else
    run_scale "scale_10m_selected" "$TEN_M_ROWS" "$TEN_M_QUERIES" "$TEN_M_BATCH_SIZE" "$TEN_M_BACKFILL_ROWS" "$TEN_M_MAINTENANCE_UPDATES" "$TEN_M_MAINTENANCE_DELETES" "$TEN_M_CANDIDATE_LIMIT"
  fi
fi

if [[ "$RUN_GO_BENCH" == "true" || "$RUN_GO_BENCH" == "1" || "$RUN_GO_BENCH" == "yes" ]]; then
  run_go_benchmarks
fi

cat > "$RUN_DIR/README.md" <<EOF_README
# TreeDB text-v2/hybrid scale run

Context: \`$RUN_DIR/context.txt\`

Primary artifacts:

- smoke/current scale reports under \`$RUN_DIR/scale_*/*scale_report.md\`
- JSON reports under \`$RUN_DIR/scale_*/*scale_report.json\`
- selected phase selector: \`$PHASES\` (use \`PHASES=retrieval\` for bounded #4327 retrieval qualification)
- exact 10M command plan: \`$RUN_DIR/10m_selected_matrix_commands.md\`
- optional Go benchmark logs under \`$RUN_DIR/go_bench_*\`

Full 10M runs are gated by \`RUN_10M=true APPROVE_10M=true\` and should only be
started after explicit coordinator approval.
EOF_README

echo "scale run artifacts: $RUN_DIR"
