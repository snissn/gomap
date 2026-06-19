# TreeDB Hybrid Search Benchmark Runbook (#2506)

This runbook closes out the first TreeDB hybrid lexical + vector search stack by
making text-only, vector-only, and hybrid executor measurements reproducible on
the same fixture. It is a benchmark/docs contract, not an optimization task.

Parent tracker: <https://github.com/snissn/gomap/issues/2501>. Closeout issue:
<https://github.com/snissn/gomap/issues/2506>.

## Boundaries

- `BenchmarkSearchHybridCloseout2506` lives in `TreeDB/collections` and uses one
  synthetic collection fixture for all rows.
- Single-source rows call `SearchHybrid` with only `Text` or only `Vector` so the
  executor, stats, scalar-filter, and final-fetch boundary stay comparable.
  Dedicated `SearchText` and vector-only hot-path benchmarks remain their own
  evidence lanes.
- Candidate generation must fetch zero full documents. The benchmark reports
  `docs_fetched/search`; no-document rows must stay at `0`. Compact/score-only
  result modes must also stay at `0`. The `hybrid_fetch_topk` row enables full
  final document fetch and must remain `<= topK/search`.
- The fixture is small enough for local closeout/profile smoke. Treat the rows as
  same-host context unless a later quiet-host campaign reruns the same command
  contract.

## Fixture shape

Default fixture:

| Setting | Default | Override |
| --- | ---: | --- |
| documents | 256 | `TREEDB_HYBRID_BENCH_DOCS` |
| vector dimensions | 16 | `TREEDB_HYBRID_BENCH_DIMS` |
| vector graph `M` | 8 | `TREEDB_HYBRID_BENCH_M` |
| vector metric/route | cosine / exact column graph | code change only |
| text index | `lexical` over `title` (weight 3) and `body` | code change only |
| scalar filters | `tenant-rare-06pct`, `tenant-narrow-25pct` | code change only |
| query | text `refund policy`; vector query near refund docs | code change only |

Benchmark rows cover:

- modes: `text_only_no_docs`, `vector_only_no_docs`, `hybrid_no_docs`,
  `hybrid_fetch_topk`;
- candidate/topK sensitivity: `topK_5/candidates_16` and
  `topK_10/candidates_64`;
- scalar selectivity: no filter, rare ~6%, and narrow 25% where the candidate
  budget is large enough for the bounded scalar allow-set.

## Main benchmark command

```sh
OUT=/tmp/gomap_2506_hybrid_closeout_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"
set -o pipefail

GOWORK=off \
TREEDB_HYBRID_BENCH_DOCS=256 \
TREEDB_HYBRID_BENCH_DIMS=16 \
TREEDB_HYBRID_BENCH_M=8 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkSearchHybridCloseout2506$' \
  -benchmem \
  -benchtime=100x \
  -count=3 \
  | tee "$OUT/hybrid_bench.txt"
```

For a quick smoke use `-benchtime=1x -count=1`. For less noisy local context use
larger `-benchtime` and run under the quietest available host conditions.

## Representative profile command

```sh
OUT=${OUT:-/tmp/gomap_2506_hybrid_closeout_$(date +%Y%m%d_%H%M%S)}
mkdir -p "$OUT"
set -o pipefail
BENCH='^BenchmarkSearchHybridCloseout2506/mode_hybrid_fetch_topk/topK_10/candidates_64/filter_rare_06pct$'

GOWORK=off \
TREEDB_HYBRID_BENCH_DOCS=256 \
TREEDB_HYBRID_BENCH_DIMS=16 \
TREEDB_HYBRID_BENCH_M=8 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench "$BENCH" \
  -benchmem \
  -benchtime=200x \
  -count=1 \
  -cpuprofile "$OUT/hybrid_fetch_cpu.pprof" \
  -memprofile "$OUT/hybrid_fetch_mem.pprof" \
  | tee "$OUT/hybrid_fetch_profile_bench.txt"

go tool pprof -top "$OUT/hybrid_fetch_cpu.pprof" > "$OUT/hybrid_fetch_cpu_top.txt"
go tool pprof -top "$OUT/hybrid_fetch_mem.pprof" > "$OUT/hybrid_fetch_mem_top.txt"
```

Go CPU profiles cover the benchmark process; use enough timed iterations that
query work dominates fixture setup and test-package initialization.

## Metrics to report

Every accepted table should include:

- commit SHA, branch, host, load notes, Go version, and exact command;
- artifact root and raw output/profile paths;
- fixture shape: documents, average text shape, text fields, scalar filter
  selectivity, vector dimensions, graph `M`, `efSearch`, `topK`, and candidate
  budgets;
- `ns/op`, `ops/sec`, `B/op`, `allocs/op`;
- text counters: `text_requested/search`, `text_candidates/search`,
  `text_postings/search`, `text_scored/search`;
- vector counters: `vector_requested/search`, `vector_candidates/search`,
  `vector_examined/search`, `vector_edges/search`;
- scalar/fusion/fetch counters: `scalar_prefilter_ids/search`,
  `scalar_matched/search`, `scalar_rejected/search` (`scalar_rejected/search`
  includes vector rows pruned by a selective scalar allow-set when that pushdown
  route is active),
  `candidates_fused/search`, `candidates_after_filter/search`,
  `fusion_text_only/search`, `fusion_vector_only/search`,
  `fusion_both/search`, `docs_fetched/search`;
- adaptive budget counters: `text_effective_budget/search`,
  `vector_effective_budget/search`, `adaptive_budget/search`,
  `budget_iterations/search`, and `budget_fallbacks/search`;
- guardrails: `full_doc_fallbacks/search`, `fail_closed/search`,
  `truncated/search`, `docs_missing/search`;
- profile top summaries for at least one representative hybrid+fetch cell.

Compute ops/sec from `ns/op` as `1e9 / ns_per_op` when the Go benchmark output
does not already show it.

## Interpreting rows

- No-document single-source and hybrid rows prove candidate/fusion boundaries;
  they are not final materialization latency.
- `hybrid_fetch_topk` includes bounded final document projection with the
  embedding excluded. Keep it separate from no-document rows.
- `truncated/search` is expected when a source scores more candidates than the
  requested returned candidate budget. It is not a scan-all-documents fallback.
- `adaptive_budget/search=1` means the row stopped on an exact RRF/top-k proof;
  `budget_fallbacks/search>0` means the executor kept fixed requested budgets
  because exact bounds were unsupported or insufficient.
- `full_doc_fallbacks/search` and `fail_closed/search` must remain zero for
  successful benchmark rows.
- Do not compare these small hybrid rows as product speedups against #2490
  vector-only matrix results. The vector evidence remains route-specific and
  includes larger exact/scalar_u8/RaBitQ/USearch/pgvector context with its own
  caveats.

## Evidence cross-links

- Text substrate/search evidence: #1764 and
  `TreeDB/docs/spec/collection-text-search.md`.
- Hybrid contract/executor: #2502 through #2505 and
  `TreeDB/docs/spec/hybrid-search-contract.md`.
- Vector-only crossover context: #2490, especially #2492/#2493/#2494. Preserve
  the no-universal-claim/load caveats from those issue comments.
- User-facing hybrid examples: `TreeDB/docs/guides/hybrid-search.md`.
- Indexed insertion/search lifecycle rows and #2589 optimized allocation closeout evidence: `docs/benchmarks/treedb_index_insert_search_benchmarks.md`.
