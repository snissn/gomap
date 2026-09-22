# TreeDB Indexed Insertion And Search Benchmark (#2564 / #2589)

This runbook covers the collection index lifecycle boundary: inserting documents into a collection with scalar, text, and vector index definitions enabled, then searching those indexes on the same synthetic fixture. It also records the #2589 allocation-optimization closeout rows for the same fixture shape.

Benchmark issue: <https://github.com/snissn/gomap/issues/2564>.
Allocation optimization tracker: <https://github.com/snissn/gomap/issues/2589>.
Text v2 contract/baseline matrix: `docs/benchmarks/treedb_text_v2_contract_benchmarks.md`.

## Scope and boundaries

`BenchmarkIndexInsertSearch2564` lives in `TreeDB/collections`.

The benchmark has two timing boundaries:

1. `indexed_insert_batch_flush_vector_rebuild` times `InsertBatch`, `Flush`, and `RebuildVectorIndex` for one prepared batch. It excludes database/collection creation and JSON fixture generation. This row is a lifecycle/index-readiness row, not only an append/write row.
2. Search rows build and index the fixture before `ResetTimer`, then time only the search API call:
   - `search_text_candidates_no_docs` uses the explicit v1 compatibility text index;
   - `search_text_v2_candidates_no_docs` uses the v2/default text index;
   - `search_vector_candidates_no_docs` uses `SearchHybridVectorCandidates`;
   - `search_hybrid_no_docs_scalar_filter` uses the explicit v1 compatibility text index plus vector/scalar filtering without final document fetch;
   - `search_hybrid_v2_no_docs_scalar_filter` uses the v2/default text index plus vector/scalar filtering without final document fetch;
   - `search_hybrid_fetch_topk_scalar_filter` uses bounded final fetch and excludes `embedding` from returned documents.

Candidate-generation rows must keep `docs_fetched/search=0` and `full_doc_fallbacks/search=0`. The final-fetch hybrid row must keep `docs_fetched/search <= topk/search`.

## Fixture shape

Default fixture knobs:

| Setting | Default | Override |
| --- | ---: | --- |
| documents | 256 | `TREEDB_INDEX_BENCH_DOCS` |
| vector dimensions | 16 | `TREEDB_INDEX_BENCH_DIMS` |
| vector graph `M` | 8 | `TREEDB_INDEX_BENCH_M` |
| scalar indexes | `tenant`, `region` | code change only |
| text index | v2/default `lexical` over `title` (weight 3) and `body`; explicit v1 rows use the same fields | code change only |
| vector route | exact cosine column graph | code change only |
| search query | text `refund policy`; vector query near refund docs | code change only |
| scalar filter | `tenant-rare-06pct` (~6.25% at default shape) | code change only |

## Main command

```sh
OUT=/tmp/gomap_index_insert_search_bench_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"
set -o pipefail

GOWORK=off \
TREEDB_INDEX_BENCH_DOCS=256 \
TREEDB_INDEX_BENCH_DIMS=16 \
TREEDB_INDEX_BENCH_M=8 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkIndexInsertSearch2564$' \
  -benchmem \
  -benchtime=5x \
  -count=3 \
  | tee "$OUT/index_insert_search_bench.txt"
```

For a quick compile/guardrail smoke use `-benchtime=1x -count=1` and a smaller fixture, for example `TREEDB_INDEX_BENCH_DOCS=64 TREEDB_INDEX_BENCH_DIMS=8 TREEDB_INDEX_BENCH_M=4`.

## Original #2564 context evidence

Artifact root: `/tmp/gomap_index_insert_search_bench_20260607_162015`
Raw output: `/tmp/gomap_index_insert_search_bench_20260607_162015/index_insert_search_bench.txt`
Context: `/tmp/gomap_index_insert_search_bench_20260607_162015/context.txt`

Context:

- commit: `840bff52062e3b2c1c2818cde7d94efe3b9a45ce`
- branch: `snissn/2564-index-bench`
- Go: `go1.26.0 darwin/arm64`
- host: Apple M3, 8 CPUs, Darwin arm64
- load note: active laptop/Orca environment; load averages were about `6.40 4.69 3.66`. Treat this as current-main context/smoke evidence, not a universal throughput claim.

Selected averages from `-benchtime=5x -count=3`:

| Row | ns/op avg | ops/sec | B/op | allocs/op | docs/op | docs/sec | docs fetched | text cand | vector cand | fused | scalar rejected | fail/fallback | trunc | insert ns/doc | vector rebuild ns/doc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| `indexed_insert_batch_flush_vector_rebuild` | 79,928,531 | 12.5 | 11,014,446 | 113,174 | 256 | 3,202.9 | 0 | 0 | 0 | 0 | 0 | 0/0 | 0 | 174,590 | 137,618 |
| `search_text_candidates_no_docs` | 275,850 | 3,625.2 | 425,584 | 7,591 | 0 | 0.0 | 0 | 64 | 0 | 0 | 0 | 0/0 | 64 | 0 | 0 |
| `search_vector_candidates_no_docs` | 20,475 | 48,840.0 | 36,408 | 82 | 0 | 0.0 | 0 | 0 | 64 | 0 | 0 | 0/0 | 192 | 0 | 0 |
| `search_hybrid_no_docs_scalar_filter` | 328,450 | 3,044.6 | 514,723 | 7,791 | 0 | 0.0 | 0 | 64 | 64 | 16 | 112 | 0/0 | 258 | 0 | 0 |
| `search_hybrid_fetch_topk_scalar_filter` | 546,853 | 1,828.6 | 572,528 | 8,768 | 0 | 0.0 | 10 | 64 | 64 | 16 | 112 | 0/0 | 258 | 0 | 0 |

Interpretation notes:

- Insert `docs/sec` is derived as `ops/sec * docs/op`; the Go row reports one prepared batch per op.
- The insertion row includes vector-index rebuild so the fixture is search-ready. Use `insert_batch_ns/doc`, `flush_ns/doc`, and `vector_rebuild_ns/doc` to separate the measured subphases.
- No-document candidate/search rows kept `docs_fetched/search=0` and `full_doc_fallbacks/search=0`.
- `search_hybrid_fetch_topk_scalar_filter` fetched exactly `10` documents with `topk/search=10`, preserving the bounded final-fetch contract.
- `truncated/search` reflects bounded candidate budgets, not a scan-all-documents fallback.
- These are current-main context rows only. Do not present them as before/after optimization evidence without rerunning an identical baseline/candidate comparison on the same host.

## Optimized #2589 closeout evidence

Artifact root: `/tmp/gomap_2589_insert_alloc_final_20260610_210450`
Primary insert/readiness summary: `/tmp/gomap_2589_insert_alloc_final_20260610_210450/before_after_primary.md`
Final benchmark summary: `/tmp/gomap_2589_insert_alloc_final_20260610_210450/final_benchmark_summary.md`
Primary raw output: `/tmp/gomap_2589_insert_alloc_final_20260610_210450/primary_insert_bench.txt`
Guardrail raw output: `/tmp/gomap_2589_insert_alloc_final_20260610_210450/guardrail_bench.txt`
Profile tops: `/tmp/gomap_2589_insert_alloc_final_20260610_210450/insert_alloc_space_top.txt`, `/tmp/gomap_2589_insert_alloc_final_20260610_210450/insert_alloc_objects_top.txt`

Context:

- optimized head: `acb2db4f433a81851cb3bdfc6eac546bd0df4218`, merged to `main` as `24df4d192eb0db512c0eaf6ff555ee915b614b39` via #2595 after the #2590 text/hybrid allocation PR;
- branch for final artifact: `snissn/2589-insert-alloc-manager`;
- Go/toolchain: `go1.26.0 darwin/arm64`; benchmark output reports `goos=darwin`, `goarch=arm64`, CPU `Apple M3`;
- active laptop/Orca environment caveats still apply. Treat these rows as same-host/context benchmark evidence, not universal speedup claims.

Primary command:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkIndexInsertSearch2564/indexed_insert_batch_flush_vector_rebuild$' \
  -benchmem \
  -benchtime=5x \
  -count=3
```

Guardrail command:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkIndexInsertSearch2564/(search_text_candidates_no_docs|search_vector_candidates_no_docs|search_hybrid_no_docs_scalar_filter|search_hybrid_fetch_topk_scalar_filter)$' \
  -benchmem \
  -benchtime=3x \
  -count=2
```

Selected #2589 optimized medians:

| Row | ns/op median | ops/sec | B/op | allocs/op | Delta vs #2564 context | Key guardrail counters |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| `indexed_insert_batch_flush_vector_rebuild` | 68,588,625 | 14.58 ops/sec / 3,732.4 docs/sec | 7,902,142 | 31,953 | B/op -28.3%; allocs/op -71.8% | 256 docs/op; 141,939 insert ns/doc; 130,717 vector rebuild ns/doc |
| `search_text_candidates_no_docs` | 140,917 | 7,096.38 | 109,298 | 878 | B/op -74.3%; allocs/op -88.4% | `docs_fetched/search=0`; 64 text candidates |
| `search_vector_candidates_no_docs` | 20,584 | 48,582.60 | 36,408 | 82 | B/op 0.0%; allocs/op 0.0% | `docs_fetched/search=0`; 64 vector candidates |
| `search_hybrid_no_docs_scalar_filter` | 163,924 | 6,100.41 | 198,426 | 1,078 | B/op -61.4%; allocs/op -86.2% | `docs_fetched/search=0`; 64 text + 64 vector candidates; 16 fused |
| `search_hybrid_fetch_topk_scalar_filter` | 377,382 | 2,649.83 | 256,232 | 2,055 | B/op -55.2%; allocs/op -76.6% | `docs_fetched/search=10`; `topk/search=10`; 112 scalar rejections |

The insert/readiness row also has same-host M4 paired evidence from the final artifact. Against that M4 baseline, the #2589 final median was `68,588,625 ns/op` versus `68,075,817 ns/op` (+0.8%), `7,902,142 B/op` versus `11,000,368 B/op` (-28.2%), and `31,953 allocs/op` versus `113,175 allocs/op` (-71.8%). Use this paired row for the strict insert/readiness before/after claim. The comparison against the older #2564 context rows above is still useful for discoverability, but it crosses commits and active-laptop load/commitlog context.

Guardrails and remaining floors:

- No-document candidate rows still fetched zero documents and kept fallback/fail counters at zero.
- The final hybrid fetch remained bounded at `docs_fetched/search=10` with `topk/search=10`.
- The vector candidate row did not materially move from its allocation floor: `36,408 B/op` and `82 allocs/op`.
- Representative allocation profiles include benchmark/process setup and are dominated by DB open/commitlog setup. Within the insert/readiness row, remaining allocation owners include value-log append buffers, typed-column image builders, text analysis/posting construction, and vector rebuild state. Further allocation cuts likely require deeper analyzer/token, vector-state, and typed-column builder work rather than small benchmark-closeout tweaks.

## Required PR evidence checklist

When updating this benchmark or publishing a new row, include:

- exact command and artifact path;
- commit, branch, hardware, host-load context, and Go version;
- fixture shape and timing boundary;
- `ns/op`, `ops/sec`, `B/op`, `allocs/op`;
- insertion metrics (`docs/op`, derived docs/sec, `insert_batch_ns/doc`, `flush_ns/doc`, `vector_rebuild_ns/doc`);
- search metrics for both explicit-v1 and default/v2 rows (`text_candidates/search`, `text_postings/search`, `posting_blocks_visited/search`, `posting_blocks_skipped/search`, `blockmax_fallbacks/search`, `text_state_lookups/search`, `text_norm_lookups/search`, `text_match_details/search`, `vector_candidates/search`, `candidates_fused/search`, `docs_fetched/search`, scalar counters, fallback/fail/truncation counters);
- caveats for local/context-only runs.
