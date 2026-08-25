# TreeDB text-v2/hybrid scale report

- schema: `treedb_text_hybrid_scale/v2`
- generated: `2026-08-25T03:14:37Z`
- branch/commit: `codex/4327-highdf-requalification` / `afa5290556ec9a89f2aaed256fb631b36d4fd635`
- base: `origin/main` / `6b07740e25bf663b2df3594ed74532601c23ac96`
- rows: `100000`, dims: `16`, batch: `16384`, queries/row: `25`
- phases: selected `load,queries,reopen`; completed `load,queries,reopen`; status **COMPLETE**
- db dir: `artifacts/4327/retrieval_100k/rep2/primary_db` (kept=false)

## Load/storage

| phase | seconds | rows/s | storage bytes | bytes/doc | text bytes/doc | vector native bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| load | 9.315 | 10735.5 | 155128257 | 1551.3 | 109.7 | 54615840 |

Load breakdown: generation `0.116s`, insert `1.588s`, flush `0.000s`, vector rebuild `7.514s`, checkpoint `0.026s`.

### Text-v2 lane bytes/doc

| snapshot | docid | docmap | postings | norms | positions | terms | status/format |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `after_load` | 20.8 | 19.0 | 62.6 | 7.0 | 0.0 | 0.2 | 0.0 |
| `after_reopen` | 20.8 | 19.0 | 62.6 | 7.0 | 0.0 | 0.2 | 0.0 |

## Retrieval latency

| row | modality | boundary | p50 | p95 | p99 | mean | ops/sec | results | guardrail | key counters |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `text_common_score_only` | text | warm no-document text-v2 score-only search | 4.821ms | 5.826ms | 7.279ms | 5.084ms | 196.7 | 10 | PASS | docs_fetched=0, fail_closed=0, postings=32, blocks_visited=1, blocks_skipped=1562, scored=32 |
| `text_rare_score_only` | text | warm no-document text-v2 score-only search | 1.936ms | 2.594ms | 2.743ms | 2.026ms | 493.6 | 10 | PASS | docs_fetched=0, fail_closed=0, postings=101, blocks_visited=7, blocks_skipped=0, scored=101 |
| `text_multi_term_and_score_only` | text | warm no-document text-v2 score-only search | 5.640ms | 6.810ms | 6.982ms | 5.833ms | 171.4 | 10 | PASS | docs_fetched=0, fail_closed=0, postings=64, blocks_visited=2, blocks_skipped=3124, scored=32 |
| `text_multi_term_or_score_only` | text | warm no-document text-v2 score-only search | 5.685ms | 6.530ms | 7.010ms | 5.830ms | 171.5 | 10 | PASS | docs_fetched=0, fail_closed=0, postings=64, blocks_visited=2, blocks_skipped=3124, scored=11 |
| `hybrid_text_only_no_docs` | hybrid | warm no-document hybrid candidate generation/fusion | 5.679ms | 6.329ms | 6.361ms | 5.803ms | 172.3 | 10 | PASS | docs_fetched=0, fail_closed=0, text_budget=10/65536, vector_budget=0/0, text_candidates=10, vector_candidates=0, scalar_prefilter=0, fused=10, budget_policy=adaptive_rrf, budget_stop=single_source_topk, budget_fallback= |
| `hybrid_text_scalar_no_docs` | hybrid | warm no-document hybrid candidate generation/fusion | 13.475ms | 14.332ms | 16.662ms | 13.609ms | 73.5 | 10 | PASS | docs_fetched=0, fail_closed=0, text_budget=65536/65536, vector_budget=0/0, text_candidates=6250, vector_candidates=0, scalar_prefilter=6250, fused=6250, budget_policy=fixed, budget_stop=exact_bound_insufficient, budget_fallback=exact_bound_insufficient |
| `hybrid_text_vector_no_docs` | hybrid | warm no-document hybrid candidate generation/fusion | 71.745ms | 75.527ms | 113.913ms | 73.458ms | 13.6 | 10 | PASS | docs_fetched=0, fail_closed=0, text_budget=65536/65536, vector_budget=65536/65536, text_candidates=50000, vector_candidates=65536, scalar_prefilter=0, fused=115536, budget_policy=fixed, budget_stop=exact_bound_insufficient, budget_fallback=exact_bound_insufficient |
| `hybrid_text_vector_scalar_no_docs` | hybrid | warm no-document hybrid candidate generation/fusion | 23.616ms | 24.328ms | 24.512ms | 23.642ms | 42.3 | 10 | PASS | docs_fetched=0, fail_closed=0, text_budget=65536/65536, vector_budget=65536/65536, text_candidates=6250, vector_candidates=65536, scalar_prefilter=6250, fused=10445, budget_policy=fixed, budget_stop=exact_bound_insufficient, budget_fallback=exact_bound_insufficient |

## Reopen

Close `0.007s`, open `0.183s`, open collection `0.000s`, probe `0.122s`, total `0.312s`.

## Ranked bottlenecks / follow-ups

| rank | row | metric | value | follow-up |
| ---: | --- | --- | ---: | --- |
| 1 | `fixture_load` | `total_seconds` | 9.315 s | Investigate write/index build batching, text-v2 append block density, and vector rebuild split if load dominates scale runs. |
| 2 | `vector_rebuild` | `seconds` | 7.514 s | If vector rebuild dominates, isolate column_graph rebuild scheduling from text-v2 scale evidence. |
| 3 | `hybrid_text_vector_no_docs` | `p95_ns` | 75526916.000 ns | Profile this retrieval row first if it is on the target production query mix. |
| 4 | `hybrid_text_vector_scalar_no_docs` | `p95_ns` | 24328375.000 ns | Profile this retrieval row first if it is on the target production query mix. |
| 5 | `hybrid_text_scalar_no_docs` | `p95_ns` | 14331500.000 ns | Profile this retrieval row first if it is on the target production query mix. |

## Guardrails

- `text_common_score_only`: PASS
- `text_rare_score_only`: PASS
- `text_multi_term_and_score_only`: PASS
- `text_multi_term_or_score_only`: PASS
- `hybrid_text_only_no_docs`: PASS
- `hybrid_text_scalar_no_docs`: PASS
- `hybrid_text_vector_no_docs`: PASS
- `hybrid_text_vector_scalar_no_docs`: PASS

## Caveats

- Synthetic corpus uses deterministic customer-support text, scalar tenants, and small dense vectors; do not use as relevance-quality evidence.
- Retrieval rows time warm in-process queries after fixture load/reopen; B/op and allocs/op should be captured with the companion Go benchmark commands when making allocation claims.
