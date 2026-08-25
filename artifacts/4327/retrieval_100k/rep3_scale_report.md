# TreeDB text-v2/hybrid scale report

- schema: `treedb_text_hybrid_scale/v2`
- generated: `2026-08-25T03:14:51Z`
- branch/commit: `codex/4327-highdf-requalification` / `afa5290556ec9a89f2aaed256fb631b36d4fd635`
- base: `origin/main` / `6b07740e25bf663b2df3594ed74532601c23ac96`
- rows: `100000`, dims: `16`, batch: `16384`, queries/row: `25`
- phases: selected `load,queries,reopen`; completed `load,queries,reopen`; status **COMPLETE**
- db dir: `artifacts/4327/retrieval_100k/rep3/primary_db` (kept=false)

## Load/storage

| phase | seconds | rows/s | storage bytes | bytes/doc | text bytes/doc | vector native bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| load | 9.161 | 10916.1 | 155128257 | 1551.3 | 109.7 | 54615840 |

Load breakdown: generation `0.112s`, insert `1.529s`, flush `0.000s`, vector rebuild `7.431s`, checkpoint `0.020s`.

### Text-v2 lane bytes/doc

| snapshot | docid | docmap | postings | norms | positions | terms | status/format |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `after_load` | 20.8 | 19.0 | 62.6 | 7.0 | 0.0 | 0.2 | 0.0 |
| `after_reopen` | 20.8 | 19.0 | 62.6 | 7.0 | 0.0 | 0.2 | 0.0 |

## Retrieval latency

| row | modality | boundary | p50 | p95 | p99 | mean | ops/sec | results | guardrail | key counters |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `text_common_score_only` | text | warm no-document text-v2 score-only search | 4.855ms | 5.785ms | 5.823ms | 5.066ms | 197.4 | 10 | PASS | docs_fetched=0, fail_closed=0, postings=32, blocks_visited=1, blocks_skipped=1562, scored=32 |
| `text_rare_score_only` | text | warm no-document text-v2 score-only search | 1.921ms | 2.446ms | 2.869ms | 2.004ms | 499.0 | 10 | PASS | docs_fetched=0, fail_closed=0, postings=101, blocks_visited=7, blocks_skipped=0, scored=101 |
| `text_multi_term_and_score_only` | text | warm no-document text-v2 score-only search | 5.728ms | 6.716ms | 7.302ms | 5.885ms | 169.9 | 10 | PASS | docs_fetched=0, fail_closed=0, postings=64, blocks_visited=2, blocks_skipped=3124, scored=32 |
| `text_multi_term_or_score_only` | text | warm no-document text-v2 score-only search | 5.812ms | 6.576ms | 7.420ms | 5.972ms | 167.4 | 10 | PASS | docs_fetched=0, fail_closed=0, postings=64, blocks_visited=2, blocks_skipped=3124, scored=11 |
| `hybrid_text_only_no_docs` | hybrid | warm no-document hybrid candidate generation/fusion | 5.785ms | 6.718ms | 6.868ms | 5.913ms | 169.1 | 10 | PASS | docs_fetched=0, fail_closed=0, text_budget=10/65536, vector_budget=0/0, text_candidates=10, vector_candidates=0, scalar_prefilter=0, fused=10, budget_policy=adaptive_rrf, budget_stop=single_source_topk, budget_fallback= |
| `hybrid_text_scalar_no_docs` | hybrid | warm no-document hybrid candidate generation/fusion | 13.552ms | 14.198ms | 14.370ms | 13.604ms | 73.5 | 10 | PASS | docs_fetched=0, fail_closed=0, text_budget=65536/65536, vector_budget=0/0, text_candidates=6250, vector_candidates=0, scalar_prefilter=6250, fused=6250, budget_policy=fixed, budget_stop=exact_bound_insufficient, budget_fallback=exact_bound_insufficient |
| `hybrid_text_vector_no_docs` | hybrid | warm no-document hybrid candidate generation/fusion | 70.642ms | 72.644ms | 75.164ms | 70.524ms | 14.2 | 10 | PASS | docs_fetched=0, fail_closed=0, text_budget=65536/65536, vector_budget=65536/65536, text_candidates=50000, vector_candidates=65536, scalar_prefilter=0, fused=115536, budget_policy=fixed, budget_stop=exact_bound_insufficient, budget_fallback=exact_bound_insufficient |
| `hybrid_text_vector_scalar_no_docs` | hybrid | warm no-document hybrid candidate generation/fusion | 22.671ms | 23.265ms | 23.349ms | 22.587ms | 44.3 | 10 | PASS | docs_fetched=0, fail_closed=0, text_budget=65536/65536, vector_budget=65536/65536, text_candidates=6250, vector_candidates=65536, scalar_prefilter=6250, fused=10445, budget_policy=fixed, budget_stop=exact_bound_insufficient, budget_fallback=exact_bound_insufficient |

## Reopen

Close `0.006s`, open `0.181s`, open collection `0.000s`, probe `0.112s`, total `0.299s`.

## Ranked bottlenecks / follow-ups

| rank | row | metric | value | follow-up |
| ---: | --- | --- | ---: | --- |
| 1 | `fixture_load` | `total_seconds` | 9.161 s | Investigate write/index build batching, text-v2 append block density, and vector rebuild split if load dominates scale runs. |
| 2 | `vector_rebuild` | `seconds` | 7.431 s | If vector rebuild dominates, isolate column_graph rebuild scheduling from text-v2 scale evidence. |
| 3 | `hybrid_text_vector_no_docs` | `p95_ns` | 72644250.000 ns | Profile this retrieval row first if it is on the target production query mix. |
| 4 | `hybrid_text_vector_scalar_no_docs` | `p95_ns` | 23265125.000 ns | Profile this retrieval row first if it is on the target production query mix. |
| 5 | `hybrid_text_scalar_no_docs` | `p95_ns` | 14197541.000 ns | Profile this retrieval row first if it is on the target production query mix. |

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
- Retrieval rows time warm in-process queries after fixture load, before close/reopen; B/op and allocs/op should be captured with the companion Go benchmark commands when making allocation claims.
