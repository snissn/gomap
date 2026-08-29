# TreeDB text-v2/hybrid scale report

- schema: `treedb_text_hybrid_scale/v3`
- generated: `2026-08-29T10:19:56Z`
- commit/tree: `c1c644ead34839570bcdcdac6ee4747cbbbaca6e` / `fb8f32dac0889e22fee0bb9070a9f9c56d8939d1`
- TreeDB/harness/binary: `848d455b64cdd13ba7d76e66a2934f32734eff40` / `15a76c34bc05ca39d55f9bb003faa6054974bb6b` / `b02847b9459f9d0cfbe26c2f7f34bc631f2944d291d6f27fd38ef7f20d5ea23a`
- frozen config/fixture/query/relevance: `80341e126af74686137d8bc6b6ea2ff3ea1163803791cf7b1aa4872dcc88ab61` / `cb03bc9c3a361d0321ac871cfeedd23914cf6cf5c42ff595658b3e08aefdb615` / `d08bb38ebfb616b5c5c9acb42d3b2504778dd2d2720bd00cadae44c3a1713012` / `3f878a33543da1ad78fb72f38a0ebde877f8a04f551509118b4c746a269a1feb`
- base: `origin/main` / `8fe678f057913d2af729b5ccf832f40713ea3b3c`
- rows: `10000000`, dims: `16`, batch: `32768`, queries/row: `3`
- phases: selected `load,queries,reopen,concurrent,maintenance,backfill,text_only,source_chunk`; completed `load`; report status `running`; phase execution **INCOMPLETE (partial evidence; not a completed qualification)**; retained 10M artifact **INCOMPLETE**
- db dir: `/Users/michaelseiler/orca/workspaces/gomap/4329-final-10m-v1/scale_10m/primary_db` (kept=false)

## Load/storage

| phase | seconds | rows/s | storage bytes | bytes/doc | text bytes/doc | vector native bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| load | 8566.689 | 1167.3 | 18867259265 | 1886.7 | 404.1 | 6181096504 |

Load breakdown: generation `12.455s`, insert `4418.519s`, flush `120.735s`, vector rebuild `3981.051s`, checkpoint `2.516s`.

### Text-v2 lane bytes/doc

| snapshot | docid | docmap | postings | norms | positions | terms | status/format |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `after_load` | 21.8 | 19.9 | 61.8 | 7.9 | 292.7 | 0.0 | 0.0 |

### Physical storage/WAL accounting

| snapshot | index pages | value log | WAL | other | total | WAL-excluded |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `after_load` | 7228882944 | 932793357 | 3753309695 | 6952273269 | 18867259265 | 15113949570 |

## Retrieval latency

| row | status | modality | boundary | p50 | p95 | p99 | mean | ops/sec | results | result digest | guardrail | key counters |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| `text_common_score_only` | passed | text | warm no-document text-v2 score-only search | 887.878ms | 930.170ms | 930.170ms | 846.338ms | 1.2 | 10 | `0e31d57dce7ba558132b73d5aadbb4ce1d4a9ebef46457d66e09e8088a567900` | PASS | docs_fetched=0, fail_closed=0, postings=32, blocks_visited=1, blocks_skipped=156249, scored=32 |
| `text_rare_score_only` | passed | text | warm no-document text-v2 score-only search | 260.586ms | 284.707ms | 284.707ms | 267.038ms | 3.7 | 10 | `ebff3d5773e94afe18ea9b62fb6144c0c10803f94ebf28f8ffb3075d98a99c7e` | PASS | docs_fetched=0, fail_closed=0, postings=9766, blocks_visited=306, blocks_skipped=265, scored=9766 |
| `text_multi_term_and_score_only` | passed | text | warm no-document text-v2 score-only search | 865.838ms | 903.511ms | 903.511ms | 866.473ms | 1.2 | 10 | `ba4e84e049729354334ba30f60ef0eae5cdb1a3f48474dd85c43a0dcab05cf45` | PASS | docs_fetched=0, fail_closed=0, postings=64, blocks_visited=2, blocks_skipped=312498, scored=32 |
| `text_multi_term_or_score_only` | passed | text | warm no-document text-v2 score-only search | 757.994ms | 762.626ms | 762.626ms | 740.772ms | 1.3 | 10 | `ba4e84e049729354334ba30f60ef0eae5cdb1a3f48474dd85c43a0dcab05cf45` | PASS | docs_fetched=0, fail_closed=0, postings=64, blocks_visited=2, blocks_skipped=312498, scored=11 |
| `text_phrase_score_only` | passed | text | warm no-document text-v2 score-only search | 48.838s | 323.179s | 323.179s | 139.495s | 0.0 | 10 | `ba4e84e049729354334ba30f60ef0eae5cdb1a3f48474dd85c43a0dcab05cf45` | PASS | docs_fetched=0, fail_closed=0, postings=10000000, blocks_visited=312500, blocks_skipped=0, scored=5000000 |
| `text_common_top_k_fetch` | passed | text | warm bounded final top-k document fetch | 591.417ms | 784.199ms | 784.199ms | 649.049ms | 1.5 | 10 | `0e31d57dce7ba558132b73d5aadbb4ce1d4a9ebef46457d66e09e8088a567900` | PASS | docs_fetched=10, fail_closed=0, postings=32, blocks_visited=1, blocks_skipped=156249, scored=32 |
| `hybrid_text_only_no_docs` | passed | hybrid | warm no-document hybrid candidate generation/fusion | 675.372ms | 686.938ms | 686.938ms | 675.468ms | 1.5 | 10 | `82bb729bfbb970fccf2af01304a420922387e9016428c15ce524f2679cb8057b` | PASS | docs_fetched=0, fail_closed=0, text_budget=10/655360, vector_budget=0/0, text_candidates=10, vector_candidates=0, scalar_prefilter=0, fused=10, budget_policy=adaptive_rrf, budget_stop=single_source_topk, budget_fallback= |
| `hybrid_text_scalar_rare_no_docs` | failed | hybrid | warm hybrid search | 0ns | 0ns | 0ns | 0ns | 0.0 | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | FAIL: `fail_closed=1 reason=text_index_unavailable; error=warm hybrid_text_scalar_rare_no_docs: collections: hybrid search index unavailable: text index "lexical" candidate generation unavailable: collections: text index unavailable: collection "docs" text-v2 index "lexical" exceeded bounded candidate generation` | docs_fetched=0, fail_closed=1, text_budget=655360/655360, vector_budget=0/0, text_candidates=0, vector_candidates=0, scalar_prefilter=625000, fused=0, budget_policy=fixed, budget_stop=exact_bound_insufficient, budget_fallback=exact_bound_insufficient |

## Ranked bottlenecks / follow-ups

| rank | row | metric | value | follow-up |
| ---: | --- | --- | ---: | --- |
| 1 | `fixture_load` | `total_seconds` | 8566.689 s | Investigate write/index build batching, text-v2 append block density, and vector rebuild split if load dominates scale runs. |
| 2 | `vector_rebuild` | `seconds` | 3981.051 s | If vector rebuild dominates, isolate column_graph rebuild scheduling from text-v2 scale evidence. |
| 3 | `text_phrase_score_only` | `p95_ns` | 323178597917.000 ns | Profile this retrieval row first if it is on the target production query mix. |
| 4 | `text_common_score_only` | `p95_ns` | 930169667.000 ns | Profile this retrieval row first if it is on the target production query mix. |
| 5 | `text_multi_term_and_score_only` | `p95_ns` | 903510583.000 ns | Profile this retrieval row first if it is on the target production query mix. |

## Guardrails

- `text_common_score_only`: PASS
- `text_rare_score_only`: PASS
- `text_multi_term_and_score_only`: PASS
- `text_multi_term_or_score_only`: PASS
- `text_phrase_score_only`: PASS
- `text_common_top_k_fetch`: PASS
- `hybrid_text_only_no_docs`: PASS
- `hybrid_text_scalar_rare_no_docs`: FAIL: `fail_closed=1 reason=text_index_unavailable; error=warm hybrid_text_scalar_rare_no_docs: collections: hybrid search index unavailable: text index "lexical" candidate generation unavailable: collections: text index unavailable: collection "docs" text-v2 index "lexical" exceeded bounded candidate generation`

## Caveats

- Synthetic corpus uses deterministic customer-support text, scalar tenants, and small dense vectors; do not use as external relevance-quality evidence.
- Every retained query row includes repeated result-order parity, path counters, and process resource snapshots.
## Cleanup/failure record

- cleanup: ``; kept=false; removed ``; errors ``
- failed/interrupted rows: none

