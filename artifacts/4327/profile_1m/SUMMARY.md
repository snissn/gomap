# #4327 1M hybrid-row CPU/allocation attribution

Two retained query-isolated 1M rows at commit `d83b70cfc16d85fbe1bba60c37bd010dacc46c7f`, base `6b07740e25bf663b2df3594ed74532601c23ac96`, Go 1.26.0, Apple M3. Each run has a clean worktree, uses the retrieval-only phases, 25 timed samples, top-k 10, candidate limit 65,536, exact vector mode, score-only/no-document boundary, and passed all guardrails. Commands, raw row reports, CPU/allocation top and cumulative text views, and SHA-256 provenance are retained here. Binary `.pprof` files were summarized then removed from `/tmp`.

| row | CPU-profile p50 / p95 (ms) | allocation-profile p50 / p95 (ms) | allocation space / 25 queries |
| --- | ---: | ---: | ---: |
| `hybrid_text_scalar_no_docs` | 150.673 / 155.423 | 1125.884 / 1153.562 | 5.03 GB / 201 MB per query |
| `hybrid_text_vector_no_docs` | 142.268 / 146.816 | 714.970 / 756.113 | 6.83 GB / 273 MB per query |

CPU runs omit allocation sampling. Allocation runs disable sampling during fixture construction and enable `MemProfileRate=1` only after the selected row's warm query, so their allocation profiles attribute the 25 selected queries; their latency is not used as performance evidence because allocation sampling intentionally perturbs it.

## Attribution and disposition

Scalar CPU/alloc attribution is dominated by exact text candidate work: `executeTextV2ORBlockMaxSearchAtSnapshot` is 80.6% cumulative allocation, with `decodeTextV2SearchNormBlock` (27.9%), `decodeTextV2SearchDocMapBlock` (24.1%), and `lookupLeafValueView` (14.0%) flat. The scalar-specific allow-set path is only 6.6% cumulative allocation. The row scans 1,000,000 postings, admits/fuses 62,500 candidates, and ends `fixed` / `exact_bound_insufficient`.

Vector CPU/alloc attribution adds fixed-budget candidate materialization/fusion rather than an anomalous scalar path: `FuseHybridSearchCandidates` is 14.5% flat allocation, `appendHybridSearchCandidates` 8.7%, vector scratch/result buffers 15.5% combined, while text norm/doc-map decoding remains 38.4%. The row requests and returns 65,536 candidates from each source, visits 1,048,737 vector edges, fuses 131,072 candidates, and ends `fixed` / `exact_bound_insufficient`.

No issue-owned product optimization target is activated. The named costs are causally consistent with the explicit fixed exact-bound-insufficient candidate budget and retained row semantics, not a broad-text pruning failure. Changing those directional rows merely to lower their number would alter the query/candidate contract. The broad text rows already demonstrate exact pruning in `retrieval_100k/` and `retrieval_1m/`.
