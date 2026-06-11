# TreeDB quantized prepared HNSW fast-path closeout (#2588)

Status: final evidence closeout for the #2584 quantized prepared fast-path
stack. The stack landed in four implementation PRs:

| Issue | PR | Status | Merge commit | Notes |
| --- | --- | --- | --- | --- |
| #2591 | #2592 | promoted | `e30ac7384a579d301e8a384fab6977e8e8367879` | Added the 10,000-row x 768-dim production benchmark gate. |
| #2585 | #2593 | promoted | `ca34466d7951a88e94c8d89b53264eff75b7d63b` | Added the prepared traversal/score-plane seam with no scalar/RaBitQ route promotion. |
| #2586 | #2596 | promoted | `ab68056b698f713d60d0740bf7f58d98067f6a4a` | Routed `scalar_u8` through prepared traversal and preserved exact scratch shape. |
| #2587 | #2606 | promoted | `ae23f683e781507b0566228e00d98148de2da68d` | Routed `rabitq_1bit` through prepared pack traversal and fixed rerank breadth semantics. |

No child in the #2584/#2588 stack was closed as no-promote. Earlier RaBitQ
Sublane A no-promote decisions (#2478/#2479) remain documented separately in
[`rabitq-performance-lane-closeout-2482.md`](rabitq-performance-lane-closeout-2482.md).

## Final 10k x 768 evidence matrix

Source artifact: `/tmp/issue2587_10k768_final2_20260611_000046.txt`.
This is the #2591 production gate shape: 10,000 docs, 768 dimensions,
`M=16`, `efConstruction=128`, `efSearch=128`, `topK=10`, 16 queries,
`GOMAXPROCS=8`, `-benchtime=1x`. All hot rows are `0 B/op`,
`0 allocs/op`.

| Mode | Boundary | c | ns/op | ops/sec | B/op | allocs/op | recall/counters | code B/search | exact vector/norm reads | artifact |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- | --- |
| exact_fp32 | SearchWithBuffer | 1 | 24917 | 40133 | 0 | 0 | recall=100%; pack=1; fallback=0 | 0 | 589824/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| exact_fp32 | CollectionSearchVectorIndexWithBuffer | 1 | 30458 | 32832 | 0 | 0 | recall=100%; pack=1; fallback=0 | 0 | 589824/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| exact_fp32 | SearchWithBuffer | 8 | 45208 | 22120 | 0 | 0 | recall=100%; pack=1; fallback=0 | 0 | 589824/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| exact_fp32 | CollectionSearchVectorIndexWithBuffer | 8 | 48334 | 20689 | 0 | 0 | recall=100%; pack=1; fallback=0 | 0 | 589824/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| scalar_u8 | SearchWithBuffer quantized_only | 1 | 50458 | 19818 | 0 | 0 | recall=81.25%; qscore=208; rerank_exact=0; pack=0; qroute=1/0; fallback=0 | 159744 | 0/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| scalar_u8 | SearchWithBuffer quantized_only | 8 | 196250 | 5096 | 0 | 0 | recall=81.25%; qscore=208; rerank_exact=0; pack=0; qroute=1/0; fallback=0 | 159744 | 0/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| scalar_u8 | SearchWithBuffer quantized_rerank cand=32 | 1 | 28208 | 35451 | 0 | 0 | recall=81.25%; qscore=208; rerank_exact=32; pack=0; qroute=0/1; fallback=0 | 159744 | 98304/128 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| scalar_u8 | SearchWithBuffer quantized_rerank cand=32 | 8 | 68000 | 14706 | 0 | 0 | recall=81.25%; qscore=208; rerank_exact=32; pack=0; qroute=0/1; fallback=0 | 159744 | 98304/128 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| scalar_u8 | CollectionSearchVectorIndexWithBuffer quantized_only | 1 | 72750 | 13746 | 0 | 0 | recall=81.25%; qscore=208; rerank_exact=0; pack=0; qroute=1/0; fallback=0 | 159744 | 0/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| scalar_u8 | CollectionSearchVectorIndexWithBuffer quantized_only | 8 | 72042 | 13881 | 0 | 0 | recall=81.25%; qscore=208; rerank_exact=0; pack=0; qroute=1/0; fallback=0 | 159744 | 0/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| scalar_u8 | CollectionSearchVectorIndexWithBuffer quantized_rerank cand=32 | 1 | 185917 | 5379 | 0 | 0 | recall=81.25%; qscore=208; rerank_exact=32; pack=0; qroute=0/1; fallback=0 | 159744 | 98304/128 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| scalar_u8 | CollectionSearchVectorIndexWithBuffer quantized_rerank cand=32 | 8 | 31542 | 31704 | 0 | 0 | recall=81.25%; qscore=208; rerank_exact=32; pack=0; qroute=0/1; fallback=0 | 159744 | 98304/128 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| rabitq_1bit | SearchWithBuffer quantized_only | 1 | 83542 | 11970 | 0 | 0 | recall=68.75%; qscore=208; rerank_exact=0; pack=1; qroute=1/0; fallback=0 | 26624 | 0/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| rabitq_1bit | SearchWithBuffer quantized_only | 8 | 98708 | 10131 | 0 | 0 | recall=68.75%; qscore=208; rerank_exact=0; pack=1; qroute=1/0; fallback=0 | 26624 | 0/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| rabitq_1bit | SearchWithBuffer quantized_rerank cand=32 | 1 | 78875 | 12678 | 0 | 0 | recall=68.75%; qscore=208; rerank_exact=32; pack=1; qroute=0/1; fallback=0 | 26624 | 98304/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| rabitq_1bit | SearchWithBuffer quantized_rerank cand=32 | 8 | 101292 | 9872 | 0 | 0 | recall=68.75%; qscore=208; rerank_exact=32; pack=1; qroute=0/1; fallback=0 | 26624 | 98304/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| rabitq_1bit | CollectionSearchVectorIndexWithBuffer quantized_only | 1 | 83542 | 11970 | 0 | 0 | recall=68.75%; qscore=208; rerank_exact=0; pack=1; qroute=1/0; fallback=0 | 26624 | 0/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| rabitq_1bit | CollectionSearchVectorIndexWithBuffer quantized_only | 8 | 84750 | 11799 | 0 | 0 | recall=68.75%; qscore=208; rerank_exact=0; pack=1; qroute=1/0; fallback=0 | 26624 | 0/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| rabitq_1bit | CollectionSearchVectorIndexWithBuffer quantized_rerank cand=32 | 1 | 86958 | 11500 | 0 | 0 | recall=68.75%; qscore=208; rerank_exact=32; pack=1; qroute=0/1; fallback=0 | 26624 | 98304/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |
| rabitq_1bit | CollectionSearchVectorIndexWithBuffer quantized_rerank cand=32 | 8 | 87292 | 11456 | 0 | 0 | recall=68.75%; qscore=208; rerank_exact=32; pack=1; qroute=0/1; fallback=0 | 26624 | 98304/0 | `/tmp/issue2587_10k768_final2_20260611_000046.txt` |

Notes:

- Exact FP32 rows select the exact `hnsw_search_pack_v1` route and keep
  quantized route counters at zero.
- `scalar_u8` rows keep the codec-generic column-graph prepared route
  (`search_route_column_graph_prepared/search=1`) and do not report
  `search_route_hnsw_search_pack/search`.
- `rabitq_1bit` rows are still quantized routes
  (`search_route_quantized_only/search=1` or
  `search_route_quantized_rerank/search=1`) but also report
  `search_route_hnsw_search_pack/search=1` because their score plane uses
  prepared pack traversal. They report nonzero `candidates/search`,
  `edges/search`, and `visited_edges/search` in production evidence.
- Rerank rows exact-score only the configured shortlist
  (`quantized_rerank_exact_score_calls/search=32`).
- Document/fallback guardrails stay quiet: `docs_fetched/search=0`,
  `graph_row_fallbacks/search=0`, `typed_column_vector_fallbacks/search=0`,
  `hnsw_search_pack_fallbacks/search=0`, and
  `quantized_asset_unavailable/search=0` for the healthy rows.

## Same-host before/after assessment

Exact FP32 guardrail, `origin/main` at `ab68056b` vs #2587 final candidate:

- artifact: `/tmp/issue2587_exact_gate_ab680_vs_final2_20260610_235353/benchstat.txt`
- result: no statistically significant exact FP32 regressions; exact gate
  geomean was -21.63% in that run; all exact rows stayed `0 B/op`,
  `0 allocs/op` and kept route/counter identity.

`scalar_u8` prepared traversal, #2586 same-host before/after:

- artifact: `/tmp/issue2586_scalar_after_b7a632_20260610_215837/benchstat_scalar.txt`
- result: lower `quantized_only` c=1/c=8 improved -22.86%/-17.91%; collection
  `quantized_only` c=1 improved -55.05%; rerank rows were neutral to modestly
  faster in that run; geomean -19.70%; hot rows stayed allocation-free.

`rabitq_1bit` prepared pack traversal, #2587 same-host before/after:

- artifact: `/tmp/issue2587_rabitq_bench_ab680_vs_final2_rerun_20260610_235811/benchstat.txt`
- result: lower qonly c=1/c=8 improved -17.65%/-21.18%; lower rerank32
  c=1/c=8 improved -15.48%/-16.83%; collection qonly c=1/c=8 improved
  -20.51%/-20.75%; collection rerank32 c=1/c=8 improved -8.85%/-11.80%; geomean
  -16.73%; hot rows stayed allocation-free.

## Profile summary

Profile artifacts:

- source bottleneck profile before this stack:
  `/tmp/rabitq_mode_bottlenecks_20260610_142050`;
- final `scalar_u8` collection hot-row profiles:
  `/tmp/issue2588_scalar_profiles_20260611_004414`;
- final `rabitq_1bit` collection hot-row profiles:
  `/tmp/issue2587_profiles_final2b_20260611_000238`.

Profile interpretation:

- Exact FP32 was already dominated by prepared pack traversal/scoring; the stack
  keeps exact route ordering first so quantized eligibility does not sit on the
  exact hot path.
- `scalar_u8` promoted the prepared traversal seam; profiles are dominated by
  `searchCosinePreparedScorePlane`, frontier maintenance, adjacency access,
  and `dotScalarU8CenteredIndexed*` scoring.
- `rabitq_1bit` promoted prepared pack traversal; profiles are dominated by
  `searchCosinePreparedScorePlane`, RaBitQ byte-table mismatch scoring,
  frontier maintenance, and exact rerank work on rerank rows.
- Allocation profiles can include setup/off-timer fixture work, but the timed hot
  rows above are the source of truth and remain `0 B/op`, `0 allocs/op`.

## Route and semantic closeout

The final route state is intentionally split by codec:

- Exact/default query mode uses the exact `hnsw_search_pack_v1` route.
- `scalar_u8` quantized modes use the prepared column-graph quantized route.
- `rabitq_1bit` quantized modes use prepared `hnsw_search_pack_v1`
  traversal with a RaBitQ score plane; this is still a quantized route and must
  not be relabeled as exact FP32.
- Quantized modes still fail closed on missing, stale, corrupt, mismatched,
  unsupported, closed, or unprepared score-plane assets; they must not silently
  fall back to exact search.
- `QuantizedRerankCandidates` limits only the exact rerank shortlist. HNSW
  traversal keeps the normalized `ef_search` breadth.

## `rabitq_1bit` v1 invariants

#2587 changed traversal plumbing only. It did **not** change `rabitq_1bit` v1
storage, LSB bit order, high-bit padding, weighted sign-dot score formula,
asset identity, codec name/version/config identity, or fail-closed behavior.
The durable codec contract remains
[`rabitq-1bit-v1.md`](rabitq-1bit-v1.md).

## Remaining bottlenecks and follow-up boundaries

- Prepared traversal/frontier work remains a shared scalar/RaBitQ cost center.
- RaBitQ still spends material time in byte-table query/scoring code; this
  closeout does not add SIMD/popcount, multi-bit RaBitQ, BRQ promotion, PQ/OPQ,
  IVF, or graph-topology changes.
- The benchmark gate is deterministic and hot-cache; broader crossover claims
  still belong in the vector crossover benchmark workflow, not this closeout.
