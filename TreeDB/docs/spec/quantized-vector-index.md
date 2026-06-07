# Quantized Vector Index Score Planes (#1926, #2454)

Status: pre-alpha `column_graph` score-plane contract for explicit exact,
`quantized_only`, and `quantized_rerank` query modes. `scalar_u8` v1,
`rabitq_1bit` v1, and prototype `brq_1bit` v1 are durable named score planes
with fail-closed search. Exact / default behavior remains the production
baseline. The `rabitq_1bit` v1 codec and bit/storage contract are specified
separately in [`rabitq-1bit-v1.md`](rabitq-1bit-v1.md); the BRQ v1 contract is
specified in [`brq-1bit-v1.md`](brq-1bit-v1.md). Current route guidance and
accepted benchmark boundaries are summarized in
[`vector-search-closeout-2483.md`](vector-search-closeout-2483.md). The #2482
RaBitQ performance-lane closeout for the #2477 semantics-preserving query
byte-table scorer, no-promote decisions, and later Sublane B outcome is in
[`rabitq-performance-lane-closeout-2482.md`](rabitq-performance-lane-closeout-2482.md).

## User-visible query modes

`VectorIndexDefinition.QuantizedIndexes` declares one or more named derived score
planes for a `column_graph` vector index. Current normalized declarations default
to `codec="scalar_u8"` and `version=1`; they also accept explicit
`codec="rabitq_1bit", version=1` and `codec="brq_1bit", version=1`
declarations.

Search uses an explicit mode:

| Mode | Score plane | Returned scores | Exact vector/norm reads | Notes |
| --- | --- | --- | --- | --- |
| `exact` / zero value | Authoritative `float32_vector` / prepared exact pack | Exact cosine | Yes, for scored candidates or prepared pack scoring | Existing/default behavior. Quantized-only options are rejected in exact mode. |
| `quantized_only` | Selected prepared `scalar_u8`, `rabitq_1bit`, or `brq_1bit` codes | Selected-codec estimated cosine | No | Traversal and final ranking use the quantized scorer. No hidden exact fallback, vector reads, norm reads, or document materialization. |
| `quantized_rerank` | Selected prepared `scalar_u8`, `rabitq_1bit`, or `brq_1bit` codes for traversal, authoritative vectors for rerank | Exact cosine over the quantized shortlist | Yes, only for the trimmed quantized shortlist | Traversal still collects the normalized `ef_search` candidate pool, then trims to `QuantizedRerankCandidates` before exact rerank. |

`QuantizedRerankCandidates=0` means the normalized `ef_search` candidate set.
Non-zero values must be at least `TopK`. Returned `quantized_rerank` results are
ranked by exact cosine over the trimmed quantized candidate set; they are not a
silent full-exact search. Quantized route counters identify the selected query
mode and may be reported alongside the `column_graph` prepared physical route;
`hnsw_search_pack_v1` route/active/fallback counters remain zero for quantized
search. No-document buffered serving is available through both lower-level
`VectorIndexSearcher.SearchWithBuffer` and collection-level
`Collection.SearchVectorIndexWithBuffer` when `QueryMode` and
`QuantizedIndexName` select an explicit quantized mode; document materialization,
projections, filters, and benchmark-debug stats remain outside this buffered
collection route.

## Durable asset model

Each declared scalar score plane rebuilds one TVIS vector-index-state asset with
role `quantized_codes`, asset id `quantized/<name>/codes`, logical type
`byte_vector`, and physical encoding `raw_fixed_bytes`. Rows are in graph ordinal
order and bind to the base graph identity (index, field, metric, dimensions, row
count, base manifest identity, graph schema hash, codec name/version/config, and
asset ref/checksum identity). For cosine, persisted scalar_u8 codes are built
from inverse-norm-normalized vector components so equivalent directions encode to
the same row.

Each declared `rabitq_1bit` v1 score plane also rebuilds one TVIS
vector-index-state asset, with role `quantized_codes`, asset id
`quantized/<name>/packed_codes`, logical type `packed_bit_vector`, and physical
encoding `raw_packed_bit_vector`. Each declared prototype `brq_1bit` v1 score
plane uses the same role/logical type/physical encoding with asset id
`quantized/<name>/brq_1bit/packed_codes` so it cannot be confused with
`rabitq_1bit` storage. These typed-column parts store:

- `packed_codes`: `quantizedasset.RolePackedCodes` backed by
  `packed_bit_vector` / `raw_packed_bit_vector`, LSB-first with zero high-bit
  padding;
- `code_count`: `uint32` / `raw_uint32` side array;
- `quantized_dot_product_inv`: `float32` / `raw_float32` side array.

For RaBitQ and BRQ, `CodeDimensions=next_power_of_two(VectorDimensions)` and
logical code bytes per vector are `ceil(CodeDimensions/8)`. The typed-column
schema identity includes the canonical codec config hash and bytes. Prepare
validates config identity, base graph identity, row count, typed-column
schema/ref/checksum, packed shape/padding, `code_count`, and positive finite
`quantized_dot_product_inv` before returning prepared readers.

Typed-column assets also contain part headers, primary-id/sort-key metadata,
layout certification, and padding, so benchmark evidence must report both
logical `quantized_code_B/vector` and actual `quantized_asset_B/vector`.

## Scoring and fail-closed behavior

Quantized modes validate the selected name, declaration, codec/version, prepared
asset, row count, dimensions, metric, source schema, base graph identity, asset
ref identity, and typed-column layout before traversal/scoring. Missing, stale,
corrupt, mismatched, unsupported, closed, or unprepared assets return
`ErrVectorIndexSearchUnavailable`; they must not fall back to exact traversal or
document reconstruction. Fail-closed stats use codec-generic counters such as
`quantized_asset_missing`, `quantized_asset_invalid`, `quantized_asset_stale`,
`quantized_asset_closed`, and `quantized_asset_unavailable`. Exact mode rejects
quantized-mode fields so callers do not accidentally depend on no-op options.

`scalar_u8` v1 `quantized_only` consumes the prepared `codes` reader and scores
normalized query/candidate code rows. `rabitq_1bit` v1 `quantized_only` consumes
`packed_codes`, `code_count`, and `quantized_dot_product_inv` and uses the
weighted sign-dot estimator specified in `rabitq-1bit-v1.md`. Prototype
`brq_1bit` v1 consumes the same side arrays but uses the `uint4` runtime query
bit-product estimator. This `brq_1bit` v1 prototype uses the score label
`brq_1bit_estimated_cosine_q4` specified
in `brq-1bit-v1.md`; its stats include
`quantized_score_codec_brq_1bit/search=1`, `brq_1bit_query_weight_bits/search`,
`brq_1bit_bitproduct_passes/search`, and `brq_1bit_query_weight_scale/search`.
For all codecs, `quantized_rerank` uses the selected quantized scorer for
traversal/candidate collection, then exact-scores only the configured shortlist
through the authoritative float32 vector/norm path and returns final topK in
exact cosine score order. Search stats report `quantized_score_calls`,
`quantized_code_B/search`, route counters, and
`quantized_rerank_exact_score_calls/search` for rerank.

## Benchmark and storage evidence

The permanent #1926/#2454/#2481 evidence harness compares exact, `scalar_u8`,
pure-Go `rabitq_1bit`, and prototype lower-level `brq_1bit` rows on
deterministic fixtures and separately reports rebuild/storage overhead:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^(BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926|BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414|BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451|BenchmarkVectorIndexSearcherColumnGraphBRQQuantizedSearchWithBuffer2481|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452|BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926|BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450|BenchmarkColumnGraphBRQQuantizedRebuildStorage2481)$' \
  -benchmem -benchtime=100x -count=3
```

`BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926` emits exact and scalar_u8
per-query rows; `BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414`
emits explicit lower-level buffered scalar_u8 rows;
`BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451`
emits explicit lower-level buffered RaBitQ rows;
`BenchmarkVectorIndexSearcherColumnGraphBRQQuantizedSearchWithBuffer2481`
emits prototype lower-level buffered BRQ rows;
`BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415`
emits collection-level buffered scalar_u8 rows;
`BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452`
emits collection-level buffered RaBitQ rows; and rebuild/storage rows are emitted
by `BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926`,
`BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450`, and
`BenchmarkColumnGraphBRQQuantizedRebuildStorage2481`.

The buffered benchmark families include `route=quantized_only/c=1`,
`route=quantized_only/c=8`, `route=quantized_rerank/candidates=32/c=1`, and
`route=quantized_rerank/candidates=32/c=8`. Rows report `ns/op`, `ops/sec`,
`B/op`, `allocs/op`, `recall_at_k_pct` versus exact-mode topK on the same
fixture, `search_route_quantized_only/search`,
`search_route_quantized_rerank/search`, `quantized_scorer_active/search`,
`quantized_asset_unavailable/search`, `candidates/search`,
`quantized_rerank_candidates/search`,
`quantized_rerank_exact_score_calls/search`, `quantized_code_B/search`,
`vector_B/search`, `norm_B/search`, logical code bytes/vector, and actual
`quantized_asset_B/vector`. The rebuild rows report build throughput, allocation
cost, total graph/vector-index-state storage, quantized asset bytes, and
quantized asset bytes/vector.

Representative #2454/#2482/#2487 evidence on Apple M3 / darwin arm64 / Go
`go1.26.0` is recorded in `rabitq-closeout-2454.md`,
`rabitq-performance-lane-closeout-2482.md`, and
`vector-search-closeout-2483.md`. Those runs show `0 B/op`, `0 allocs/op`, 100%
fixture recall, fail-closed/route counters, no exact vector/norm reads for
`quantized_only`, exact shortlist reads for `quantized_rerank`, and much smaller
RaBitQ code bytes (`16` logical B/vector on 128 dims) than scalar_u8 (`128`
logical B/vector). They also show that the current pure-Go RaBitQ weighted
scorer is slower than scalar_u8 on the checked fixture; this evidence does
**not** claim a current speedup or universal replacement.

Representative #2507 BRQ prototype evidence is in
`/tmp/2481_runtime_bench_20260606_165236` and summarized in
`vector-search-closeout-2483.md`. It shows lower-level BRQ `quantized_only` and
`quantized_rerank` rows with `0 B/op`, `0 allocs/op`, expected BRQ counters,
16 logical code B/vector, and about 74.47 quantized asset B/vector in its rebuild
shape. This is prototype evidence, not a promotion or crossover claim.

## Acceleration and future work boundaries

RaBitQ go-highway acceleration did not land for this stack. The #2453
investigation found that go-highway's fast bit-product kernel is not compatible
with TreeDB's exact weighted RaBitQ cosine scorer and durable LSB-first packed
asset contract without semantic changes or a more complex bit-plane/residual
scorer. The #2477 query-byte-table scorer is the promoted semantics-preserving
`rabitq_1bit` v1 performance update and is summarized in
[`rabitq-performance-lane-closeout-2482.md`](rabitq-performance-lane-closeout-2482.md);
it does not change storage, bit order, score formula, codec/asset identity, or fail-closed behavior. Do not report an accelerated RaBitQ row unless a future
issue lands one with parity tests, same-fixture benchmarks, and profiles.

Future quantization work should be separate from this scalar/RaBitQ/BRQ closeout.
BRQ keeps an explicit query `uint4` score label and must not be folded into
RaBitQ evidence.

- [`brq-1bit-v1.md`](brq-1bit-v1.md) is the selected #2480 codec contract and
  #2507 lower-level prototype for bit-product `brq_1bit` v1. Promotion beyond
  prototype status still needs future scale-sensitive same-host/crossover
  evidence, especially #2494.
- PQ/OPQ/residual-PQ codecs, multi-bit BRQ variants, and other packed popcount
  scorers still need their own codec specs, tests, recall sweeps, and benchmark
  gates.
- Batch scorer kernels, SIMD/popcount integration, graph control-flow changes,
  block-planner/windowing changes, and traversal scheduling changes are not part
  of #1926/#2454 acceptance. They must not be smuggled into quantized docs or
  used to reinterpret the current evidence.
- Incremental quantized maintenance and stable-format migrations remain future
  pre-alpha work; current benchmark directories should be rebuilt across branch
  changes.
