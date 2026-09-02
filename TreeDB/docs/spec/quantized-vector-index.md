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
The #2584/#2588 prepared HNSW fast-path closeout for `scalar_u8` and
`rabitq_1bit` route promotion, exact FP32 guardrails, and 10k x 768 evidence is
in [`quantized-prepared-hnsw-closeout-2588.md`](quantized-prepared-hnsw-closeout-2588.md).
The #2845 per-granule-alpha scalar_u8 default gate is recorded in
[`scalar-u8-alpha-default-gate-2845.md`](scalar-u8-alpha-default-gate-2845.md);
it kept per-granule alpha explicit/opt-in and did not promote it as the default
for new `scalar_u8` declarations. The #2864 optimization graph starts from the
baseline/profile runbook in
[`scalar-u8-alpha-optimization-2865.md`](scalar-u8-alpha-optimization-2865.md).

## User-visible query modes

`VectorIndexDefinition.QuantizedIndexes` declares one or more named derived score
planes for a `column_graph` vector index. Current normalized declarations default
to `codec="scalar_u8"` and `version=1`; they also accept explicit
`codec="rabitq_1bit", version=1` and `codec="brq_1bit", version=1`
declarations.

`scalar_u8` declarations may also carry `scalar_u8_calibration`. Omitted config
preserves the legacy scalar_u8 v1 contract and remains the default for new
`scalar_u8` declarations after the #2845 no-promote gate. Explicit legacy config
is `{"mode":"legacy"}` and is behaviorally identical to omission. Persisted
collection metadata for these semantics uses collection metadata version `5`.
The explicit per-existing-granule alpha opt-in contract is:

```json
{
  "mode": "per_granule_alpha",
  "grouping": "storage_layout_granule",
  "alpha_policy": { "name": "max_abs" }
}
```

The only grouping source in this contract is `storage_layout_granule`: builders
must use existing disk-local storage/layout granules or row groups exposed by the
base column-graph/typed-column layout and must not choose a new vector-specific
granule size. The deterministic finite alpha policies are `max_abs` with no
parameters, and `abs_quantile` with `quantile_ppm=999000` (0.999) encoded as an
integer parameter. `scalar_u8_calibration` is invalid on non-`scalar_u8` codecs;
legacy mode rejects grouping and alpha policy fields.

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
mode and may be reported alongside a codec-specific physical route: `scalar_u8`
currently reports the `column_graph` prepared physical route, while
`rabitq_1bit` reports `search_route_hnsw_search_pack/search=1` when its score
plane uses prepared `hnsw_search_pack_v1` traversal. This RaBitQ pack traversal
is still a quantized route and must not be relabeled as exact FP32 search.
No-document buffered serving is available through both lower-level
`VectorIndexSearcher.SearchWithBuffer` and collection-level
`Collection.SearchVectorIndexWithBuffer` when `QueryMode` and
`QuantizedIndexName` select an explicit quantized mode; document materialization,
projections, filters, and benchmark-debug stats remain outside this buffered
collection route.

## Durable asset model

Each declared legacy scalar_u8 score plane rebuilds one TVIS vector-index-state
asset with role `quantized_codes`, asset id `quantized/<name>/codes`, logical
type `byte_vector`, and physical encoding `raw_fixed_bytes`. Rows are in graph
ordinal order and bind to the base graph identity (index, field, metric,
dimensions, row count, base manifest identity, graph schema hash, codec
name/version/config, and asset ref/checksum identity). For cosine, persisted
legacy scalar_u8 codes are built from inverse-norm-normalized vector components
so equivalent directions encode to the same row.

Existing legacy scalar_u8 declarations with omitted calibration config keep the
legacy empty codec config identity: `quantizedasset.CodecDescriptor.Config` is
empty and `CodecDescriptor.ConfigHash` is zero. Any non-legacy
`scalar_u8_calibration` is encoded into `CodecDescriptor.Config` canonical bytes
and `ConfigHash`, and also participates in vector-index-state asset identity
(asset id/path/schema hash) so assets built under one calibration policy cannot
be reused under another.

For `mode="per_granule_alpha"`, rebuild publishes two TVIS assets for the named
score plane:

- `quantized_codes`: asset id `quantized/<name>/scalar_u8/<config-hash>/codes`,
  logical type `byte_vector`, physical encoding `raw_fixed_bytes`, with dense
  u8 rows encoded by dividing each normalized component by that row's granule
  alpha before applying the legacy scalar_u8 byte map.
- `quantized_alpha`: asset id `quantized/<name>/scalar_u8/<config-hash>/alpha`,
  logical type `scalar_u8_alpha`, physical encoding `raw_float32_uint32`, backed
  by a typed-column part with one row per storage-layout granule. It stores
  `scalar_u8_alpha` (`float32` / `raw_float32`) and `granule_row_count`
  (`uint32` / `raw_uint32`). Row counts are cumulative in graph-ordinal order and
  must sum to the code row count.

The alpha grouping source is the existing typed-column storage-layout granule
policy used by the scalar_u8 quantized code part; the mode does not define or
promote a new vector-specific granule size. Prepare validates code row count,
alpha granule count, config hash/bytes, base graph identity, schema/ref/checksum
identity, finite positive alpha values, and row-count sums before exposing code
rows plus alpha lookup state.

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
asset, row count, dimensions, metric, source schema, base graph identity, codec
config identity, asset ref identity, and typed-column layout before
traversal/scoring. Missing, stale, corrupt, mismatched, unsupported, closed, or
unprepared assets return `ErrVectorIndexSearchUnavailable`; they must not fall
back to exact traversal or document reconstruction. Downstream alpha asset
builders must fail closed when the selected scalar_u8 calibration contract is
missing or mismatched. Per-granule-alpha scalar_u8 additionally requires the
matching `quantized_alpha` asset and rejects missing, stale, mismatched,
non-positive, NaN, or infinite alpha metadata during prepared open. Fail-closed stats use codec-generic counters such as
`quantized_asset_missing`, `quantized_asset_invalid`, `quantized_asset_stale`,
`quantized_asset_closed`, and `quantized_asset_unavailable`. Exact mode rejects
quantized-mode fields so callers do not accidentally depend on no-op options.

Legacy `scalar_u8` v1 `quantized_only` consumes the prepared `codes` reader and
scores normalized query/candidate code rows. The per-granule-alpha scalar_u8 mode
requires the matching alpha lookup state, encodes the query with the selected
query alpha policy, and reports approximate scores as
`alpha_q * alpha_g * centered_dot(query_code, row_code) / (255*255)`. The
query-alpha factor is constant for a query, but returned `quantized_only` scores
include it; candidate comparisons include each row's existing-granule alpha and
preserve lower-ordinal tie behavior for equal adjusted scores. `rabitq_1bit` v1 `quantized_only` consumes
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
  -bench '^(BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926|BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414|BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedAlphaSearchWithBuffer2414|BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451|BenchmarkVectorIndexSearcherColumnGraphBRQQuantizedSearchWithBuffer2481|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452|BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926|BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450|BenchmarkColumnGraphBRQQuantizedRebuildStorage2481)$' \
  -benchmem -benchtime=100x -count=3
```

`BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926` emits exact and scalar_u8
per-query rows; `BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414`
emits explicit lower-level buffered legacy scalar_u8 rows;
`BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedAlphaSearchWithBuffer2414`
emits explicit lower-level buffered per-granule-alpha scalar_u8 rows;
`BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451`
emits explicit lower-level buffered RaBitQ rows;
`BenchmarkVectorIndexSearcherColumnGraphBRQQuantizedSearchWithBuffer2481`
emits prototype lower-level buffered BRQ rows;
`BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415`
emits collection-level buffered legacy scalar_u8 rows;
`BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415`
emits collection-level buffered per-granule-alpha scalar_u8 rows;
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
`vector_B/search`, `norm_B/search`, `quantized_score_codec_scalar_u8_alpha/search`,
logical code bytes/vector, and actual `quantized_asset_B/vector`. The rebuild
rows report build throughput, allocation cost, total graph/vector-index-state
storage, quantized asset bytes, quantized asset bytes/vector, and for
per-granule-alpha scalar_u8 definitions the alpha metadata asset bytes,
alpha min/mean/max, and code-boundary rate separately from dense u8 code bytes.

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

The #2845 per-granule-alpha scalar_u8 gate recorded a latest-main count=10 10k x 768
matrix in `scalar-u8-alpha-default-gate-2845.md`. Alpha improved the production
gate collection recall from 81.25% to 100% on that fixture and kept hot rows at
`0 B/op`, `0 allocs/op`, but collection buffered runtime regressed on several
rows. Therefore omitted `scalar_u8_calibration` remains legacy, and
per-granule-alpha remains explicit opt-in.

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
