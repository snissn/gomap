# Quantized Asset Schema and Ordinal Access Contract (#1932)

Status: pre-alpha typed-column contract for quantized vector-index assets. This
owns schema validation and prepared ordinal readers. The scalar `scalar_u8` v1
`column_graph` score-plane behavior, query modes, exact rerank semantics, and
benchmark closeout are documented in `quantized-vector-index.md`.

## Role schema

A quantized asset manifest maps logical roles to typed-column columns. Supported
roles are:

- code rows: `codes`, `packed_codes`;
- scalar float32 side arrays: `norm`, `step`, `lower`, `code_sum`, `norm2`,
  `centroid_distance`, `quantized_dot_product_inv`, `centroid_dot_product`;
- integer side arrays: `code_count`, `centroid_id`, `list_id`.

Roles use generic typed-column storage. `codes` admits `byte_vector` /
`fixed_bytes` or unsigned dense numeric vector rows (`uint8_vector`,
`uint16_vector`, `uint32_vector`, `uint64_vector`). `packed_codes` admits
`packed_bit_vector`, `packed_uint2_vector`, or `packed_uint4_vector` with zero
padding already certified by typed-column layout validation. Float side arrays are
non-null `float32` / `raw_float32`; integer side arrays are non-null `uint32` /
`raw_uint32` or `uint64` / `raw_uint64` where a wider identifier is required.

The `rabitq_1bit` v1 contract in `rabitq-1bit-v1.md` chooses
`RolePackedCodes` / `packed_bit_vector` for one-bit code rows, with
`CodeDimensions=next_power_of_two(VectorDimensions)`, `CodeWidthBits=1`,
TreeDB LSB-first bit order, and zero high-bit padding. Its required side arrays
are `code_count` (`uint32`) and `quantized_dot_product_inv` (`float32`). The
`brq_1bit` v1 contract/prototype in `brq-1bit-v1.md` deliberately uses a new
codec identity and asset id while reusing the same one-bit data-code roles plus
explicit runtime query `uint4` bit-product score semantics.

## Query-mode guardrail

Collection vector-index metadata can declare named `scalar_u8` v1,
`rabitq_1bit` v1, and prototype `brq_1bit` v1 quantized score planes under
`VectorIndexDefinition.QuantizedIndexes`. Public search options expose explicit
`exact`, `quantized_only`, and `quantized_rerank` query modes. The zero/default
mode remains exact. As of #2451/#2452, `rabitq_1bit` assets are built,
persisted, prepared/validated, and consumed by the pure-Go RaBitQ scorer in
lower-level and collection-level buffered no-document search. As of #2507,
`brq_1bit` assets and lower-level `SearchWithBuffer` scoring are available as a
prototype route with separate counters.

The #1926 scalar lifecycle builds and loads `scalar_u8` v1 `codes` assets for
declared `column_graph` quantized indexes. For the current cosine
`column_graph` metric, scalar_u8 codes are encoded from inverse-norm-normalized
vector components so equivalent directions persist the same score-plane rows.
Rebuild emits one dense fixed-byte typed-column part per declared scalar
quantized index in graph ordinal order, records it in vector-index state as
`quantized_codes` with asset id `quantized/<name>/codes`, and validates the
prepared asset against the base graph generation/checksum/schema identity on
open.

The #2450 RaBitQ asset lifecycle builds one typed-column part per declared
`rabitq_1bit` v1 score plane, also in graph ordinal order. The vector-index-state
asset role remains `quantized_codes`, but the asset id is
`quantized/<name>/packed_codes`, the state logical type/encoding are
`packed_bit_vector` / `raw_packed_bit_vector`, and the part contains three
required role columns: `packed_codes`, `code_count`, and
`quantized_dot_product_inv`. #2507 uses the same role columns for `brq_1bit` but
with asset id `quantized/<name>/brq_1bit/packed_codes` and BRQ config identity.
The codec descriptor records canonical config bytes plus FNV-1a config hash; the
generated typed-column schema hash includes that config hash so
stale/config-mismatched assets fail closed before prepare.

Quantized modes validate the selected name/options and asset load status before
graph traversal/scoring. The `scalar_u8` v1 `quantized_only` scorer consumes the
prepared `codes` reader and scores normalized query/candidate code rows. The
`rabitq_1bit` v1 `quantized_only` scorer consumes the prepared `packed_codes`,
`code_count`, and `quantized_dot_product_inv` readers and scores with the
weighted sign-dot estimator specified in `rabitq-1bit-v1.md`.
`quantized_rerank` uses the selected codec scorer for graph traversal/candidate
collection, then exact scores only the resulting quantized shortlist by graph
ordinal through the authoritative `float32_vector`/inverse-norm score path and
returns final topK in exact cosine-score order.
`QuantizedRerankCandidates` bounds that shortlist; zero uses the normalized
`ef_search` candidate set, and non-zero values below `TopK` are rejected.
Quantized modes must not silently return exact results when selected assets are
missing, stale, mismatched, or unprepared. Exact mode rejects quantized-only
fields so future callers do not accidentally rely on no-op options.

## Fail-closed validation

`TreeDB/internal/quantizedasset` prepares immutable readers from typed-column part
images only after validating:

- required roles and referenced columns are present exactly once;
- typed-column logical type, physical type, encoding, compression, null/default
  wrappers, direct-view certification, section identity, row count, and payload
  length;
- vector dimensions, code dimensions, and code width;
- metric, codec name/version/config identity;
- graph ordinal order (`vector_ordinal`) and base graph identity (index, field,
  metric, dimensions, row count, base manifest generation/checksum/schema hash,
  and graph schema hash);
- persisted asset refs and checksums when supplied.

Mismatches fail before any scorer-shaped loop receives a prepared reader. There is no silent fallback to exact search or document reconstruction before a
quantized mode has validated and used its selected score-plane asset. In
`quantized_rerank`, exact scoring is limited to the validated quantized candidate
set and is reported through search stats (`quantized_score_calls` for traversal
and `quantized_rerank_exact_score_calls` for rerank).

## Prepared ordinal API

`Prepared` is immutable and safe for concurrent read-only access. Returned code
row byte slices alias the prepared typed-column image and are valid for that
prepared object's lifetime. Caller scratch must not be shared concurrently.

Hot APIs include:

- `CodeRowBytes(role, ordinal)` for fixed-byte, packed-code, and dense unsigned
  code rows;
- `Float32`, `Uint32`, and `Uint64` for scalar side arrays;
- `RowWords(role, ordinal, scratch)` when callers need little-endian uint64 word
  views over row bytes;
- `PackedElements(role, ordinal, scratch)` for unpacked packed-code elements;
- `DenseUint32Row(role, ordinal, scratch)` for decoded `uint32_vector` rows.

Steady-state row/metadata lookup is allocation-free when direct row bytes are
used or when adequate caller scratch is provided for decoded/word views.

## Footprint evidence

Prepare captures whole-asset bytes/vector and per-role section bytes/vector in
`Footprint`. Benchmarks in `TreeDB/internal/quantizedasset` report prepared open
cost, random ordinal lookup, scorer-shaped loops, `B/op`, `allocs/op`, and
representative asset/column bytes per vector. The end-to-end #1926 collection
harnesses `BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926` and
`BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926` report exact vs
`quantized_only` vs `quantized_rerank` latency/allocation/recall counters plus
actual scalar_u8 code-asset bytes per vector. The #2454 closeout adds RaBitQ
pure-Go lower-level and collection buffered rows plus
`BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450`, reporting RaBitQ
logical code bytes/vector, actual asset bytes/vector, exact-read counters, route
counters, and profile artifacts for c=1/c=8 collection rows.
