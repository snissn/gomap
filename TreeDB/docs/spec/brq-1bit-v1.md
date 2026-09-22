# TreeDB `brq_1bit` v1 Codec Contract (#2480/#2481)

Status: pre-alpha **selected codec contract** for #2480 and lower-level runtime
prototype for #2481. The spec gate landed first; #2481 subsequently added oracle
goldens plus lower-level durable asset/search support. This document still does
not claim a throughput win or production promotion for BRQ.

## Decision

Issue #2480 selected `brq_1bit` version `1` as the single codec candidate for
#2481, and #2481 landed the first lower-level prototype. `brq_1bit` is a
TreeDB-owned bit-product binary quantizer: it
keeps one durable sign bit per rotated data dimension, makes query-weight
quantization explicit, and labels returned quantized scores as approximate
`brq_1bit` estimates. It exists because the #2453 go-highway probe showed that a
fast bit-product scorer is a different storage/scoring contract from
[`rabitq_1bit` v1](rabitq-1bit-v1.md).

This contract MUST NOT reinterpret or mutate `rabitq_1bit` v1. Existing
`rabitq_1bit` assets keep their codec name/version/config bytes, LSB-first
layout, weighted sign-dot score formula, side arrays, and fail-closed behavior.
Any `brq_1bit` implementation must use the new identity below.

## License survey and implementation boundary

- TreeDB's `brq_1bit` oracle, config bytes, storage schema, and scoring formula
  are clean-room TreeDB design/spec text.
- `github.com/ajroetker/go-highway` v0.0.12 was evaluated for #2453. Its module
  and RaBitQ package are Apache-2.0 and may be used only as an optional kernel
  dependency with normal notices if future BRQ acceleration proves compatibility
  and value.
- The upstream go-highway `QuantizeVectors` MSB-first word output is not TreeDB
  durable storage. Any future accelerated implementation must either write the
  TreeDB layout directly or transcode before persisting assets.
- Do not copy code from CockroachDB Software License, ELv2, AGPL, Antfly, or
  incompatible C++ RaBitQ/BRQ libraries. Papers may be cited for algorithmic
  background, but production code must be clean-room or from compatible
  Apache-2.0/BSD-3 sources with notices.

## Identity

| Field | v1 value |
| --- | --- |
| Codec name | `brq_1bit` |
| Codec version | `1` |
| First metric/scope | `cosine` over `column_graph` float32 vectors |
| Normalization | unit-L2 normalized data/query vectors |
| Rotation | `signed_permutation_fwht_padded_v1` |
| Default seed | `0x6272713162697401` |
| Data code width | `CodeWidthBits=1` |
| Query weight width | `QueryWeightBits=4` unsigned runtime-only bit planes |
| Code dimensions | `CodeDimensions=next_power_of_two(VectorDimensions)` |
| Durable data-code layout | `packed_bit_vector` / `raw_packed_bit_vector`, LSB0 |
| Score label | `brq_1bit_estimated_cosine_q4` |

The default seed differs from `rabitq_1bit` so assets cannot accidentally share
config identity. Reusing the same rotation algorithm is allowed; sharing the
`rabitq_1bit` codec name/version is not.

## Canonical config bytes and hash

`brq_1bit` v1 must define a small oracle package before assets/search land. Its
`Config.CanonicalBytes` format is line-oriented ASCII and must be exactly:

```text
codec=brq_1bit
version=1
metric=cosine
normalization=unit_l2
rotation=signed_permutation_fwht_padded_v1
seed=0x6272713162697401
storage_role=packed_codes
storage_logical_type=packed_bit_vector
storage_encoding=raw_packed_bit_vector
bit_order=lsb0
word_order=little_endian_uint64
padding=zero
code_width_bits=1
query_weight_bits=4
query_weight_quantizer=max_abs_uint4_round_half_up
score=brq_1bit_estimated_cosine_q4
data_scale_side_array=quantized_dot_product_inv
```

`Config.Hash64` is FNV-1a over these bytes and is recorded in
`quantizedasset.CodecDescriptor.ConfigHash`; the full canonical bytes are
recorded in `CodecDescriptor.Config`. Vector dimensions and row counts remain
schema/base-graph identity fields, not config bytes.

## Durable asset schema

Each declared `QuantizedVectorIndexDefinition{Codec:"brq_1bit", Version:1}`
for a cosine `column_graph` vector index writes one vector-index-state asset in
graph ordinal order.

| State field | v1 value |
| --- | --- |
| Vector-index-state role | `quantized_codes` |
| Asset id | `quantized/<name>/brq_1bit/packed_codes` |
| State logical type / encoding | `packed_bit_vector` / `raw_packed_bit_vector` |
| Ordinal order | `vector_ordinal` (`row i == graph ordinal i`) |

The typed-column part contains:

| Role | Column | Type / encoding | Shape | Required validation |
| --- | --- | --- | --- | --- |
| `packed_codes` | `packed_codes` | `packed_bit_vector` / `raw_packed_bit_vector` | `Rows=graph.RowCount`, `ElementsPerRow=CodeDimensions`, `BitsPerElement=1` | row bytes `ceil(CodeDimensions/8)`, zero padding |
| `code_count` | `code_count` | `uint32` / `raw_uint32` | one scalar per graph ordinal | equals popcount over logical code bits |
| `quantized_dot_product_inv` | `quantized_dot_product_inv` | `float32` / `raw_float32` | one scalar per graph ordinal | finite and in `[1/sqrt(CodeDimensions), 1]` with oracle tolerance |

`brq_1bit` has no durable query arrays. Query sign bits, positive/negative
query-weight bit planes, and query-weight scale are built per query and may be
cached only within caller-owned search buffers.

## Bit and word order

Durable `packed_codes` rows use the existing TreeDB packed-code layout:

- logical bit `i` is byte `i/8`, bit `i%8` (LSB0 within each byte);
- unused high bits in the final byte are zero and fail validation if set;
- little-endian `uint64` word views map logical bit `i` to word `i/64`, bit
  `i%64`; partial final words are zero-filled in scratch only;
- implementations may use SIMD/popcount kernels over word views, but must not
  persist MSB-first words or rely on host-endian reinterpretation without the
  typed-column direct-view/scratch safety checks.

## Reference data encoding

For a finite non-zero vector `x` with `VectorDimensions=d`:

1. Normalize `x` to unit L2.
2. Pad to `m = CodeDimensions` with zeros.
3. Apply the deterministic signed permutation and normalized FWHT rotation named
   in the config, using the `brq_1bit` seed.
4. Store one bit per rotated component: `1` for `value >= 0`, `0` for
   `value < 0`.
5. Emit `code_count = popcount(packed_codes)`.
6. Emit `quantized_dot_product_inv = 1 / sum(abs(rotated_data[i]))`.

Zero vectors, non-finite values, dimension mismatches, invalid padding,
`code_count` mismatches, and invalid side-array values fail closed in the oracle
and in asset prepare/search validation.

## Query encoding and score semantics

A query uses the same unit-L2 normalization and rotation. Let:

- `s_i = 1` when rotated query component `i` is non-negative, else `0`;
- `a_i = abs(rotated_query[i])`;
- `max_abs = max_i(a_i)`.

`max_abs` must be finite and positive. Runtime query-weight quantization is:

```text
raw_i = a_i * 15 / max_abs
w_i = clamp_uint4(floor(raw_i + 0.5))
query_weight_scale = max_abs / 15
query_weight_sum_int = sum_i w_i
```

Build eight runtime bit-plane masks over little-endian word views:

- `pos_q1`, `pos_q2`, `pos_q4`, `pos_q8`: bit `i` set when `s_i == 1` and the
  corresponding bit of `w_i` is set;
- `neg_q1`, `neg_q2`, `neg_q4`, `neg_q8`: bit `i` set when `s_i == 0` and the
  corresponding bit of `w_i` is set.

For one candidate data-code word row, the oracle bit-product is:

```text
bitproduct(code, q1, q2, q4, q8) =
    1*popcount(code & q1) + 2*popcount(code & q2) +
    4*popcount(code & q4) + 8*popcount(code & q8)

pos_set = bitproduct(code, pos_q1, pos_q2, pos_q4, pos_q8)
neg_set = bitproduct(code, neg_q1, neg_q2, neg_q4, neg_q8)
neg_weight_sum_int = sum_i (s_i == 0 ? w_i : 0)
match_weight_int = pos_set + (neg_weight_sum_int - neg_set)
signed_weight_int = 2*match_weight_int - query_weight_sum_int
score = signed_weight_int * query_weight_scale /
        (quantized_dot_product_inv * CodeDimensions)
```

`quantized_only` returns this approximate score and must label it
`brq_1bit_estimated_cosine_q4` in docs, debug output, and benchmark rows. It is
not exact cosine and is not the `rabitq_1bit` weighted sign-dot score.
`quantized_rerank` uses `brq_1bit` only for traversal/candidate collection and
returns exact cosine over the configured exact-rerank shortlist.

A future optimized kernel may fuse the two bit-products or use XNOR-style masks,
but oracle/golden tests must prove bit-for-bit agreement with the formula above
before search can use it.

## Fail-closed prepare/search validation plan

Follow-on PRs must fail closed before any scorer-shaped loop when any of these
checks fail:

- selected declaration has codec/version other than `brq_1bit`/`1`;
- metric is not cosine, dimensions are not positive, or `CodeDimensions` is not
  `next_power_of_two(VectorDimensions)`;
- codec canonical bytes or FNV-1a `ConfigHash` mismatch;
- base graph identity, generation/checksum/schema hash, row count, or ordinal
  order mismatch;
- asset id, role, typed-column logical type, physical encoding, section length,
  compression, direct-view certification, source schema hash, ref length, or
  checksum mismatch;
- packed-code padding is non-zero or `code_count` does not equal logical
  popcount;
- `quantized_dot_product_inv` is non-finite or outside the valid range;
- query vectors are zero, non-finite, dimension-mismatched, or cannot build valid
  query masks and `query_weight_scale`;
- prepared handles are stale/closed, missing, unsupported, or not allocation-safe
  for the selected hot path.

Failures return `ErrVectorIndexSearchUnavailable` through the existing
codec-generic quantized asset unavailable/invalid/stale/closed counters. There is
no silent exact fallback. Exact/default mode continues to reject quantized-only
fields.

## Required oracle and golden/runtime tests

Issue #2481 landed oracle/spec coverage before durable asset/search support, and
#2507 added lower-level runtime tests for the same contract. Required coverage
includes:

- canonical config bytes and `Config.Hash64` golden;
- rotation metadata, code dimensions, row bytes, and seed golden;
- encode golden for finite deterministic vectors, including LSB0 bytes,
  `code_count`, and `quantized_dot_product_inv`;
- query golden for sign bits, `uint4` weights, positive/negative q1/q2/q4/q8
  planes, `query_weight_scale`, and score label;
- score oracle golden comparing the slow formula to any bit-product or fused
  implementation;
- padding and word-order tests, including non-multiple-of-64 dimensions;
- zero vector, non-finite, dimension mismatch, config mismatch, stale/corrupt
  asset, missing side-array, bad code-count, invalid side-array, and closed-handle
  fail-closed tests;
- asset build/prepare/reopen tests and `quantized_only` exact-read-zero plus
  `quantized_rerank` exact-read-bound guardrails;
- zero steady-state allocation tests for warmed buffered rows.

## Recall, storage, and performance gates

`brq_1bit` is selected only as a prototype candidate. It may be closed
no-promote if the gates below are not met.

Required matrix versus exact FP32, `scalar_u8`, and `rabitq_1bit` for the
current lower-level prototype:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^(BenchmarkVectorIndexSearcherColumnGraphBRQQuantizedSearchWithBuffer2481|BenchmarkColumnGraphBRQQuantizedRebuildStorage2481|BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414|BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451|BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926|BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450)$' \
  -benchmem -benchtime=100000x -count=5
```

The #2507 artifact root is `/tmp/2481_runtime_bench_20260606_165236`; a full
collection-level BRQ promotion matrix remains future work. Rows must include
lower-level `quantized_only` plus `quantized_rerank/candidates=32` at c=1/c=8,
with the same fixture and same-host baseline/candidate discipline used by the
RaBitQ profile gate. Report `ns/op`,
`ops/sec`, `B/op`, `allocs/op`, recall@K versus exact, exact vector/norm bytes,
exact rerank calls, route/fallback/unavailable counters, `quantized_code_B/search`,
logical code bytes/vector, actual asset bytes/vector, graph total storage, and
CPU/alloc profiles for claimed c=1/c=8 wins.

Promotion gates:

- `quantized_only` must preserve `0 B/op`, `0 allocs/op`, zero exact vector/norm
  reads, zero document fetches, and zero fallback/unavailable counters on healthy
  prepared assets.
- `quantized_rerank` exact reads must be bounded by the configured shortlist and
  report exact cosine final scores.
- Recall must be reported against exact on every row; public support is blocked
  if recall is materially worse than same-fixture `scalar_u8` or `rabitq_1bit`
  without an explicit no-promote/experimental decision.
- Storage must remain in the one-bit class: logical code bytes/vector are
  `ceil(CodeDimensions/8)`, no durable query arrays are written, and actual
  asset bytes/vector should stay within the same order as `rabitq_1bit` unless a
  measured trade-off is accepted.
- A speedup claim requires same-host BRQ rows to beat optimized `rabitq_1bit` for
  both c=1 and c=8 rows in the relevant lower-level and collection serving
  shapes, with no scalar_u8/exact guardrail regressions where shared code is
  touched.
- Compactness-only or mixed/noisy/regressing evidence must be documented as
  no-promote and must not be presented as acceleration.

## Public counter and label requirements

Search stats/benchmarks keep the existing generic quantized counters and add
BRQ-specific visibility when this codec is selected:

- `quantized_score_codec_brq_1bit/search=1`;
- `brq_1bit_query_weight_bits/search=4`;
- `brq_1bit_bitproduct_passes/search` for the logical bit-product passes used
  per search;
- `brq_1bit_query_weight_scale/search` or equivalent debug/stat field for the
  query-local scale;
- existing `quantized_score_calls/search`, `quantized_code_B/search`,
  `quantized_asset_unavailable/search`, route counters, exact vector/norm bytes,
  exact rerank calls, and document-fetch counters.

Benchmark rows, issue comments, and docs must label `quantized_only` returned
scores as `brq_1bit_estimated_cosine_q4`. `quantized_rerank` rows must state that
final scores are exact cosine over the BRQ shortlist.

## Migration and compatibility

TreeDB is pre-alpha. `brq_1bit` introduces a new codec identity, config hash,
asset id, and schema hash, so no migration from `rabitq_1bit` or `scalar_u8` is
required. Old DB directories without matching assets should fail closed or be
rebuilt for experiments. New binaries may reject stale/mismatched assets rather
than attempting complex migration scaffolding.

## #2481 dependency status

The `brq_1bit` v1 contract unblocked #2481. #2481 completed in two steps: PR
#2489 added internal oracle/golden coverage, and PR #2507 added lower-level BRQ
quantized asset/search runtime with fail-closed validation, exact-read guardrails,
zero-allocation warmed buffered rows, and benchmark evidence. The implementation
must not change `rabitq_1bit` v1 semantics, and broader promotion/crossover
claims remain future work.
