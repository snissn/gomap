# TreeDB `rabitq_1bit` v1 Codec Contract (#2449)

Status: pre-alpha design/contract gate, pure-Go reference oracle, and durable
asset storage contract. The #2450 asset path publishes and prepares
`rabitq_1bit` typed-column assets; search/scoring, SIMD backends, and exact FP32
/ `scalar_u8` behavior changes remain out of scope.

## Identity

The TreeDB codec identity is:

| Field | v1 value |
| --- | --- |
| Codec name | `rabitq_1bit` |
| Codec version | `1` |
| First metric | `cosine` over `column_graph` float32 vectors |
| Normalization | unit-L2 normalized data/query vectors |
| Rotation | `signed_permutation_fwht_padded_v1` |
| Default seed | `0x7261626974710001` |
| Code width | `CodeWidthBits=1` |

`TreeDB/internal/rabitq.Config.CanonicalBytes` is the canonical serialized config
identity. It records the codec name/version, cosine/unit-L2 scope, rotation name,
seed, storage role/type/encoding, LSB0 bit order, zero padding, and code width.
`Config.Hash64` is FNV-1a over those bytes for future
`quantizedasset.CodecDescriptor.ConfigHash` use. Vector dimensions and row counts
remain schema identity fields rather than config bytes.

## Storage shape decision

V1 stores data codes as `quantizedasset.RolePackedCodes` (`packed_codes`) backed
by typed-column `packed_bit_vector` / `raw_packed_bit_vector` rows. In
vector-index state, each declared RaBitQ score plane is one `quantized_codes`
asset with id `quantized/<name>/packed_codes`; the asset's primary state
logical type/encoding are `packed_bit_vector` / `raw_packed_bit_vector`.

- `VectorDimensions` is the authoritative float32 source dimensionality.
- `CodeDimensions = next_power_of_two(VectorDimensions)` for the padded FWHT
  rotation length.
- `CodeWidthBits = 1`.
- Physical bytes per row are `ceil(CodeDimensions / 8)`.
- Logical bit `i` is stored LSB-first at byte `i/8`, bit `i%8`.
- Unused high bits in the final byte are zero and are validation failures if set.

This chooses the semantic TreeDB packed-code layout instead of a `uint64_vector`
word layout. Future go-highway/SIMD work may adapt the same bytes through
little-endian word views; it must not reinterpret the durable bit order.

## Reference encoding

For a finite non-zero vector `x` with `VectorDimensions=d`:

1. Normalize `x` to unit L2.
2. Pad to `m = CodeDimensions` with zeros.
3. Apply the deterministic v1 signed permutation derived from `(seed, d, m)`.
4. Apply a normalized Walsh-Hadamard transform (`1/sqrt(m)` scale).
5. Store one bit per rotated component: bit `1` for `value >= 0`, bit `0` for
   `value < 0`.
6. Emit side arrays:
   - `code_count`: number of set logical code bits (`uint32`).
   - `quantized_dot_product_inv`: `1 / sum(abs(rotated_data[i]))` (`float32`).
     Because the rotated data vector has unit L2 norm, valid values are in the
     range `[1/sqrt(CodeDimensions), 1]` modulo float32 rounding tolerance.

Zero vectors, non-finite values, dimension mismatches, invalid padding,
side-array mismatches, and out-of-range `quantized_dot_product_inv` values fail
closed in the reference APIs and in TreeDB asset prepare validation.

The deterministic rotation is fully specified by the reference code and these
constants so future non-Go or accelerated implementations can reproduce v1:

```text
rotation_seed = seed
  ^ 0x8f3f73b5d8f24429
  ^ uint64(d) * 0x9e3779b97f4a7c15
  ^ rotl64(uint64(m) * 0xbf58476d1ce4e5b9, 17)

splitmix64_next:
  state += 0x9e3779b97f4a7c15
  z = state
  z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
  z = (z ^ (z >> 27)) * 0x94d049bb133111eb
  return z ^ (z >> 31)

permutation:
  perm[i] = i for i in [0, m)
  for i = m-1 down to 1:
    j = splitmix64_next() mod (i+1)
    swap perm[i], perm[j]

signs:
  after the permutation loop, for i = 0..m-1:
    sign[i] = +1 if splitmix64_next()&1 == 0 else -1

pre-FWHT vector:
  work[i] = padded_unit_vector[perm[i]] * sign[i]

FWHT:
  for width = 1, 2, 4, ... while width < m:
    for start = 0; start < m; start += 2*width:
      for i = 0; i < width; i++:
        a = work[start+i]
        b = work[start+i+width]
        work[start+i] = a + b
        work[start+i+width] = a - b
  work[i] *= 1/sqrt(m)
```

## Query encoding and score estimator

A query uses the same unit-L2 normalization and rotation. Query encoding emits:

- query sign bits in the same LSB-first packed layout;
- `AbsWeights[i] = abs(rotated_query[i])`;
- `WeightSum = sum(AbsWeights)`.

For one candidate code row, v1 computes the weighted sign dot product:

```text
weighted_sign_dot = sum_i AbsWeights[i] * (code_bit_i == query_sign_bit_i ? +1 : -1)
```

The reference estimated cosine is:

```text
score = weighted_sign_dot / (quantized_dot_product_inv * CodeDimensions)
```

The formula is intentionally simple and clean-room: it reconstructs the data
vector from its rotated one-bit sign code with the per-vector least-squares scale
`sum(abs(rotated_data)) / CodeDimensions`. It is the correctness oracle for v1
asset/scorer PRs; it is not a production speed claim.

## Pure-Go oracle APIs

`TreeDB/internal/rabitq` owns the v1 reference surface:

- `NewPlan(vectorDimensions, Config)` binds config to shape and deterministic
  rotation metadata.
- `Plan.CodeDimensions`, `Plan.BytesPerCode`, and `CodeWidthBits` expose durable
  shape decisions.
- `Plan.Encode` emits `EncodedVector{Code, CodeCount, QuantizedDotProductInv}`.
- `Plan.EncodeQuery` emits weighted-popcount query inputs.
- `Plan.ScoreCosine` / `Plan.ScoreEncoded` compute the pure-Go reference score.
- `Plan.ValidateCode` checks row width, padding, and `code_count`.
- `Plan.ValidateQuery` checks query shape, sign-bit padding, finite non-negative
  weights, and finite positive weight sums before scoring exported query inputs.
- `Plan.ValidateQuantizedDotProductInv` checks the side-array range implied by
  unit-L2 vectors and the orthonormal v1 rotation.

The package is allocation-disciplined when callers reuse `Workspace` and code
buffers, but it is a reference/oracle implementation. SIMD/go-highway backends
belong to #2453 and must prove parity with these APIs before being used by
search.

## Durable TreeDB asset shape (#2450)

For each declared `QuantizedVectorIndexDefinition{Codec:"rabitq_1bit", Version:1}`
on a cosine `column_graph` vector index, rebuild writes one typed-column part in
graph ordinal order. The part contains:

| Role | Column | Type / encoding | Shape |
| --- | --- | --- | --- |
| `packed_codes` | `packed_codes` | `packed_bit_vector` / `raw_packed_bit_vector` | `Rows=graph.RowCount`, `ElementsPerRow=CodeDimensions`, `BitsPerElement=1` |
| `code_count` | `code_count` | `uint32` / `raw_uint32` | one scalar per graph ordinal |
| `quantized_dot_product_inv` | `quantized_dot_product_inv` | `float32` / `raw_float32` | one scalar per graph ordinal |

Prepare requires all three roles, checks typed-column logical/physical type,
encoding, compression, direct-view certification, row count, section length,
asset ref/checksum, zero packed padding, code-count parity, positive finite
`quantized_dot_product_inv`, codec config bytes/hash, and base graph identity.
Missing, stale, corrupt, config-mismatched, schema-mismatched, or checksum-mismatched
assets fail closed before any future scorer can consume row readers.

## Non-goals and boundaries

- No collection/search scorer integration and no public route/counter changes.
- No changes to exact/default FP32 search or `scalar_u8` score-plane behavior.
- No multi-bit RaBitQ, PQ/OPQ, IVF, or graph topology changes.
- No dependency on CockroachDB, Antfly, AGPL, cgo, or go-highway code.

## Required evidence

The #2449 reference package tests cover:

- golden encode/query/score output for a fixed seed/config;
- LSB-first bit order and zero padding validation;
- zero/degenerate vector fail-closed behavior;
- malformed scorer side-input failures for query weights, query padding,
  `code_count`, and missing/out-of-range `quantized_dot_product_inv`;
- pure-Go scorer sanity versus exact cosine on deterministic fixtures;
- zero steady-state allocations after warmup for encode/query/score.

`BenchmarkReferenceScoreCosine2449` is an optional microbenchmark for the pure-Go
reference scorer only. It must not be cited as production RaBitQ search speed.
