# Column Vector Graph Deep1B Benchmarks

This track runs the persisted-column `ColumnVectorGraph.SearchCosine` path on
Yandex Deep1B vectors without committing the dataset to the repository.

Data source:

- Overview: https://research.yandex.com/blog/benchmarks-for-billion-scale-similarity-search
- Deep1B 10M base subset: https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.10M.fbin
- Deep1B public queries: https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/query.public.10K.fbin
- JZIP reference implementation: https://github.com/jina-ai/jzip-compressor

Yandex stores embeddings in `.fbin` format:

```text
num_vectors uint32
vector_dim  uint32
vector data float32[num_vectors][vector_dim]
```

Deep1B vectors are 96-dimensional float32 embeddings. The benchmark uses
`SearchCosine` and persists `embedding_inv_norm`, so the product path does not
depend on the source rows being exactly unit-normalized. The current benchmark
is throughput, disk, and local-ranking evidence rather than recall evidence
because this track builds its own graph/neighborhood probes from the public
base/query files.

## Quick Run

The helper script defaults to the 1M shape, downloads the missing files into
`$HOME/.cache/gomap/deep1b`, and runs `none` plus `zstd` compression variants:

```sh
scripts/bench_column_vector_deep1b.sh
```

The script sets `go test -timeout` explicitly via `GO_TEST_TIMEOUT`, defaulting
to `60m`, so opt-in 10M/download runs do not inherit Go's 10-minute package
timeout.

To choose a different cache directory:

```sh
COLUMN_VECTOR_DEEP1B_DIR=/data/deep1b scripts/bench_column_vector_deep1b.sh
```

To run the 10M shape, which requires the full 3.84 GB `base.10M.fbin` file:

```sh
RUN_10M=true scripts/bench_column_vector_deep1b.sh
```

To include explicit build/open/decode benchmark timing in addition to search:

```sh
RUN_BUILD_OPEN_DECODE=true scripts/bench_column_vector_deep1b.sh
```

To run all compression variants:

```sh
COLUMN_VECTOR_DEEP1B_COMPRESSIONS=all scripts/bench_column_vector_deep1b.sh
```

To test whether the persisted graph adjacency-list column is compressed, set
adjacency compression separately from vector compression:

```sh
COLUMN_VECTOR_DEEP1B_COMPRESSIONS=none \
COLUMN_VECTOR_DEEP1B_ADJACENCY_COMPRESSIONS=none,zstd \
scripts/bench_column_vector_deep1b.sh
```

This changes the compression sub-benchmark label to
`vec_<vector>_adj_<neighbors>`. When the adjacency env var is unset, benchmark
names stay compatible with the original `none`, `snappy`, `lz4`, and `zstd`
vector-compression labels, and the `neighbors` column remains uncompressed.

To run the off-band neighborhood-local vector compression smoke test:

```sh
RUN_NEIGHBORHOOD_SMOKE=true \
COLUMN_VECTOR_DEEP1B_COMPRESSIONS=none,zstd \
scripts/bench_column_vector_deep1b.sh
```

That smoke test exact-scans the 1M Deep1B prefix for the top
`COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS` query neighbors, packs those vectors
into one raw float32 granule, and compares compression against a source-order
prefix granule. It does not change the persisted engine layout.

The same smoke run also includes
`BenchmarkColumnVectorGraphDeep1BJZIPNeighborhoodCompressionSmoke`, an off-band
geometric codec matrix. It transforms one 8192-vector block into transposed and
byte-shuffled float32 streams before applying byte codecs. Defaults:

- `COLUMN_VECTOR_DEEP1B_JZIP_TRANSFORMS=all`: cartesian raw row-major,
  cartesian transpose-only, cartesian transpose plus byte-shuffle, spherical
  angles, spherical center-delta, spherical previous-row delta, and
  Householder-centered cartesian.
- `COLUMN_VECTOR_DEEP1B_JZIP_CODECS=all`: raw, snappy, lz4, zstd-fast,
  zstd-default, and zstd-better.

`BenchmarkColumnVectorGraphDeep1BJZIPDecodeAndScoreSmoke` uses the same
nearest-ranked block, keeps encode/setup outside the timed loop, warms decoder
scratch, and times `decode 8192 vectors + score all 8192 candidates against one
query`. The `resident_fp32` row is the no-decode scoring ceiling. The benchmark
also reports a stage split for decoded paths: byte decompression, float32
layout/unshuffle, delta restore, Cartesian reconstruction, and final scoring.

`BenchmarkColumnVectorGraphDeep1BSphericalDirectScore` scores directly from
the stored spherical angle-major byte stream, without reconstructing a
Cartesian fp32 block. It compares exact `math.Sincos` scoring against a
half-pi-centered polynomial approximation with fallback to exact math outside
the configured local interval. The zstd rows include byte decompression plus
direct scoring; the raw row measures the stored angle-major stream without
byte-codec decompression. It also reports scalar profile probes for angle-byte
loads, trig scans, and full scoring, plus an optimistic fast-kernel estimate
anchored to the resident fp32 dot-product baseline.

`BenchmarkColumnVectorGraphDeep1BLocalFrameApproxScore` uses the same
nearest-ranked block, computes a block-centroid Householder frame, transforms
the query once into that frame, and benchmarks progressive local-frame scoring
sketches. The sketch variants are explicit storage representations:
`fp32`, IEEE-fp16-quantized values, and symmetric int8-quantized values with
per-dimension scales. It reports top-10 overlap/recall, score error, sketch
bytes, and a bounded-prune smoke path.

`BenchmarkColumnVectorGraphDeep1BGranuleNativeScore` is the first
storage-native scoring probe for the target granule design. It computes the
same Householder local frame, stores vectors as residuals to the local center
in transposed coordinate-major streams, transforms the query once per granule,
and scores directly from the stored representation. The initial layouts are
fp32 byte-shuffled residual columns and int8 residual columns with
per-coordinate scales. It reports scalar Go decode/score time, fast-kernel
estimates, bytes, top-k quality, and Cauchy-bound prune metrics.

`BenchmarkColumnVectorGraphDeep1BGranuleNativeMicroKernels` breaks down the
int8 residual scorer. It compares the current fused scalar scorer against
accumulation-only and finalization-only loops, `github.com/kelindar/simd`
int8 byte-column primitives, and `github.com/axiomhq/simd-go` int16/fp32
row-major dot loops. The third-party rows are implementation-shape probes:
they measure SIMD headroom, not a drop-in scorer for the transposed int8
residual layout.

## Manual Commands

Search-only benchmark:

```sh
COLUMN_VECTOR_DEEP1B=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BPersistedSearchCosine/1m/(none|zstd)/(serial|parallel)$' \
  -benchmem \
  -benchtime 500ms \
  -count 1
```

Build/open/decode benchmark:

```sh
COLUMN_VECTOR_DEEP1B_BUILD_OPEN_DECODE=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BPersistedBuildOpenDecode/1m/(none|zstd)$' \
  -benchmem \
  -benchtime 1x \
  -count 1
```

10M shape:

```sh
COLUMN_VECTOR_DEEP1B=1 \
COLUMN_VECTOR_DEEP1B_10M=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BPersistedSearchCosine/10m/(none|zstd)/(serial|parallel)$' \
  -benchmem \
  -benchtime 500ms \
  -count 1
```

Adjacency compression comparison:

```sh
COLUMN_VECTOR_DEEP1B=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_COMPRESSIONS=none \
COLUMN_VECTOR_DEEP1B_ADJACENCY_COMPRESSIONS=none,zstd \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BPersistedSearchCosine/1m/vec_none_adj_(none|zstd)/(serial|parallel)$' \
  -benchmem \
  -benchtime 500ms \
  -count 1
```

Neighborhood-local compression smoke:

```sh
COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BNeighborhoodCompressionSmoke/(source_prefix|nearest_ranked|nearest_ordinal)/(none|zstd)$' \
  -benchmem \
  -benchtime 200ms \
  -count 1
```

JZIP/geometric codec smoke:

```sh
COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BJZIPNeighborhoodCompressionSmoke/nearest_ranked/(cartesian_raw|cartesian_transpose|cartesian_byte_shuffle|spherical|spherical_center_delta|spherical_prev_delta|householder_cartesian)/(raw|snappy|lz4|zstd_fast|zstd_default|zstd_better)$' \
  -benchmem \
  -benchtime 200ms \
  -count 1
```

## Scope

- The 1M benchmark range-downloads the first 1M rows from `base.10M.fbin` and
  rewrites the local cached prefix header to `num_vectors=1_000_000`.
- The 10M benchmark uses the full Yandex `base.10M.fbin` file.
- Search setup, fbin source reads, column build, manifest reopen, and column
  decode are outside the timed search loop.
- Search uses one shared immutable `ColumnVectorGraph` plus one warmed
  `ColumnVectorGraphSearchScratch` per parallel worker.
- The benchmark does not fetch full documents.
- Adjacency is a deterministic degree-16 local ring over row ordinals, because
  the public Deep1B assets provide vectors and queries, not graph edges.

## Local 1M Evidence

Representative local run on 2026-05-18, Apple M3:

```sh
COLUMN_VECTOR_DEEP1B=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_COMPRESSIONS=none,zstd \
COLUMN_VECTOR_DEEP1B_ADJACENCY_COMPRESSIONS=none,zstd \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BPersistedSearchCosine/1m/vec_(none|zstd)_adj_(none|zstd)/(serial|parallel)$' \
  -benchmem \
  -benchtime 500ms \
  -count 3
```

Median search throughput from the run:

| Vector compression | Adjacency compression | Settled disk B/entry | Vector B/entry | InvNorm B/entry | Adjacency B/entry | Serial searches/s | Parallel searches/s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| none | none | 496.1 | 384.0 | 4.0000 | 68.00 | 58,697 | 289,348 |
| none | zstd | 448.9 | 384.0 | 4.0000 | 20.81 | 60,503 | 290,297 |
| zstd | none | 492.4 | 384.0 | 0.3556 | 68.00 | 58,892 | 267,466 |
| zstd | zstd | 445.2 | 384.0 | 0.3556 | 20.81 | 59,777 | 276,487 |

Interpretation:

- The graph adjacency-list column is compressed when requested:
  `actual_neighbors_zstd_blocks=130`, with adjacency storage dropping from
  `68.00` to `20.81 B/entry`.
- Raw Deep1B float32 vectors are not compressed by zstd admission in this
  layout: `actual_embedding_none_blocks=130` and `vector_stored_B/entry`
  remains `384.0`.
- `settled_over_asset_B/entry` was `0.006672`, so the fresh benchmark
  workspace has negligible manifest/namespace overhead after publish, close,
  and reopen. No GC or rewrite step is needed to make this 1M fresh-build disk
  report minimal. Later compaction/rewrite tests should still report
  pre-cleanup directory bytes versus post-cleanup active-asset bytes.
- Hot search remained `0 B/op`, `0 allocs/op` for every serial and parallel
  variant. Search timings are over the already-opened persisted graph and do
  not include `.fbin` reads, asset build, manifest reopen, decode, or document
  fetch.

Neighborhood-local raw-vector compression smoke:

```sh
COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_COMPRESSIONS=none,zstd \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BNeighborhoodCompressionSmoke/(source_prefix|nearest_ranked|nearest_ordinal)/(none|zstd)$' \
  -benchmem \
  -benchtime 200ms \
  -count 1
```

This exact-scanned the 1M prefix in `203.8 ms`, selected 8192 nearest rows for
query 0, and built one raw float32 vector granule in source order,
nearest-neighbor score order, and nearest-neighbor ordinal order. All zstd cases
reported `fallback_not_smaller`, `stored_raw_ratio=1.000`, and
`vector_stored_B/entry=384.0`. This means neighborhood-local granule ordering
does not make the current raw float32 Deep1B encoding compress; a future vector
codec would need a different transform, such as quantization, residuals, or
delta coding, before engine integration is worth doing for raw vector bytes.

JZIP/geometric codec smoke:

```sh
COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BJZIPNeighborhoodCompressionSmoke/nearest_ranked/(cartesian_raw|cartesian_transpose|cartesian_byte_shuffle|spherical|spherical_center_delta|spherical_prev_delta|householder_cartesian)/(raw|snappy|lz4|zstd_fast|zstd_default|zstd_better)$' \
  -benchmem \
  -benchtime 200ms \
  -count 1
```

Representative nearest-ranked 8192-vector results from the same machine:

| Transform | Byte codec | Stored B/entry | Ratio vs 384 B | Encode ms | Decode ms | Max abs error | Mean cosine error |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| spherical | zstd_better | 271.4 | 1.415x | 16.63 | 13.49 | 0.0000001 | 0.0000000 |
| spherical | zstd_default | 288.3 | 1.332x | 13.61 | 12.33 | 0.0000001 | 0.0000000 |
| spherical | zstd_fast | 288.5 | 1.331x | 13.51 | 12.72 | 0.0000001 | 0.0000000 |
| spherical | lz4 | 292.6 | 1.312x | 12.81 | 12.52 | 0.0000001 | 0.0000000 |
| spherical | snappy | 293.9 | 1.306x | 12.52 | 12.04 | 0.0000001 | 0.0000000 |
| spherical_center_delta | zstd_better | 290.0 | 1.324x | 29.45 | 15.60 | 0.0000001 | 0.0000000 |
| spherical_prev_delta | zstd_better | 292.8 | 1.311x | 25.16 | 15.15 | 0.0000040 | 0.0000000 |
| cartesian_raw | zstd_better | 356.0 | 1.079x | 8.137 | 5.061 | 0 | 0 |
| cartesian_transpose | zstd_better | 355.4 | 1.080x | 8.466 | 4.991 | 0 | 0 |
| cartesian_byte_shuffle | zstd_fast | 326.6 | 1.176x | 6.475 | 3.279 | 0 | 0 |
| householder_cartesian | zstd_fast | 327.5 | 1.173x | 9.019 | 5.450 | 0.0000000 | 0.0000000 |

Interpretation:

- Direct JZIP-style spherical angles plus byte-shuffle and zstd lands in the
  expected Deep1B range: `271.4 B/entry` with zstd-better, or `288.3-288.5
  B/entry` with faster zstd settings.
- Snappy and lz4 also compress the spherical stream (`293.9` and `292.6
  B/entry`), which confirms the geometry/byte-shuffle transform is doing the
  useful entropy reduction rather than zstd alone.
- Raw row-major zstd-fast and transpose-only zstd-fast effectively do not
  compress Deep1B fp32 blocks. zstd-better can squeeze raw/transpose-only to
  about `355-356 B/entry`, but that is slow and far weaker than byte-shuffle or
  spherical geometry.
- Cartesian transpose plus byte-shuffle is the useful non-trig baseline:
  `326.6 B/entry`, or `1.176x` versus raw, with much cheaper decode than
  spherical reconstruction.
- Center-angle delta and previous-row delta did not beat plain spherical on
  this block. Previous-row delta is order-sensitive but was still worse for
  nearest-ranked rows.
- Householder-centered cartesian did not improve over plain cartesian
  byte-shuffle. It is cheap and exact, but this Deep1B block did not become
  more compressible without a stronger residual/quantization step.
- A full 1x all-order matrix gave the same qualitative result:
  nearest-ranked, nearest-ordinal, and source-prefix blocks all selected plain
  spherical as the best exact transform.

Decode-and-score smoke:

```sh
COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_JZIP_TRANSFORMS=cartesian_raw,cartesian_transpose,cartesian_byte_shuffle,spherical \
COLUMN_VECTOR_DEEP1B_JZIP_CODECS=raw,zstd_fast,zstd_better \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BJZIPDecodeAndScoreSmoke' \
  -benchmem \
  -benchtime 200ms \
  -count 1
```

Spherical direct-score smoke:

```sh
COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_JZIP_CODECS=raw,zstd_fast,zstd_better \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BSphericalDirectScore/spherical_angle_major_(raw|zstd_fast|zstd_better)/(exact_math_sincos|halfpi_poly_with_fallback)$' \
  -benchmem \
  -benchtime 200ms \
  -count 1
```

Representative nearest-ranked 8192-vector decode plus candidate-score results:

| Path | Stored B/entry | Ratio vs 384 B | Decode+score ms | Candidates/s | Raw MB/s | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| resident_fp32 | 384.0 | 1.000x | 0.097 | 84.2M | 32328 | 0 | 0 |
| cartesian_raw/raw | 384.0 | 1.000x | 0.904 | 9.07M | 3482 | 0 | 0 |
| cartesian_transpose/raw | 384.0 | 1.000x | 0.793 | 10.3M | 3967 | 0 | 0 |
| cartesian_byte_shuffle/zstd_fast | 326.6 | 1.176x | 2.919 | 2.81M | 1078 | 0 | 0 |
| cartesian_byte_shuffle/zstd_better | 326.5 | 1.176x | 3.052 | 2.68M | 1031 | 0 | 0 |
| spherical/raw | 380.0 | 1.011x | 10.95 | 0.748M | 287 | 0 | 0 |
| spherical/zstd_fast | 288.5 | 1.331x | 11.60 | 0.706M | 271 | 0 | 0 |
| spherical/zstd_better | 271.4 | 1.415x | 12.44 | 0.659M | 253 | 0 | 0 |

Follow-up stage split for the spherical rows, same nearest-ranked block,
Apple M3, `-benchtime=200ms -count=1`:

| Path | Decode+score ms | Decompress ms | Layout ms | Reconstruct ms | Score ms | Candidates/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| spherical/raw | 10.25 | 0.00004 | 0.936 | 9.218 | 0.094 | 0.799M |
| spherical/zstd_fast | 10.97 | 0.506 | 0.977 | 9.383 | 0.098 | 0.747M |
| spherical/zstd_better | 11.80 | 1.386 | 0.975 | 9.341 | 0.098 | 0.694M |

Spherical direct-score smoke over the same stored angle-major byte stream:

| Path | Stored B/entry | Decode ms | Direct score ms | Decode+score ms | Candidates/s | Fast-kernel est ms | Top10 overlap | Poly fallback |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| raw/exact_math_sincos | 380.0 | 0.00015 | 15.85 | 15.85 | 0.517M | 0.095 | 10/10 | 0 |
| raw/halfpi_poly_with_fallback | 380.0 | 0.00010 | 14.94 | 14.94 | 0.548M | 0.095 | 10/10 | 7.54% |
| zstd_fast/exact_math_sincos | 288.5 | 0.560 | 15.59 | 16.15 | 0.507M | 0.655 | 10/10 | 0 |
| zstd_fast/halfpi_poly_with_fallback | 288.5 | 0.508 | 15.46 | 15.97 | 0.513M | 0.603 | 10/10 | 7.54% |
| zstd_better/exact_math_sincos | 271.4 | 1.412 | 15.28 | 16.70 | 0.491M | 1.507 | 10/10 | 0 |
| zstd_better/halfpi_poly_with_fallback | 271.4 | 1.386 | 15.16 | 16.54 | 0.495M | 1.481 | 10/10 | 7.54% |

Scalar direct-score profile probes:

| Scorer | Angle load ms | Sin/cos scan ms | Full score ms | Sin/cos minus load ms |
| --- | ---: | ---: | ---: | ---: |
| exact_math_sincos | 2.57-3.19 | 15.25-15.61 | 14.55-15.49 | 12.12-13.04 |
| halfpi_poly_with_fallback | 2.51-2.55 | 19.49-19.64 | 14.58-15.71 | 16.94-17.13 |

Interpretation:

- The no-decode resident fp32 scoring ceiling for this block is about `84M`
  candidates/s. Any compressed hot path must be compared against that number,
  not just byte decode throughput.
- Raw row-major and transpose-only no-codec decode plus score are
  `0.79-0.90 ms` for 8192 candidates. This is the true no-compression block
  decode baseline now missing from the persisted search table.
- Cartesian byte-shuffle plus zstd gives a real non-trig storage win, but costs
  roughly `2.9-3.1 ms` to decode and score an 8192-vector block.
- Spherical is clearly a storage/cold-decode candidate in this Go prototype,
  not a hot search path as implemented: trig reconstruction dominates, even
  before considering graph traversal.
- The stage split should be used to separate byte-codec cost from spherical
  Cartesian reconstruction cost before promoting any compressed vector path.
  `BenchmarkColumnVectorGraphDeep1BSphericalDirectScore` is the follow-up gate:
  it must move spherical direct scoring materially above the current
  reconstruction path before engine integration is worth considering.
- The current scalar Go spherical-direct path is a deliberately under-optimized
  baseline, not a final verdict on storage-native scoring. Its profile says
  angle byte loads are about `2.5-3.2 ms` and full scalar scoring lands around
  `14.6-15.9 ms`. The standalone scan probes are intentionally rough and do
  not isolate every recurrence effect, but they keep the broad conclusion clear:
  the Go direct scorer is spending most of its time in scalar angle math and
  recurrence work, not byte decompression.
- The optimistic "dot-equivalent kernel" estimate is the useful ceiling: if
  spherical direct scoring could run like the resident fp32 dot kernel, raw
  angle scoring would be about `0.095 ms` for the block, zstd-fast would be
  about `0.60-0.66 ms`, and zstd-better would be about `1.48-1.51 ms`. That says
  the idea is worth separating into representation design and kernel design;
  the current Go loop is not the shape we would promote.

Granule-native local-residual scoring smoke:

```sh
COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_GRANULE_NATIVE_DIMS=16,32,96 \
COLUMN_VECTOR_DEEP1B_GRANULE_NATIVE_CODECS=raw,zstd_fast,zstd_better \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BGranuleNativeScore/(local_residual_(fp32_byte_shuffle|int8_columns)_top(16|32|96))/(raw|zstd_fast|zstd_better)$' \
  -benchmem \
  -benchtime 200ms \
  -count 1
```

This benchmark stores `H_mu x - e1` residuals in coordinate-major columns and
scores with `H_mu q` transformed once per granule. `stored_B/entry` includes
the shared centroid metadata and int8 scales, but not the separately reported
`4 B/entry` inverse-norm column.

| Path | Stored B/entry | Decode ms | Native score ms | Decode+score ms | Candidates/s | Fast-kernel est ms | Top10 overlap | Mean error | Bound pruned |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fp32_top96/raw | 384.0 | 0.00002 | 0.797 | 0.797 | 10.3M | 0.085 | 10/10 | 0.0000001 | 99.88% |
| fp32_top96/zstd_fast | 327.5 | 1.753 | 0.795 | 2.548 | 3.21M | 1.838 | 10/10 | 0.0000001 | 99.88% |
| int8_top96/raw | 96.09 | 0.00002 | 0.310 | 0.310 | 26.4M | 0.085 | 10/10 | 0.00132 | 99.88% |
| int8_top96/zstd_fast | 96.10 | 0.015 | 0.310 | 0.325 | 25.2M | 0.101 | 10/10 | 0.00132 | 99.88% |
| int8_top96/zstd_better | 86.73 | 0.966 | 0.299 | 1.265 | 6.47M | 1.052 | 10/10 | 0.00132 | 99.88% |
| int8_top32/raw | 32.06 | 0.00002 | 0.114 | 0.114 | 71.8M | 0.028 | 3/10 | 0.0453 | 0.085% |
| int8_top16/raw | 16.05 | 0.00002 | 0.0688 | 0.0689 | 119M | 0.014 | 3/10 | 0.0626 | 0% |

Interpretation:

- This is the first benchmark that matches the target architecture: a
  granule-local frame, residual storage relative to the local center,
  transposed native streams, one query transform per granule, and direct
  scoring from that stored layout.
- Full local int8 residuals are the most interesting hot-representation result
  so far. On this block they retain `10/10` exact top-10 overlap at about
  `96 B/entry` plus the inverse-norm column, and the scalar Go scorer reaches
  `~26M candidates/s` without zstd. That is still below resident fp32 but is
  now in the same order of magnitude.
- zstd-fast does not help the full int8 row on this block, while zstd-better
  reduces the full int8 row to `86.7 B/entry` but spends almost `1 ms` in
  decompression. That makes specialized numeric codecs the right next storage
  question; plain zstd is a useful baseline, not the likely final codec.
- Prefix rows confirm the earlier local-frame result: low-rank tangent prefixes
  are very fast and tiny, but they only recover `3/10` top-10 candidates on
  this nearest-ranked block. The bound is safe, but it only becomes tight when
  the full 96 dimensions are present; `M=16` prunes nothing and `M=32` prunes
  only `7/8192`.
- The fast-kernel estimate is now meaningful because the scalar loop is already
  storage-native. A vectorized/native int8 residual scorer has an optimistic
  block target near `0.085-0.10 ms` for full dimensions before graph traversal
  overhead, which is the right bar for deciding whether to build a real kernel.

Granule-native int8 micro-kernel smoke:

```sh
COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_GRANULE_NATIVE_DIMS=16,32,96 \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BGranuleNativeMicroKernels/top(16|32|96)/(current_fused_score_best|current_fused_score_top10|current_accumulate_only|current_finalize_best_only|current_finalize_top10_only|kelindar_sum_int8_columns|kelindar_mul_int8_payload|axiomhq_dot_int16_row_major|axiomhq_dot_fp32_row_major)$' \
  -benchmem \
  -benchtime 200ms \
  -count 1
```

Representative run on 2026-05-18, Apple M3:

| Path | Dims | Kernel scan B/entry | Kernel ms | Candidates/s | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| current_fused_score_best | 16 | 16 | 0.0734 | 111.7M | 0 | 0 |
| current_accumulate_only | 16 | 16 | 0.0714 | 114.8M | 0 | 0 |
| kelindar_sum_int8_columns | 16 | 16 | 0.00194 | 4.21B | 0 | 0 |
| kelindar_mul_int8_payload | 16 | 48 | 0.00323 | 2.54B | 0 | 0 |
| axiomhq_dot_int16_row_major | 16 | 32 | 0.0425 | 192.6M | 0 | 0 |
| current_fused_score_best | 32 | 32 | 0.126 | 64.9M | 0 | 0 |
| current_accumulate_only | 32 | 32 | 0.113 | 72.3M | 0 | 0 |
| kelindar_sum_int8_columns | 32 | 32 | 0.00383 | 2.14B | 0 | 0 |
| kelindar_mul_int8_payload | 32 | 96 | 0.00628 | 1.30B | 0 | 0 |
| axiomhq_dot_int16_row_major | 32 | 64 | 0.0460 | 178.1M | 0 | 0 |
| current_fused_score_best | 96 | 96 | 0.344 | 23.8M | 0 | 0 |
| current_accumulate_only | 96 | 96 | 0.335 | 24.4M | 0 | 0 |
| current_finalize_best_only | 96 | 8 | 0.00914 | 896.4M | 0 | 0 |
| current_finalize_top10_only | 96 | 8 | 0.0217 | 376.7M | 0 | 0 |
| kelindar_sum_int8_columns | 96 | 96 | 0.0113 | 724.2M | 0 | 0 |
| kelindar_mul_int8_payload | 96 | 288 | 0.0204 | 402.3M | 0 | 0 |
| axiomhq_dot_int16_row_major | 96 | 192 | 0.0735 | 111.5M | 0 | 0 |
| axiomhq_dot_fp32_row_major | 96 | 384 | 0.146 | 56.0M | 0 | 0 |

Interpretation:

- The current full int8 residual scorer is accumulation-bound. At full
  `M=96`, `current_fused_score_best` is `0.344 ms`, while
  `current_accumulate_only` is `0.335 ms`; best-row finalization is only
  `0.0091 ms`, and top-10 finalization is `0.0217 ms`.
- `kelindar/simd` confirms that raw int8 column primitives are much faster than
  the scalar fused scorer, but its exported API is reductions and element-wise
  int8 ops. It does not do the needed `int8 column * float32 coefficient ->
  float32 accum[row]` fused operation.
- `axiomhq/simd-go` does not expose int8 dot products, but its int16 dot row
  probe gives a useful lower-bound implementation target: full `M=96` row-major
  int16 dot over the block is `0.0735 ms`, scanning `192 B/entry`. That is a
  different storage shape than the preferred transposed `96 B/entry` int8
  residual layout, but it shows that a native kernel target below `0.1 ms` is
  plausible.

Local-frame approximate scoring smoke:

```sh
COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphDeep1BLocalFrameApproxScore/(exact_fp32|centroid_frame_top(1|8|16|32)_(fp32|fp16|int8)|centroid_frame_bound_prune)$' \
  -benchmem \
  -benchtime 200ms \
  -count 1
```

This benchmark is an off-band search-sketch probe, not a persisted engine
codec. It stores/scans block-local Householder-frame prefixes as full fp32,
IEEE fp16, or symmetric int8 with per-dimension scales, then compares each
sketch against exact fp32 top-10 ranking for the same 8192-row nearest-ranked
block. The bounded-prune row uses the exact threshold as a smoke-test oracle to
measure the potential prune rate and false-negative count.

Representative nearest-ranked 8192-vector local-frame sketch results. Sketch
bytes include encoded prefix values and per-dimension int8 scales; the shared
centroid/Householder metadata adds another `0.0469 B/entry` for local-frame
rows.

| Path | Sketch B/entry | ns/vector | Candidates/s | Top10 overlap | Recall@10 | Mean score error | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| exact_fp32 | 384.0 | 84.4 | 11.8M | 10 | 1.0 | 0 | 0 | 0 |
| top1_fp32 | 4.0 | 3.75 | 266.6M | 1 | 0.1 | 0.0713 | 0 | 0 |
| top1_int8 | 1.0 | 3.97 | 252.1M | 1 | 0.1 | 0.0713 | 0 | 0 |
| top8_fp32 | 32.0 | 9.64 | 103.8M | 3 | 0.3 | 0.0648 | 0 | 0 |
| top8_int8 | 8.004 | 8.95 | 111.8M | 3 | 0.3 | 0.0649 | 0 | 0 |
| top16_fp32 | 64.0 | 12.4 | 80.6M | 3 | 0.3 | 0.0625 | 0 | 0 |
| top16_int8 | 16.01 | 15.4 | 64.7M | 3 | 0.3 | 0.0626 | 0 | 0 |
| top32_fp32 | 128.0 | 26.1 | 38.3M | 3 | 0.3 | 0.0453 | 0 | 0 |
| top32_int8 | 32.02 | 30.5 | 32.8M | 3 | 0.3 | 0.0453 | 0 | 0 |
| bound_prune_M16 | n/a | 11.1 | 90.3M | n/a | n/a | n/a | 0 | 0 |

Interpretation:

- The local-frame prefix sketch is fast and compact, but in this first
  nearest-block smoke it is not a strong top-k surrogate: even 32 dimensions
  only overlaps 3 of the exact top 10.
- fp16/int8 quantization is not the limiting factor here. Quantized rows have
  nearly the same recall/error as fp32 prefixes, so the main loss is coordinate
  truncation rather than reduced precision.
- The M=16 Cauchy bound had `0` false negatives but also pruned `0` candidates;
  the bound is safe but too loose on this block to be useful without a tighter
  sketch, smaller candidate set, or a later thresholding stage.
- fp16 scoring is currently slower than fp32/int8 because the smoke benchmark
  converts IEEE half values back to float32 in the inner loop. That is an
  implementation cost, not a storage-size conclusion.

Useful reported metrics:

- `searches/s`, `B/op`, `allocs/op`: hot `SearchCosine` loop evidence.
- `build_ms`, `open_ms`, `decode_ms`: persisted-column setup/load evidence.
- `source_read_ms`: time spent reading `.fbin` base-vector batches while
  building column assets.
- `settled_disk_B/entry`, `vector_stored_B/entry`, `source_fbin_B/entry`,
  `source_cache_B/entry`: disk/accounting evidence with and without
  compression. `source_fbin_B/entry` is the bytes needed for the benchmarked
  row prefix; `source_cache_B/entry` is the actual local cache file size
  divided by benchmark rows.
- `actual_*_blocks` and `fallback_*_blocks`: actual compression mix and
  fallback reasons. Column-scoped forms such as
  `actual_neighbors_zstd_blocks` and `fallback_embedding_not_smaller_blocks`
  show whether vectors, inverse norms, and adjacency lists were actually
  compressed.
- `parts`, `codec_blocks`, `asset_header_B/entry`,
  `settled_over_asset_B/entry`: physical layout overhead. The fresh benchmark
  directory publishes parts, saves manifests, closes, and reopens before
  measuring `settled_disk_B/entry`; there is no row-store value-log GC or
  checkpoint rewrite in this isolated column workspace. If future compaction
  rewrites old column assets, report both the pre-cleanup settled directory and
  the post-cleanup active-asset footprint.
- `vector_stored_B/entry` inside
  `BenchmarkColumnVectorGraphDeep1BNeighborhoodCompressionSmoke`: theoretical
  compression of one raw float32 granule ordered by source rows versus exact
  nearest-neighbor rows. This is an off-band compressibility probe, not a
  persisted search benchmark.
- `stored_B/entry`, `transform_raw_B/entry`, `metadata_B/entry`,
  `ratio_vs_raw`, `warm_transform_ms`, `warm_transpose_shuffle_ms`,
  `warm_compress_ms`, `decode_ms`, `max_abs_error`, and `mean_cosine_error`
  inside `BenchmarkColumnVectorGraphDeep1BJZIPNeighborhoodCompressionSmoke`:
  off-band geometric codec evidence for future vector-column admission. These
  bytes are not yet persisted as engine assets.
- `decode_score_ms`, `candidates/s`, `raw_MB/s`, `score_only_ms`, `B/op`, and
  `allocs/op` inside
  `BenchmarkColumnVectorGraphDeep1BJZIPDecodeAndScoreSmoke`: cold block decode
  plus exact candidate scoring evidence. This benchmark also reports
  `decompress_ms`, `layout_ms`, `restore_ms`, and `reconstruct_ms` so spherical
  reconstruction cost can be separated from byte-codec and scoring costs. It
  intentionally does not fetch source documents or traverse a graph.
- `decode_score_ms`, `decode_ms`, `direct_score_ms`, `candidates/s`,
  `max_score_error`, `mean_score_error`, `top10_overlap`, `recall@10`,
  `poly_fallback_angle_ratio`, `profile_angle_load_ms`,
  `profile_sincos_scan_ms`, `fast_kernel_est_decode_score_ms`,
  `stored_B/entry`, and `allocs/op` inside
  `BenchmarkColumnVectorGraphDeep1BSphericalDirectScore`: direct spherical
  scoring evidence over the stored angle-major byte stream. The exact-math row
  is the correctness/performance baseline; the half-pi polynomial row tests
  whether local-angle approximation can reduce trig cost without corrupting
  top-k ranking. The profile metrics are scalar Go probes, while the
  fast-kernel metrics are estimates anchored to the resident fp32 dot baseline.
- `dims_used`, `top10_overlap`, `recall@10`, `max_score_error`,
  `mean_score_error`, `search_sketch_B/entry`, `quantized`,
  `bound_pruned_ratio`, and `bound_false_negative_count` inside
  `BenchmarkColumnVectorGraphDeep1BLocalFrameApproxScore`: local-frame
  approximate scoring evidence for future hot search sketches. The fp16/int8
  rows are actual quantized score paths, not hypothetical byte estimates.
- `native_score_ms`, `decode_ms`, `fast_kernel_est_decode_score_ms`,
  `native_raw_B/entry`, `stored_B/entry`, `top10_overlap`, `recall@10`,
  `bound_pruned_ratio`, and `bound_false_negative_count` inside
  `BenchmarkColumnVectorGraphDeep1BGranuleNativeScore`: storage-native
  local-residual scoring evidence. These rows score directly from transposed
  residual streams and separate scalar Go time from a dot-equivalent native
  kernel estimate.
- `kernel_ms`, `candidates/s`, `kernel_scan_B/entry`, `dims_used`, and
  `native_raw_B/entry` inside
  `BenchmarkColumnVectorGraphDeep1BGranuleNativeMicroKernels`: int8 residual
  scorer breakdown and SIMD primitive comparison. The `kelindar` rows are
  byte-column primitive probes, and the `axiomhq` rows are row-major dot probes,
  so they should be read as kernel-design evidence rather than storage-codec
  replacements.
