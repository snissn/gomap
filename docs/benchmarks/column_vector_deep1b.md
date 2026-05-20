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

`TestColumnVectorGraphDeep1BGroundtruthLocality` is an opt-in official
Deep1B top100 locality and oracle compression tournament. It reads
`groundtruth.public.10K.ibin`, loads each query's official top100 database
rows, and reports locality, PCA/adaptive-rank gates, full-dim scalar int8,
lower-bit scalar int4/int2/sign variants, scale-policy variants,
norm-explicit variants, boundary-weighted PCA, pairwise-difference PCA,
query-axis oracle projections, and random-rotation scalar/sign probes. These
rows are explicitly labeled as official top100 local-neighborhood upper-bound
probes: they are valid for one query plus its 100 nearest-neighbor vectors,
but they do not prove TreeDB can build equivalent production granules and they
do not validate codebook-trained methods such as PQ, OPQ, residual PQ, LOPQ,
ScaNN/AVQ, QINCo, or Matryoshka.

`docs/benchmarks/column_vector_float_quantization_recall_tournament.md` is the
standalone home for the codec-only float quantization extension of the top100
tournament. The executable lane is enabled with
`COLUMN_VECTOR_DEEP1B_FLOAT_QUANT_TOURNAMENT=1`. It reframes the arena as
float-value quantization rather than only integer scalar quantization, with
affine `u1..u32`, mantissa-truncated fp32, bf16/fp16/fp32, block-floating,
random-rotation scalar quantization, mixed per-dimension precision, mixed
per-row precision, whole-row exceptions, sparse row-dimension exception
bitplanes, and norm-explicit variants. The primary output is inverted: for each
candidate-survival or ranking gate, find the minimum vector payload bits/vector
that passes. Metadata is reported secondarily because official top100 clouds are
not production-amortized granules. The 2026-05-20 q0..99 v2 run found a
theoretical top100 oracle floor near 192 bits/vector, or 24 B/vector, using u2
per-dimension affine plus sparse row-dimension exception bitplanes for the
tested gates. That lane intentionally does not prove an optimized exception-map
layout. Among simple uniform lanes, `u4` per-dimension affine remains the
conservative candidate-generation baseline: it passed exact top10 in
approx@20/@50 and exact top20 in approx@50 across all 100 queries at
384 payload bits/vector, or 48 B/vector. The recommended interpretation remains
compressed candidate scan followed by exact or near-exact rerank.

`TestColumnVectorGraphDeep1BBuildableGranuleScout` is an opt-in
production/buildable granule scout. It supports `row_id_contiguous` as a weak
storage-order control, `ivf_kmeans` as a deterministic cosine k-means locality
builder over the base prefix, and `ivf_kmeans_sorted_blocks` as a fixed-size
TreeDB-style storage-block proxy that sorts rows by assigned k-means locality
before chunking. In all modes, it ranks granules by centroid similarity and
reports routing recall separately from codec recall inside the selected granule
union. The codec rows include full-dim
per-dimension scalar u8/u4, local PCA int8 ranks, optional trained global
8-bit PQ/OPQ-style lanes, an optional per-buildable-granule local residual-PQ
lane, and an optional per-buildable-granule local residual-OPQ/LOPQ-style lane.
The global PQ/OPQ lanes use held-out base-prefix rows for codebook and
rotation training and evaluate on a disjoint base slice; they are
production/buildable codebook probes, not top100 oracle fits. The local
residual-PQ lane fits one residual codebook per sealed buildable granule. The
local OPQ lane adds a learned per-granule residual rotation before PQ, so it is
the first LOPQ-style scout rather than a top100 oracle fit.

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

Official groundtruth top100 oracle-locality tournament:

```sh
OUT=/tmp/gomap_deep1b_top100_tournament_ext_q0_99_$(date +%Y%m%d_%H%M%S)
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_LOCALITY=1 \
COLUMN_VECTOR_DEEP1B_FLOAT_QUANT_TOURNAMENT=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_BASE1B=1 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_CONCURRENCY=16 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_QUERIES=$(seq -s, 0 99) \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_PCA_RANKS=8,16,24,32,40,48,56,64,80,96 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_SCAN_ITERS=8 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_OUT="$OUT" \
GOWORK=off go test ./experiments/colgranule \
  -run '^TestColumnVectorGraphDeep1BGroundtruthLocality$' \
  -count 1 \
  -v
```

The run writes `results.json` and `report.md` under `$OUT`. The report's
candidate-survival gates are the promotion metrics: exact top10 contained in
approx@20/approx@50, exact top20 contained in approx@50, and final recall after
exact rerank from fixed shortlists. Compressed final top10 order is diagnostic,
not the primary promotion gate.

First buildable row-id-contiguous granule scout:

```sh
OUT=/tmp/gomap_deep1b_buildable_scout_q0_32768_$(date +%Y%m%d_%H%M%S)
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_SCOUT=1 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=row_id_contiguous \
COLUMN_VECTOR_DEEP1B_BUILDABLE_BASE_ROWS=32768 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_ROWS=4096 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_QUERIES=0 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_TOP_GRANULES=1,4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PCA_RANKS=32,64 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_SCAN_ITERS=4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_OUT="$OUT" \
GOWORK=off go test ./experiments/colgranule \
  -run '^TestColumnVectorGraphDeep1BBuildableGranuleScout$' \
  -count 1 \
  -v
```

This writes `results.json` and `report.md` under `$OUT`. The routing table is
the first-order production signal: row-id-contiguous granules are buildable but
not expected to have nearest-neighbor locality, so codec recall is reported as
conditional on whichever exact winners reached the selected granule union.

IVF/k-means buildable granule scout:

```sh
OUT=/tmp/gomap_deep1b_buildable_ivf_q0_9_32768_$(date +%Y%m%d_%H%M%S)
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_SCOUT=1 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=ivf_kmeans \
COLUMN_VECTOR_DEEP1B_BUILDABLE_BASE_ROWS=32768 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_ROWS=4096 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=8 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_QUERIES=0,1,2,3,4,5,6,7,8,9 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_TOP_GRANULES=1,4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PCA_RANKS=32,64 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_SCAN_ITERS=4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_OUT="$OUT" \
GOWORK=off go test ./experiments/colgranule \
  -run '^TestColumnVectorGraphDeep1BBuildableGranuleScout$' \
  -count 1 \
  -v
```

This builder is buildable from the base prefix and is a stronger production
locality scout than row-id order, but it is still not a trained-codebook PQ/OPQ
result.

IVF/k-means buildable granule scout with trained global PQ/residual-PQ/OPQ
lanes at the fixed byte budgets used by the compression tournament:

```sh
OUT=/tmp/gomap_deep1b_buildable_fullbudget_ivf_q0_9_$(date +%Y%m%d_%H%M%S)
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_SCOUT=1 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=ivf_kmeans \
COLUMN_VECTOR_DEEP1B_BUILDABLE_BASE_ROWS=32768 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_ROWS=4096 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=8 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_QUERIES=0,1,2,3,4,5,6,7,8,9 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_TOP_GRANULES=1,4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PCA_RANKS=32,48,64,80,96 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_BYTES=32,48,64,80,96 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_RESIDUAL_PQ_BYTES=32,48,64,80,96 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_OPQ_BYTES=32,48,64,80,96 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_TRAIN_ROWS=8192 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_ITERS=4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_OPQ_ITERS=3 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_SCAN_ITERS=4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_OUT="$OUT" \
GOWORK=off go test ./experiments/colgranule \
  -run '^TestColumnVectorGraphDeep1BBuildableGranuleScout$' \
  -count 1 \
  -timeout=60m \
  -v
```

This is the first trained-codebook production lane in this PR. It trains global
8-bit PQ codebooks, a global centroid-residual PQ variant, and OPQ-style
learned rotations on held-out rows, then evaluates them on disjoint
IVF/k-means granules. The residual-PQ row is a global-centroid residual scout;
it is not local LOPQ. This run does not claim ScaNN/AVQ, QINCo, or Matryoshka
coverage.

Representative artifact:

```text
/tmp/gomap_deep1b_buildable_fullbudget_ivf_q0_9_20260519_220119/report.md
```

Key aggregate conditional codec gates, queries `0..9`, `32768` eval rows,
`8192` held-out codebook training rows, top `1` and top `4` centroid-routed
IVF granules. PQ metadata includes f16 codebooks plus per-row f16 inverse
norms; residual PQ also stores one f16 global residual centroid; OPQ also
stores f16 rotation metadata:

| Method | Top granules | Row-code B/vector | Metadata B/vector | p50 compressed top10 | Worst compressed top10 | Worst top10@50 | Worst top10@100 | Worst top20@100 | Avg score err | Avg scan ns/vector |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| global PQ 32B x8 | 1 | 32 | 3.50 | 7/10 | 5/10 | 10/10 | 10/10 | 20/20 | 0.01829 | 16.06 |
| global residual PQ 32B x8 | 1 | 32 | 3.51 | 7/10 | 5/10 | 10/10 | 10/10 | 20/20 | 0.01830 | 15.88 |
| global OPQ 32B x8 | 1 | 32 | 4.06 | 7/10 | 4/10 | 10/10 | 10/10 | 20/20 | 0.01834 | 16.01 |
| local PCA rank32 int8 | 1 | 32 | 3.49 | 6/10 | 5/10 | 7/10 | 9/10 | 18/20 | 0.02206 | 25.01 |
| global PQ 48B x8 | 1 | 48 | 3.50 | 8/10 | 7/10 | 10/10 | 10/10 | 20/20 | 0.00851 | 25.12 |
| global residual PQ 48B x8 | 1 | 48 | 3.51 | 8/10 | 7/10 | 10/10 | 10/10 | 20/20 | 0.00851 | 25.36 |
| global OPQ 48B x8 | 1 | 48 | 4.06 | 8/10 | 7/10 | 10/10 | 10/10 | 20/20 | 0.00845 | 25.09 |
| local PCA rank48 int8 | 1 | 48 | 4.22 | 8/10 | 6/10 | 10/10 | 10/10 | 20/20 | 0.01413 | 39.26 |
| scalar u4 per-dim reconstructed | 1 | 48 | 0.18 | 8/10 | 8/10 | 10/10 | 10/10 | 20/20 | 0.00961 | 966.14 |
| global PQ 64B x8 | 1 | 64 | 3.50 | 8/10 | 7/10 | 10/10 | 10/10 | 20/20 | 0.00686 | 34.19 |
| global residual PQ 64B x8 | 1 | 64 | 3.51 | 8/10 | 7/10 | 10/10 | 10/10 | 20/20 | 0.00686 | 34.87 |
| global OPQ 64B x8 | 1 | 64 | 4.06 | 8/10 | 7/10 | 10/10 | 10/10 | 20/20 | 0.00678 | 33.76 |
| local PCA rank64 int8 | 1 | 64 | 4.94 | 8/10 | 8/10 | 10/10 | 10/10 | 20/20 | 0.00849 | 53.54 |
| global PQ 80B x8 | 1 | 80 | 3.50 | 9/10 | 8/10 | 10/10 | 10/10 | 20/20 | 0.00506 | 45.12 |
| global residual PQ 80B x8 | 1 | 80 | 3.51 | 9/10 | 8/10 | 10/10 | 10/10 | 20/20 | 0.00506 | 45.06 |
| global OPQ 80B x8 | 1 | 80 | 4.06 | 9/10 | 7/10 | 10/10 | 10/10 | 20/20 | 0.00500 | 45.43 |
| local PCA rank80 int8 | 1 | 80 | 5.67 | 9/10 | 8/10 | 10/10 | 10/10 | 20/20 | 0.00451 | 69.69 |
| global PQ 96B x8 | 1 | 96 | 3.50 | 9/10 | 9/10 | 10/10 | 10/10 | 20/20 | 0.00161 | 55.02 |
| global residual PQ 96B x8 | 1 | 96 | 3.51 | 9/10 | 9/10 | 10/10 | 10/10 | 20/20 | 0.00161 | 54.94 |
| global OPQ 96B x8 | 1 | 96 | 4.06 | 9/10 | 8/10 | 10/10 | 10/10 | 20/20 | 0.00160 | 55.61 |
| local PCA rank96 int8 | 1 | 96 | 6.39 | 10/10 | 9/10 | 10/10 | 10/10 | 20/20 | 0.00093 | 85.67 |
| scalar u8 per-dim reconstructed | 1 | 96 | 0.18 | 10/10 | 9/10 | 10/10 | 10/10 | 20/20 | 0.00056 | 980.64 |
| global PQ 32B x8 | 4 | 32 | 3.50 | 6/10 | 5/10 | 10/10 | 10/10 | 20/20 | 0.01717 | 15.83 |
| global OPQ 48B x8 | 4 | 48 | 4.06 | 9/10 | 7/10 | 10/10 | 10/10 | 20/20 | 0.00804 | 25.38 |
| global PQ 64B x8 | 4 | 64 | 3.50 | 9/10 | 7/10 | 10/10 | 10/10 | 20/20 | 0.00662 | 33.99 |
| global PQ 80B x8 | 4 | 80 | 3.50 | 9/10 | 8/10 | 10/10 | 10/10 | 20/20 | 0.00486 | 45.09 |
| local PCA rank80 int8 | 4 | 80 | 5.86 | 9/10 | 9/10 | 10/10 | 10/10 | 20/20 | 0.00637 | 69.78 |
| global PQ 96B x8 | 4 | 96 | 3.50 | 10/10 | 9/10 | 10/10 | 10/10 | 20/20 | 0.00146 | 57.36 |
| local PCA rank96 int8 | 4 | 96 | 6.62 | 10/10 | 9/10 | 10/10 | 10/10 | 20/20 | 0.00082 | 86.16 |
| scalar u8 per-dim reconstructed | 4 | 96 | 0.19 | 10/10 | 9/10 | 10/10 | 10/10 | 20/20 | 0.00056 | 962.58 |

Interpretation:

- Routing is still the first-order production risk. Top4 IVF routed all global
  top10 for 9 of 10 queries, but query 6 routed only `6/10`, so codec success
  remains conditional on the selected granule union.
- At the same `32 B/vector` row-code budget, trained global PQ beats local PCA rank32 on
  candidate survival and scan cost. PQ32 kept exact top10 inside approx@50 for
  every routed candidate set; PCA32 did not.
- At `48 B/vector`, global PQ/OPQ and scalar u4 all preserve candidate gates in this run.
  The PQ/OPQ LUT scorer is about `25 ns/vector`, while the scalar u4 lane is
  still a reconstructed Go scorer around `1 us/vector`.
- At `64 B/vector`, global PQ/OPQ are stronger same-byte competitors to rank64 local PCA
  on score error and scan cost, though none should be treated as a final ranker
  without exact rerank.
- At `80 B/vector` and `96 B/vector`, candidate gates are saturated in the
  aggregate rows, but compressed final top10 still has worst-query misses for
  PQ/OPQ and even full-rank local PCA or scalar u8. The extra bytes reduce
  score error; they do not remove the need for exact rerank.
- Global centroid-residual PQ is measured and does **not** move the frontier in
  this shape. Its candidate gates, score error, and scan cost are effectively
  the same as global PQ, with only the extra f16 residual centroid in metadata.
  The next residual-codebook question is per-granule residual coding, not more
  global-centroid subtraction; the local residual-PQ section below starts that
  LOPQ-lite comparison.
- OPQ-style learned rotation is now measured, but it is not promoted yet. It
  slightly improves mean score error in some rows, but it adds about
  `0.56 B/vector` metadata at this eval size, takes roughly `3.9-4.3x` PQ
  training time, and does not produce a decisive worst-query or candidate-gate
  win over global PQ.
- PQ training for 32B/48B/64B/80B/96B on 8192 rows and 4 iterations took about
  `706 ms`, `944 ms`, `1058 ms`, `1123 ms`, and `1222 ms`; global residual PQ
  took about `704 ms`, `942 ms`, `1033 ms`, `1126 ms`, and `1227 ms`; OPQ with
  3 outer iterations took about `3092 ms`, `4156 ms`, `4765 ms`, `5381 ms`,
  and `6048 ms`. The method build milliseconds in the table are per-run encode
  costs for selected rows; production query-time would read stored codes
  instead.

IVF/k-means buildable granule scout with per-granule local residual-PQ
(`LOPQ-lite`) lanes:

```sh
OUT=/tmp/gomap_deep1b_buildable_local_residual_pq_ivf_q0_9_$(date +%Y%m%d_%H%M%S)
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_SCOUT=1 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=ivf_kmeans \
COLUMN_VECTOR_DEEP1B_BUILDABLE_BASE_ROWS=32768 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_ROWS=4096 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=8 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_QUERIES=0,1,2,3,4,5,6,7,8,9 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_TOP_GRANULES=1,4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PCA_RANKS=32,48,64,80,96 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_BYTES=32,48,64,80,96 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_LOCAL_RESIDUAL_PQ_BYTES=32,48,64,80,96 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_TRAIN_ROWS=8192 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_ITERS=4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_SCAN_ITERS=4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_OUT="$OUT" \
GOWORK=off go test ./experiments/colgranule \
  -run '^TestColumnVectorGraphDeep1BBuildableGranuleScout$' \
  -count 1 \
  -timeout=60m \
  -v
```

Representative artifact:

```text
/tmp/gomap_deep1b_buildable_local_residual_pq_ivf_q0_9_20260519_222818/report.md
```

This run fits one residual PQ codebook per buildable IVF granule. It is a
sealed-granule storage probe rather than a top100 oracle fit, but it is still
LOPQ-lite: the lane has local residual codebooks without local OPQ rotations.

Selected aggregate rows:

| Method | Top granules | Row-code B/vector | Metadata B/vector | p50 compressed top10 | Worst compressed top10 | Worst top10@20 | Worst top10@50 | Worst top20@50 | Avg score err | Avg scan ns/vector |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| global PQ 32B x8 | 1 | `32` | `3.50` | `7/10` | `5/10` | `7/10` | `10/10` | `19/20` | `0.01829` | `16.27` |
| local residual PQ 32B x8 | 1 | `32` | `13.51` | `8/10` | `7/10` | `9/10` | `10/10` | `20/20` | `0.01396` | `16.65` |
| local PCA rank32 int8 | 1 | `32` | `3.49` | `6/10` | `5/10` | `6/10` | `7/10` | `14/20` | `0.02206` | `25.30` |
| global PQ 48B x8 | 1 | `48` | `3.50` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00851` | `25.54` |
| local residual PQ 48B x8 | 1 | `48` | `13.51` | `9/10` | `8/10` | `9/10` | `10/10` | `20/20` | `0.00673` | `25.84` |
| scalar u4 reconstructed | 1 | `48` | `0.18` | `8/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00961` | `984.61` |
| global PQ 64B x8 | 4 | `64` | `3.50` | `9/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00662` | `34.48` |
| local residual PQ 64B x8 | 4 | `64` | `14.13` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00543` | `33.98` |
| local PCA rank64 int8 | 4 | `64` | `5.10` | `9/10` | `8/10` | `9/10` | `10/10` | `19/20` | `0.01114` | `52.89` |
| global PQ 96B x8 | 4 | `96` | `3.50` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00146` | `56.07` |
| local residual PQ 96B x8 | 4 | `96` | `14.13` | `9/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00126` | `55.66` |
| scalar u8 reconstructed | 4 | `96` | `0.19` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00056` | `976.13` |

Interpretation:

- Local residual-PQ is now measured as a per-buildable-granule residual
  codebook lane, not as an official top100 oracle method.
- At 32B/48B/64B row-code budgets it improves candidate gates and score error
  over both global PQ and local PCA in the weakest rows, with PQ-like LUT scan
  costs around `16-34 ns/vector`.
- The improvement is not free. The local codebooks add about
  `12.05 B/eval-vector` of metadata in this slice, and selected rows show
  roughly `13.5-14.1 B/vector` metadata versus `3.5 B/vector` for global PQ.
- Local residual-PQ training cost is materially higher: the five budgets train
  per-granule codebooks in about `2.8-4.9 s` on this 32K eval slice, versus
  about `0.7-1.2 s` for the global PQ codebooks.
- At 80B/96B, the quality edge narrows and full-dim scalar/local-PCA lanes
  remain competitive. Local residual-PQ is still a candidate generator, not a
  final-rank replacement.

Follow-on IVF/k-means buildable granule scout with local residual-OPQ
(`LOPQ-style`) lanes:

```sh
OUT=/tmp/gomap_deep1b_buildable_local_opq_ivf_q0_9_$(date +%Y%m%d_%H%M%S)
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_SCOUT=1 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=ivf_kmeans \
COLUMN_VECTOR_DEEP1B_BUILDABLE_BASE_ROWS=32768 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_GRANULE_ROWS=4096 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_KMEANS_ITERS=8 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_QUERIES=0,1,2,3,4,5,6,7,8,9 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_TOP_GRANULES=1,4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PCA_RANKS=32,48,64 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_BYTES=32,48,64 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_LOCAL_RESIDUAL_PQ_BYTES=32,48,64 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_LOCAL_OPQ_BYTES=32,48,64 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_TRAIN_ROWS=8192 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_PQ_ITERS=4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_OPQ_ITERS=3 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_SCAN_ITERS=4 \
COLUMN_VECTOR_DEEP1B_BUILDABLE_OUT="$OUT" \
GOWORK=off go test ./experiments/colgranule \
  -run '^TestColumnVectorGraphDeep1BBuildableGranuleScout$' \
  -count=1 \
  -timeout=60m \
  -v
```

Representative artifact:

```text
/tmp/gomap_deep1b_buildable_local_opq_ivf_q0_9_20260519_225007/report.md
```

This run adds one learned residual OPQ rotation per buildable IVF granule before
the local PQ codebooks. It closes the first LOPQ-style scout gap, but it still
uses a small IVF/k-means slice rather than graph-neighborhood or actual TreeDB
granules.

Selected aggregate rows:

| Method | Top granules | Row-code B/vector | Metadata B/vector | p50 compressed top10 | Worst compressed top10 | Worst top10@20 | Worst top10@50 | Worst top20@50 | Avg score err | Avg scan ns/vector |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| global PQ 32B x8 | 1 | `32` | `3.50` | `7/10` | `5/10` | `7/10` | `10/10` | `19/20` | `0.01829` | `16.00` |
| local residual PQ 32B x8 | 1 | `32` | `13.51` | `8/10` | `7/10` | `9/10` | `10/10` | `20/20` | `0.01396` | `15.81` |
| local residual OPQ 32B x8 | 1 | `32` | `17.81` | `8/10` | `7/10` | `8/10` | `10/10` | `20/20` | `0.01380` | `15.79` |
| local residual PQ 48B x8 | 1 | `48` | `13.51` | `9/10` | `8/10` | `9/10` | `10/10` | `20/20` | `0.00673` | `25.46` |
| local residual OPQ 48B x8 | 1 | `48` | `17.81` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00661` | `25.34` |
| local residual PQ 64B x8 | 1 | `64` | `13.51` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00552` | `36.70` |
| local residual OPQ 64B x8 | 1 | `64` | `17.81` | `9/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00540` | `36.52` |
| local residual PQ 32B x8 | 4 | `32` | `14.13` | `8/10` | `6/10` | `9/10` | `10/10` | `20/20` | `0.01374` | `16.21` |
| local residual OPQ 32B x8 | 4 | `32` | `18.66` | `8/10` | `6/10` | `9/10` | `10/10` | `19/20` | `0.01354` | `15.92` |
| local residual PQ 64B x8 | 4 | `64` | `14.13` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00543` | `37.18` |
| local residual OPQ 64B x8 | 4 | `64` | `18.66` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00531` | `36.83` |

Interpretation:

- Local OPQ slightly reduces mean score error versus local residual-PQ at each
  matched row-code budget and improves the top1/64B worst compressed top10 row
  from `8/10` to `9/10`.
- It does not materially move the candidate-survival gates in this run. Both
  lanes keep exact top10 in approx@50 and top20 in approx@50 for the important
  top1/top4 rows, with one 32B top4 OPQ row slipping to `19/20` top20@50.
- The extra rotation is expensive. Local OPQ training for 32/48/64B takes about
  `12.4/16.7/19.2 s`, versus `2.9/3.8/4.2 s` for local residual-PQ and
  `0.7/1.0/1.0 s` for global PQ. Metadata rises to `16.55 B/eval-vector`,
  versus `12.05 B/eval-vector` for local residual-PQ and `1.50 B/eval-vector`
  for global PQ.
- This keeps local residual-PQ as the better local-codebook lane for now.
  Local OPQ needs larger query/train/eval coverage or actual graph/TreeDB
  granules to justify its metadata and build cost.

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

Granule-native int8 storage tournament:

```sh
OUT=/tmp/gomap_deep1b_int8_tournament_$(date +%Y%m%d_%H%M%S)
COLUMN_VECTOR_DEEP1B_INT8_TOURNAMENT=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_INT8_TOURNAMENT_OUT="$OUT" \
GOWORK=off go test ./experiments/colgranule \
  -run '^TestColumnVectorGraphDeep1BInt8StorageTournament$' \
  -count 1 \
  -v
```

The opt-in tournament exports the real nearest-neighborhood int8 residual
matrix as `matrix_i8_dim_major.bin`, `matrix_u8_dim_major.bin`,
`matrix_u8_row_major.bin`, centroid/scales/query metadata, row ids, and exact
scores. It then runs native Go storage candidates over three row orders
(`nearest_ranked`, `row_id`, and `local_coord_sort`) and writes
`results.json`, `report.md`, local PCA artifacts under `$OUT/local_pca`, and
ClickHouse TSV/SQL fixtures under `$OUT/clickhouse`. The local PCA add-on fits
one granule-local basis over the raw fp32 vectors, stores fp16
centroid/basis/scales/invNorm metadata, and stores dim-major int8 row
coefficients for configurable ranks (`COLUMN_VECTOR_DEEP1B_LOCAL_PCA_RANKS`,
default `8,16,32,64,96`). The ClickHouse fixture set includes wide `UInt8`
rows, per-dimension blob rows, and whole-granule blob rows for later part-size
tests with codecs such as `NONE`, `LZ4`, `ZSTD`, `T64+ZSTD`, `Delta+ZSTD`, and
`DoubleDelta+ZSTD`. Blob fixtures are hex-encoded in TSV for transport, and the
generated runner decodes them with `unhex()` before ClickHouse stores the raw
byte strings.

The ClickHouse fixture runner is:

```sh
cd "$OUT/clickhouse"
CLICKHOUSE_BIN=/path/to/clickhouse \
CLICKHOUSE_PATH="$OUT/clickhouse/chdb" \
  ./run_clickhouse_local.sh
```

Initial native storage-only run, 8192 rows x 96 dimensions:

| Row order | Best native candidate | Stored B/vector | Ratio vs 96 B int8 | Best varint-family row | Stored B/vector |
| --- | --- | ---: | ---: | --- | ---: |
| nearest_ranked | `i8_dim_major/zstd_better` | 86.64 | 1.108x | `varint_i8_values` / `uvarint_zigzag_i8_values` + `zstd_better` | 90.24 |
| row_id | `i8_dim_major/zstd_better` | 86.64 | 1.108x | `varint_i8_values` / `uvarint_zigzag_i8_values` + `zstd_better` | 90.24 |
| local_coord_sort | `i8_dim_major/zstd_better` | 85.88 | 1.118x | `varint_i8_values` / `uvarint_zigzag_i8_values` + `zstd_better` | 89.35 |

Interpretation:

- The real int8 residual matrix has high byte entropy (`~7.15 bits/value`) and
  broad per-dimension cardinality (`~207` distinct values on average), so
  byte-level numeric hacks do not yet beat plain dim-major bytes plus
  zstd-better.
- Local coordinate row ordering is measurable: it improves the best zstd-better
  row from `86.64` to `85.88 B/vector` and also improves the varint-family
  rows, which means row-order-sensitive codecs remain worth testing.
- The added varint probes are useful negative evidence. Plain signed varint,
  zigzag uvarint, delta, double-delta, modulo delta, XOR-prev, zero-run/literal,
  RLE, and frame-of-reference varints are all present in the tournament, but
  their best compressed rows currently land around `89-90 B/vector`, behind
  the `85.9-86.6 B/vector` zstd-better byte-stream result.

ClickHouse local part-size run with ClickHouse `26.4.2.10`, using
`run_clickhouse_local.sh`:

| Row order | Best ClickHouse layout | Disk B/vector | Data-compressed B/vector | Best wide UInt8 layout | Disk B/vector |
| --- | --- | ---: | ---: | --- | ---: |
| nearest_ranked | `granule_dim_major_zstd` | 86.71 | 86.65 | `wide_zstd` | 114.80 |
| row_id | `granule_dim_major_zstd` | 86.71 | 86.65 | `wide_zstd` | 114.79 |
| local_coord_sort | `granule_dim_major_zstd` | 85.88 | 85.82 | `wide_zstd` | 113.95 |

ClickHouse interpretation:

- Whole-granule `String CODEC(ZSTD(1))` storage over the raw dim-major byte
  stream matches the native zstd-better result closely: `85.88-86.71 B/vector`
  on disk.
- Per-dimension blob rows are slightly larger (`86.06-86.89 B/vector`) because
  they split the stream into 96 separate rows but still preserve the
  coordinate-major byte layout.
- Wide `UInt8` tables are not competitive for this 8192-vector granule:
  the best wide row is `wide_zstd` at `113.95-114.80 B/vector`; `T64+ZSTD` is
  slightly worse, `Delta+ZSTD` is worse again, and `DoubleDelta+ZSTD` expands
  substantially. This includes the wide table's `order_name`, `row_idx`, and
  `source_row` columns, but the gap is large enough that the important lesson
  is still table/column overhead, not just metadata bytes.

Local PCA quantization add-on, same 8192-row nearest granule:

| Rank | PCA variance | Code zstd-better B/vector | fp16 metadata B/vector | Total B/vector | Ratio vs fp32 | Ratio vs 96B int8 | Top10 overlap | Mean rel L2 | Mean score error |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 47.27% | 7.48 | 2.21 | 9.70 | 39.61x | 9.902x | 0/10 | 0.6076 | 0.03306 |
| 16 | 63.91% | 14.69 | 2.40 | 17.09 | 22.47x | 5.616x | 0/10 | 0.5008 | 0.03264 |
| 32 | 81.04% | 28.73 | 2.78 | 31.51 | 12.19x | 3.047x | 0/10 | 0.3611 | 0.03180 |
| 64 | 95.21% | 56.11 | 3.54 | 59.65 | 6.44x | 1.609x | 7/10 | 0.1800 | 0.02182 |
| 96 | 100.00% | 82.35 | 4.30 | 86.65 | 4.43x | 1.108x | 10/10 | 0.0074 | 0.00041 |

Local PCA interpretation:

- This is the first storage curve for the proposed
  `centroid + local basis + per-row coordinate code` format. The bytes are
  self-contained for approximate cosine scoring: fp16 centroid, fp16 basis,
  fp16 coordinate scales, fp16 row invNorms, and zstd-better-compressed int8
  coefficient columns.
- The low-rank compression is real: rank 32 is only `31.51 B/vector` total, and
  rank 64 is `59.65 B/vector`. The quality curve is also clear: rank 64 recovers
  `7/10` exact top-10 rows on this query/granule, while ranks 8/16/32 recover
  none of the exact top 10 despite preserving substantial variance. On this
  block, preserving variance is not enough to preserve the query's nearest-row
  ranking.
- Full-rank local PCA is a sanity check. It lands at `86.65 B/vector` including
  fp16 metadata and recovers `10/10`, essentially matching the previous full
  local int8 residual byte-stream result. The interesting next gate is therefore
  not "can PCA compress"; it is whether rank 64, or a query-aware/residual
  variant, has enough candidate recall at wider cutoffs such as recall@100 or
  recall@1000 before exact rerank.

Groundtruth top100 locality probe:

```sh
OUT=/tmp/gomap_deep1b_groundtruth_locality_$(date +%Y%m%d_%H%M%S)
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_LOCALITY=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_BASE1B=1 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_QUERIES=0 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_OUT="$OUT" \
GOWORK=off go test ./experiments/colgranule \
  -run '^TestColumnVectorGraphDeep1BGroundtruthLocality$' \
  -count 1 \
  -v
```

This opt-in probe reads `groundtruth.public.10K.ibin`, loads each selected
query's top100 full-1B neighbor vectors, and computes locality plus local-PCA
ranking quality. It can use locally available base rows first; with
`COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_BASE1B=1`, missing rows are read from
`base.1B.fbin` by HTTP byte range so the run does not require downloading the
full 1B base file. The output is `$OUT/results.json` and `$OUT/report.md`.

Initial query-0 full-1B groundtruth top100 run:

| Query | Loaded top100 | Local | Remote | Missing | Centroid norm | Centroid cos(query) | Avg pairwise cos | Score p50 | Score p90 | GT top10 agreement |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 100/100 | 0 | 100 | 0 | 0.7997 | 0.8960 | 0.6359 | 0.7128 | 0.7326 | 10/10 |

Local PCA recall curve on the same query-0 top100:

| Rank | PCA variance | Recall@10 | Recall@20 | Recall@50 | Mean rel L2 | Mean score error | Max score error |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 51.12% | 3/10 | 5/20 | 30/50 | 0.4127 | 0.00941 | 0.04309 |
| 16 | 72.16% | 2/10 | 7/20 | 33/50 | 0.3111 | 0.00884 | 0.04297 |
| 32 | 90.91% | 5/10 | 11/20 | 36/50 | 0.1791 | 0.00757 | 0.03497 |
| 64 | 99.37% | 7/10 | 14/20 | 37/50 | 0.0469 | 0.00527 | 0.01722 |
| 96 | 100.00% | 9/10 | 20/20 | 50/50 | 0.0038 | 0.00007 | 0.00027 |

Groundtruth locality interpretation:

- The true full-1B query-neighbor cloud is much tighter than the earlier
  first-1M top8192 oracle block: query-0 top100 has average pairwise cosine
  `0.6359` and centroid norm `0.7997`, versus the broader top8192 smoke's
  average pairwise cosine around `0.278`.
- Even in this tight top100 cloud, ranking remains sensitive. Rank 64 captures
  `99.37%` of local variance but recovers only `7/10` exact top10 rows. The
  full-rank fp16-metadata/int8-code path reaches `9/10` top10 and perfect
  top20/top50 recall, so the remaining top10 miss is quantization/metadata
  noise rather than low-rank truncation.
- This makes the next locality gate clear: run several groundtruth queries, then
  compare true top100 locality against production-plausible granule builders
  such as IVF/k-means clusters, graph-neighborhood blocks, and actual graph
  visited sets.

Follow-up candidate-generation report:

- Full report: `docs/benchmarks/column_vector_deep1b_groundtruth_compression_report.md`.
- Command variant: same opt-in probe, with queries `0..99`,
  `COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_BASE1B=1`, and
  `COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_CONCURRENCY=16`.
- Result path for this run:
  `/tmp/gomap_deep1b_groundtruth_tracka_q0_99_20260519_181941/report.md`.

100-query summary over official full-1B groundtruth top100 neighborhoods:

| Metric | Result |
| --- | ---: |
| Loaded groundtruth rows | `10000/10000` |
| Local first-1M rows | `9` |
| Remote base.1B rows | `9991` |
| Average centroid norm | `0.8902` |
| Average centroid cos(query) | `0.9427` |
| Average pairwise cosine | `0.7927` |
| Rank64 average PCA variance | `99.24%` |
| Rank64 final top10 recall | `7.79/10` average, `4/10` worst |
| Rank64 exact top10 in approx@20 | `9.47/10` average, `7/10` worst |
| Rank64 exact top10 in approx@50 | `9.97/10` average, `9/10` worst |
| Rank80 exact top10 in approx@50 | `10/10` average, `10/10` worst |
| Rank64 mean score error | `0.00341` average |
| Exact rank10/rank11 score gap | `0.001109` average, `0.000020` minimum |

This changes the local-PCA interpretation from "not enough for final ranking" to
"potentially useful for candidate generation, with rank80 as the first
100-query-safe top50 gate in this ladder." Rank64 score error is larger than
the score margins deciding final top10 membership, so exact final ranking
misses are expected. Rank64 retained the exact top10 inside approximate top50
for `97/100` queries; rank80 retained it for `100/100`.

Minimum-rank gates from the 100-query run:

| Gate | Passed queries | Failed queries | p50 K | p90 K | Worst K |
| --- | ---: | ---: | ---: | ---: | ---: |
| Final top10 recall >= `9/10` | `100` | `0` | `80` | `96` | `96` |
| Final top10 recall = `10/10` | `96` | `4` | `96` | `96` | `96` |
| Exact top10 in approx@20 = `10/10` | `100` | `0` | `64` | `80` | `96` |
| Exact top10 in approx@50 = `10/10` | `100` | `0` | `24` | `56` | `80` |
| Exact top20 in approx@50 >= `19/20` | `100` | `0` | `40` | `64` | `80` |

The follow-on literature synthesis in the full report sharpens the next
benchmark path:

- Use candidate-recall gates as the promotion target: `exact top10 in
  approx@50/@100`, then exact rerank, rather than compressed top10 order alone.
- Treat dimensionality reduction as an out-of-place acceleration layer unless it
  proves final-ranking quality. Local PCA is a baseline candidate generator, not
  the objective.
- Same-byte PQ/residual-PQ/OPQ baselines now cover the fixed Deep1B `D=96`
  budgets: local PCA `K=32/48/64/80/96` competes against
  32/48/64/80/96-byte global PQ, global centroid-residual PQ, and OPQ-style
  codes on buildable IVF/k-means granules. A per-buildable-granule local
  residual-PQ lane and a per-buildable-granule local residual-OPQ/LOPQ-style
  lane are now also measured. The next extensions are larger query/train/eval
  coverage, production-grade local OPQ/LOPQ amortization, and actual
  graph-neighborhood and TreeDB granules.
- Prioritize granule-local residual encoders: `centroid + residual code +
  compressed scan + exact rerank`, which matches the LOPQ-style lesson that
  local residuals are easier to encode than raw global vectors.
- Add score-aware paths after the first baseline frontier: pairwise-difference
  PCA, boundary-weighted PCA, reduced-rank score approximation, and later
  ScaNN/AVQ or query-aware quantization when representative queries exist.
- Separate true-neighborhood probes from production-buildable granules:
  official top100 clouds are an upper-bound locality test, while IVF/k-means,
  graph-neighborhood, visited-set, and actual stored granules need their own
  candidate-recall rows.
- Treat LVQ/SVS, RaBitQ, and TurboQuant as CPU-friendly or low-build
  challengers; treat QINCo/QINCo2 and Matryoshka as later research/model-owned
  tracks.

The product-shaped output should be `quality gate -> minimum bytes/vector`,
with p50/p90/worst query and per-granule escalation statistics.

First buildable-granule control:

- Report:
  `/tmp/gomap_deep1b_buildable_scout_q0_32768_20260519_204341/report.md`.
- Regime: row-id-contiguous Deep1B prefix blocks, `32768` base rows,
  `4096` rows/granule, one query, granules ranked by centroid similarity.
- Routing is weak, as expected for row-id order: top1 granule routes only
  `2/10` global exact top10 and `2/20` top20; top4 granules route `4/10`
  top10 and `10/20` top20.
- Conditional codec recall inside the selected granule union remains useful:
  u4 scalar at `48 B/vector` and u8 scalar at `96 B/vector` keep the
  conditional exact top10 inside approx@20/50 and top20 inside approx@50 for
  both top1 and top4 selections; local PCA rank64 keeps conditional exact
  top10 inside approx@50 and top20 inside approx@50, while rank32 degrades on
  the less coherent top4 union.

This is not production locality proof. Its value is that the harness now
separates **routing recall** from **conditional codec recall**, which is the
required shape for comparing future graph-neighborhood, visited-set,
graph-sorted, and actual TreeDB granules.

IVF/k-means buildable scout:

- Report:
  `/tmp/gomap_deep1b_buildable_ivf_q0_9_32768_20260519_205538/report.md`.
- Regime: deterministic cosine k-means over the same `32768` base-row prefix,
  `4096` target rows/granule, `8` clusters, queries `0..9`, granules ranked by
  centroid similarity.
- Routing is much stronger than row-id order but still not solved: top1 IVF
  granule routes p50 `9/10` global exact top10 but worst `3/10`; top4 IVF
  granules route p50 `10/10` top10, worst `8/10`, p50 `50/50` top50, worst
  `41/50`, while scanning about `17.3K` candidate rows on average.
- Conditional codec result: scalar u4 at `48 B/vector` preserves top10@20,
  top10@50, and top20@50 across the q0..q9 routed unions but scans around
  `972 ns/vector` in the current Go reconstruct path. Local PCA rank64 scans
  around `53 ns/vector` and preserves top10@50/top20@50 well, but compressed
  final top10 is still not reliable. Rank32 is too fragile on less coherent
  routed unions.
- This is a buildable locality probe, not a trained-codebook result. The
  PQ/residual-PQ/OPQ scout above adds the first real train/eval split and
  metadata-amortized codebook accounting; global centroid-residual PQ is already
  measured and did not beat global PQ. The local residual-PQ lane is now
  measured as a per-granule residual codebook, and the local residual-OPQ lane
  is measured as a first LOPQ-style scout. Local OPQ reduces score error
  slightly but has not yet moved the candidate-survival gates enough to promote
  over local residual-PQ.

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
- `local_pca_results` inside
  `TestColumnVectorGraphDeep1BInt8StorageTournament`: storage and quality
  evidence for the `centroid + basis + row coordinate code` representation.
  It reports PCA rank, code bytes, compressed code bytes, fp16 metadata bytes,
  total bytes/vector, variance captured, top-10 overlap, score error, and
  reconstruction-relative L2 error.
- `kernel_ms`, `candidates/s`, `kernel_scan_B/entry`, `dims_used`, and
  `native_raw_B/entry` inside
  `BenchmarkColumnVectorGraphDeep1BGranuleNativeMicroKernels`: int8 residual
  scorer breakdown and SIMD primitive comparison. The `kelindar` rows are
  byte-column primitive probes, and the `axiomhq` rows are row-major dot probes,
  so they should be read as kernel-design evidence rather than storage-codec
  replacements.
