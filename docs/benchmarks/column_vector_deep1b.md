# Column Vector Graph Deep1B Benchmarks

This track runs the persisted-column `ColumnVectorGraph.SearchCosine` path on
Yandex Deep1B vectors without committing the dataset to the repository.

Data source:

- Overview: https://research.yandex.com/blog/benchmarks-for-billion-scale-similarity-search
- Deep1B 10M base subset: https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.10M.fbin
- Deep1B public queries: https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/query.public.10K.fbin

Yandex stores embeddings in `.fbin` format:

```text
num_vectors uint32
vector_dim  uint32
vector data float32[num_vectors][vector_dim]
```

Deep1B vectors are 96-dimensional and L2-normalized. The benchmark uses
`SearchCosine`; for normalized vectors cosine and Euclidean rankings are
monotonic, but the current benchmark is throughput and disk evidence rather
than recall evidence because Deep1B does not publish an ANN graph.

## Quick Run

The helper script defaults to the 1M shape, downloads the missing files into
`$HOME/.cache/gomap/deep1b`, and runs `none` plus `zstd` compression variants:

```sh
scripts/bench_column_vector_deep1b.sh
```

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
