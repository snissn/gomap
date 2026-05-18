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
  fallback reasons.
