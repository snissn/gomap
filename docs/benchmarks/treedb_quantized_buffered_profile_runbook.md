# TreeDB Quantized Buffered Per-Row Profiling

Use this workflow when optimizing TreeDB column-graph quantized buffered search
(#2460-#2464). It isolates one benchmark subrow per profile so CPU attribution
is not mixed across `quantized_only`, `quantized_rerank`, `c=1`, and `c=8` rows.

The workflow does not change benchmark semantics. It only runs the existing
`TreeDB/collections` benchmarks with exact subbenchmark regexes and stores the
raw outputs/profiles in a stable artifact layout.

## Matrix

Fixture identity for all rows: 1024 rows, 128 dimensions, `topK=10`,
`efSearch=128`, query ordinal 37, scalar_u8 quantized index benchmark fixture.
Public benchmark counters remain codec-generic `quantized_*`; `scalar_u8` is
only part of existing internal benchmark names.

| row id | API boundary | mode | c | benchmark regex |
| --- | --- | --- | ---: | --- |
| `lower_quantized_only_c1` | `VectorIndexSearcher.SearchWithBuffer` | `quantized_only` | 1 | `^BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414$/^route=quantized_only$/^c=1$` |
| `lower_quantized_only_c8` | `VectorIndexSearcher.SearchWithBuffer` | `quantized_only` | 8 | `^BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414$/^route=quantized_only$/^c=8$` |
| `lower_quantized_rerank_c1` | `VectorIndexSearcher.SearchWithBuffer` | `quantized_rerank` | 1 | `^BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414$/^route=quantized_rerank$/^candidates=32$/^c=1$` |
| `lower_quantized_rerank_c8` | `VectorIndexSearcher.SearchWithBuffer` | `quantized_rerank` | 8 | `^BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414$/^route=quantized_rerank$/^candidates=32$/^c=8$` |
| `collection_quantized_only_c1` | `Collection.SearchVectorIndexWithBuffer` | `quantized_only` | 1 | `^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415$/^route=quantized_only$/^c=1$` |
| `collection_quantized_only_c8` | `Collection.SearchVectorIndexWithBuffer` | `quantized_only` | 8 | `^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415$/^route=quantized_only$/^c=8$` |
| `collection_quantized_rerank_c1` | `Collection.SearchVectorIndexWithBuffer` | `quantized_rerank` | 1 | `^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415$/^route=quantized_rerank$/^candidates=32$/^c=1$` |
| `collection_quantized_rerank_c8` | `Collection.SearchVectorIndexWithBuffer` | `quantized_rerank` | 8 | `^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415$/^route=quantized_rerank$/^candidates=32$/^c=8$` |

## Standard capture

```sh
RUN_DIR=/tmp/gomap_2465_rows_$(date +%Y%m%d_%H%M%S) \
  GOMAXPROCS=8 \
  GOWORK=off \
  scripts/treedb_quantized_buffered_row_profiles.sh
```

Defaults run unprofiled timing (`-benchtime=100000x -count=5`) and one profiled
pass (`-benchtime=100000x -count=1`) per row. The Go pprof files cover one
selected benchmark subrow plus that row's fixture setup/teardown; use the
unprofiled timing output for `ns/op`, `B/op`, and `allocs/op`. Override `ROWS`
with a comma list or `lower` / `collection` for focused work:

```sh
ROWS=lower_quantized_only_c1 BENCHTIME=100000x TIMING_COUNT=5 PROFILE_COUNT=1 \
  scripts/treedb_quantized_buffered_row_profiles.sh
```

Smoke check for script/report changes (use enough fixed iterations for guardrail counters such as `B/op` to remain stable):

```sh
ROWS=lower_quantized_only_c1 BENCHTIME=1000x TIMING_COUNT=1 PROFILE_COUNT=1 \
  scripts/treedb_quantized_buffered_row_profiles.sh
```

## Artifact layout

The run root contains:

- `context.txt`: commit, branch, Go version, OS/arch, `GOMAXPROCS`, `uptime`,
  and visible competing benchmark/test processes.
- `matrix.tsv`: exact selected rows and subbenchmark regexes.
- `summary.md` / `summary.tsv`: median `ns/op`, `ops/sec`, `B/op`,
  `allocs/op`, selected vector-search counters, and guardrail status from
  `bench_timing.txt` when timing is enabled (falling back to `bench_profile.txt`
  only when timing is skipped).
- one directory per row id.

Each row directory contains:

- `bench_timing.txt`: unprofiled timing/guardrail output.
- `bench_profile.txt`: profiled benchmark output for the same row.
- `cpu.pprof`, `allocs.pprof`, `block.pprof`, `mutex.pprof`.
- `collections.test`: test binary emitted by Go profiling flags for later raw-profile analysis.
- `cpu_top.txt`, `allocs_top.txt`, `block_top.txt`, `mutex_top.txt`.
- `pprof_lists/*.txt`: line-level CPU excerpts.

Default line-level frames:

- `SearchCosine`
- `scoreAndPushFrontierVisitedTile`
- `frontierSiftDown`
- `insertTop`
- `fetchTopPreparedSearchResults`
- `flushBufferedWrites`
- `acquireCollectionVectorIndexPreparedSearch`
- `dotScalarU8CenteredIndexedARM64Int32`

## Guardrails to preserve

A row is not promotable for downstream optimization evidence unless the raw
benchmark output and generated summary show:

- timed buffered rows stay `0 B/op` and `0 allocs/op`;
- `docs_fetched/search`, fallback counters, and scratch materialization counters
  stay zero;
- `quantized_only` has zero exact vector/norm bytes and zero rerank exact score
  calls;
- `quantized_rerank` exact vector/norm reads and exact score calls stay bounded
  by `quantized_rerank_candidates/search` (32 in this matrix);
- public counters remain `quantized_*` rather than codec-specific public names.

## Noisy-host and promotion policy

Always read `context.txt` before using a run for claims. If it shows high load,
other active benchmark/test processes, or suspicious per-row variance, rerun on a
quieter host. The per-row harness removes mixed-row attribution noise, but it
cannot make contaminated data reliable.

For candidate optimization PRs, collect a fresh latest-main baseline and a
candidate run on the same host using distinct fresh run directories and identical
`ROWS`, `BENCHTIME`, `TIMING_COUNT`, `PROFILE_COUNT`, Go version, `GOMAXPROCS`,
and fixture. During the candidate run, set
`BASELINE_DIR=/tmp/baseline_dir scripts/treedb_quantized_buffered_row_profiles.sh`
to emit `benchstat_vs_baseline.txt` when `benchstat` is installed.

Flat/noisy is not enough. A downstream candidate should be abandoned or kept
draft unless the target row improves and no c=1/c=8, allocation, recall, or
guardrail regression remains. Runtime search changes are intentionally out of
scope for #2465; move any optimization found while using this workflow to its
own issue/PR.
