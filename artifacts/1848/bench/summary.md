# Issue 1848 C4 benchmark/profile summary

Environment: `darwin/arm64`, Apple M3, branch `snissn/1848-bench-docs`, head `33160d2477871d8e3acb6671799b76269ac30c68` at collection time.

## Commands run

```sh
mkdir -p artifacts/1848/bench

go test ./TreeDB/collections -run '^$' \
  -bench 'ColumnVectorGraphNativeSearchCosine(TypedColumn)?V3|VectorIndexSearch' \
  -benchmem \
  -cpuprofile artifacts/1848/bench/collections_cpu.pprof \
  -memprofile artifacts/1848/bench/collections_mem.pprof \
  | tee artifacts/1848/bench/collections_bench.txt

go test ./TreeDB/internal/typedcolumn -run '^$' \
  -bench 'Dense.*DirectView|Dense.*Section|DenseFloat32Dot' \
  -benchmem \
  -cpuprofile artifacts/1848/bench/typedcolumn_cpu.pprof \
  -memprofile artifacts/1848/bench/typedcolumn_mem.pprof \
  | tee artifacts/1848/bench/typedcolumn_bench.txt

go tool pprof -top artifacts/1848/bench/collections_cpu.pprof > artifacts/1848/bench/collections_cpu_top.txt
go tool pprof -top artifacts/1848/bench/collections_mem.pprof > artifacts/1848/bench/collections_mem_top.txt
go tool pprof -top artifacts/1848/bench/typedcolumn_cpu.pprof > artifacts/1848/bench/typedcolumn_cpu_top.txt
go tool pprof -top artifacts/1848/bench/typedcolumn_mem.pprof > artifacts/1848/bench/typedcolumn_mem_top.txt
```

Additional focused adjacency direct-view payload benchmark, because the required collection regex covers graph search but not the adjacency direct-view microbenchmark:

```sh
go test ./TreeDB/collections -run '^$' \
  -bench 'TypedColumnAdjacencyDenseDirectViewScan' \
  -benchmem \
  -cpuprofile artifacts/1848/bench/adjacency_direct_cpu.pprof \
  -memprofile artifacts/1848/bench/adjacency_direct_mem.pprof \
  | tee artifacts/1848/bench/adjacency_direct_bench.txt

go tool pprof -top artifacts/1848/bench/adjacency_direct_cpu.pprof > artifacts/1848/bench/adjacency_direct_cpu_top.txt
go tool pprof -top artifacts/1848/bench/adjacency_direct_mem.pprof > artifacts/1848/bench/adjacency_direct_mem_top.txt
```

No command reported `no matching benchmarks`.

## Vector graph/search benchmarks

| Benchmark | ns/op | ops/sec | B/op | allocs/op | vector bytes/read | adjacency bytes/read | candidate rows | candidates | visited nodes | visited edges | direct-view/materialization notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `BenchmarkColumnVectorGraphNativeSearchCosineV3` | 25,688 | 38,929 | 8,904 | 212 | 83,968/search | 14,184/search | 1,024/search | 128/search | 164/search | 3,230/search | baseline physical graph reader; `vector_direct_views/search=0`, `vector_scratch_decodes/search=164`, `adjacency_direct_views/search=51`, `adjacency_scratch_decodes/search=53` |
| `BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3` | 24,806 | 40,313 | 8,904 | 212 | 83,968/search | 14,184/search | 1,024/search | 128/search | 164/search | 3,230/search | shared typed-column path; `typed_column_vector=1`, `vector_direct_views/search=164`, `vector_scratch_decodes/search=0`, same allocation envelope as baseline |
| `BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderV4` | 25,519 | 39,186 | 9,688 | 214 | 83,968/search | 14,184/search | 1,024/search | 128/search | 164/search | 3,230/search | `docs_fetched/search=0`, `rows_fetched/search=0`; optimized path does not fetch full documents |
| `BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderSetupV6` | 54,595 | 18,317 | 11,059 | 81 | 0/search | 0/search | 0/search | 0/search | 0/search | 0/search | setup/open cost: `open_granules/op=1`, `open_physical_B/op=765006` |
| `BenchmarkCollectionVectorIndexSearch` | 62,321 | 16,046 | 26,670 | 13 | n/a | n/a | n/a | n/a | n/a | n/a | existing public vector-index search benchmark; no operation-specific counters emitted |
| `BenchmarkCollectionVectorIndexSearchInt8` | 782,141 | 1,279 | 297,485 | 803 | n/a | n/a | n/a | n/a | n/a | n/a | existing int8 public vector-index search benchmark; no operation-specific counters emitted |

Key graph-search guardrail: the typed-column variant is slightly faster in this run (24,806 ns/op vs 25,688 ns/op) with identical `B/op` and `allocs/op`. It replaces vector scratch decodes with direct views (`164/search`) without changing candidate rows, visited nodes, visited edges, or bytes read.

## Dense direct-view payload benchmarks

| Benchmark | ns/op | ops/sec | B/op | allocs/op | vector bytes/read | adjacency bytes/read | rows | elements | direct-view notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `BenchmarkTypedColumnDenseFloat32Dot1790/optimized_simd_axiomhq` | 52,116 | 19,188 | 0 | 0 | n/a | n/a | reported `78,593,835 rows/s` | reported `10,060,010,840 elements/s` | SIMD dot kernel, no allocation |
| `BenchmarkTypedColumnDenseFloat32Dot1790/scalar_portable` | 315,525 | 3,169 | 0 | 0 | n/a | n/a | reported `12,981,557 rows/s` | reported `1,661,639,350 elements/s` | portable scalar comparison, no allocation |
| `BenchmarkTypedColumnVectorDenseDirectViewScan` | 12,257 | 81,586 | 0 | 0 | 65,536/op | n/a | 1,024/op | 16,384/op | `direct_views/op=1`, `scratch_decodes/op=0`, heap-backed section view |
| `BenchmarkTypedColumnVectorDenseMmapHeapDirectViewScan/mapped` | 12,383 | 80,756 | 0 | 0 | 65,536/op | n/a | 1,024/op | 16,384/op | `direct_views/op=1`, `scratch_decodes/op=0`, `mapped_B=65536` |
| `BenchmarkTypedColumnVectorDenseMmapHeapDirectViewScan/heap` | 12,200 | 81,967 | 0 | 0 | 65,536/op | n/a | 1,024/op | 16,384/op | `direct_views/op=1`, `scratch_decodes/op=0`, `heap_copy_B=65536` setup only |
| `BenchmarkTypedColumnVectorDenseSectionScan` | 30,485 | 32,803 | 131,168 | 6 | 65,536/op | n/a | 1,024/op | 16,384/op | materializing section baseline; allocates about 2x payload bytes/op |
| `BenchmarkTypedColumnAdjacencyDenseDirectViewScan` | 38,826 | 25,756 | 0 | 0 | n/a | 524,288/op | 8,192/op | 131,072 uint32/op | mapped adjacency direct view; reported `13503.59 MB/s` |

Direct-view guardrail: vector and adjacency direct-view scans report `0 B/op` and `0 allocs/op`. The materializing vector section baseline remains visible at `131,168 B/op` and `6 allocs/op`, which confirms the direct-view path is not materializing vectors per run.

## Profile interpretation

- `collections_cpu_top.txt`: hot samples are in vector/graph work: `vectorIndexNode.vectorValueAt` (13.80% flat), `dotProductFloat32NEON` (9.02% flat), HNSW candidate diversity/search functions, `columnVectorGraphPhysicalRowReader.SearchCosine`, and typed direct-view validation. No scalar aggregate/predicate substrate function appears in the hot list.
- `collections_mem_top.txt`: allocation space is dominated by benchmark setup/index construction, validation error construction, JSON/document setup, and public result attachment. The hot typed-column graph benchmark itself retained the same `8,904 B/op` and `212 allocs/op` as the native baseline; `BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderV4` additionally reports `docs_fetched/search=0` and `rows_fetched/search=0`.
- `typedcolumn_cpu_top.txt`: direct vector scans spend time in the benchmark scan loop and dense float32 kernels; dot products route through `vectorops.DotFloat32`/`axiomhq` SIMD for the optimized case. No document reconstruction or scalar aggregate substrate appears.
- `typedcolumn_mem_top.txt`: allocation space is from the intentional materializing `BenchmarkTypedColumnVectorDenseSectionScan` (`DecodeRawFloat32VectorPayload` and `denseFloat32ColumnInto`). Direct-view vector benchmarks report zero per-op allocation.
- `adjacency_direct_cpu_top.txt`: 95.07% flat is the adjacency direct-view scan loop itself.
- `adjacency_direct_mem_top.txt`: allocation space is setup/image construction and profiler overhead; the benchmark result is `0 B/op`, `0 allocs/op` for the timed direct-view scan.

## Risks / notes

- Profiles are combined per `go test` invocation, so collection profiles include setup/rebuild/searcher benchmarks matched by the required regex in addition to the hot search loops.
- The public `BenchmarkCollectionVectorIndexSearch*` benchmarks still do not emit vector/adjacency bytes, candidate rows, or visited counters; table entries are marked `n/a` rather than inferred.
- Results are single-run local measurements on Apple M3; use them as regression guardrails for this branch, not as cross-machine absolute performance claims.
