# TreeDB vs USearch vector benchmark workflow

This guide owns the #2410 benchmark snapshot and counter workflow for the
post-#2360 TreeDB collection vector-search docs stack. It is a dated exact-FP32
`hnsw_search_pack_v1` benchmark contract, not a general claim that TreeDB beats
or matches USearch outside the stated fixture, hardware, commit, route, and
counters.

For the #2483 final vector docs closeout, use
[`vector-search-closeout-2483.md`](../spec/vector-search-closeout-2483.md) as the
current route/evidence index. It incorporates the #2487 unified current-main
snapshot for exact FP32, `scalar_u8`, and `rabitq_1bit`, plus the #2507
prototype `brq_1bit` lower-level evidence. The scale-sensitive crossover
campaign (#2490-#2494) is still pending, so final exact-vs-quantized-vs-USearch
positioning must either consume #2494 or say that crossover evidence is pending.

## Current performance snapshot: Tier S exact no-document comparison

Current public numbers should cite this exact snapshot until a newer #2399 or
#2412-derived evidence bundle is merged and routed back into the docs. The
snapshot was captured at the close of #2366/#2379 on 2026-06-05 at commit
`2feb1f0e35459d1b3d044008203d0c8afcf5630f`, Apple M3 (`darwin/arm64`).

Fixture: 10k documents, 64 dimensions, `M=16`, `efConstruction=128`,
`efSearch=128`, `topK=10`, query stream length 16, `BENCHTIME=1000x`,
`COUNT=3`, `CPU_LIST=1,8`. USearch is a pure in-memory external ANN baseline,
not TreeDB persistent storage. TreeDB vectors cross the collection JSON insert,
persisted `column_graph`, derived `hnsw_search_pack_v1`, and warmed search
boundary described in the row labels.

| Row | cpu | median ns/op | B/op | allocs/op | Boundary |
| --- | ---: | ---: | ---: | ---: | --- |
| `TreeDB_CollectionSearchVectorIndexWithBuffer` | 1 | 43,049 | 0 | 0 | collection buffered, caller-owned results |
| `TreeDB_CollectionSearchVectorIndexWithBuffer` | 8 | 43,307 | 0 | 0 | collection buffered, caller-owned results |
| `TreeDB_CollectionSearchVectorIndexNoDocsOneShot` | 1 | 43,016 | 816 | 2 | public one-shot convenience, response-owned results |
| `TreeDB_CollectionSearchVectorIndexNoDocsOneShot` | 8 | 44,679 | 816 | 2 | public one-shot convenience, response-owned results |
| `TreeDB_SearchWithBuffer` | 1 | 43,612 | 0 | 0 | reusable searcher, caller-owned buffer |
| `TreeDB_SearchWithBufferParallel` | 8 | 8,610 | 0 | 0 | reusable searcher, one buffer/searcher per worker |
| `USearch_Search` | 1 | 30,065 | 136 | 3 | pure in-memory USearch baseline |
| `USearch_SearchParallel` | 8 | 6,906 | 139 | 3 | pure in-memory USearch baseline |

Source artifacts from the closeout bundle:

- `/tmp/gomap_2366_final_20260605_030355/closeout_summary.md`
- `/tmp/gomap_2366_final_20260605_030355/tier_s_bench.log`
- `/tmp/gomap_2366_final_20260605_030355/no_doc_c1_100k_cpu_top.txt`
- `/tmp/gomap_2366_final_20260605_030355/no_doc_c8_100k_cpu_top.txt`
- `/tmp/gomap_2366_final_20260605_030355/with_buffer_alloc_proof.log`

## Required no-document route counters and row contracts

Healthy exact no-document rows must prove all of the following before claiming
the high-QPS path:

- `search_route_hnsw_search_pack/search=1`
- `hnsw_search_pack_active/search=1`
- `docs_fetched/search=0`
- `graph_row_fallbacks/search=0`
- `typed_column_vector_fallbacks/search=0`
- `vector_scratch_decodes/search=0`

Collection-level one-shot/buffer rows must additionally prove there is no
per-query open/setup in the timed loop:

- `open_searcher_calls/op=0`
- `open_setup_in_timed_loop=0`

Reusable-searcher `SearchWithBuffer` rows open the searcher before `ResetTimer`
and may not emit those collection-level open/setup counters; their open/setup
boundary is proven by the benchmark shape plus the route/fallback counters above.

Allocation accounting is part of the contract:

- `Collection.SearchVectorIndexWithBuffer` and
  `VectorIndexSearcher.SearchWithBuffer` target `0 B/op`, `0 allocs/op` after
  setup/warmup with caller-owned buffers.
- `Collection.SearchVectorIndex` with `IncludeDocuments=false` is a
  response-owned convenience row. The current Tier S snapshot expects
  `816 B/op`, `2 allocs/op`, and `response_owned_result_alloc/op=1`; that row
  can be fast and healthy without being the zero-allocation target.
- `Collection.SearchVectorIndex` with `IncludeDocuments=true` is an explicit
  with-documents/materialization row. It must report document counters such as
  `docs_fetched/search` and must not be included in no-document high-QPS success
  claims.

The closeout guardrail sample for the first c=1 Tier S run was:

| Row | `search_route_hnsw_search_pack/search` | `hnsw_search_pack_active/search` | `docs_fetched/search` | `open_searcher_calls/op` | `open_setup_in_timed_loop` | `graph_row_fallbacks/search` | `typed_column_vector_fallbacks/search` | `vector_scratch_decodes/search` | `response_owned_result_alloc/op` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `TreeDB_CollectionSearchVectorIndexWithBuffer` | 1.000 | 1.000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `TreeDB_CollectionSearchVectorIndexNoDocsOneShot` | 1.000 | 1.000 | 0 | 0 | 0 | 0 | 0 | 0 | 1.000 |
| `TreeDB_SearchWithBuffer` | 1.000 | 1.000 | 0 | n/a | n/a | 0 | 0 | 0 | n/a |
| `TreeDB_CollectionSearchVectorIndexWithDocumentsOneShot` | 0 | 1.000 | 10.00 | 1.000 | 1.000 | 0 | 0 | 0 | 1.000 |

The exact FP32 `hnsw_search_pack_v1` counters above are distinct from future
quantized route counters. Do not put quantized-only or quantized-rerank rows in
this exact no-document success claim unless they have separate fail-closed route
rows, fixtures, counters, and evidence.

## Canonical TreeDB vs USearch workflow

Use `scripts/bench_vector_search_compare.sh` for the optional external ANN
comparison. The script records a reproducibility README and full benchmark output
under `RUN_DIR`.

### USearch bootstrap and platform library paths

- Set `USEARCH_ROOT` when you already have a USearch C install. It must contain
  `usearch.h` under either `$USEARCH_ROOT/include` or `$USEARCH_ROOT`, and the
  platform library under either `$USEARCH_ROOT/lib` or `$USEARCH_ROOT`.
- If `USEARCH_ROOT` is unset, the script downloads USearch `USEARCH_VERSION`
  (default `2.25.2`) into `/tmp` and extracts it to the platform default:
  - macOS: `/tmp/usearch_${USEARCH_VERSION}_macos_<arch>/root`
  - Linux: `/tmp/usearch_${USEARCH_VERSION}_linux_<arch>/root/usr/local`
- The script searches for `libusearch_c.dylib` on Darwin and
  `libusearch_c.so` on Linux, then exports the correct `CGO_CFLAGS`,
  `CGO_LDFLAGS`, and `DYLD_LIBRARY_PATH` or `LD_LIBRARY_PATH` for the `go test`
  subprocess.
- Override `USEARCH_ARCH` only when the host architecture auto-detection is not
  the package architecture you want (`arm64`, `aarch64`, `x86_64`, `amd64`).

### Artifact directory, CPU list, benchtime, and count

- Set `RUN_DIR=/tmp/<stable-name>` when collecting publishable evidence. The
  default is `/tmp/gomap_vector_search_compare_<timestamp>`.
- The script writes `$RUN_DIR/README.md` with the branch, commit, USearch paths,
  fixture, regexes, and row-boundary notes, and `$RUN_DIR/bench.txt` with the
  raw benchmark stream.
- `CPU_LIST=1,8` means the script runs one `go test -cpu=1` block and one
  `go test -cpu=8` block; it does not rely on a combined Go benchmark CPU list.
- `BENCHTIME` and `COUNT` map directly to `go test -benchtime` and `-count`.
  Use `BENCHTIME=1000x COUNT=3` for the current Tier S snapshot shape, and a
  smaller `BENCHTIME=1x COUNT=1` only for smoke validation.

### Tier S publishable command

```sh
RUN_DIR=/tmp/gomap_vector_search_compare_tier_s_$(date +%Y%m%d_%H%M%S) \
TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 CPU_LIST=1,8 BENCHTIME=1000x COUNT=3 \
  BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare$' \
  scripts/bench_vector_search_compare.sh
```

### Tier F scaling command template

Tier F is the 100k-document, 128-dimension scaling fixture. Use it when measuring
scaling-sensitive exact no-document work; do not replace the Tier S snapshot
table with Tier F numbers unless the PR or tracker cites the exact command,
hardware, commit, route counters, `ns/op`, derived `ops/sec`, `B/op`, and
`allocs/op`. The publishable no-document Tier F command intentionally uses a
focused regex that excludes the with-documents/materialization row.

```sh
RUN_DIR=/tmp/gomap_vector_search_compare_tier_f_$(date +%Y%m%d_%H%M%S) \
TREEDB_VECTOR_BENCH_DOCS=100000 TREEDB_VECTOR_BENCH_DIMS=128 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 CPU_LIST=1,8 BENCHTIME=1000x COUNT=3 \
  BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare/(TreeDB_SearchWithBuffer|TreeDB_SearchWithBufferParallel|TreeDB_CollectionSearchVectorIndexWithBuffer|TreeDB_CollectionSearchVectorIndexNoDocsOneShot|USearch_Search|USearch_SearchParallel)$' \
  scripts/bench_vector_search_compare.sh
```

For smoke checks of either tier, keep the same fixture variables and use
`BENCHTIME=1x COUNT=1` with a throwaway `RUN_DIR`.

### Focused profile capture

The wrapper does not create pprof files by default. To capture focused CPU or
allocation profiles, first run the wrapper once and reuse the host-specific
USearch include/library paths recorded in `$RUN_DIR/README.md`:

```sh
RUN_DIR=/tmp/gomap_vector_search_compare_profile_bootstrap
export RUN_DIR
TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 CPU_LIST=1 BENCHTIME=1x COUNT=1 \
  BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare$' \
  scripts/bench_vector_search_compare.sh

USEARCH_INCLUDE_DIR=$(awk -F'`' '/USearch include dir:/ { print $2 }' "$RUN_DIR/README.md")
USEARCH_LIB_DIR=$(awk -F'`' '/USearch lib dir:/ { print $2 }' "$RUN_DIR/README.md")
export CGO_CFLAGS="-I$USEARCH_INCLUDE_DIR"
case "$(uname -s)" in
  Linux)
    export CGO_LDFLAGS="-L$USEARCH_LIB_DIR -lusearch_c"
    export LD_LIBRARY_PATH="$USEARCH_LIB_DIR:${LD_LIBRARY_PATH:-}"
    ;;
  Darwin)
    export CGO_LDFLAGS="-L$USEARCH_LIB_DIR -Wl,-rpath,$USEARCH_LIB_DIR -lusearch_c"
    export DYLD_LIBRARY_PATH="$USEARCH_LIB_DIR:${DYLD_LIBRARY_PATH:-}"
    ;;
esac

PROFILE_DIR=/tmp/gomap_vector_search_profiles_$(date +%Y%m%d_%H%M%S)
mkdir -p "$PROFILE_DIR"
TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 \
  go test -tags usearch_bench ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkCollectionVectorUSearchProductionCompare/TreeDB_CollectionSearchVectorIndexNoDocsOneShot$' \
  -benchmem -benchtime=100000x -count=1 -cpu=1 \
  -cpuprofile "$PROFILE_DIR/no_doc_c1_cpu.pprof" \
  -memprofile "$PROFILE_DIR/no_doc_c1_mem.pprof" \
  > "$PROFILE_DIR/no_doc_c1.log" 2>&1

go tool pprof -top -nodecount=40 "$PROFILE_DIR/no_doc_c1_cpu.pprof" \
  > "$PROFILE_DIR/no_doc_c1_cpu_top.txt"
go tool pprof -top -alloc_space -nodecount=40 "$PROFILE_DIR/no_doc_c1_mem.pprof" \
  > "$PROFILE_DIR/no_doc_c1_alloc_top.txt"
```

Repeat with `-cpu=8` on `TreeDB_SearchWithBufferParallel` for the actual c=8
concurrent profile; that subbenchmark uses `b.RunParallel` with one searcher and
one caller-owned buffer per worker. Running `-cpu=8` against non-parallel
subbenchmarks such as `TreeDB_CollectionSearchVectorIndexNoDocsOneShot` or
`TreeDB_CollectionSearchVectorIndexWithBuffer` only raises `GOMAXPROCS`; it does
not create concurrent benchmark workers. Use those non-parallel rows for
single-goroutine response-owned or caller-owned allocation profiles, and use the
parallel row for c=8 bottleneck/scaling evidence. Go benchmark CPU profiles
include fixture setup, rebuild, and package initialization work before the timed
loop; use the route counters above to separate setup from steady-state query
costs.

In healthy no-document rows, expected steady-state costs are HNSW pack traversal,
dot/scoring, frontier/top-k maintenance, and final result-ID copy for
response-owned convenience calls. Dominant document/JSON materialization,
graph-row fallback, typed-column vector fallback, per-query open/prepare, or
allocation/GC costs are blocking evidence for a no-document fast-row claim.

### Parallel scaling profile interpretation

Use the `TreeDB_SearchWithBufferParallel` row, not a serial row with only
`-cpu=8`, when classifying c=8+ reusable-buffer scaling. The parallel row must
show one opened searcher and one `VectorIndexSearchBuffer` per worker,
`parallel_workers=<GOMAXPROCS>`, `0 B/op`, `0 allocs/op`, and the no-document
route counters above. Treat the profile as evidence of avoidable contention only
when the steady-state timed branch shows material time in shared locks, atomics,
resource-manager lookups, collection prepared-cache synchronization, per-query
open/setup, or response-owned allocation/GC.

When the c=8 profile is instead dominated by
`columnHNSWSearchPackPreparedView.searchCosine`, `vectorops.DotFloat32Indexed` /
platform SIMD kernels, `insertTop`, `pushFrontier`, `popFrontier`, or
frontier/top-k heap maintenance, classify the remaining gap as traversal,
scoring, or ranking-kernel work. Keep that follow-up in the exact FP32
optimization lane, and hand SIMD/scoring-kernel work to #2403 instead of mixing
it into a parallel-contention PR.
