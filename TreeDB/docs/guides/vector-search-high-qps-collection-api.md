# TreeDB high-QPS collection vector search

This guide summarizes the collection-level vector-search API boundary after the
`#2360` high-QPS work. It owns the API-selection and caveat guidance from #2406;
benchmark snapshots and full counter workflow are documented separately by
#2410.

## Choosing a vector search API

| User goal | API | Result ownership | Document/materialization boundary | Notes |
| --- | --- | --- | --- | --- |
| Production high-QPS exact no-document serving through the collection API | `Collection.SearchVectorIndexWithBuffer` | caller-owned `VectorIndexSearchBuffer` | `IncludeDocuments=false`; document fetch/projection/filter/fallback controls rejected | Primary collection-level fast path. Warm the collection-owned prepared `hnsw_search_pack_v1` state before the timed/serving loop; steady state targets `0 B/op`, `0 allocs/op`. |
| Simple no-document call when per-call result allocation is acceptable | `Collection.SearchVectorIndex` with `IncludeDocuments=false` | response-owned results/IDs | no documents materialized | Convenience route. Healthy exact calls use the cached `hnsw_search_pack_v1` route, but returned result/ID storage is response-owned and intentionally allocates. |
| Search and materialize documents in the same call | `Collection.SearchVectorIndex` with `IncludeDocuments=true` | response-owned results/documents | with-document materialization is part of the call | explicit materialization path. Do not mix these rows into no-document high-QPS claims; report document fetch counters separately. |
| Search first, fetch top-k documents later | `CollectionReadView.FetchDocumentsForVectorIndexSearchResults` after a no-document search | response-owned documents | separate fetch/materialization phase | Use when a service can keep ANN search no-document and fetch only selected top-k IDs later. Buffered-search results alias the caller buffer, so do not reuse/reset the buffer until this fetch returns. |
| Reusable low-level serving when the caller owns snapshot/open lifetime | `OpenVectorIndexSearcher` + `SearchWithBuffer` | caller-owned `VectorIndexSearchBuffer` | buffered search rejects document materialization | Open/warm one searcher and one buffer per worker. Use this when explicit searcher lifetime control matters more than the collection-level convenience seam. |

## Production high-QPS serving recipe

For the collection-level serving path:

1. Build or rebuild the declared `column_graph` vector index for the current
   generation, and keep rebuild/setup outside the timed request loop.
2. Use the exact/zero query mode with `IncludeDocuments=false`; leave document
   fetch options, filters, projections, legacy fallback controls, and
   `benchmark_debug` stats out of the high-QPS request shape.
3. Allocate one `VectorIndexSearchBuffer` per goroutine/worker. A buffer is not
   concurrency-safe, and returned `Results`/ID byte slices alias that buffer
   until it is reused or reset.
4. Warm once before serving or before `ResetTimer` so the collection-owned
   prepared `hnsw_search_pack_v1` state is built outside the measured loop.
5. Fetch documents only after the no-document search, as a separately measured
   materialization phase, or choose an explicitly with-document API row instead.

```go
var buffer collections.VectorIndexSearchBuffer
opts := collections.VectorIndexSearchOptions{
    IndexName: "embedding_graph",
    Query:     warmupQuery,
    TopK:      10,
    EfSearch:  128,
    StatsMode: collections.VectorIndexSearchStatsModeProduction,
}

// Warm the collection-owned prepared search state outside the timed loop.
if _, err := col.SearchVectorIndexWithBuffer(opts, &buffer); err != nil {
    return err
}

for query := range queries {
    opts.Query = query
    response, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
    if err != nil {
        return err
    }
    // response.Results aliases buffer; copy IDs/results before reusing buffer
    // if another goroutine or later stage must retain them.
    consumeTopK(response.Results)
}
```

For lower-level serving, open `OpenVectorIndexSearcher` once per worker, warm
`SearchWithBuffer` with that worker's own buffer, and close/reopen the searcher
when the worker must move to a newer collection/vector-index generation.

## Runnable exact buffered demo

For an instructional smoke that builds a collection, checkpoints and reopens it,
warms the prepared state, reuses a caller-owned buffer, times no-document
searches, and prints top-k IDs/scores plus route guardrails, run:

```sh
GOWORK=off go run ./cmd/treedb_vector_highqps_demo \
  -docs 1000 \
  -dims 64 \
  -queries 1000 \
  -warmup-queries 16 \
  -top-k 10
```

The demo is exact-only and intentionally excludes document materialization and
quantized modes. It is not a benchmark replacement; use the
[benchmark workflow](vector-search-benchmark-workflow.md) for performance
evidence.

## Do not overclaim

- Say TreeDB is close to USearch only for the warmed exact no-document parallel
  buffered serving route measured in the #2360/#2379 snapshot: Apple M3
  (`darwin/arm64`), 2026-06-05, commit
  `2feb1f0e35459d1b3d044008203d0c8afcf5630f`, Tier S fixture (10k docs,
  64 dims, M=16, efConstruction=128, efSearch=128, topK=10, query stream
  length 16, `BENCHTIME=1000x`, `COUNT=3`, `CPU_LIST=1,8`). USearch in that
  comparison is a pure in-memory external ANN baseline, not TreeDB persistent
  storage.
- With-document search is a different materialization path. It includes final
  document fetch/reconstruction work and must not be mixed into no-document
  high-QPS claims.
- Filters, projections, debug-only stats, and quantized modes are outside the
  exact FP32 `hnsw_search_pack_v1` no-document success claim unless a separate
  fail-closed route row and benchmark/counter evidence are published.
- Collection-level buffered quantized search support is unavailable/planned
  follow-up until #2415 lands. Keep any low-level quantized search evidence
  separate from exact FP32 `hnsw_search_pack_v1` route claims.

## Required no-document route counters

#2410 owns the full benchmark snapshot and counter workflow. As a contract
summary, healthy exact no-document rows must prove all of the following before
claiming the high-QPS path:

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

`Collection.SearchVectorIndexWithBuffer` must also report `0 B/op` and
`0 allocs/op`. `Collection.SearchVectorIndex` no-document convenience rows
should report `response_owned_result_alloc/op=1` and account for the small
response-owned allocation separately.

## Benchmark command

Use the production comparison benchmark for final c=1/c=8 evidence:

```sh
TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 CPU_LIST=1,8 BENCHTIME=1000x COUNT=3 \
  BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare$' \
  scripts/bench_vector_search_compare.sh
```

Canonical rows:

- `TreeDB_CollectionSearchVectorIndexWithBuffer`: collection-level
  caller-owned-buffer no-document target.
- `TreeDB_CollectionSearchVectorIndexNoDocsOneShot`: response-owned
  no-document convenience route.
- `TreeDB_SearchWithBuffer` and `TreeDB_SearchWithBufferParallel`: reusable
  searcher/buffer route.
- `TreeDB_CollectionSearchVectorIndexWithDocumentsOneShot`: explicit
  with-documents/materialization row.
- `USearch_Search` and `USearch_SearchParallel`: pure in-memory external ANN
  baseline.

### Tier F no-document scaling command

For 100k/128-class Tier F evidence, keep the same exact FP32 fixture and route
contract but focus the benchmark regex on the no-document TreeDB rows and the
USearch baseline. The with-documents/materialization row is intentionally
excluded here because it is not part of the high-QPS no-document contract and can
be measured separately as a document-fetch row.

```sh
TREEDB_VECTOR_BENCH_DOCS=100000 TREEDB_VECTOR_BENCH_DIMS=128 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 CPU_LIST=1,8 BENCHTIME=1000x COUNT=3 \
  BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare/(TreeDB_SearchWithBuffer|TreeDB_SearchWithBufferParallel|TreeDB_CollectionSearchVectorIndexWithBuffer|TreeDB_CollectionSearchVectorIndexNoDocsOneShot|USearch_Search|USearch_SearchParallel)$' \
  scripts/bench_vector_search_compare.sh
```

Report the same guardrail counters as the Tier S run, especially
`search_route_hnsw_search_pack/search=1`, `hnsw_search_pack_active/search=1`,
zero document/fallback/scratch counters, collection `open_*` counters at 0, and
allocation accounting for the caller-owned-buffer rows.

## Profile capture notes

For focused CPU profiles of the response-owned no-document convenience row,
bootstrap USearch with `scripts/bench_vector_search_compare.sh` and reuse the
host-specific include/library directories recorded in that run's README:

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
export CGO_LDFLAGS="-L$USEARCH_LIB_DIR -Wl,-rpath,$USEARCH_LIB_DIR -lusearch_c"
case "$(uname -s)" in
  Linux) export LD_LIBRARY_PATH="$USEARCH_LIB_DIR:${LD_LIBRARY_PATH:-}" ;;
  Darwin) export DYLD_LIBRARY_PATH="$USEARCH_LIB_DIR:${DYLD_LIBRARY_PATH:-}" ;;
esac

TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64 \
  TREEDB_VECTOR_BENCH_M=16 TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=128 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 TREEDB_VECTOR_BENCH_TOPK=10 \
  TREEDB_VECTOR_BENCH_QUERIES=16 \
  go test -tags usearch_bench ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkCollectionVectorUSearchProductionCompare/TreeDB_CollectionSearchVectorIndexNoDocsOneShot$' \
  -benchmem -benchtime=100000x -count=1 -cpu=1 \
  -cpuprofile /tmp/treedb_collection_searchvector_c1.pprof
```

Repeat with `-cpu=8` for the c=8 profile. Go benchmark CPU profiles include
fixture setup/rebuild work before the timed loop; use the benchmark route
counters above to distinguish setup from steady-state query costs. In steady
state, expected dominant query costs are HNSW pack traversal, dot/scoring,
frontier/top-k maintenance, and final result-ID copy for response-owned
convenience calls. Dominant document/JSON materialization, graph-row fallback,
typed-column vector fallback, per-query open/prepare, or allocation/GC costs are
not acceptable in no-document fast rows.
