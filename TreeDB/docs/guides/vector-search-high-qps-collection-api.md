# TreeDB high-QPS collection vector search

This guide summarizes the collection-level vector-search API boundary after the
`#2360` high-QPS work.

## API boundary

| API | Result ownership | Document fetch | Intended use |
| --- | --- | --- | --- |
| `Collection.SearchVectorIndexWithBuffer` | caller-owned `VectorIndexSearchBuffer` | rejected | primary high-QPS no-document serving path; steady state should be `0 B/op`, `0 allocs/op` |
| `Collection.SearchVectorIndex` with `IncludeDocuments=false` | response-owned results/IDs | no documents | convenience no-document path; uses the same cached `hnsw_search_pack_v1` route when healthy, but allocates response-owned result storage |
| `Collection.SearchVectorIndex` with `IncludeDocuments=true` | response-owned results/documents | included in timed call | explicit with-documents/materialization path; not a no-document high-QPS success row |
| `CollectionReadView.FetchDocumentsForVectorIndexSearchResults` | response-owned documents | separate post-search call | explicit split search/fetch materialization phase for top-k IDs |
| `OpenVectorIndexSearcher` + `SearchWithBuffer` | caller-owned buffer | rejected by buffered search | reusable-searcher path when callers control snapshot/open lifetime |

## Required no-document route counters

Healthy no-document rows must prove all of the following before claiming the
high-QPS path:

- `search_route_hnsw_search_pack/search=1`
- `hnsw_search_pack_active/search=1`
- `docs_fetched/search=0`
- `graph_row_fallbacks/search=0`
- `typed_column_vector_fallbacks/search=0`
- `vector_scratch_decodes/search=0`
- `open_searcher_calls/op=0`
- `open_setup_in_timed_loop=0`

`Collection.SearchVectorIndexWithBuffer` must also report `0 B/op` and
`0 allocs/op`. `Collection.SearchVectorIndex` no-document convenience rows
should report `response_owned_result_alloc/op=1` and account for the small
response-owned allocation separately. Filters, projections, debug-only stats,
quantized modes, and document materialization are outside this exact no-document
contract unless they have an explicitly separate fail-closed route and benchmark
row.

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
