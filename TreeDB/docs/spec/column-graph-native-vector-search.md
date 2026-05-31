# Column Graph Native Vector Search

This guide covers the `column_graph` vector-index path that searches persisted
TreeDB column assets through the physical column row reader.

## What Path Runs

`column_graph` is distinct from two older/comparator paths:

- Native runtime vector indexes are the legacy collection vector path and do not
  use physical column graph assets.
- Decoded `ColumnVectorGraph` benchmarks fully load vector, invNorm, and
  adjacency columns into Go slices, then search that in-memory graph. That path
  remains useful as an oracle and throughput ceiling.
- `column_graph_native_reader` opens the collection manifest/root snapshot,
  uses the physical column row reader/cache for vector, invNorm, adjacency, and
  doc-id rows, and fetches full documents only after top-k when callers request
  materialized documents.

The native-reader path must not materialize a full decoded `ColumnVectorGraph` copy as its search substrate.

The role-specific prepared runtime-view policy for current-format typed-column
graph search is owned by
[`typed-column-graph-search-prepared-views.md`](typed-column-graph-search-prepared-views.md).
The #2044 readiness/admission table is owned by
[`typed-column-graph-search-admission.md`](typed-column-graph-search-admission.md).
Healthy search must use the documented prepared/direct state for vectors,
adjacency, inverse norms, row refs, and document IDs; legacy graph-row payloads
are compatibility inputs only and must not be counted as healthy-path evidence.

## Quickstart

Declare a collection with JSON documents, physical column storage for the vector
field, and an explicit `column_graph` vector index:

```go
meta := &collections.CollectionMeta{
    Name: "docs",
    Options: collections.CollectionOptions{
        DocumentFormat: collections.DocumentFormatJSON,
        ColumnStore: &collections.ColumnStoreConfig{
            Enabled: true,
            Columns: []collections.ColumnStoreColumn{
                {Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64},
                {Name: "did", Path: "did", ValueType: collections.ColumnStoreValueString, Dictionary: true},
                {Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 128},
            },
            SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
        },
    },
    VectorIndexes: []collections.VectorIndexDefinition{{
        Name:       "embedding_graph",
        Field:      "embedding",
        Metric:     collections.VectorMetricCosine,
        Dimensions: 128,
        M:          16,
        Strategy:   collections.VectorIndexStrategyColumnGraph,
    }},
}
```

Then load documents, build the graph, close/reopen, and search through a reusable
searcher for steady-state queries:

```go
_, _ = collections.NewCollectionManager(db).CreateCollection(meta)
col, _ := collections.NewCollectionManager(db).OpenCollection("docs")
_, _ = col.InsertBatch(ids, docs)
status, _ := col.RebuildVectorIndex("embedding_graph")

searcher, _ := col.OpenVectorIndexSearcher(collections.VectorIndexSearcherOptions{
    IndexName:        "embedding_graph",
    MaxDecodedBlocks: 4,
})
defer searcher.Close()

response, _ := searcher.Search(collections.VectorIndexSearcherSearchOptions{
    Query:    query,
    TopK:     10,
    EfSearch: 128,
})
fmt.Println(status.State, response.Path, response.Stats.RowFetches)
```

Use `SearchVectorIndex` for one-shot calls. It opens and closes the native reader
per call, so it measures setup/open cost in addition to graph search. Use
`OpenVectorIndexSearcher` when benchmarking or serving steady-state query load.

## Demo

Run a synthetic close/reopen demo:

```sh
GOWORK=off go run ./cmd/treedb_column_graph_demo \
  -dir /tmp/treedb-column-graph-demo \
  -reset \
  -rows 1024 \
  -dims 128 \
  -degree 16 \
  -top-k 10 \
  -ef-search 128 \
  -max-decoded-blocks 4
```

Representative output shape:

```text
TreeDB column_graph native-reader demo
db_dir=/tmp/treedb-column-graph-demo rows=1024 dims=128 degree=16 top_k=10 ef_search=128
rebuild status=column_graph_loaded loaded=true reason=
search path=column_graph_native_reader status=column_graph_loaded loaded=true results=10 include_docs=false
stats candidate_rows=... candidates=... edges=... visited_nodes=... visited_edges=... vector_B=... adjacency_B=... row_fetches=... cache_hits=... cache_misses=... decoded_blocks=... granules_touched=... physical_B=... max_resident_B=... docs_fetched=0
result[0] id=doc-000000 ordinal=0 score=1.000000
```

Run an opt-in real public dataset smoke using GloVe 6B 50d:

```sh
scripts/treedb_column_graph_glove_demo.sh
scripts/treedb_column_graph_glove_demo.sh --run
```

The script defaults to dry-run mode. It does not vendor the dataset into the
repository. With `--run`, it downloads GloVe into
`$HOME/.cache/treedb-column-graph/glove`, loads a configurable row subset, builds
the index, reopens the DB, and prints native-reader search status and accounting.

## Benchmark Commands

CI-safe smoke:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run 'TestColumnVectorGraphNativeSearch|TestSearchVectorIndexColumnGraph|TestOpenVectorIndexSearcher' \
  -count=1
```

Focused #2037 truth matrix for source/boundary comparisons:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphSearchTruthMatrix2037$' \
  -benchmem \
  -benchtime=500ms \
  -count=5
```

The stable row labels and prepared typed-column placeholders are defined in
[`typed-column-graph-search-benchmark-matrix.md`](typed-column-graph-search-benchmark-matrix.md).
Use the legacy/canonical benchmark set when comparing with older artifacts:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'Benchmark(ColumnVectorGraphNativeSearchCosineV3|ColumnVectorGraphNativeSearchCosineParallelV3|OpenVectorIndexSearcherColumnGraphNativeReaderSetupV6|OpenVectorIndexSearcherColumnGraphNativeReaderV4|SearchVectorIndexColumnGraphNativeReaderV4|SearchVectorIndexColumnGraphNativeReaderWithDocumentsV4)$' \
  -benchmem \
  -benchtime=500ms \
  -count=5
```

The expected categories are:

- `BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderSetupV6`: cold
  native-reader setup/open. It does not search.
- `BenchmarkColumnVectorGraphNativeSearchCosineV3`: lower-level native reader
  graph traversal, vector scoring, and top-k over the physical row reader.
- `BenchmarkColumnVectorGraphNativeSearchCosineParallelV3`: real
  `b.RunParallel`; workers use local scratch and readers.
- `BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderV4`: public reusable
  searcher steady-state query. Setup/open is outside the timed loop.
- `BenchmarkSearchVectorIndexColumnGraphNativeReaderV4`: public one-shot search.
  Setup/open is inside each timed operation.
- `BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsV4`: public
  one-shot search plus post-top-k document materialization.

Report and compare:

- `ops/sec` and `ns/op`,
- `B/op` and `allocs/op`,
- candidate_rows/search, candidates/search, edges/search, visited_nodes/search, and visited_edges/search,
- vector_B/search and adjacency_B/search,
- row_fetches/search, batch_fetches/search, rows_fetched/search,
- cache_hits/search, cache_misses/search, cache_hit_ratio,
- decoded_blocks/search, granules_touched/search, physical_B/search,
- max_resident_B,
- open_granules/op and open_physical_B/op for setup/one-shot paths,
- docs_fetched/search only for materializing public API benchmarks.

## Block Planner Follow-On

The next native search optimization is planned in
[`column-graph-native-block-planner.md`](column-graph-native-block-planner.md).
It aligns graph search with the column store by moving from per-candidate point
fetches to bounded block views, batched scoring, lazy adjacency expansion, and
final top-k-only ID/document materialization.

## Best Practices

- Use `column_graph` explicitly. Existing native runtime vector indexes remain a
  separate path.
- Batch inserts, then call `RebuildVectorIndex` before serving column graph
  queries.
- Reopen or checkpoint in validation flows when proving durable root/manifest
  discovery.
- Prefer `OpenVectorIndexSearcher` for steady-state query throughput.
- Keep one searcher per concurrent worker. Searchers and scratch are not
  concurrency-safe; the persisted graph snapshot is immutable.
- Tune `MaxDecodedBlocks` as bounded cache capacity. It must not become an
  unbounded decoded graph copy.
- Request documents only when needed. Graph traversal/scoring should remain
  document-fetch-free; document fetch belongs after top-k.
- Treat update/delete support according to current status. Unsupported mutation
  visibility must return rebuild-needed or unsupported-visibility status rather
  than stale results.

## Caveats

- The current V6 evidence is correctness and product-path evidence over
  CI-sized fixtures. Larger public dataset runs are opt-in and should be
  reported separately with hardware labels.
- Generic column-store reader/cache improvements from #1621/#1634 may change
  absolute throughput. PR-local changes should still avoid avoidable CPU,
  memcopies, allocation churn, repeated setup, or hidden full decode.
- Decoded `ColumnVectorGraph` numbers are ceiling/comparator numbers, not proof
  that native-reader search is using a fully decoded in-memory graph.
