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

Default scoring policy after #2103: when callers leave the internal score-batch
mode at `default`, eligible prepared typed-column graph search uses
prepared gathered/indexed scoring. Eligibility is deliberately narrow: the
combined prepared graph-search view must be healthy, with a single-part prepared
vector view and ready inverse norms. Explicit scalar remains available for parity
tests and diagnostics, and legacy graph-row/direct or non-prepared fallback
routes keep default scalar scoring. This policy does not change traversal
semantics, frontier behavior, candidate order/tie behavior, `ef_search`, `topK`,
result ordering, or persistent formats.

Explicit #1926/#2454 quantized modes are documented in
[`quantized-vector-index.md`](quantized-vector-index.md) and the RaBitQ closeout
workflow is in [`rabitq-closeout-2454.md`](rabitq-closeout-2454.md). The
zero/default query mode remains exact. `quantized_only` uses a selected prepared
`scalar_u8`, pure-Go `rabitq_1bit`, or prototype `brq_1bit` score plane and
returns estimated scores without exact vector/norm reads. `quantized_rerank`
traverses with the selected quantized scorer over the normalized `ef_search`
candidate pool, trims to `QuantizedRerankCandidates`, exact-reranks that
shortlist by graph ordinal through the authoritative float32 vector/norm path,
and returns exact cosine scores. Missing, stale, mismatched, unsupported,
closed, or unprepared quantized assets fail closed; quantized modes must not hide
an exact fallback. #2483 summarizes current exact/scalar/RaBitQ/BRQ evidence in
[`vector-search-closeout-2483.md`](vector-search-closeout-2483.md); #2588
summarizes the promoted `scalar_u8` and `rabitq_1bit` prepared fast paths in
[`quantized-prepared-hnsw-closeout-2588.md`](quantized-prepared-hnsw-closeout-2588.md).

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

Use `SearchVectorIndex` for response-owned convenience calls. Exact
no-document calls use the collection-owned prepared `hnsw_search_pack_v1` cache
when healthy and own/copy the returned result storage for the response.
With-document or unsupported shapes still use the one-shot reader path and report
that setup/materialization cost. Use `SearchVectorIndexWithBuffer` when
collection-level code needs the exact no-document caller-owned result-buffer
seam; healthy current `hnsw_search_pack_v1` state is prepared once in a
collection-owned warmed cache. Use `OpenVectorIndexSearcher` plus
`SearchWithBuffer` when callers need explicit snapshot/open lifetime control
outside the hot loop.

## Collection-level no-document high-QPS contract (#2361)

This is the public contract downstream collection API work must preserve before
claiming high-QPS vector search:

- Query shape: explicit `column_graph` vector index, cosine metric, exact/zero
  `QueryMode`, `IncludeDocuments=false`, no `DocumentFetchOptions`, and no
  legacy filter/range-filter semantics in the high-QPS timed boundary.
- Result shape: return IDs/ordinals/scores only. Result/document ID bytes may be
  response-owned for convenience APIs or caller-buffer-owned for buffered APIs;
  document JSON must not be materialized unless `IncludeDocuments=true` is set
  on an API that supports document fetch.
- Serving state: raw collection vectors remain authoritative. The
  `hnsw_search_pack_v1` is derived vector-index serving state; it is healthy
  evidence only when validated against the current vector-index identity.
- Preferred high-QPS route: exact no-document search through a validated
  vector-index-owned `hnsw_search_pack_v1`, with reusable open/prepared state and
  caller-owned buffers where the API exposes them.
- Current buffered collection seam: `Collection.SearchVectorIndexWithBuffer`
  exposes caller-owned reusable result/ID storage for exact no-document searches
  and fails closed when a healthy `hnsw_search_pack_v1` route is not selected.
  On warmed healthy current pack state, it reuses collection-owned prepared pack
  state and benchmark rows must report no per-search open/setup inside the timed
  boundary.
- Current response-owned convenience boundary: `Collection.SearchVectorIndex`
  exact no-document calls use the same collection-owned prepared pack route when
  healthy, but still allocate response-owned results/IDs. It is a convenience
  fast route, not the zero-allocation target; use `SearchVectorIndexWithBuffer`
  for the caller-owned-buffer target.
- Document boundary: `IncludeDocuments=true` is an explicit post-top-k fetch
  path. It must report document counters (`docs_fetched/search`, output bytes,
  row-ref/point-fetch counters as applicable) and remains outside the
  zero-allocation no-document contract. Callers that first run a no-document
  search can also open a `CollectionReadView` and call
  `FetchDocumentsForVectorIndexSearchResults` later; that helper returns a
  separate `DocumentFetchResponse`/counter set and must be benchmarked as a
  fetch/materialization row, not as ANN hot-path work.
- Unsupported exact-pack high-QPS shapes: document fetch, projection, non-exact
  quantized modes, stale/missing packs, unsupported metrics/strategies, and
  future filter shapes must fail closed or run through clearly labeled
  non-high-QPS convenience/fallback rows with counters. Collection-level
  buffered quantized rows are supported by their own `quantized_only` /
  `quantized_rerank` benchmarks and must not be relabeled as exact
  `hnsw_search_pack_v1` rows. `rabitq_1bit` quantized rows may use prepared
  `hnsw_search_pack_v1` traversal, but their query-mode counters remain
  quantized and their closeout evidence lives in
  `quantized-prepared-hnsw-closeout-2588.md`.

### Vector-partition local packs

M3 vector partitions reuse `hnsw_search_pack_v1`; they do not define a second
ANN encoding or a document-bearing shard format. A partition generation binds
the native source generation/checksum/schema and maps the M2 artifact's stable
IDs to authoritative source ordinals before selecting rows. Pack construction
preserves layered HNSW adjacency framing, remaps retained neighbors to local
ordinals, drops cross-partition neighbors, and places the retained
highest-level node at local ordinal zero. An overflow-safe exact byte preflight
accounts for the version-2 membership header, directory/alignment, stable IDs,
authoritative layered adjacency, row references, and vectors before allocating
the full vector/topology pack. The actual encoded length is checked again
before asset append.

The generation's M1 manifest owns exact asset refs, lengths, CRCs, SHA-256
digests, canonical membership digests, home/overlap memberships, and bounded
overlap accounting. The same membership digest is persisted in the partition
pack header and covers the generation, partition, ordered authoritative stable
IDs, and membership kinds. A reopened partition searcher recomputes and
re-verifies the manifest/descriptor/header and source binding, reports
mapped/heap/open and candidate/edge counters, and returns stable IDs plus
scores without fetching documents. Corrupt, missing, stale, malformed, or
cross-membership/foreign-generation packs fail closed.

Healthy no-document fast-path evidence must include `ns/op`, `ops/sec`, `B/op`,
`allocs/op`, and route/fallback counters proving:

- `search_route_hnsw_search_pack/search=1` and
  `hnsw_search_pack_active/search=1`;
- `docs_fetched/search=0`;
- `graph_row_fallbacks/search=0`;
- `typed_column_vector_fallbacks/search=0`;
- `vector_scratch_decodes/search=0`;
- no per-search open/setup/validation/decode bottleneck in the timed boundary.

The response-owned no-document convenience row should show the cached pack route
explicitly (`open_searcher_calls/op=0`, `open_setup_in_timed_loop=0`,
`search_route_hnsw_search_pack/search=1`, documents/fallback/scratch counters at
0) while separately reporting its response-owned result allocation. With-document
one-shot rows should show `open_searcher_calls/op=1`,
`open_setup_in_timed_loop=1`, document counters, and the selected non-pack
`column_graph` route.

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
search path=column_graph_native_reader status=column_graph_loaded loaded=true results=10 include_docs=false doc_projection=none
stats candidate_rows=... candidates=... edges=... visited_nodes=... visited_edges=... vector_B=... adjacency_B=... row_fetches=... cache_hits=... cache_misses=... decoded_blocks=... granules_touched=... physical_B=... max_resident_B=... docs_fetched=0 doc_output_B=0 doc_fields_skipped=0
result[0] id=doc-000000 ordinal=0 score=1.000000
```

When exercising document final fetch through the demo, `-include-docs` uses the
preferred projection-oriented path and applies
`ProjectionOrientedVectorDocumentFetchPresetForField("embedding")`; add
`-include-doc-embedding` only for explicit full-document/embedding-echo
comparison runs. Keep these document-fetch rows separate from graph-search
throughput claims.

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
Use the #1926/#2454/#2845 quantized score-plane matrix when collecting exact
FP32, legacy scalar_u8, explicit per-granule-alpha scalar_u8, and pure-Go RaBitQ
evidence:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^(BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926|BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414|BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedAlphaSearchWithBuffer2414|BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452|BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926|BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450)$' \
  -benchmem -benchtime=100x -count=3
```

Use the legacy/canonical benchmark set when comparing with older artifacts:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'Benchmark(ColumnVectorGraphNativeSearchCosineV3|ColumnVectorGraphNativeSearchCosineParallelV3|OpenVectorIndexSearcherColumnGraphNativeReaderSetupV6|OpenVectorIndexSearcherColumnGraphNativeReaderV4|SearchVectorIndexColumnGraphNativeReaderV4|SearchVectorIndexColumnGraphNativeReaderWithDocumentsExcludeEmbedding1875|SearchVectorIndexColumnGraphNativeReaderWithDocumentsV4)$' \
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
- `BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsExcludeEmbedding1875`:
  public one-shot search plus preferred projection-oriented post-top-k document
  materialization; documents omit the vector field.
- `BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsV4`: explicit
  full-document comparison path; documents include the vector field.

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

When `StatsMode=benchmark_debug` is selected for #1979 evidence, also report
neighbor tile histograms, score-batch histograms, scored/skipped neighbor
buckets, already-visited skips, upper-layer versus layer-0 scores/edges,
frontier/top-k operation counts, visited-mark hits/misses, and exact-mode
candidate-order summaries. These counters are opt-in diagnostics and must not be
used to claim a default hot-path optimization by themselves.

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
