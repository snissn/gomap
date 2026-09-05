# Vector Search with Typed-Column Dense Sections

TreeDB can publish fixed-dimension `float32_vector` fields into typed-column dense
sections and can run the `column_graph` native-reader search path over persisted
physical assets. This guide explains how to place vector payloads, where to keep
metadata, and how to measure search without overclaiming maturity.

TreeDB remains **pre-alpha**. Vector storage metadata, graph/search APIs, and
on-disk formats may change. Rebuild demo/benchmark DB directories across branch
changes.

## Recommended layout

Current mutation boundary: the #4617 typed base-plus-suffix search consumer is
internal and experimental. Public typed batch writes preserve their selected
durability profile, but they do not automatically make an existing graph ready
for mutable search. Ordinary stale-base search remains unavailable pending the
M3 installation/fold lifecycle. The internal consumer reuses an immutable graph
and cold scalar plan, binds bounded current typed changes, and materializes only
results from that current pin. Its explicitly labeled exact path covers complete
eligible sets up to 4,096; larger supported sets use bounded ANN, with exhaustion
reported as an error. See the [admission and ownership contract](../spec/typed-column-graph-search-admission.md).

| Data | Recommended owner | Why |
| --- | --- | --- |
| Embedding/vector payload | `typed_column_part` fixed-dimension `float32_vector` | Contiguous row-major `float32` sections can be viewed directly after validation. |
| Document title/body/source text | Retained document / residual payload | Usually needed only after top-k; keep flexible. |
| Filter/sort metadata | `typed_row_asset` or scalar `typed_column_part` depending on query shape | Use typed-row for point reconstruction; use typed-column when filtering/scanning dominates. |
| Vector graph/ANN data | `derived_accelerator` plus TVIS control state | It accelerates search but is not the authoritative owner of the embedding field. Current healthy rebuilds do not publish duplicate physical graph row payloads. |
| Adjacency list | typed-column `uint32_list` assets owned by vector-index state | `raw_uint32_offsets_list` is the physical encoding for HNSW adjacency state. Legacy `column_graph` adjacency direct sources and the `adjacency_layout: "uint32_offsets_list"` selector are quarantined compatibility only; new graph builds should not publish those graph-specific source assets or legacy graph row adjacency payloads. |
| Ordinal-to-base-row references | vector-index state `row_refs` assets (`int64` / `raw_int64`) | Search uses row-ref state to map HNSW ordinals to base rows and to materialize documents without an ID-to-row-ref locator lookup. |
| Returned opaque document IDs | vector-index state `document_ids` asset (`bytes` / `raw_bytes_offsets`) | Exact arbitrary binary IDs are opaque bytes state. Legacy graph row ID bytes are compatibility or quarantine fallback only. |
| Optional quantized score planes | vector-index state `quantized_codes` assets (`byte_vector` / `raw_fixed_bytes` for legacy `scalar_u8`, or config-hashed scalar_u8 codes plus `quantized_alpha` metadata for explicit per-granule alpha mode; `packed_bit_vector` / `raw_packed_bit_vector` plus side arrays for `rabitq_1bit` and prototype `brq_1bit`) | Declared score planes are derived assets for explicit `quantized_only` and `quantized_rerank` modes. They are not authoritative vector storage and fail closed when missing, stale, mismatched, unsupported, or unprepared. Omitted `scalar_u8_calibration` remains legacy after the #2845 no-promote gate; per-granule-alpha scalar_u8 scoring is explicit opt-in. |

Best practice: keep vector payloads out of retained JSON for search-heavy
workloads when the typed-column vector section is the intended search data plane.
Keep metadata/final documents separate so search/scoring does not fetch full
JSON unless the caller requests it.

## Illustrative collection metadata

This snippet shows the current shape. Use the runnable demo below for a
copy/paste validation command.

```go
meta := &collections.CollectionMeta{
    Name: "docs",
    Options: collections.CollectionOptions{
        DocumentFormat: collections.DocumentFormatJSON,
        ColumnStore: &collections.ColumnStoreConfig{
            Enabled: true,
            RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
            Columns: []collections.ColumnStoreColumn{
                {Name: "doc_id", Path: "doc_id", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset, Dictionary: true},
                {Name: "published_at", Path: "published_at", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerRowAsset},
                {Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, Owner: collections.TypedStorageOwnerColumnPart, VectorDims: 128},
            },
        },
    },
    VectorIndexes: []collections.VectorIndexDefinition{{
        Name:       "embedding_graph",
        Field:      "embedding",
        Metric:     collections.VectorMetricCosine,
        Dimensions: 128,
        M:          16,
        Strategy:   collections.VectorIndexStrategyColumnGraph,
        QuantizedIndexes: []collections.QuantizedVectorIndexDefinition{{
            Name: "embedding.scalar_u8.fast", // codec/version default to scalar_u8 v1
        }, {
            Name:  "embedding.rabitq_1bit.fast",
            Codec: "rabitq_1bit",
            Version: 1,
        }},
    }},
}
```

Ownership rules:

- `embedding` has one authoritative owner: `typed_column_part`.
- The graph is a derived accelerator tied to the owner/generation.
- A quantized score plane is also derived state. It can be selected only through
  explicit quantized query modes and must not mask stale/missing assets with an
  exact fallback.
- The retained document can still hold source text or metadata, but do not treat
  a retained duplicate embedding as the search source of truth.

## Optional quantized query modes

The default/zero query mode is exact and preserves current prepared float32
scoring. To use a declared `scalar_u8`, `rabitq_1bit`, or prototype `brq_1bit`
score plane, select it explicitly:

```go
estimated, err := searcher.Search(collections.VectorIndexSearcherSearchOptions{
    Query:              query,
    QueryMode:          collections.VectorIndexQueryModeQuantizedOnly,
    QuantizedIndexName: "embedding.scalar_u8.fast",
    TopK:               10,
    EfSearch:           128,
})

reranked, err := searcher.Search(collections.VectorIndexSearcherSearchOptions{
    Query:                     query,
    QueryMode:                 collections.VectorIndexQueryModeQuantizedRerank,
    QuantizedIndexName:        "embedding.scalar_u8.fast",
    QuantizedRerankCandidates: 32,
    TopK:                      10,
    EfSearch:                  128,
})
```

`quantized_only` returns estimated selected-codec scores and should report
`search_route_quantized_only=1`, `quantized_scorer_active=1`, and zero exact
vector/norm reads in stats. `quantized_rerank` reports
`search_route_quantized_rerank=1`, keeps quantized traversal over the normalized
`ef_search` pool, trims to `QuantizedRerankCandidates`, exact-reranks that
shortlist, and returns exact cosine scores. See
[`quantized-vector-index.md`](../spec/quantized-vector-index.md),
[`rabitq-closeout-2454.md`](../spec/rabitq-closeout-2454.md), and
[`vector-search-closeout-2483.md`](../spec/vector-search-closeout-2483.md) for
benchmark commands, RaBitQ/BRQ storage/recall evidence, and current no-speedup /
crossover-pending caveats.

## Retained-payload final-fetch policy

For vector-heavy responses, start with `ColumnRetainedPayloadNonColumn`. It keeps
residual/non-column fields in the primary retained payload, reconstructs declared
typed fields from typed storage when callers need full documents, and avoids
storing a second copy of declared typed fields such as `embedding` in the primary
row value. This is the storage-efficient full-document path.

For projection-oriented responses, also use `ColumnRetainedPayloadNonColumn` and
apply the opt-in `ProjectionOrientedVectorDocumentFetchPreset` helper. The preset
sets `IncludeDocuments=true` and configures `DocumentFetchOptions` to exclude the
vector field declared by `VectorIndexDefinition.Field` (for example
`ExcludePaths: []string{"embedding"}`) when the response does not return
embeddings. This is the post-#1875/#1903 baseline and is the recommended default
starting point for vector search APIs that fetch top-k documents but normally
suppress the vector payload.

```go
fetchPreset, err := collections.ProjectionOrientedVectorDocumentFetchPreset(def)
if err != nil {
    // Current projection supports top-level JSON vector fields only.
    return err
}
opts := collections.VectorIndexSearcherSearchOptions{
    Query:    query,
    TopK:     10,
    EfSearch: 128,
}
fetchPreset.ApplyToSearcherSearchOptions(&opts)
response, err := searcher.Search(opts)
```

Use `ProjectionOrientedVectorDocumentFetchPresetForField` when the vector field
path comes from a custom API surface rather than an already-loaded
`VectorIndexDefinition`.

`ColumnRetainedPayloadFull` is supported for latency-oriented compatibility: the
full retained document can be fetched directly from the primary root/value-log
record without typed-storage JSON reconstruction. It duplicates storage with the
typed assets, so choose it only after local post-projection benchmarks prove it
wins for a workload that really needs full documents. Full-retained projection is
not a cheap projection path: it still fetches and decodes the full retained JSON
before applying the projection.

`ColumnRetainedPayloadNone` is storage-minimal but should be used only when
non-column retained fields are not needed. Final documents reconstructed under
this policy omit residual fields such as source body text or display-only tags.

Account for bytes and write amplification explicitly when comparing policies:
input document bytes, retained-payload bytes, typed-column/typed-row asset bytes,
graph asset bytes, total DB directory bytes, and write amplification
(`db_dir_B_total / input_doc_B_total`). Value-log pointers referenced by primary
rows are persistent storage managed by reachability, GC, and rewrite/compaction;
they are not ephemeral WAL records or transient large-value records. See the
[value-log lifecycle spec](../spec/value-log-lifecycle.md) for storage-lifetime
rules.

Recommended retained-payload policy matrix command:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkOpenVectorIndexSearcherColumnGraphRetainedPayloadPolicy1876$' \
  -benchmem \
  -benchtime=500ms \
  -count=5
```

Focused helper-route validation for the #1903 preset:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkOpenVectorIndexSearcherProjectionOrientedFetchPreset1903$' \
  -benchmem \
  -benchtime=500ms \
  -count=5
```

The benchmark opens/builds the fixture and reusable searcher outside the timed
loop, then times steady-state search plus top-k document fetch. Report
`doc_fetch_ns/search`, typed-column counters, JSON reconstruction counters,
`output_B/search`, retained/input/storage byte counters, and
`write_amplification`. The #1876 retained-payload policy matrix did not justify
making full-retained payloads a new default latency preset: non-column plus
embedding exclusion was fastest among document-producing modes, while
full-retained full documents reduced allocations but had higher `ns/op` and
duplicated storage.
The matrix should be rerun after document reconstruction optimizations land;
[#1887](https://github.com/snissn/gomap/issues/1887) remains a pending follow-up
before making stronger default recommendations.

## Runnable smoke demo

Run a deterministic synthetic close/reopen demo:

```sh
go run ./cmd/treedb_column_graph_demo \
  -dir /tmp/treedb-column-graph-doc-smoke \
  -reset \
  -rows 64 \
  -dims 8 \
  -degree 4 \
  -top-k 5 \
  -ef-search 32 \
  -max-decoded-blocks 2
```

Expected output shape:

```text
TreeDB column_graph native-reader demo
db_dir=/tmp/treedb-column-graph-doc-smoke rows=64 dims=8 degree=4 top_k=5 ef_search=32
rebuild status=column_graph_loaded loaded=true reason=
search path=column_graph_native_reader status=column_graph_loaded loaded=true results=5 include_docs=false doc_projection=none
stats candidates=... edges=... row_fetches=0 cache_hits=0 cache_misses=0 decoded_blocks=0 granules_touched=0 physical_B=0 max_resident_B=0 docs_fetched=0 doc_output_B=0 doc_fields_skipped=0
result[0] id=doc-... ordinal=... score=...
```

Interpretation:

- `search path=column_graph_native_reader` means the native reader path was used.
- `docs_fetched=0` means search/scoring did not materialize final documents.
- `physical_B=0` and `row_fetches=0` on current healthy rebuilds mean search did
  not read legacy graph row payloads. Non-zero values indicate an explicit
  legacy compatibility path or a benchmark using old physical graph fixtures.
- Add `-include-docs` when you intentionally want projected final document
  fetch included; the demo applies
  `ProjectionOrientedVectorDocumentFetchPresetForField("embedding")`, so
  documents omit the embedding field and `docs_fetched` plus
  `doc_fields_skipped` should be non-zero. Add `-include-doc-embedding` only
  for explicit full-document/embedding-echo comparison runs.

## Dense-section microbenchmarks

Use this to isolate dense vector section scan/direct-view behavior from the
collection graph search layer:

```sh
go test -run '^$' \
  -bench 'BenchmarkTypedColumnVectorDense(DirectView|Section)Scan' \
  -benchmem \
  -benchtime=100x \
  -count=1 \
  ./TreeDB/internal/typedcolumn
```

Expected interpretation:

- Direct-view scan variants should report `0 B/op` and `0 allocs/op` in the core
  loop when the section is valid for direct view.
- Section/decode scan variants may allocate because they exercise safe decode
  paths.
- Direct views are valid only while the mappedresource handle is live and only
  after lifetime, range, checksum/integrity policy, endian/format, length, and
  alignment validation.

## Vector search benchmark tiers

Use the tier aliases below when you need a quick serial/parallel matrix with
clear timing boundaries. All six aliases use the same synthetic shape:
`rows=1024`, `dims=128`, `M=16`, `topK=10`, and `efSearch=128`. Setup,
fixture load, graph rebuild, searcher open, and warmup are outside the timed
loop; each timed operation is one no-document search.

```sh
GOMAXPROCS=8 go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkVectorSearch(CoreGraphSerialTypedColumn1961|CoreGraphParallelTypedColumn1961|PublicSearchSerialTypedColumn1961|PublicSearchParallelTypedColumn1961|ReusableBufferSerialTypedColumn1961|ReusableBufferParallelTypedColumn1961)$' \
  -benchmem \
  -benchtime=2s \
  -count=5
```

Report both `ns/op` and the explicit `ops/sec` metric emitted by the benchmark
helpers. For parallel benchmarks, `ops/sec` is the aggregate operation rate
implied by Go's parallel `ns/op` measurement. Always include `B/op`,
`allocs/op`, and the direct/fallback counters
(`adjacency_prepared_csr_mmap_direct/search`,
`adjacency_typed_list_mmap_direct/search`,
`adjacency_typed_list_scratch_decodes/search`, `norm_mmap_direct/search`,
`norm_scratch_decodes/search`, `vector_mmap_direct/search`, and
`vector_scratch_decodes/search`). Legacy aliases such as
`adjacency_mmap_direct/search` remain compatibility telemetry, not the primary
healthy-path adjacency proof; generic typed-list counters are fallback evidence
once prepared CSR adjacency is active.

| Tier alias | Canonical benchmark | Boundary |
| --- | --- | --- |
| `BenchmarkVectorSearchCoreGraphSerialTypedColumn1961` | `BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3` | Core reader `SearchCosine`; graph traversal/scoring/top-k only, no public response materialization. |
| `BenchmarkVectorSearchCoreGraphParallelTypedColumn1961` | `BenchmarkColumnVectorGraphNativeSearchCosineParallelTypedColumnV3` | Same core boundary with one reader/scratch per worker. |
| `BenchmarkVectorSearchPublicSearchSerialTypedColumn1961` | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4` | Existing public `VectorIndexSearcher.Search`; response-owned result and ID buffers; no documents. |
| `BenchmarkVectorSearchPublicSearchParallelTypedColumn1961` | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4` | Existing public `Search` with one opened searcher per worker; no documents. |
| `BenchmarkVectorSearchReusableBufferSerialTypedColumn1961` | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferV4` | Public no-document `SearchWithBuffer`; caller-owned reusable result/ID storage. |
| `BenchmarkVectorSearchReusableBufferParallelTypedColumn1961` | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferParallelV4` | Reusable-buffer path with one independent searcher and buffer per worker. |

Use the reusable-buffer tier only for no-document callers that can honor the
buffer lifetime contract. `VectorIndexSearcher.SearchWithBuffer` and
`Collection.SearchVectorIndexWithBuffer` reject `IncludeDocuments`; callers that
fetch documents should continue using `Search`/`SearchVectorIndex`, or run the
no-document search first and explicitly call
`CollectionReadView.FetchDocumentsForVectorIndexSearchResults` outside the ANN
hot path. In either split shape, report the document-fetch counters separately.
A `VectorIndexSearchBuffer` is not concurrency-safe, and returned `Results`/`ID`
slices are valid only until the same buffer is reused or reset.

Use the #2037 truth matrix when comparing legacy/direct graph-row controls,
current TVIS/base typed-column routing, and combined prepared typed-column rows
with stable labels:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphSearchTruthMatrix2037$' \
  -benchmem \
  -benchtime=500ms \
  -count=5
```

The truth-matrix row labels, final #2043 evidence table, and profile caveats are
specified in
[`typed-column-graph-search-benchmark-matrix.md`](../spec/typed-column-graph-search-benchmark-matrix.md).
As of #2043, healthy current-format `current_tvis_base_typed_column` rows select
the combined prepared view rather than an unprepared hot-loop source route. Treat
that as the admitted current-format route for architecture/readiness/correctness,
not proof that the prepared path beats the old legacy graph-row direct control:
the closeout run kept graph-only search zero-allocation and fallback-free, but
#2035 is not fully performance-satisfied. The final matrix legacy/current
`graph_only` rows are also not apples-to-apples storage-path evidence because
the legacy control visits 612 edges/search while current prepared rows visit 3340
edges/search (about 5.5x). The #2043 focused adjacency-access microbenchmark is
not an end-to-end search benchmark; it only shows prepared CSR adjacency access
is not the source of that mismatch. #1979 adds opt-in benchmark-debug
batchability/control-flow counters on the #2091 topology-parity fixture: the
bounded `ef_search=128` row's 612 visited edges are mostly already-visited
layer-0 skips over degree-16 neighbor tiles, while exact mode scores all 8192
candidates and visits 100748 edges/search. #2098 adds an opt-in prepared
single-part indexed-scoring fast path with scalar/default result-equivalence
coverage, but indexed scoring remains non-default until broader #2035 promotion
evidence justifies a runtime change. Use #1980 for frontier/top-k or
already-visited optimization if future profiles continue to justify it, and keep
#1977 normalized vectors deferred until a prototype beats raw vectors plus
inverse norms including storage/rebuild cost.

## USearch comparison boundary

See `vector-search-high-qps-collection-api.md` for the final collection API
boundary, route guardrails, and profile capture notes. Use
`scripts/bench_vector_search_compare.sh` for the optional external ANN baseline.
Its current production comparison benchmark is
`BenchmarkCollectionVectorUSearchProductionCompare`:

- `TreeDB_CollectionSearchVectorIndexNoDocsOneShot` times the public
  collection-level no-document convenience API. Exact healthy calls use the
  cached `hnsw_search_pack_v1` route with `open_searcher_calls/op=0` and
  `open_setup_in_timed_loop=0`, but still allocate response-owned result/ID
  storage. Use this row as the response-owned convenience route, not the
  zero-allocation target.
- `TreeDB_SearchWithBuffer` / `TreeDB_SearchWithBufferParallel` time persisted
  `column_graph` search through `Collection.OpenVectorIndexSearcher` and
  `VectorIndexSearcher.SearchWithBuffer`; setup, inserts, rebuild, open, and
  warmup are outside the timed loop. Parallel rows use one searcher and one
  `VectorIndexSearchBuffer` per worker.
- `TreeDB_CollectionSearchVectorIndexWithBuffer` times the collection-level
  caller-owned result-buffer seam on a warmed collection-owned prepared cache.
  It mirrors the `SearchWithBuffer` no-document route contract: exact mode,
  caller-owned buffer, no documents/projection, and the healthy
  `hnsw_search_pack_v1` route. Cache warmup/build happens outside the timed
  loop; report `open_searcher_calls/op=0`, `open_setup_in_timed_loop=0`,
  `response_owned_result_alloc/op=0`, and collection prepared-cache counters.
- `TreeDB_CollectionSearchVectorIndexWithDocumentsOneShot` is an explicit
  with-documents/materialization row. It reports `docs_fetched/search`, document
  bytes/output bytes, and document fetch sub-counters; do not include it in
  high-QPS no-document success claims.
- `USearch_Search` / `USearch_SearchParallel` time the pure in-memory USearch Go
  binding with cosine/f32 HNSW.
- Both sides use the same deterministic synthetic vector/query generator and the
  same docs, dims, `M`, `efConstruction`, `efSearch`, `topK`, and `-cpu` list.
  TreeDB vectors cross the collection JSON insert/rebuild boundary; USearch is
  built directly from generated float32 vectors, so describe that data boundary
  when publishing numbers.

Quick c=1/c=8 smoke (the script runs one `go test -cpu=<n>` block per
`CPU_LIST` entry so worker counts stay explicit):

```sh
TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64 \
  TREEDB_VECTOR_BENCH_EF_SEARCH=128 CPU_LIST=1,8 COUNT=1 BENCHTIME=1x \
  scripts/bench_vector_search_compare.sh
```

Older `BenchmarkCollectionVectorIndex*` and graph-only rows remain useful
historical controls, but they are not the current persisted production
no-document fast path. The response-owned
`TreeDB_CollectionSearchVectorIndexNoDocsOneShot` row should use the cached pack
route but still reports convenience-wrapper allocations; use the buffered rows
for zero-allocation high-QPS production claims. With-document one-shot rows still
include setup/open and document materialization cost per operation.

The broader legacy/canonical matrix remains useful when comparing with older
artifacts or when you also need one-shot open/setup names:

```sh
go test ./TreeDB/collections \
  -run '^$' \
  -bench 'Benchmark(ColumnVectorGraphNativeSearchCosineV3|ColumnVectorGraphNativeSearchCosineParallelV3|OpenVectorIndexSearcherColumnGraphNativeReaderSetupV6|OpenVectorIndexSearcherColumnGraphNativeReaderV4|SearchVectorIndexColumnGraphNativeReaderV4|SearchVectorIndexColumnGraphNativeReaderWithDocumentsExcludeEmbedding1875|SearchVectorIndexColumnGraphNativeReaderWithDocumentsV4)$' \
  -benchmem \
  -benchtime=500ms \
  -count=5
```

Read the benchmark names and row labels before comparing numbers:

| Benchmark category | What is timed |
| --- | --- |
| `OpenVectorIndexSearcher...Setup...` | Native-reader setup/open only; no search. |
| `ColumnVectorGraphNativeSearchCosine...` | Lower-level graph traversal/scoring/top-k over the physical reader. |
| `OpenVectorIndexSearcher...V4` | Reusable searcher steady-state query; setup/open outside timed loop. |
| `SearchVectorIndex...V4` | Public response-owned search; exact no-doc collection convenience rows use the cached pack route when healthy, while with-doc/unsupported one-shot rows include setup/open inside each operation. |
| `...ReusableBuffer...` | Opened public no-document search with caller-owned reusable response buffers. |
| `...WithDocumentsExcludeEmbedding1875` | Preferred projection-oriented final-fetch row: search plus post-top-k documents with the vector field excluded. |
| `...WithDocumentsV4` | Explicit full-document comparison row: search plus post-top-k documents including the vector field. |

Profile public response allocation and the reusable-buffer ceiling separately:

```sh
OUT=$(mktemp -d /tmp/treedb_vector_search_response_XXXXXX)
GOMAXPROCS=8 go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4$|^BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferV4$' \
  -benchmem -count=1 -benchtime=5s \
  -cpuprofile "$OUT/cpu.pprof" -memprofile "$OUT/mem.pprof"
go tool pprof -top -nodecount=40 "$OUT/cpu.pprof" > "$OUT/cpu_top.txt"
go tool pprof -top -alloc_space -nodecount=40 "$OUT/mem.pprof" > "$OUT/alloc_space_top.txt"
```

## Search/fetch timing boundary

Recommended service/query flow:

1. Build or load the vector graph for the current generation.
2. Open a reusable `OpenVectorIndexSearcher` per worker for steady-state queries.
3. Search/scoring phase returns top-k IDs and scores without full document fetch.
4. Fetch full documents only for the final top-k results when the caller needs
   them, either by setting `IncludeDocuments=true` on an explicitly labeled
   with-documents search row or by calling
   `CollectionReadView.FetchDocumentsForVectorIndexSearchResults` as a separate
   fetch/materialization phase.

Do not compare a reusable-searcher benchmark to a one-shot public API benchmark
without calling out setup/open cost. Do not compare a search-only benchmark to a
with-documents benchmark without reporting `docs_fetched/search` and allocation
counters.

## Current limitations

| Limitation | Status/link |
| --- | --- |
| Native vector graph reads from typed-column dense sections are landed for the current `column_graph` path, but broader vector product tuning remains pre-alpha. | [#1782](https://github.com/snissn/gomap/issues/1782), [column graph native vector search spec](../spec/column-graph-native-vector-search.md). |
| Certified all-layer `column_graph` adjacency direct sources are legacy compatibility only; #1989 quarantines them after #1987/#1988 moved adjacency publication/search to generic `uint32_list` vector-index state. Row-asset adjacency remains a legacy/corruption fallback, and dense fixed-degree compatibility remains a separate fallback path. | [#1989](https://github.com/snissn/gomap/issues/1989), [#1987](https://github.com/snissn/gomap/issues/1987), [#1988](https://github.com/snissn/gomap/issues/1988) |
| SIMD/vectorized dense-section kernels are follow-up optimization work. | [#1790](https://github.com/snissn/gomap/issues/1790) |
| Row+column COW maintenance uses shared reachability and active mappedresource pin protection for typed assets; vector graph bytes remain derived, not authoritative. | [#1788](https://github.com/snissn/gomap/issues/1788), parent [#1736](https://github.com/snissn/gomap/issues/1736), [maintenance spec](../spec/typed-asset-maintenance-1788.md) |
| Nullable/missing vector and adjacency typed-column support remains staged/fail-closed. | See typed-column adapter/spec caveats and follow-up roadmap. |
| Graph-search prepared-view admission is tiered by generic typed-column optimized-consumer capability. | See [typed-column optimized-consumer capabilities](../spec/typed-column-optimized-consumer-capabilities.md), [prepared graph-search runtime views](../spec/typed-column-graph-search-prepared-views.md), and the [#2044 admission table](../spec/typed-column-graph-search-admission.md); #2046 owns reusable direct-view certifiers. |
| `scalar_u8`, pure-Go `rabitq_1bit`, and prototype `brq_1bit` score planes are behavior/storage/recall evidence, not a speedup claim or universal replacement. | See the [#1926/#2454/#2481 quantized score-plane spec](../spec/quantized-vector-index.md), [RaBitQ closeout](../spec/rabitq-closeout-2454.md), and [#2483 vector closeout](../spec/vector-search-closeout-2483.md). PQ/OPQ/residual codecs, accelerated RaBitQ backends, batch/control-flow, and windowing optimizations are future work. |

## Best practices

- Store vector payloads in fixed-dimension `float32_vector` typed-column dense
  sections where possible.
- Keep one authoritative owner per vector field; graph/index data is derived.
- Keep source documents and metadata out of the search/scoring hot loop.
- Use retained document or typed-row metadata for final fetch and display data;
  use typed-column scalar metadata only when filters/scans justify it.
- Treat vector-index `row_refs` as the healthy ordinal-to-base-row mapping. Do
  not treat graph row ID scans as the target row-reference source.
- Treat vector-index `document_ids` bytes state as the healthy returned-ID
  source. Graph row ID bytes are compatibility fallback only and should be
  counted when used.
- Treat healthy current-format graph-search state as requiring the `mmap_direct`
  optimized-consumer tier from the typed-column capability matrix, the
  role-specific prepared runtime views in
  [`typed-column-graph-search-prepared-views.md`](../spec/typed-column-graph-search-prepared-views.md),
  and the readiness status in
  [`typed-column-graph-search-admission.md`](../spec/typed-column-graph-search-admission.md)
  unless #2044 explicitly admits a weaker tier with benchmark, allocation, and
  memory evidence.
- Prefer reusable searchers for serving throughput; use one-shot APIs when you
  intentionally want setup/open included.
- Run checkpoint/reopen demos when proving durable manifest/root discovery.
- Use stable row counts, dimensions, degree, top-k, `ef-search`, and random/data
  seeds when collecting profiles.
- Report `docs_fetched`, candidates/search, edges/search, physical bytes,
  `B/op`, and `allocs/op` with every vector benchmark summary.

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Demo fails to load column graph | Invalid dimensions, unsupported metadata, or command-WAL/format setup problem. | Use the exact demo command first; then change one flag at a time. |
| `docs_fetched` is non-zero in a search-only comparison | You included final document materialization. | Drop `-include-docs` or move document fetch into a separate benchmark row. |
| Search benchmark allocates heavily | You may be timing setup/open, public document materialization, or fallback decode. | Compare reusable-searcher vs one-shot names and inspect allocation profiles. |
| Results differ across branches | On-disk formats/APIs are pre-alpha. | Rebuild DB directories and rerun with the same rows/dims/degree/top-k/seed. |
| Adjacency typed-list counters show scratch decodes or legacy fallback | The vector-index state `uint32_list` direct source is missing, stale, disabled, or failed validation. Current healthy indexes fail closed when no legacy graph row asset is available; old fixtures may report explicit compatibility fallback. | Rebuild the graph so vector-index state owns certified `uint32_list` / `raw_uint32_offsets_list` adjacency assets. Treat old `column_graph` adjacency-source assets as compatibility-only, not a fix target. |
| `row_ref_vector_source_legacy_graph_ids` is non-zero | A legacy physical graph row asset was used to map graph ordinals to base typed-column rows because row-ref state was missing or stale. | Rebuild the graph so TVIS publishes `row_refs` assets; keep graph ID reads only as explicit compatibility fallback. |
| `result_id_graph_fallbacks` is non-zero | A legacy physical graph row asset supplied returned IDs because vector-index document-ID bytes state was missing or failed validation. Current healthy indexes fail closed instead of silently recanonicalizing graph row IDs. | Rebuild the graph so TVIS publishes `document_ids` bytes assets; inspect `result_id_state_validation_failures` for corrupt or stale state. |
