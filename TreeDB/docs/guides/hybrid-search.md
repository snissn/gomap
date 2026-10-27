# TreeDB Hybrid Search Guide

TreeDB hybrid search is the pre-alpha collection API that combines indexed
lexical candidates, vector candidates, supported indexed scalar filters,
deterministic rank fusion, and bounded final document fetch.

`column_graph` is the default and only vector serving strategy. Ordinary Hybrid
with both a vector source and a scalar filter is unavailable and fails closed;
unfiltered vector-bearing Hybrid and filtered text-only retrieval remain
supported. Admitted typed ColumnGraph retains the selected filtering and
quantized-rerank paths below. Explicit ordinary filtered exact dense search is a
separate API and never an automatic Hybrid fallback.

Current entry point:

```go
Collection.SearchHybrid(HybridSearchOptions)
```

See the canonical contract in
[`../spec/hybrid-search-contract.md`](../spec/hybrid-search-contract.md). Text
index storage/query behavior is owned by
[`../spec/collection-text-search.md`](../spec/collection-text-search.md), and
vector typed-column placement is covered by
[`vector-search-typed-column.md`](vector-search-typed-column.md).

## When to use each mode

| Mode | Use when | Boundary/caveat |
| --- | --- | --- |
| Text-only (`SearchText`) | Keyword, metadata-like text, exact terms, or BM25F ranking are the main signal. | Current analyzer is `simple`; no phrase/proximity/stemming/trigram/fuzzy/highlighting. |
| Vector-only (`SearchVectorIndex` / `SearchHybrid` with only `Vector`) | Semantic nearest-neighbor recall is the main signal. | Keep vector route choices (`exact`, `quantized_only`, `quantized_rerank`) and their recall/storage caveats separate from hybrid claims. |
| Hybrid (`SearchHybrid` with `Text` + `Vector`) | You need lexical precision, semantic candidates and final materialized documents. | Ordinary queries must be unfiltered; selected typed filtering requires explicit admission. Default fusion is rank-based RRF. Caller/future layers own learned relevance and reranking. |

Hybrid candidate generation must not fetch full documents. Newly-created text indexes use text-v2 by default; set `TextIndexDefinition.Version: TextIndexVersionV1` only for the legacy compatibility path. Text candidates are score-only by default; set `HybridTextQuery.IncludeTextMatches=true` only when a bounded compact field/term summary is needed. `QueryMode: TextSearchQueryModeLiteral` is the natural-language path: quotes, parentheses, and standalone `and`/`or` are analyzer input rather than syntax. `Operator` still chooses OR/AND across analyzed terms, and `MaxPostingsScanned` is one fail-closed allowance across scan, fallback, and attribution. Use `ResultMode` to choose `score_only`, `compact`, or `full`; documents are fetched only in full mode (or legacy `IncludeDocuments=true`) after fusion/filtering and are bounded by final `TopK`.

For explicitly admitted typed `column_graph` collections, hybrid vector
candidates use the same captured base and current mutation overlay as ordinary
vector queries. Keyword postings and combined hybrid results reflect typed
updates, deletes, and inserts before fold; after reopen, explicitly ensure graph
serving again. The document service supports declared string EQ/AND filters and
full current payloads, including requested embeddings, on this selected lane.
Keyword queries do not require graph admission. Hybrid scalar admission and
typed graph filter/work limits both apply; prefilter runs within the vector
source, while postfilter keeps its existing position after source ranking.
Candidate generation uses production statistics and fetches no documents.
These are correctness contracts, separate from dense-query performance results.

Hybrid vectors default to FP32 scoring on graph search; this is separate from
exhaustive document-scan exact search. To use scalar-u8 candidates plus
canonical FP32 reranking, declare and admit a typed cosine `column_graph` with a
legacy scalar-u8/v1 plane, then set `QueryMode: VectorIndexQueryModeQuantizedRerank`,
and `QuantizedIndexName`. `QuantizedRerankCandidates` is optional: zero uses the
effective traversal width, while a nonzero width must be at least the effective
vector candidate limit. Selected queries keep fixed source budgets. A small
complete scalar filter may execute `typed_exact`, while an empty filter performs
no vector traversal. Both cases still validate the named asset. Missing/stale
assets, `quantized_only`, and unsupported codec/representation combinations fail
closed without an exact retry.

The selected path retains one read owner through scalar/text work, fusion,
parent collapse, and final fetch. Its compact route receipt reports actual
`typed_hnsw` or `typed_exact` work plus SQ8/packed-rerank counters. Excluding the
embedding from final documents avoids output-vector reads and bytes; FP32 reads
for reranking remain expected.

## Index creation sketch

```go
meta := &collections.CollectionMeta{
    Name: "docs",
    Options: collections.CollectionOptions{
        DocumentFormat: collections.DocumentFormatJSON,
        ColumnStore: &collections.ColumnStoreConfig{
            Enabled: true,
            RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
            Columns: []collections.ColumnStoreColumn{
                {
                    Name: "embedding", Path: "embedding",
                    Owner: collections.TypedStorageOwnerColumnPart,
                    ValueType: collections.ColumnStoreValueFloat32Vector,
                    VectorDims: 768,
                },
            },
        },
    },
    Indexes: []collections.IndexDefinition{
        {Name: "tenant", Field: "tenant", ValueType: collections.IndexValueString},
        {Name: "workspace", Field: "workspace", ValueType: collections.IndexValueString},
        {Name: "created_at", Field: "created_at", ValueType: collections.IndexValueInt64},
    },
    TextIndexes: []collections.TextIndexDefinition{
        {
            Name: "lexical",
            Fields: []collections.TextIndexField{
                {Field: "title", Weight: 3},
                {Field: "body"},
            },
            StorePositions: true,
        },
    },
    VectorIndexes: []collections.VectorIndexDefinition{
        {
            Name: "embedding_graph", Field: "embedding",
            Metric: collections.VectorMetricCosine,
            Dimensions: 768, M: 16,
            Strategy: collections.VectorIndexStrategyColumnGraph,
        },
    },
}
```

For existing documents, create/backfill indexes or rebuild the vector index after
inserting fixture data. For new code paths, keep vector payloads in typed-column
storage and keep retained payload/projection settings aligned with whether final
responses should echo embeddings.

## Query sketch

The ordinary collection declared above supports an unfiltered mixed query:

```go
resp, err := col.SearchHybrid(collections.HybridSearchOptions{
    TopK: 10,
    Text: &collections.HybridTextQuery{
        IndexName: "lexical",
        Query: "refund policy",
        QueryMode: collections.TextSearchQueryModeLiteral,
        Operator: collections.TextSearchOperatorAND,
        CandidateLimit: 64,
        MaxPostingsScanned: 4096,
    },
    Vector: &collections.HybridVectorQuery{
        IndexName: "embedding_graph",
        Query: queryEmbedding,
        CandidateLimit: 64,
        EfSearch: 128,
    },
    Fusion: collections.HybridFusionOptions{
        Method: collections.HybridFusionMethodRRF,
        RRFK: 60,
        // Optional alternatives:
        // Method: collections.HybridFusionMethodWeightedRRF,
        // TextWeight: 1.2, VectorWeight: 0.8,
        // Method: collections.HybridFusionMethodNormalizedScore,
    },
    ResultMode: collections.HybridResultModeFull,
    DocumentFetchOptions: collections.DocumentFetchOptions{
        ExcludePaths: []string{"embedding"},
    },
})
```

For an already declared and admitted typed collection with the named scalar-u8
plane, the selected filtered quantized query remains supported. Its typed
strings, scalar indexes, text index and positive serving limits must be set up
through the [typed lifecycle](vector-search-typed-column.md#explicit-mutable-serving-lifecycle);
adding these options to the ordinary collection above does not admit it:

```go
resp, err := typedCol.SearchHybrid(collections.HybridSearchOptions{
    TopK: 10,
    Text: &collections.HybridTextQuery{
        IndexName: "lexical", Query: "refund policy",
        CandidateLimit: 64, MaxPostingsScanned: 4096,
    },
    Vector: &collections.HybridVectorQuery{
        IndexName: "embedding_graph", Query: queryEmbedding,
        CandidateLimit: 64, EfSearch: 128,
        QueryMode: collections.VectorIndexQueryModeQuantizedRerank,
        QuantizedIndexName: "embedding.scalar_u8.fast",
        QuantizedRerankCandidates: 64,
    },
    ScalarFilter: &collections.HybridScalarFilter{
        And: []collections.HybridScalarFilter{
            {IndexName: "tenant", Value: "acme"},
            {IndexName: "workspace", Value: "support"},
        },
    },
    ResultMode: collections.HybridResultModeFull,
    DocumentFetchOptions: collections.DocumentFetchOptions{
        ExcludePaths: []string{"embedding"},
    },
})
```

For supported text-only and admitted typed paths, the collection grammar is
deliberately small: use a single equality/range filter, or one flat `And`
containing 2..16 equality or
one/two-sided range leaves. The executor resolves every leaf through its declared
scalar index, stable-sorts complete ID sets by cardinality, and intersects
smallest-first. `OR`, `NOT`, `!=`, membership, nested `And`, primary-document
scans, and partial results are not supported. Empty intersections skip source
work; a missing/corrupt index, truncation, or snapshot change fails closed.

Inspect `resp.Plan` for scalar lookup count/per-lookup/aggregate bounds. Inspect
`resp.Stats` for scalar lookups, input IDs, intersection steps, final IDs, source
candidate work, fusion counts, final document fetch count, fail-closed reason,
fallback count, and truncation signal.

## Caveats and evidence links

- Default score fusion is deterministic reciprocal-rank fusion. Weighted RRF and
  exact per-source normalized-score fusion are available, but TreeDB does not
  learn relevance weights or estimate scores for candidates outside the requested
  source budgets.
- Text analyzer/query limits come from #1764: current `simple` analyzer,
  whitespace terms plus `AND`/`OR`; no phrase/proximity/fuzzy/trigram support.
- Vector route evidence remains separate. #2490/#2492/#2493/#2494 provide
  exact/scalar_u8/RaBitQ/USearch/pgvector context; do not present those
  vector-only rows as hybrid executor performance.
- Reranking is a caller/future-layer seam. TreeDB does not run cross-encoders,
  LLM rerankers, or learned fusion in this implementation.
- Reproduce current hybrid benchmark rows with
  [`../../../docs/benchmarks/treedb_hybrid_search_runbook.md`](../../../docs/benchmarks/treedb_hybrid_search_runbook.md).
