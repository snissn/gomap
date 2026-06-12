# TreeDB Hybrid Search Guide

TreeDB hybrid search is the pre-alpha collection API that combines indexed
lexical candidates, vector candidates, optional indexed scalar filters,
deterministic rank fusion, and bounded final document fetch.

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
| Hybrid (`SearchHybrid` with `Text` + `Vector`) | You need both lexical precision and semantic candidates, usually with a metadata/scalar filter and final materialized documents. | Default fusion is rank-based RRF, not learned relevance. Caller/future layers own reranking/cross-encoder/LLM scoring. |

Hybrid candidate generation must not fetch full documents. Text candidates are
score-only by default; set `HybridTextQuery.IncludeTextMatches=true` only when a
bounded compact field/term summary is needed. Documents are fetched only after
fusion/filtering and are bounded by final `TopK` when `IncludeDocuments` is true.

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

```go
resp, err := col.SearchHybrid(collections.HybridSearchOptions{
    TopK: 10,
    Text: &collections.HybridTextQuery{
        IndexName: "lexical",
        Query: "refund policy",
        CandidateLimit: 64,
    },
    Vector: &collections.HybridVectorQuery{
        IndexName: "embedding_graph",
        Query: queryEmbedding,
        CandidateLimit: 64,
        EfSearch: 128,
        QueryMode: collections.VectorIndexQueryModeExact,
    },
    ScalarFilter: &collections.HybridScalarFilter{
        IndexName: "tenant",
        Value: "acme",
    },
    Fusion: collections.HybridFusionOptions{
        Method: collections.HybridFusionMethodRRF,
        RRFK: 60,
    },
    IncludeDocuments: true,
    DocumentFetchOptions: collections.DocumentFetchOptions{
        ExcludePaths: []string{"embedding"},
    },
})
```

Inspect `resp.Plan` and `resp.Stats` for the actual scalar strategy, candidate
budgets, postings/vector counters, fusion counts, final document fetch count,
fail-closed reason, fallback count, and truncation signal.

## Caveats and evidence links

- Score fusion is deterministic reciprocal-rank fusion. It does not normalize or
  learn across BM25F/vector score scales.
- Text analyzer/query limits come from #1764: current `simple` analyzer,
  whitespace terms plus `AND`/`OR`; no phrase/proximity/fuzzy/trigram support.
- Vector route evidence remains separate. #2490/#2492/#2493/#2494 provide
  exact/scalar_u8/RaBitQ/USearch/pgvector context; do not present those
  vector-only rows as hybrid executor performance.
- Reranking is a caller/future-layer seam. TreeDB does not run cross-encoders,
  LLM rerankers, or learned fusion in this implementation.
- Reproduce current hybrid benchmark rows with
  [`../../../docs/benchmarks/treedb_hybrid_search_runbook.md`](../../../docs/benchmarks/treedb_hybrid_search_runbook.md).
