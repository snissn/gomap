# Document Embedding Guide

TreeDB collections ship a pluggable embedder seam: a batch-first interface for
turning text into fixed-width float32 vectors, a provider registry, and a
deterministic reference embedder that keeps in-repo tests and benchmarks
hermetic (no network, no model downloads). RAG ingestion does not need to
hand-roll embedding or dimension checks.

TreeDB is **pre-alpha**: these APIs may change without migration guarantees.

## Package seam

`TreeDB/collections/embedding` is a pure, stdlib-only package.

```go
import "github.com/snissn/gomap/TreeDB/collections/embedding"

emb, err := embedding.DefaultRegistry().Create(embedding.Config{
    Provider:   embedding.ProviderHashing, // deterministic reference embedder
    Dimensions: 256,
})
vectors, err := emb.EmbedBatch(ctx, texts) // one vector per text, index-aligned
```

- **Batch-first.** `EmbedBatch` is the primary API: throughput on the
  ingestion hot path matters, and the bounded-concurrency runner
  (`embedding.RunBatch`) caps workers at `GOMAXPROCS`, the same write-domain
  parallelism the collection layer already uses. No unbounded fan-out.
- **Ordered and fail-closed.** Output is index-aligned to inputs; an empty
  batch is `embedding.ErrEmptyBatch`; a failure at any position — or of the
  context — fails the whole batch and returns no partial vectors.
- **Provider concurrency.** An arbitrary `Embedder` instance need not be
  thread-safe; `Collection.IngestSources` serializes `EmbedBatch` calls on its
  one shared instance even when source `Concurrency` is greater than one.
  Built-in `hashing` and `openai_compat` embedders hold only immutable config
  fields plus a concurrency-safe `*http.Client`, so they are safe for concurrent
  `EmbedBatch` calls across goroutines.
- **Registry.** Providers register a `Factory` by name
  (`Registry.Register`); unknown providers fail with
  `embedding.ErrUnknownProvider`. A factory error retains its cause and
  provider context. A factory returning a nil or typed-nil `Embedder` fails
  with `embedding.ErrInvalidEmbedder` before any method is invoked.
  `DefaultRegistry()` ships with both the deterministic reference provider
  (`embedding.ProviderHashing`) and the stdlib-only OpenAI-compatible HTTP
  provider (`embedding.ProviderOpenAICompat`) pre-registered.
- **Determinism.** The reference `hashing` embedder is a pure function of
  (text, config): FNV-1a feature hashing over whitespace-delimited tokens
  into a signed integer sketch, L2-normalized with IEEE-754 correctly rounded
  operations only. Identical inputs plus config produce bit-identical vectors
  on every platform and run. Committed parity fixtures
  (`testdata/hashing_parity.json`) pin this guarantee; regenerate only
  deliberately via `EMBEDDING_REGENERATE_FIXTURE=1 go test
  ./TreeDB/collections/embedding/ -run TestRegenerateParityFixture` and
  justify any fixture diff in review.
- **Degenerate inputs.** Text with no tokens yields the all-zero vector, not
  NaN.

## Ingest-path dimension gate

Dimension validation against the target vector index definition happens at
ingest time and fails closed **before any write lands**, mirroring the
chunked-ingest plan-before-mutation pattern:

```go
vectors, err := col.EmbedForIngest(ctx, "embedding", texts,
    embedding.Config{Provider: embedding.ProviderHashing, Dimensions: 256})
```

- Unknown vector index name → `collections.ErrIndexNotFound`.
- Config dimensions ≠ declared index dimensions, or created-provider
  dimensions ≠ config dimensions → `embedding.ErrDimensionMismatch` before
  any embedding work or write.
- Unknown provider → `embedding.ErrUnknownProvider`.
- Wrong output count or invalid vector shape, finiteness, or cosine magnitude
  → `embedding.ErrInvalidOutput`; a wrong vector width also matches
  `embedding.ErrDimensionMismatch`.
- Provider and cancellation causes remain available through `errors.Is`.
- `EmbedForIngest` performs no mutations of its own; a failed call leaves the
  collection untouched.

Callers that hold their own `Embedder` instance gate it with
`col.ValidateEmbedderForVectorIndex(indexName, emb)` before writing vectors
through any mutation path. Composing this with the chunked ingest seam
(chunk children, embed each child, write vectors) is the one-call ingestion
surface future work builds on.

## OpenAI-compatible HTTP provider (`openai_compat`)

`embedding.ProviderOpenAICompat` (`"openai_compat"`) calls any OpenAI-compatible
`/v1/embeddings` HTTP endpoint (OpenAI, Ollama, vLLM, llama.cpp server,
HuggingFace TEI, and compatible gateways) using only the Go standard library:

```go
emb, err := embedding.DefaultRegistry().Create(embedding.Config{
    Provider:   embedding.ProviderOpenAICompat,
    Dimensions: 768,
    Options: map[string]string{
        "base_url":           "http://127.0.0.1:8080/v1",
        "model":              "nomic-embed-text",
        "batch_size":         "64",
        "encoding_format":    "base64", // "float" (default, omitted on wire) or "base64"
        "max_context_tokens": "8192",   // optional positive int; exposed via MaxContextTokens()
    },
})
```

- **`encoding_format`**: defaults to `"float"` (omitted from the JSON request
  body for maximum server compatibility). Set `"base64"` to request compact
  little-endian IEEE-754 float32 payloads and avoid ASCII float formatting and
  parsing on both server and client. Decoding is strict in both directions
  (`ErrInvalidOutput` on format mismatch, invalid base64, non-multiple-of-4 byte
  length, NaN, or Inf; `ErrDimensionMismatch` on a valid multiple-of-4 float32
  count != configured `Dimensions`).
- **`max_context_tokens`**: optional positive integer metadata describing the
  model's context window, exposed via `(*OpenAICompatEmbedder).MaxContextTokens()`
  (`0` when unset).
- **Concurrency**: `*OpenAICompatEmbedder` stores only immutable configuration
  and a stdlib `*http.Client`, so callers may invoke `EmbedBatch` concurrently
  from multiple goroutines.

## Fast Native Vector Ingest & No-Vector Document Fetch

For collections with scalar indexes, text-v2 indexes, and a `column_graph`
float32 vector index, avoid serializing float32 vectors into JSON on ingest and
avoid reading or encoding `"embedding"` back out during search:

1. **Store non-column JSON with a dedicated vector column**: configure
   `ColumnStoreConfig` with `RetainedPayload: ColumnRetainedPayloadNonColumn`,
   `RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON`, and one
   `ColumnStoreValueFloat32Vector` column owned by `TypedStorageOwnerColumnPart`:

```go
meta := &collections.CollectionMeta{
    Name: "chunks",
    Options: collections.CollectionOptions{
        DocumentFormat:          collections.DocumentFormatJSON,
        AllowArrayValuesInIndex: true,
        ColumnStore: &collections.ColumnStoreConfig{
            Enabled:                 true,
            RetainedPayload:         collections.ColumnRetainedPayloadNonColumn,
            RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
            Columns: []collections.ColumnStoreColumn{{
                Name:       "embedding",
                Path:       "embedding",
                Owner:      collections.TypedStorageOwnerColumnPart,
                ValueType:  collections.ColumnStoreValueFloat32Vector,
                VectorDims: 768,
            }},
        },
    },
    Indexes: []collections.IndexDefinition{
        {Name: "kind", Field: "kind", ValueType: collections.IndexValueString},
        {Name: "rank", Field: "rank", ValueType: collections.IndexValueInt64},
    },
    TextIndexes: []collections.TextIndexDefinition{{
        Name:     "content_text",
        Fields:   []collections.TextIndexField{{Field: "content"}},
        Analyzer: collections.TextAnalyzerSimple,
    }},
    VectorIndexes: []collections.VectorIndexDefinition{{
        Name:       "embedding_graph",
        Field:      "embedding",
        Metric:     collections.VectorMetricCosine,
        Dimensions: 768,
        Strategy:   collections.VectorIndexStrategyColumnGraph,
    }},
}
```

2. **Insert raw `[][]float32` vectors alongside retained JSON**: pass
   `retainedDocs` (JSON objects **without** `"embedding"`, each containing a
   top-level `"id"` matching `string(ids[row])`) and raw `vecs` directly to
   `InsertBatchWithStatsValidatedFloat32Projection`. Non-overlapping scalar
   indexes and text-v2 indexes are extracted directly from `retainedDocs`, while
   `Collection.Get` continues to reconstruct the full document including
   `"embedding"`. Any scalar or text index overlapping the vector column (for
   example `"embedding"` or `"embedding.foo"`) fails closed before any write
   lands:

```go
// Each retainedDocs[i] MUST be a valid JSON object containing top-level
// "id" equal to string(ids[i]) and MUST NOT contain the "embedding" field.
_, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(
    ids, retainedDocs, "embedding", collections.VectorMetricCosine, vecs, retainedDocs,
)
```

3. **Exclude `"embedding"` when fetching search result documents**: use
   `DocumentFetchOptions{ExcludePaths: []string{"embedding"}}` — or
   `ProjectionOrientedVectorDocumentFetchPreset` for `SearchVectorIndex` — so
   `ColumnRetainedPayloadNonColumn` serves the retained JSON directly without
   reading vector column assets (`DocumentEmbeddingVectorReads == 0` /
   `EmbeddingOutputBytes == 0`):

```go
// Vector search via the projection-oriented preset:
preset, err := collections.ProjectionOrientedVectorDocumentFetchPreset(meta.VectorIndexes[0])
if err != nil {
    return err
}
vecOpts := collections.VectorIndexSearchOptions{
    IndexName: "embedding_graph",
    Query:     queryVec,
    TopK:      10,
    EfSearch:  64,
}
preset.ApplyToSearchOptions(&vecOpts)
vecResp, err := col.SearchVectorIndex(vecOpts)

// Hybrid or text search excluding "embedding":
noVecFetch := collections.DocumentFetchOptions{ExcludePaths: []string{"embedding"}}
hybridResp, err := col.SearchHybrid(collections.HybridSearchOptions{
    TopK:                 10,
    Text:                 &collections.HybridTextQuery{IndexName: "content_text", Query: query},
    Vector:               &collections.HybridVectorQuery{IndexName: "embedding_graph", Query: queryVec, EfSearch: 64},
    IncludeDocuments:     true,
    DocumentFetchOptions: noVecFetch,
})
textResp, err := col.SearchText(collections.TextSearchOptions{
    IndexName:            "content_text",
    Query:                query,
    TopK:                 10,
    IncludeDocuments:     true,
    DocumentFetchOptions: noVecFetch,
})
```

## Error reference

| Error | Meaning |
|-------|---------|
| `embedding.ErrUnknownProvider` | Config names no registered provider |
| `embedding.ErrProviderAlreadyRegistered` | Duplicate `Registry.Register` name |
| `embedding.ErrInvalidEmbedder` | Factory returned a nil or typed-nil embedder |
| `embedding.ErrEmptyBatch` | `EmbedBatch` called with zero texts |
| `embedding.ErrDimensionMismatch` | Config, provider, target index, or output dimensions disagree |
| `embedding.ErrInvalidOutput` | Batch count or vector metric/shape/finiteness contract failed |
| `collections.ErrIndexNotFound` | Embed request names no vector index on the collection |

All are tested with `errors.Is`; wrappers add positional context.

## Throughput baseline

Measured on Apple M3 (darwin/arm64, go1.26), 10,000 documents of ~40 tokens
each, `hashing` provider at 256 dimensions, full corpus per iteration (see
`BenchmarkHashingEmbedBatch10kDocs`):

| Provider | docs/sec | B/op     | allocs/op |
|----------|----------|----------|-----------|
| hashing (256 dims) | ~1.2M | ~52 MB | ~50k |

These numbers are the baseline that performance follow-ups and future
real-model providers improve against.
