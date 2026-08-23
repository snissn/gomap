# One-call document ingestion

`Collection.IngestSources` composes chunking, embedding, and collection indexing for a batch of source documents. It is the smallest path from long text to searchable RAG children.

```go
import (
    "github.com/snissn/gomap/TreeDB/collections/chunking"
    "github.com/snissn/gomap/TreeDB/collections/embedding"
)

cfg := collections.IngestSourcesConfig{
    Chunking: chunking.Config{
        Strategy: chunking.StrategyFixedWindow,
        SizeUnit: chunking.SizeUnitRunes,
        Size: 512,
        Overlap: 64,
    },
    Embedding: embedding.Config{
        Provider: embedding.ProviderHashing,
        Dimensions: 256,
    },
    VectorIndexName: "embedding",
    TextField: "body",
    Concurrency: 4,
}
result, err := collection.IngestSources(ctx, sources, cfg)
```

The configured vector index must exist and its dimensions must exactly match the embedder configuration. Every source is validated and chunk-planned before mutation. Embedding also completes before that source is changed, so chunk or embed errors fail closed. A successful result contains one `Ingested` outcome per source, including deterministic child IDs (`<sourceID>#<ordinal>`).

## Failure and retry behavior

`*collections.IngestError` identifies the source ID, input index, and stage (`chunk`, `embed`, or `storage`); use `errors.As` and `errors.Is` to inspect it. The worker pool is bounded by `Concurrency` (zero defaults to four), and cancellation stops unstarted work while preserving completed sources.

Child deletion and insertion are separate collection mutation boundaries. A storage error after one boundary is therefore commit-ambiguous for that source; the source may still have its old children, its new children, or be between those states. Retry the same source to converge. Child IDs are deterministic, so retries do not duplicate children. The built-in test fault boundary runs before either mutation and leaves the source unchanged.

## Quick smoke path

Use the hashing embedder for deterministic local checks, then verify text, scalar, and vector queries against `result.Ingested[*].ChildIDs`. For durable evidence, call `Flush` or `Checkpoint`, close the database, reopen it, and repeat the index checks.

The focused benchmark is bounded to 10,000 documents and 256 dimensions:

```sh
go test ./TreeDB/collections -run '^$' -bench '^BenchmarkIngestSources10K$' -benchtime=1x -count=1
```

Measured on an Apple M3 (one iteration, hashing embedder, 256 dimensions):

| docs/sec | chunk wall share | embed wall share | index wall share | B/op | allocs/op |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 34.35 | 0.005753% | 0.008030% | 99.99% | 176,072,008,224 | 295,077,995 |

The storage/index share includes collection mutation and index maintenance; this is a before-state measurement, not a production capacity claim.
