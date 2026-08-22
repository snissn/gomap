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
- **Registry.** Providers register a `Factory` by name
  (`Registry.Register`); unknown providers fail with
  `embedding.ErrUnknownProvider`. `DefaultRegistry()` ships with the built-in
  reference provider pre-registered; future real-model providers register
  there or in caller-owned registries.
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

- Unknown vector index name → `collections: unknown vector index` (typed).
- Config dimensions ≠ declared index dimensions →
  `embedding.ErrDimensionMismatch` before any embedding work.
- Unknown provider → `embedding.ErrUnknownProvider`.
- `EmbedForIngest` performs no mutations of its own; a failed call leaves the
  collection untouched.

Callers that hold their own `Embedder` instance gate it with
`col.ValidateEmbedderForVectorIndex(indexName, emb)` before writing vectors
through any mutation path. Composing this with the chunked ingest seam
(chunk children, embed each child, write vectors) is the one-call ingestion
surface future work builds on.

## Error reference

| Error | Meaning |
|-------|---------|
| `embedding.ErrUnknownProvider` | Config names no registered provider |
| `embedding.ErrProviderAlreadyRegistered` | Duplicate `Registry.Register` name |
| `embedding.ErrEmptyBatch` | `EmbedBatch` called with zero texts |
| `embedding.ErrDimensionMismatch` | Declared dimensions disagree (config vs provider, or embedder vs vector index) |
| `collections: unknown vector index` | Embed request names no vector index on the collection |

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
