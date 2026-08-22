# Document Chunking Guide

TreeDB collections ship a built-in chunking seam: a deterministic chunker
package plus a parent/child ingest lifecycle over the collection API. RAG
ingestion does not need to hand-roll splitting, and re-chunking an updated
source is maintained correctly across text, scalar, and vector indexes.

TreeDB is **pre-alpha**: these APIs may change without migration guarantees.

## Package seam

`TreeDB/collections/chunking` is a pure, stdlib-only package.

```go
import "github.com/snissn/gomap/TreeDB/collections/chunking"

cfg := chunking.Config{
    Strategy: chunking.StrategyFixedWindow, // or chunking.StrategyRecursive
    SizeUnit: chunking.SizeUnitRunes,
    Size:     512,
    Overlap:  64,
}
chunks, err := chunking.SplitChunks("parent-doc-id", sourceText, cfg)
```

- **Strategies.** `fixed_window` slices text into overlapping `Size`-rune
  windows. `recursive` splits on structural separators (paragraph breaks, line
  breaks, sentence ends, spaces — see `chunking.DefaultSeparators()`), keeps
  separator-bounded units whole whenever they fit, and falls back to overlapped
  hard splits for oversized units.
- **Size unit.** Runes only (`SizeUnitRunes`), and this is explicit in the
  config. Token-based sizing would require a tokenizer dependency and a
  model-bound definition of "token"; rune counts are deterministic across
  platforms, which the determinism contract requires.
- **Validation fails closed.** Unknown strategy/unit, non-positive size, and
  any `overlap` outside `[0, size)` are errors before any work happens.
- **Determinism.** `SplitChunks` is a pure function of (parent ID, text,
  config): the same inputs always produce an identical stream (IDs, ordinals,
  offsets, text). Golden fixtures can hash chunk streams and compare across
  runs.
- **Coverage.** Every rune offset of the input appears in at least one chunk;
  every chunk is at most `Size` runes; consecutive chunks share exactly
  `Overlap` trailing/leading runes; empty text yields no chunks.

## Linkage convention

Each child document gets a stable derived ID:

```
<parentID>#<ordinal>        e.g. "paper-17#3"
```

and carries three metadata fields mirroring the document-service
`meta.chunk_*` conventions:

| Field           | Value                          |
|-----------------|--------------------------------|
| `chunk_parent`  | parent document ID             |
| `chunk_ordinal` | zero-based position            |
| `chunk_kind`    | `"chunk"`                      |

Metadata parsing is fail-closed: a stored document with *partial* or
ill-typed chunk metadata is rejected rather than silently indexed, and a child
whose ID does not match its own `<parent>#<ordinal>` metadata fails closed.
Documents without any chunk metadata are ordinary documents.

## Ingest lifecycle

```go
res, err := col.IngestChunkedDocument(parentID, parentDocJSON,
    chunking.Config{Strategy: chunking.StrategyFixedWindow,
        SizeUnit: chunking.SizeUnitRunes, Size: 512, Overlap: 64},
    collections.ChunkedIngestOptions{}) // TextField defaults to "body"
```

- The child plan is built and validated before any mutation: invalid config,
  a missing text field, or chunker failure leaves the collection untouched.
- The parent is upserted, stale children are tombstoned with one atomic
  `DeleteBatch`, then replacements are inserted with one atomic `InsertBatch`.
- Child IDs derive from parent ID + ordinal (never from content), so retrying
  an interrupted ingest converges instead of orphaning rows. A crash cannot
  tear an individual child row (batch atomicity) and never half-replaces a
  child document.
- Children are ordinary documents to the index layer: text, scalar, and vector
  indexes maintain them through the normal batch paths and resolve only live
  children after a re-chunk.

`ChunkChildren(parentID)` lists live children in ordinal order;
`ValidateChunkChildDocument(id, doc)` is the exported fail-closed guard for
callers storing their own chunk rows.

## Throughput baseline

Measured on Apple M3 (darwin/arm64, go1.26), ~1.5 KB prose documents with
paragraph structure, 512/64 size/overlap, full 10k-document corpus per
iteration (see `BenchmarkChunkFixedWindow10K` /
`BenchmarkChunkRecursive10K`):

| Strategy      | docs/sec | B/op       | allocs/op |
|---------------|----------|------------|-----------|
| fixed_window  | ~280k    | ~55.6 MB   | ~99.9k    |
| recursive     | ~81k     | ~60.3 MB   | ~197.4k   |

These numbers are the baseline that performance follow-ups improve against.
