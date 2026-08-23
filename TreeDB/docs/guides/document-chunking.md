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
  windows. For each recursive window, `recursive` selects the furthest
  occurrence of the first configured separator that permits forward progress.
  When no occurrence permits forward progress, it tries the next separator.
  An empty separator immediately hard-splits that window, so entries after
  `""` are unreachable. When all configured separators fail, the window
  hard-splits at the size boundary.
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
- **Coverage.** Every rune offset of the input appears in at least one
  non-empty chunk; every chunk is at most `Size` runes; consecutive chunks
  share exactly `Overlap` trailing/leading runes on fixed, separator, and hard
  split paths. Empty text yields no chunks and trailing separators never emit
  an empty chunk.

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

Parent IDs must be non-empty valid UTF-8 and must not contain `#`. Rejection is
a typed `*chunking.ParentIDError` before mutation. This minimal policy keeps the
parent namespace disjoint from every child ID and preserves `chunk_parent`
losslessly in JSON; arbitrary non-UTF-8 byte IDs are unsupported rather than
lossily converted. `chunk_parent`, `chunk_ordinal`, and `chunk_kind` are
reserved linkage roots and cannot be selected as chunk text or vector
destinations.

## Ingest lifecycle

```go
res, err := col.IngestChunkedDocument(parentID, parentDocJSON,
    chunking.Config{Strategy: chunking.StrategyFixedWindow,
        SizeUnit: chunking.SizeUnitRunes, Size: 512, Overlap: 64},
    collections.ChunkedIngestOptions{}) // TextField defaults to "body"
```

- Parent-ID, text-field, configuration, and the complete child plan are
  validated before mutation.
- One shared per-parent lifecycle lock covers plan through replacement across
  collection handles and ingestion calls. Different parents do not share that
  lock; the collection's index publication seam may still serialize their
  brief mutation sections.
- `IngestChunkedDocument` publishes the parent upsert, stale-child
  `DeleteBatch`, and replacement `InsertBatch` as three separate durable
  boundaries. `IngestSources` publishes delete, insert, then parent upsert.
  Each individual batch is atomic, but the complete lifecycle is **not**:
  an error can report a source whose durable state is old, new, or between
  those boundaries. Retry the same deterministic ingest to converge. Atomic
  durable publication is owned by
  [#4284](https://github.com/snissn/gomap/issues/4284).
- Children are ordinary documents to the index layer: text, scalar, and vector
  indexes maintain them through the normal batch paths and resolve only live
  children after a successful re-chunk.

`ChunkChildren(parentID)` lists and validates the contiguous live ordinals.
`ChunkChildrenWithStats` also returns `ScannedPrimaryRows`,
`ReconstructedDocuments`, `RowLocatorLookups`, and `PointRowFetches`. Both
ordinary JSON and reconstructable column-store layouts scan only the bounded
`<parentID>#` primary-key range; column layouts batch row-locator lookup and
reconstruct only those matched rows. Truncation, malformed or mismatched
linkage, namespace-shaped ordinary rows, duplicate ordinals, and ordinal gaps
fail closed. `ValidateChunkChildDocument(id, doc)` is the exported linkage
guard for callers storing their own chunk rows.

## Throughput baseline

Measured on Apple M3 (darwin/arm64, Go 1.26.0), ~1.5 KB prose documents
with paragraph structure, 512/64 size/overlap, and one full 10k-document corpus
per iteration (`-benchtime=1x -count=5`; table reports medians):

| Strategy      | docs/sec | B/op       | allocs/op |
|---------------|---------:|-----------:|----------:|
| fixed_window  | ~813k    | ~43.8 MB   | ~69.9k    |
| recursive     | ~241k    | ~48.8 MB   | ~109.9k   |

These numbers are the baseline that performance follow-ups improve against.
