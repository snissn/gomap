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
- **Boundary-aware recursive options (opt-in).** Both options default to exact
  legacy behavior and are valid only for `StrategyRecursive` (`Validate` fails
  closed if enabled on `StrategyFixedWindow` or if `SeparatorPlacement` is
  unrecognized):
  - `SeparatorPlacement`: `""` or `SeparatorPlacementEnd` (default) keeps every
    matched separator at the end of chunk `N`. `SeparatorPlacementStart` keeps
    delimiter-only separators (whitespace and ASCII sentence/clause punctuation
    `.`, `!`, `?`, `;`, `,` such as `"\n\n"`, `"\n"`, `" "`, `". "`) attached
    to the end of chunk `N`, while attaching structural/prefix separators (such
    as `"\n## "`, `"\nfunc "`, `"\ndef "`, `"\n\\section"`, `"## "`) to the
    start of chunk `N+1` (keeping a leading `'\n'` at the end of chunk `N`).
  - `SnapOverlapToBoundary`: when `true` and `Overlap > 0`, snaps the raw
    overlap start (`end - Overlap`) forward within `[end-Overlap, end)` to a
    clean line boundary (`'\n'`) first, or else a whitespace word boundary,
    preserving a raw start that already sits immediately after `'\n'` or
    whitespace and falling back to `end - Overlap` for unbroken tokens.
  - **Format-aware separator presets:** in addition to `DefaultSeparators()`,
    the package provides `MarkdownSeparators()`, `CodeSeparators()`, and
    `LaTeXSeparators()` (each returning a fresh slice ending with the `""`
    hard-split sentinel). Pair them with `SeparatorPlacementStart` so headings,
    declarations, and LaTeX sections stay attached to their own chunks.
- **Size unit.** Runes only (`SizeUnitRunes`), and this is explicit in the
  config. Token-based sizing would require a tokenizer dependency and a
  model-bound definition of "token"; rune counts are deterministic across
  platforms, which the determinism contract requires.
- **Validation fails closed.** Unknown strategy/unit/placement, non-positive
  size, any `overlap` outside `[0, size)`, and recursive-only boundary options
  on `fixed_window` are errors before any work happens.
- **Determinism.** `SplitChunks` is a pure function of (parent ID, text,
  config): the same inputs always produce an identical stream (IDs, ordinals,
  offsets, text). Golden fixtures can hash chunk streams and compare across
  runs.
- **Coverage.** Every rune offset of the input appears in at least one
  non-empty chunk and every chunk is at most `Size` runes. When
  `SnapOverlapToBoundary` is `false` (default), consecutive chunks share
  exactly `Overlap` trailing/leading runes on fixed, separator, and hard split
  paths. When `SnapOverlapToBoundary` is `true` on `StrategyRecursive`,
  consecutive chunks preserve strict forward progress and full rune coverage
  with overlap bounded by `<= Overlap` runes. Empty text yields no chunks and
  trailing separators never emit an empty chunk.

## Embedder context-window validation

Because `chunking.Config.Size` is measured in **runes** while embedding models
truncate inputs by **tokens**, pairing an oversized chunk length (for example
`Size: 3000`) with a 512-token embedder silently drops the tail of every chunk.
`TreeDB/collections/embedding` provides a stdlib-only, fail-closed budget seam:

- `embedding.ContextWindowReporter` (`MaxContextTokens() int`) is an optional
  interface implemented by embedders with a known token limit.
- `embedding.ValidateChunkSizeForEmbedder(chunkSizeRunes, emb, opts)` validates
  a chunker's `Size` before ingestion, resolving `maxTokens` from
  `opts.MaxContextTokens` (when `> 0`) or `emb.(ContextWindowReporter)`.
- `embedding.ValidateTextsForContextWindow(texts, emb, opts)` validates a batch
  of UTF-8 byte slices with zero heap allocations on the happy path, failing
  closed with `embedding.ErrContextWindowExceeded` (wrapping batch index and
  rune count) on the first entry exceeding `maxSafeRunes`.
- When `opts.RequireKnownLimit` is `true` and neither `opts.MaxContextTokens`
  nor `emb` reports a positive limit, both helpers fail closed with
  `embedding.ErrContextWindowUnknown`.

```go
opts := embedding.ContextBudgetOptions{
    MinRunesPerToken:   2.5, // default; conservative for code/LaTeX
    HeaderReserveRunes: 128, // reserve space for contextual headers before chunking
    RequireKnownLimit:  true,
}
if err := embedding.ValidateChunkSizeForEmbedder(cfg.Size, emb, opts); err != nil {
    return err
}
```

**Rune-to-token guidance:**
- **Code, LaTeX, and math-heavy text:** ~2.5–3.0 runes/token — keep the default
  `MinRunesPerToken: 2.5` (e.g., `<= 1280` runes for a 512-token model before
  header reserve).
- **English prose:** ~3.5–4.0 runes/token — callers with prose-only corpora may
  raise `MinRunesPerToken` (e.g., `3.5` or `4.0`) after verifying with their
  target tokenizer.
- **Already-headered texts:** pass `HeaderReserveRunes: 0` to
  `ValidateTextsForContextWindow` when the `texts` slice already includes any
  contextual prefix so header runes are not subtracted twice.

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

An optional top-level parent `meta` value must be a JSON object. The shared
chunk plan copies that exact object value under `meta` on every child, with no
projection, filtering, or key transformation. The plan owns its copy, so later
caller-buffer changes cannot alter planned child bytes. Same-named keys such as
`meta.chunk_parent` remain nested caller metadata; they cannot replace the
authoritative top-level `chunk_parent`, `chunk_ordinal`, or `chunk_kind`.
Likewise, caller-supplied top-level linkage values are not inherited into
children. A missing `meta` field emits the same metadata-free child shape as
before. A non-object or otherwise malformed `meta` value rejects the complete
plan before parent or child mutation.

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
- `IngestChunkedDocument` still publishes the parent upsert, stale-child
  `DeleteBatch`, and replacement `InsertBatch` as three separate durable
  boundaries. Its error can therefore describe an old, new, or intermediate
  direct chunk-ingest state; retry the deterministic ingest to converge.
- Current behavior: `IngestSources` instead plans the old-row removals and complete new
  parent/child/index state, then publishes every affected collection root and
  catalog descriptor under one durable root group. Its storage outcome is a
  complete old or complete new source, never an intermediate child/parent view.
- Children are ordinary documents to the index layer: text, scalar, and vector
  indexes maintain them through the normal batch paths and resolve only live
  children after a successful re-chunk.

Re-ingestion replaces the parent and child documents, so inherited metadata and
its scalar-index values are replaced with the new `meta` object; stale values
do not remain on live children. In reconstructable column-store layouts,
`meta` follows the existing schema: declare selected paths as columns when
needed, or preserve it through the configured retained non-column payload.
Chunk ingestion does not implicitly widen the column schema.

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
