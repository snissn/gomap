# Column Graph Native Block Planner

This plan is the follow-on to the SIMD scoring and typed row-read PRs. The goal
is to make `column_graph_native_reader` search align with the physical column
store rather than treating graph rows as independent point fetches.

The current direction intentionally batches these three items as one planner
workstream:

1. Granule/block-oriented typed reader: `ordinal -> granule/block -> typed vector/invNorm/adjacency spans`.
2. Remove per-candidate generic row materialization and copies.
3. Reuse decoded fixed-width/adjoining block views safely, bounded by snapshot/root lifetime.

## Golden Path

The golden path for steady-state vector reads is:

1. Caller opens `OpenVectorIndexSearcher` once per worker.
2. Searcher binds an immutable snapshot/root, graph manifest, and physical graph asset refs.
3. Search creates or reuses a `ColumnGraphSearchPlan` owned by that searcher.
4. The plan resolves ordinals to physical graph blocks and block-local row indexes.
5. The plan loads bounded block views, not decoded rows.
6. Candidate scoring reads direct vector and invNorm spans from block views.
7. Frontier candidates retain only ordinal and score; adjacency is loaded lazily only when a candidate is popped for expansion.
8. Final IDs are fetched in one top-k batch grouped by block/ordinal order.
9. Full documents are fetched only after top-k and only when public API options request documents.

This path must avoid:

- full decoded `ColumnVectorGraph` materialization,
- generic `FetchRow` in the search hot path,
- `decodeRowFromBlock`,
- `readColumnPhysicalRowValuesIntoScratch`,
- `readSelectedColumnPhysicalValueIntoScratch`,
- per-candidate adjacency copies before the candidate is actually expanded,
- document reads before top-k.

## Planner Objects

### `ColumnGraphSearchPlan`

Searcher-owned, immutable except for bounded caches and stats:

- vector index definition: dimensions, metric, degree defaults,
- graph manifest identity and row count,
- physical asset refs with generation/part/checksum/length,
- ordinal range table: `globalOrdinal -> assetOrdinal + rowIndex`,
- bounded `ColumnGraphBlockView` cache keyed by asset ordinal and asset identity,
- search accounting hooks for cache hits/misses, block loads, physical bytes, and view-build work.

The plan lifetime is bounded by the searcher's snapshot/root lifetime. Reusing
views across searches is safe only while the immutable snapshot/root and asset
refs remain bound to the searcher.

### `ColumnGraphBlockView`

A block view owns or aliases the cached physical asset bytes and indexes the graph
row layout once per loaded block:

- raw bytes owned by the bounded physical asset cache or mmap/file cache,
- `rowOffsets []int` for the physical rows,
- `idSpans []byteSpan`,
- `vectorSpans []typedFloat32Span` or a contiguous vector matrix view,
- `invNorms []float32` or typed scalar spans,
- `adjacencySpans []typedUint32Span`,
- deleted/present/null validation state for fail-closed graph assets.

A block view is not a generic row reader. It validates graph-column shape while
building offsets/spans and exposes graph-native accessors:

```go
Vector(rowIndex int) []float32
InvNorm(rowIndex int) float32
Adjacency(rowIndex int, scratch []uint32) ([]uint32, []uint32)
ID(rowIndex int) []byte
```

For the current #1893/#1886 stack, physical row-asset adjacency is decoded into
scratch even when the bytes are already little-endian; row-asset alignment is
issue #1897 and certified adjacency direct views are issue #1901. Later phases
may return a first adjacency slice that aliases block bytes when the storage
owner/path is eligible. Callers must treat either slice as an ephemeral alias
tied to the block view or scratch.

## Search Algorithm

The HNSW-style traversal remains graph-driven, but fetch/decode is planned in
block batches.

### Initialization

1. Validate query dimensions and norm.
2. Prepare scratch: visited bitset, pending ordinals, grouped ordinals, frontier,
   top-k, result ordinals, and optional adjacency scratch.
3. Seed pending scoring with entry ordinal `0` when the graph is non-empty.

### Batched scoring loop

Instead of scoring each neighbor through an individual row fetch, the search loop
uses a pending scoring queue:

1. Pop frontier candidates until either:
   - adjacency expansion discovers unvisited neighbors, or
   - the pending score queue reaches a target batch size, or
   - frontier/top-k termination requires immediate scoring.
2. Deduplicate neighbors with the visited bitset before enqueueing them for scoring.
3. Group pending ordinals by `assetOrdinal` / block view.
4. For each block group:
   - load or reuse the `ColumnGraphBlockView`,
   - score all grouped row indexes against the query using direct vector spans,
   - multiply by query invNorm and block invNorm,
   - insert ordinal+score into frontier and top-k.
5. Frontier candidates store ordinal+score only.

This minimizes passes through the column engine: all ordinals discovered in a
small frontier wave are resolved to blocks once, each block view is touched once
for that wave, and vector spans are consumed without row materialization.

### Lazy adjacency expansion

When a frontier candidate is popped:

1. Resolve its ordinal to the block view and row index.
2. Read the adjacency span from the block view.
3. Validate neighbor ordinals as they are expanded.
4. Enqueue newly visited neighbor ordinals into the pending scoring queue.

This removes the current pattern where every scored candidate copies adjacency
into frontier storage. Adjacency is paid only for candidates that are actually
expanded.

### Final result materialization

After traversal:

1. Sort top-k ordinals by ordinal for physical locality.
2. Group by block.
3. Fetch/copy only IDs for final results.
4. Restore score order for the returned result list.
5. If the public API requested documents, fetch full documents after this step.

## Physical Encoding Plan

Phase A should work with the current physical graph asset format:

- vectors already use little-endian fixed-width encoding and can be direct typed
  `[]float32` views on little-endian hosts,
- invNorm can be decoded once into block-view scalar storage,
- adjacency can keep the existing scratch-backed big-endian decode while the
  planner removes eager per-candidate copies.

Phase B can make the graph asset more column-engine-friendly:

- allow little-endian fixed-width encoding for `float32` scalar columns used by
  invNorm,
- allow little-endian fixed-width encoding for adjacency-list payloads,
- make direct typed adjacency views possible on little-endian hosts,
- keep fail-closed fallback decode for unsupported hosts or future encodings.

TreeDB is pre-alpha, so this on-disk format adjustment is acceptable if docs and
tests are updated with the format change.

## Safety Rules

- Block views must never outlive their owning searcher snapshot/root binding.
- A block view must be invalidated when its asset identity changes.
- Bounded cache size must remain explicit (`MaxDecodedBlocks` or replacement
  `MaxGraphBlockViews`) and must not become a hidden full graph decode.
- Returned vector, adjacency, and ID spans alias block bytes or scratch. They are
  not retained across cache eviction or the next scratch reuse.
- Unsafe typed views are allowed only when the active direct-view contract says
  the storage owner/path is eligible and encoding, host endianness, length,
  absolute storage offset, actual pointer alignment, and lifetime checks pass.
  Physical row-asset direct views are deferred to #1897 and adjacency mmap
  direct views are deferred to #1901; use scratch-backed decode in the current
  stack.
- Search remains one reader/searcher per worker unless synchronization is added
  above the planner.

## Metrics And Guards

The planner should keep or add metrics that make path identity obvious:

- `candidate_fetches/search` becomes `candidate_scores/search`,
- `block_view_hits/search`, `block_view_misses/search`,
- `block_view_builds/search`,
- `ordinals_grouped/search`,
- `score_batches/search`,
- `adjacency_expansions/search`,
- `adjacency_direct_views/search` (expected zero for current row-asset fallback paths),
- `adjacency_scratch_decodes/search`,
- `decoded_blocks/search == 0` for the generic row reader,
- `physical_B/search == 0` after warmup,
- `docs_fetched/search == 0` unless documents are requested.

Regression/profile guards should assert that the warmed production benchmark does
not reintroduce generic row decode symbols in the hot path.

## PR Slicing

1. Planner skeleton and block-view interfaces behind the existing native reader.
2. Block-view builder for current graph assets, with tests against malformed
   graph rows and snapshot/cache lifetime behavior.
3. Search loop switch from point fetch to pending-score grouped batches.
4. Lazy adjacency expansion: frontier stores ordinal+score only.
5. Optional physical encoding extension for invNorm and certified adjacency direct views (#1901).
6. Heap/frontier tuning after fetch/decode no longer dominates.

The expected outcome is a native vector search path where the column store sees
small block-oriented batches rather than hundreds of independent row fetches,
while keeping top-k document materialization outside the graph traversal path.
