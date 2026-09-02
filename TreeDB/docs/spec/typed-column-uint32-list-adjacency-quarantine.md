# Uint32 List Adjacency Audit and Quarantine (#1989)

Status: #1989 quarantine/removal contract for the #1982 typed-column
integer-list stack. New `column_graph` rebuilds publish HNSW adjacency as
vector-index state `uint32_list` typed-column assets; they do **not** publish the
legacy graph-specific `TCGA`/`TCGL` adjacency-source trailers. This document does
**not** introduce a migration format and does **not** change HNSW search
behavior.

TreeDB is pre-alpha. If the corrected list/vector-index-state format fails to
open old graph assets, prefer rebuild/fail-closed guidance over migration
scaffolding unless a later issue explicitly scopes migration.

## Target model

The architectural target follows the ClickHouse `Array(T)` separation rather
than graph-specific datastore names:

| Layer | TreeDB target/current primary path | Legacy/quarantined shape |
| --- | --- | --- |
| Logical list type | `uint32_list` / `uint32[]` / conceptual `Array(UInt32)` | `ColumnStoreValueAdjacencyList` compatibility names |
| Physical encoding | `raw_uint32_offsets_list` | same useful offsets/value encoding, previously selected through `adjacency_layout` |
| Offsets/sizes substream | `offsets []uint64`, little-endian, length `rows+1`, `offsets[0] == 0` | same low-level bytes |
| Flattened values stream | `values []uint32`, little-endian, length `offsets[rows]` | same low-level bytes |
| Consumer state | vector-index state references typed-column `uint32_list` assets | `column_graph` adjacency-source metadata embedded in graph/column manifest records |

`raw_uint32_offsets_list` is a physical encoding for generic `uint32_list`; it is
not itself the HNSW adjacency type. The first-class logical semantics,
validation invariants, length-only offsets behavior, and compatibility naming
strategy are defined in `typed-column-uint32-list-semantics.md`. HNSW adjacency
is a consumer that reads row `i` as `values[offsets[i]:offsets[i+1]]` from
vector-index state.

## Inventory buckets

### Keep as low-level mechanics

These pieces are preserved as consumer-neutral storage mechanics:

- `TreeDB/internal/typedcolumn/raw_uint32_offsets_list.go`
  - `RawUint32OffsetsList` owned fallback representation;
  - `EncodeRawUint32OffsetsListOffsets`, `EncodeRawUint32OffsetsListValues`,
    and `EncodeRawUint32OffsetsListPayload`;
  - `DecodeRawUint32OffsetsListFallback` and payload fallback decode;
  - `ValidateRawUint32OffsetsListShape`, section-length validation, and
    `rows+1` sentinel-offset checks;
  - `NewRawUint32OffsetsListImageSections` split offsets/values image metadata;
  - `GranuleBuilder.BuildUint32OffsetsList` and fallback reader plumbing.
- `TreeDB/internal/typeddecode/plan.go`
  - `Uint32OffsetsListShapeRequest`, `Uint32OffsetsListDirectViewRequest`,
    `ValidateUint32OffsetsListDirectViewSections`, and `Uint32OffsetsListView`;
  - endian, exact byte-length, absolute-alignment, actual pointer-alignment,
    mappedresource lifetime, and fallback/heap-copy direct-view checks.
- `TreeDB/internal/typedcolumn/part_image*.go` and layout-contract support that
  serializes separate offsets and values sections with independent checksums and
  section identities.
- Focused tests around raw offsets-list bytes, corruption/fail-closed validation,
  split-section checksums, direct-view fallback classification, handle lifetime,
  and alignment.

### Generalized primary path

These pieces are now the primary adjacency storage/search path:

- `TreeDB/collections/column_vector_index_state_manifest.go` stores derived
  vector-index state refs outside legacy `column_graph` graph-record trailers.
- `TreeDB/collections/column_vector_index_state_adjacency.go` publishes HNSW
  adjacency layers as typed-column `uint32_list` assets with
  `raw_uint32_offsets_list` physical encoding.
- `TreeDB/collections/column_vector_graph_adjacency_state_source.go` opens those
  state assets for `column_graph` search and validates the generic
  `uint32_list`/`raw_uint32_offsets_list` contract before applying HNSW-specific
  ordinal/layer checks.
- Healthy search counters should show typed-list direct reads and no legacy
  fallback: typed-list mmap/heap/direct counters > 0, adjacency scratch decode =
  0 for direct-view-capable assets, and legacy fallback = 0.

### Quarantine/remove as graph-specific storage architecture

These pieces are not the target architecture. They may remain only for explicit
compatibility/fallback tests, old pre-alpha assets, or safe deletion/rewrite of
old refs; do not add new storage features to them:

- `TreeDB/collections/column_vector_graph_adjacency_source.go`
  - synthetic typed-column configs for graph adjacency sources;
  - `column_graph_layer0_adjacency/raw_uint32_offsets_list/v1` and
    `column_graph_adjacency_layer/raw_uint32_offsets_list/v1` schema strings;
  - layer-specific source names such as `layer0_adjacency` and
    `_layer0_neighbors`.
- `TreeDB/collections/column_vector_graph_adjacency_direct_source.go`
  - legacy `column_graph` source direct-reader wrappers and fallback counters.
    The same low-level direct-source type is reused by the typed-list state
    reader, but graph-source opening is compatibility-only.
- `TreeDB/collections/column_vector_graph_manifest.go`
  - `TCGA`/`TCGL` decode/encode support and manifest fields for old
    adjacency-source refs. New rebuilds leave these fields empty.
- Vector-search docs that mention graph-specific adjacency-source paths must
  call them legacy/quarantined and point to vector-index state `uint32_list` as
  the primitive.
- Tests whose purpose is only to keep old `column_graph` adjacency-source assets
  readable are labeled compatibility/legacy and construct those assets
  explicitly instead of relying on normal rebuild publication.

### Current #1989 behavior

- New graph builds publish vector-index state assets for healthy search state.
  HNSW adjacency state is `uint32_list` with `raw_uint32_offsets_list` sections
  owned by the vector-index state record. The graph row asset remains a legacy
  compatibility record for opaque result IDs and controlled fallback, not the
  canonical adjacency source.
- New graph builds do not publish `Layer0AdjacencySource`,
  `AdjacencyLayerSources`, `TCGA`, or `TCGL` graph-specific adjacency-source
  metadata.
- Search opens vector-index state adjacency sources on the healthy path. Legacy
  graph-source metadata, if present in old records, is compatibility-only and is
  not required for loaded status when vector-index state is healthy.
- Row-image adjacency remains an explicit legacy/corruption fallback for stale or
  disabled direct sources; it is not the target datastore abstraction.

## Remediation ownership

| Issue | Owner scope | Exact files/APIs to revisit |
| --- | --- | --- |
| #1984 | Define first-class `uint32_list` semantics and public vocabulary. | `TreeDB/docs/spec/typed-column-uint32-list-semantics.md`, `typed-column-semantics.md`, `typed-column-layout-capabilities.md`, `typed-column-adapter.md`, `typed-storage-naming.md`, docs/spec assertions. |
| #1985 | Promote raw offsets-list machinery into a generic typed-column primitive. | `TreeDB/internal/typedcolumn/raw_uint32_offsets_list.go`, `TreeDB/internal/typeddecode/plan.go`, `TreeDB/internal/columnlayout`, `TreeDB/internal/columnsemantics`, `TreeDB/collections/typed_column_adapter.go`, typed-column offsets-list tests, direct-view/conformance tests, primitive microbenchmarks. |
| #1986 | Move vector-index state refs out of special column-graph manifest records. | `TreeDB/collections/column_vector_index_state_manifest.go`, `TreeDB/collections/column_vector_graph_manifest.go`, vector-index metadata/status tests, manifest encode/decode tests, `vector-index-state-manifest.md`. |
| #1987 | Publish HNSW adjacency as typed-column `uint32_list` vector-index state. | `TreeDB/collections/vector_index_rebuild.go`, graph rebuild/publication tests, row-count/ordinal/layer validation, state refs from #1986. |
| #1988 | Switch `column_graph` search to consume typed-column list state. | `TreeDB/collections/column_vector_graph_search.go`, `column_vector_graph_row_reader.go`, search stats/counters, direct-view lifetime/fallback tests, parity benchmarks. |
| #1989 | Remove or explicitly quarantine legacy adjacency-source paths. | `column_vector_graph_adjacency_source.go`, `column_vector_graph_adjacency_direct_source.go`, legacy schema strings, `TCGA`/`TCGL` decode policy, compatibility tests and docs. |
| #1990 | Benchmark corrected topology against the quarantined baseline. | Vector search benchmarks, primitive list microbenchmarks, profile scripts/results, docs with before/after counters. |
| #1992 | Keep non-adjacency vector-index data-type work separate. | Inverse-norm `raw_float32` vector-index state; do not mix with this adjacency/list primitive PR. |

## Guardrails for new code

- Do not add new storage features to `ColumnStoreValueAdjacencyList`,
  `adjacency_layout`, or `column_graph` adjacency-source schema strings.
- Do not publish graph-specific adjacency-source assets from primary rebuild,
  insert, or rewrite paths. New durable adjacency state belongs in `TVIS`
  vector-index state records as typed-column `uint32_list` assets.
- Do reuse the offsets/value split, validation, alignment, mappedresource
  lifetime, fallback decode, and section checksum mechanics for the generic
  `uint32_list` primitive.
- Do keep graph-specific validation—neighbor ordinal bounds, layer semantics,
  deleted-row rejection, and traversal behavior—above the generic list storage
  layer.
- Do fail closed or ask callers to rebuild pre-alpha assets when the corrected
  format/state identity cannot safely interpret old graph-specific metadata.
