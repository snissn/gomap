# Uint32 List Adjacency Audit and Quarantine (#1983)

Status: audit/quarantine contract for the #1982 typed-column integer-list stack.
This document does **not** introduce a new storage format and does **not** change
HNSW search behavior. It records which pieces of the current graph-specific
adjacency-source path are reusable and which pieces are transitional compatibility
only.

TreeDB is pre-alpha. If the corrected list/vector-index-state format fails to
open old graph assets, prefer rebuild/fail-closed guidance over migration
scaffolding unless a later issue explicitly scopes migration.

## Target model

The architectural target follows the ClickHouse `Array(T)` separation rather
than the current graph-specific integration:

| Layer | TreeDB target | Current #1901-era shape |
| --- | --- | --- |
| Logical list type | `uint32_list` / `uint32[]` / conceptual `Array(UInt32)` | no first-class logical type yet; the current path uses `ColumnStoreValueAdjacencyList` |
| Physical encoding | `raw_uint32_offsets_list` | useful low-level offsets/value encoding already exists |
| Offsets/sizes substream | `offsets []uint64`, little-endian, length `rows+1`, `offsets[0] == 0` | useful validation/direct-view mechanics already exist |
| Flattened values stream | `values []uint32`, little-endian, length `offsets[rows]` | useful validation/direct-view mechanics already exist |
| Consumer state | vector-index state references typed-column `uint32_list` assets | `column_graph` adjacency-source metadata is embedded in graph/column manifest records |

`raw_uint32_offsets_list` is a physical encoding for generic `uint32_list`; it is
not itself the HNSW adjacency type. The first-class logical semantics,
validation invariants, length-only offsets behavior, and compatibility naming
strategy are defined in `typed-column-uint32-list-semantics.md`. HNSW adjacency
should become a consumer that reads row `i` as
`values[offsets[i]:offsets[i+1]]` from vector-index state.

## Inventory buckets

### Keep as low-level mechanics

These pieces are worth preserving when #1985 promotes the primitive:

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

### Generalize/rename in child issues

These pieces contain useful behavior but currently expose graph-specific names or
semantics:

- `typeddecode.AdjacencyOffsetsListPlan` should become or wrap a generic
  `uint32_list`/offsets-list direct-view plan in #1985.
- `TreeDB/collections/typed_column_adapter.go` maps the offsets-list encoding
  through `adjacency_list`; #1984/#1985 own the first-class logical value name,
  adapter admission rules, and compatibility alias decision.
- `TreeDB/collections/column_store.go` exposes `adjacency_layout:
  "uint32_offsets_list"`; #1984 owns the public vocabulary and #1989 owns any
  surviving compatibility alias/deprecation.
- `TreeDB/internal/columnlayout` and `TreeDB/internal/columnsemantics` should
  distinguish generic list direct-payload capability from graph traversal or
  adjacency metric semantics.
- Existing conformance/direct-view tests that treat the offsets-list shape as an
  adjacency selector should be split into primitive `uint32_list` tests plus
  consumer-specific vector-index/search tests.

### Quarantine/remove as graph-specific storage architecture

These pieces are not the target architecture and should not be extended except to
preserve transitional compatibility until #1989 removes or isolates them:

- `TreeDB/collections/column_vector_graph_adjacency_source.go`
  - synthetic typed-column configs for graph adjacency sources;
  - `column_graph_layer0_adjacency/raw_uint32_offsets_list/v1` and
    `column_graph_adjacency_layer/raw_uint32_offsets_list/v1` schema strings;
  - layer-specific source names such as `layer0_adjacency` and
    `_layer0_neighbors`.
- `TreeDB/collections/column_vector_graph_adjacency_direct_source.go`
  - searcher/runtime direct-source wrapper and counters tied to the legacy
    `column_graph` source records.
- `TreeDB/collections/column_vector_graph_manifest.go`
  - `TCGA`/`TCGL` trailers and manifest fields embedding adjacency-source refs
    inside the graph manifest.
- Vector-search docs that describe the graph-specific adjacency-source path as a
  desirable storage endpoint. They may document current behavior only when they
  explicitly call it transitional/quarantined and point to this audit.
- Tests whose purpose is only to keep old `column_graph` adjacency-source assets
  readable should be labeled compatibility/legacy once the generic primitive and
  vector-index state path exist.

### Current transitional behavior

The current path can remain readable and testable while the stack is in flight,
but new work must not optimize it as the target datastore primitive. Until the
child issues land, graph rebuild/search may continue using the existing optional
adjacency-source assets with row-asset fallback. Any new durable list capability
should be added through the generic primitive/vector-index-state route instead.

## Remediation ownership

| Issue | Owner scope | Exact files/APIs to revisit |
| --- | --- | --- |
| #1984 | Define first-class `uint32_list` semantics and public vocabulary. | `TreeDB/docs/spec/typed-column-uint32-list-semantics.md`, `typed-column-semantics.md`, `typed-column-layout-capabilities.md`, `typed-column-adapter.md`, `typed-storage-naming.md`, docs/spec assertions. |
| #1985 | Promote raw offsets-list machinery into a generic typed-column primitive. | `TreeDB/internal/typedcolumn/raw_uint32_offsets_list.go`, `TreeDB/internal/typeddecode/plan.go`, `TreeDB/internal/columnlayout`, `TreeDB/internal/columnsemantics`, `TreeDB/collections/typed_column_adapter.go`, `typed_column_offsets_list_test.go`, direct-view/conformance tests, primitive microbenchmarks. |
| #1986 | Move vector-index state refs out of special column-graph manifest records. | `TreeDB/collections/column_vector_index_state_manifest.go`, `TreeDB/collections/column_vector_graph_manifest.go`, vector-index metadata/status tests, manifest encode/decode tests, `vector-index-state-manifest.md`. |
| #1987 | Publish HNSW adjacency as typed-column `uint32_list` vector-index state. | `TreeDB/collections/vector_index_rebuild.go`, graph rebuild/publication tests, row-count/ordinal/layer validation, state refs from #1986. |
| #1988 | Switch `column_graph` search to consume typed-column list state. | `TreeDB/collections/column_vector_graph_search.go`, `column_vector_graph_row_reader.go`, search stats/counters, direct-view lifetime/fallback tests, parity benchmarks. |
| #1989 | Remove or explicitly quarantine legacy adjacency-source paths. | `column_vector_graph_adjacency_source.go`, `column_vector_graph_adjacency_direct_source.go`, legacy schema strings, `TCGA`/`TCGL` decode policy, compatibility tests and docs. |
| #1990 | Benchmark corrected topology against the quarantined baseline. | Vector search benchmarks, primitive list microbenchmarks, profile scripts/results, docs with before/after counters. |
| #1992 | Keep non-adjacency vector-index data-type work separate. | Inverse-norm `raw_float32` vector-index state; do not mix with this adjacency/list primitive PR. |

## Guardrails for new code

- Do not add new storage features to `ColumnStoreValueAdjacencyList`,
  `adjacency_layout`, or `column_graph` adjacency-source schema strings.
- Do reuse the offsets/value split, validation, alignment, mappedresource
  lifetime, fallback decode, and section checksum mechanics for a generic
  `uint32_list` primitive.
- Do keep graph-specific validation—neighbor ordinal bounds, layer semantics,
  deleted-row rejection, and traversal behavior—above the generic list storage
  layer.
- Do fail closed or ask callers to rebuild pre-alpha assets when the corrected
  format/state identity cannot safely interpret old graph-specific metadata.
- Do put new vector-index derived-state refs in the `TVIS` vector-index state
  control record documented by `vector-index-state-manifest.md`, not in new
  `TCGA`/`TCGL` graph-record trailer fields.
