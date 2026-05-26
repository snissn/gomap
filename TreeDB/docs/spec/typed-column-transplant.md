# Typed-Column Data-Plane Transplant (#1753)

Status: current implementation note for issue #1753 under parent tracker #1744.

`TreeDB/internal/typedcolumn` is a copy/adapt transplant of the coherent
`experiments/colgranule` typed-column data plane. Issue #1753 kept the package
non-authoritative: it could build, encode, decode, and scan typed-column part
artifacts without publishing production collection assets. Issue #1755 now uses
that data plane through the `TreeDB/collections` adapter for opt-in durable
scalar `typed_column_part` publication/reconstruction; the internal package
itself remains a data-plane package, not a manifest/WAL/query owner.

The package preserves the colgranule data-plane shape:

- versioned part images and TCS1 memory headers;
- section directories with aligned section offsets;
- granules, typed descriptors, codecs, and compression choices;
- row locators;
- sort-key mark and predicate/pruning metadata;
- dictionary descriptors;
- aggregate metadata descriptors and payloads;
- latest-visible base/delta/tombstone part-set logic over caller-owned parts;
- a narrow `SectionReader` seam for #1754 mappedresource-backed byte access.

Production control-plane adaptation begins in #1755 for scalar publication and
reconstruction. Future byte access backed by production files should continue to
adapt `SectionReader` to the #1736 mapped-resource handles rather than reshaping
this data plane into the existing typed-row physical asset format.

## Copy / Adapt / Defer Table

| Source | Destination / action | Semantic delta | Reason | Tests |
| --- | --- | --- | --- | --- |
| `experiments/colgranule/typed.go` | copied to `TreeDB/internal/typedcolumn/typed.go` | package and error prefix renamed | preserve typed codecs | `TestTypedColumnTransplantPartImageRoundTrip` |
| `experiments/colgranule/granule.go` | copied to `TreeDB/internal/typedcolumn/granule.go` | package and error prefix renamed; #1756 adds raw dense `float32_vector` and `uint32` encodings | preserve granule encodings/compression | `TestTypedColumnTransplantPartImageRoundTrip`, `TestTypedColumnTransplantPredicateMetadataRoundTrip`, `TestTypedColumnVectorDenseDirectViewAligned` |
| `experiments/colgranule/part.go` | copied/adapted to `TreeDB/internal/typedcolumn/part.go` plus #1756 `dense.go` | `ColumnStoreOptions` -> package-local `Options`; `ColumnBatch` -> `Batch`; error prefix renamed; #1756 adds fixed-width dense vector/adjacency batches | avoid adding new `ColumnStore*` names while preserving part descriptors/build/scan | `TestTypedColumnTransplantPartImageRoundTrip`, `TestTypedColumnTransplantRowLocatorRoundTrip`, `TestTypedColumnAdjacencyLittleEndianPayloadFixture` |
| `experiments/colgranule/part_image.go` | copied/adapted to `TreeDB/internal/typedcolumn/part_image.go` | package/error prefix renamed; section offsets aligned; padding accounting added | preserve sectioned image model and satisfy fixed-width alignment | `TestTypedColumnTransplantSectionDirectoryRoundTrip`, `TestTypedColumnTransplantFixedWidthSectionsAreAligned` |
| `experiments/colgranule/part_image_decode.go` | copied/adapted to `TreeDB/internal/typedcolumn/part_image_decode.go` plus `section_reader.go` seam | package/error prefix renamed; validates aligned sections and permits explicit padding gaps; exposes `SectionReader` for future mappedresource adapters | preserve decode/bounds checks for aligned section directories while deferring #1736-backed IO to #1754 | `TestTypedColumnTransplantRejectsInvalidMagicOrVersion`, `TestTypedColumnTransplantRejectsTruncatedOrOutOfBoundsSection`, `TestTypedColumnTransplantSectionDirectoryRoundTrip` |
| `experiments/colgranule/predicate.go` | copied to `TreeDB/internal/typedcolumn/predicate.go` | package/error prefix renamed | preserve sort-key mark and predicate/pruning metadata | `TestTypedColumnTransplantPredicateMetadataRoundTrip` |
| `experiments/colgranule/aggregate_metadata.go` | copied to `TreeDB/internal/typedcolumn/aggregate_metadata.go` | package/error prefix renamed | preserve aggregate metadata descriptors/payloads | `TestTypedColumnTransplantDictionaryAggregateDescriptorsRoundTrip` |
| `experiments/colgranule/aggregate.go` | copied to `TreeDB/internal/typedcolumn/aggregate.go` | package/error prefix renamed | preserve aggregate arena helpers used by metadata build | `TestTypedColumnTransplantDictionaryAggregateDescriptorsRoundTrip` |
| `experiments/colgranule/adaptive_mark.go` | copied to `TreeDB/internal/typedcolumn/adaptive_mark.go` | package/error prefix renamed | preserve adaptive mark sizing hooks | covered by package compile and part builder coverage |
| `experiments/colgranule/part_accounting.go` | copied/adapted to `TreeDB/internal/typedcolumn/part_accounting.go` | JSONBench-only helper deferred; padding bytes added | keep part/granule/accounting concepts without benchmark fixture dependency | `TestTypedColumnTransplantPartImageRoundTrip` |
| `experiments/colgranule/tcs1.go` | copied/adapted to `TreeDB/internal/typedcolumn/tcs1.go` | in-memory encode/decode/header validation retained; asset-store helpers deferred | preserve versioned header/checksum identity without adding production publication/storage | `TestTypedColumnTransplantPartImageRoundTrip`, `TestTypedColumnTransplantRejectsInvalidMagicOrVersion` |
| `experiments/colgranule/part_set.go` | copied/adapted subset to `TreeDB/internal/typedcolumn/part_set.go` | latest-visible base/delta/tombstone data-plane logic retained over caller-owned parts; workspace/manifest/compaction deferred | keep coherent part-set semantics without production control-plane integration | `TestTypedColumnTransplantPartSetLatestVisibleRows` |
| `experiments/colgranule/asset_store.go`, `asset_manager.go`, `workspace.go`, `lifecycle*.go`, `collection_manifest.go`, `control_plane_binary.go`, `mutation_adapter.go` | deferred | no production/internal copy in #1753 | control-plane, workspace, publication, recovery, or benchmark/lifecycle ownership belongs to #1754/#1755 and #1736 integration | `TestTypedColumnTransplantNoProductionPublication` |
| `experiments/colgranule/jsonbench*`, benchmark reports, local-data benchmarks | deferred / remain in experiments | none | benchmark harnesses and JSONBench fixtures are not required for the non-authoritative data-plane transplant | not hot-path integrated in #1753 |

## Naming Impact

- No public `ColumnStore*` API is added or removed.
- Internal package names use `typedcolumn` and package-local `Options`/`Batch`
  instead of adding new `ColumnStoreOptions` or `ColumnBatch` symbols.
- `ColumnPart*`, `ColumnPartImage*`, and section names are retained as true
  typed-column terminology.
- Current production `ColumnStoreConfig` and typed-row assets are unchanged.

## Boundary

`TreeDB/internal/typedcolumn` remains a data-plane package. Issue `#1754` added
the narrow `typed_column_adapter.go` seam for metadata/resource adaptation, and
Issue `#1755` routes scalar durable publication/reconstruction through that seam.
Issue `#1756` adds aligned fixed-width dense `float32_vector` sections and
internal dense `uint32` adjacency validation. Query planning, native vector graph
switching, production adjacency publication, and predicate scan integration remain
owned by later #1744 children.
