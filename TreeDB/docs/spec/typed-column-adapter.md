# Typed-Column Adapter and Durable Vector Publication (#1754/#1755/#1756)

Status: current implementation note for issues #1754, #1755, and #1756 under parent tracker #1744.

`TreeDB/collections/typed_column_adapter.go` adapts the transplanted
`TreeDB/internal/typedcolumn` data plane to TreeDB typed-storage field metadata,
legacy `ColumnStoreValueType` compatibility names, retained-payload test seams,
and #1736 `mappedresource` section access.

The #1754 adapter seam maps TreeDB metadata to `typedcolumn` parts. Issue #1755
adds an opt-in durable scalar publication path for explicit
`typed_column_part` owners: collection manifests can now reference immutable
`tcs1_typed_column_part` assets beside the compatibility typed-row `TCPA` row
locator/typed-row asset. Existing `ColumnStoreConfig` metadata still resolves to
`typed_row_asset` unless a column explicitly sets `Owner:
typed_column_part`.

Issue `#1756` extends that path with fixed-dimension `float32_vector` dense
sections. It still does not switch query planning or physical predicate scans to
typed-column sections, does not publish adjacency sections, and does not make
derived accelerators authoritative.

## Type Matrix

| TreeDB declared type | adapter / #1755 publication status | Representation |
| --- | --- | --- |
| `bool` | represented | `typedcolumn.ColumnTypeBool` bitpack/RLE encoding. |
| `int64` | represented | `typedcolumn.ColumnTypeInt64` delta-varint encoding. |
| `float32` | represented | Raw int64 column carrying `math.Float32bits` in the low 32 bits until native float sections land. |
| `double` / `float64` | represented | Raw int64 column carrying `math.Float64bits`. |
| `string` | represented | Low-cardinality uint32 codes plus typed-column dictionary section metadata. |
| `float32_vector` | represented | Fixed-dimension row-major dense little-endian `float32` sections with `vector_dims` as elements per row. |
| `adjacency_list` | fail closed | Internal dense `uint32` sections exist for #1756 validation, but production publication remains closed until fixed-degree adjacency schema metadata exists. |

Nullable/missing adapter values still fail closed because the transplanted part
builder does not yet have a TreeDB nullable typed-column representation.
Adjacency publication remains staged after #1756.

Adapter input rows are keyed by `TypedStorageField.Path`, not by display `Name`.
When `Name != Path`, the physical column name may use `Name`, but decoded rows
are restored under `Path`; display-name-only input fails closed. Adapter images
are fixed-schema: reads fail closed if the image contains unexpected columns or
is missing any expected field column/primary-id column.

## Durable Publication / Reconstruction Seam (#1755)

For inserts and updates with scalar or fixed-dimension vector
`typed_column_part` owners, TreeDB writes:

- a compatibility `tcs1_part_image`/`TCPA` typed-row asset containing row IDs,
  tombstones, and any `typed_row_asset` owned fields;
- a `tcs1_typed_column_part` asset containing the authoritative scalar and
  fixed-dimension `float32_vector` `typed_column_part` values for the same
  generation.

Deletes publish only a typed-row tombstone asset. Retained-payload
reconstruction finds the latest visible typed-row locator for a document ID and,
when that locator's generation has typed-column fields, reads the matching
`typed_column_part` by row index. Reopen/recovery uses the manifest refs and
existing typed asset manager paths; typed-column refs participate in reachability
and rewrite/GC eligibility as durable typed-storage assets.

## Resource Seam

`typedColumnAdapterResourceReader` acquires typed-column image sections through
issue #1736 `mappedresource.Manager` handles. Tests cover file-backed mmap-or-heap
reads and heap reads for the same section bytes. Fixed-width adapter reads use
mappedresource typed-view validation for `[]int64`, `[]float32`, `[]float64`,
and `[]uint32` buffers. Durable typed-column asset reads use the typed asset read
cache with `mappedresource.ClassTypedColumnAsset` when a manager is supplied.

## Retained Payload Seam

`typedColumnAdapterRetainedPayloadSplitRestore` reuses the production
retained-payload split/restore helpers as an internal test seam. It does not alter
production retained-payload behavior.

## Dense Section Safety (#1756)

`float32_vector` typed-column data is stored as uncompressed raw little-endian
`float32` payloads. The `typedcolumn` image builder keeps all sections aligned to
8-byte boundaries, which is sufficient for `float32` and `uint32` mappedresource
direct views. Readers must validate lifetime, range, length, endian mode, and
alignment through #1736 `mappedresource` handles before exposing direct typed
views; misaligned direct views fall back to heap/scratch decode or fail closed.

## Boundary

The only production `TreeDB/collections` file that imports
`TreeDB/internal/typedcolumn` is the adapter seam. Publication/reopen logic calls
through that seam. Query/vector search integration remains deferred to later
issues `#1756`/`#1757`; this PR publishes fixed-dimension `float32_vector`
values but does not switch native vector graph search to typed-column parts.
Adjacency list publication remains fail-closed.
