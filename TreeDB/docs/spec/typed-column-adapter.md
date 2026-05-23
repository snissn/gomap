# Typed-Column Adapter and Durable Scalar Publication (#1754/#1755)

Status: current implementation note for issues #1754 and #1755 under parent tracker #1744.

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

Issue `#1755` remains scoped: it does not switch query planning or physical predicate
scans to typed-column sections, does not publish vector/adjacency sections, and
does not make derived accelerators authoritative.

## Type Matrix

| TreeDB declared type | adapter / #1755 publication status | Representation |
| --- | --- | --- |
| `bool` | represented | `typedcolumn.ColumnTypeBool` bitpack/RLE encoding. |
| `int64` | represented | `typedcolumn.ColumnTypeInt64` delta-varint encoding. |
| `float32` | represented | Raw int64 column carrying `math.Float32bits` in the low 32 bits until native float sections land. |
| `double` / `float64` | represented | Raw int64 column carrying `math.Float64bits`. |
| `string` | represented | Low-cardinality uint32 codes plus typed-column dictionary section metadata. |
| `float32_vector` | fail closed | Dense vector typed-column sections are staged to #1756. |
| `adjacency_list` | fail closed | Adjacency typed-column sections are staged to #1756. |

Nullable/missing adapter values still fail closed because the transplanted part
builder does not yet have a TreeDB nullable typed-column representation. Vector
and adjacency work remains staged to #1756.

## Durable Publication / Reconstruction Seam (#1755)

For inserts and updates with scalar `typed_column_part` owners, TreeDB writes:

- a compatibility `tcs1_part_image`/`TCPA` typed-row asset containing row IDs,
  tombstones, and any `typed_row_asset` owned fields;
- a `tcs1_typed_column_part` asset containing the authoritative scalar
  `typed_column_part` values for the same generation.

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

## Boundary

The only production `TreeDB/collections` file that imports
`TreeDB/internal/typedcolumn` is the adapter seam. Publication/reopen logic calls
through that seam. Query/vector integration remains deferred to #1756/#1757, and
`typed_column_part` supports only the scalar matrix above until those issues
land.
