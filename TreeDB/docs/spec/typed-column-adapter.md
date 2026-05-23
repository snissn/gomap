# Typed-Column Adapter (#1754)

Status: current implementation note for issue #1754 under parent tracker #1744.

`TreeDB/collections/typed_column_adapter.go` adapts the transplanted
`TreeDB/internal/typedcolumn` data plane to TreeDB typed-storage field metadata,
legacy `ColumnStoreValueType` compatibility names, retained-payload test seams,
and #1736 `mappedresource` section access.

This adapter is **non-authoritative**. It does not publish collection manifests,
write recovery metadata, replay WAL state, switch query planning, or enable
`typed_column_part` ownership in production. Existing `ColumnStoreConfig`
metadata still resolves to `typed_row_asset` unless tests construct explicit
`TypedStorageOwnerColumnPart` fields for this adapter seam.

## Type Matrix

| TreeDB declared type | #1754 adapter status | Representation |
| --- | --- | --- |
| `bool` | represented | `typedcolumn.ColumnTypeBool` bitpack/RLE encoding. |
| `int64` | represented | `typedcolumn.ColumnTypeInt64` delta-varint encoding. |
| `float32` | represented | Raw int64 column carrying `math.Float32bits` in the low 32 bits until native float sections land. |
| `double` / `float64` | represented | Raw int64 column carrying `math.Float64bits`. |
| `string` | represented | Low-cardinality uint32 codes plus typed-column dictionary section metadata. |
| `float32_vector` | fail closed | Dense vector typed-column sections are staged to #1756. |
| `adjacency_list` | fail closed | Adjacency typed-column sections are staged to #1756. |

Nullable/missing adapter values fail closed in #1754 because the transplanted
part builder does not yet have a TreeDB nullable typed-column representation.
Publication/reopen work in #1755 and vector/adjacency work in #1756 may extend
this matrix without changing the existing typed-row compatibility path.

## Resource Seam

`typedColumnAdapterResourceReader` acquires typed-column image sections through
issue #1736 `mappedresource.Manager` handles. Tests cover file-backed mmap-or-heap
reads and heap reads for the same section bytes. Fixed-width adapter reads use
mappedresource typed-view validation for `[]int64`, `[]float32`, `[]float64`,
and `[]uint32` buffers.

## Retained Payload Seam

`typedColumnAdapterRetainedPayloadSplitRestore` reuses the production
retained-payload split/restore helpers as an internal test seam. It does not alter
production retained-payload behavior.

## Boundary

The only production `TreeDB/collections` file that imports
`TreeDB/internal/typedcolumn` in issue #1754 should be the adapter seam. Later
PRs must keep publication/reopen/recovery wiring in #1755 and query/vector
integration in #1756/#1757 rather than hiding those changes in the adapter.
