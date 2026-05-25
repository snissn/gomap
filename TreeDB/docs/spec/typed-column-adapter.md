# Typed-Column Adapter and Durable Vector/Adjacency Publication (#1754/#1755/#1756/#1783)

Status: current implementation note for issues #1754, #1755, #1756, and #1783 under parent tracker #1744.
Typed-column schema/version evolution and migration policy is defined in
`typed-column-schema-evolution.md`. Logical capability status and admission
rules are defined in `typed-column-semantics.md`.

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
sections. Issue `#1783` adds authoritative fixed-degree `adjacency_list` dense
sections for columns that declare positive `adjacency_degree`. These issues do
not switch vector graph/search planning to typed-column sections and do not make
derived accelerators authoritative.

## Type Matrix

This table describes durable representation only. Query/planner operation
support is governed by `typed-column-semantics.md` and is resolved at prepare
time.

| TreeDB declared type | adapter / #1755 publication status | Representation |
| --- | --- | --- |
| `bool` | represented | `typedcolumn.ColumnTypeBool` bitpack/RLE encoding. |
| `int64` | represented | `typedcolumn.ColumnTypeInt64` delta-varint encoding. |
| `float32` | represented | Raw int64 column carrying `math.Float32bits` in the low 32 bits until native float sections land; these bits must not be treated as int64 ordering/sum/min/max/stats/pruning semantics. |
| `double` / `float64` | represented | Raw int64 column carrying `math.Float64bits`; these bits must not be treated as int64 ordering/sum/min/max/stats/pruning semantics. |
| `string` | represented | Low-cardinality uint32 codes plus typed-column dictionary section metadata; code order must not imply lexical range/prefix unless dictionary order and collation proof are supplied. |
| `float32_vector` | represented | Fixed-dimension row-major dense little-endian `float32` sections with `vector_dims` as elements per row. |
| `adjacency_list` | represented | Fixed-degree row-major dense little-endian `uint32` sections with `adjacency_degree` as elements per row. |

Nullable scalar adapter support uses `nullable_int64` as the carrier encoding
for bool, int64, float32, double, and low-cardinality string fields: explicit
JSON null maps to null bitmap rows, omitted paths map to default/missing bitmap
rows, and present values map to the encoded carrier payload (`0/1` bools,
int64s, float bit patterns, or string dictionary codes). Vector and adjacency
nullable/missing support remains staged/fail-closed; `adjacency_list`
`typed_column_part` owners must declare positive `adjacency_degree` and each
present row must contain exactly that many uint32 neighbors.

Adapter input rows are keyed by `TypedStorageField.Path`, not by display `Name`.
When `Name != Path`, the physical column name may use `Name`, but decoded rows
are restored under `Path`; display-name-only input fails closed. Adapter images
are fixed-schema: reads fail closed if the image contains unexpected columns or
is missing any expected field column/primary-id column. The adapter must reject
schema hash, field ownership, value type, `vector_dims`, `adjacency_degree`, fixed-width metadata,
image/descriptor version, and manifest ref mismatches from adapter descriptors,
manifest identities, or refs before row materialization whenever those compact
records are sufficient.

## Durable Publication / Reconstruction Seam (#1755)

For inserts and updates with scalar, fixed-dimension vector, or fixed-degree
adjacency `typed_column_part` owners, TreeDB writes:

- a compatibility `tcs1_part_image`/`TCPA` typed-row asset containing row IDs,
  tombstones, and any `typed_row_asset` owned fields;
- a `tcs1_typed_column_part` asset containing the authoritative scalar,
  fixed-dimension `float32_vector`, and fixed-degree `adjacency_list`
  `typed_column_part` values for the same generation.

Manifest part records classify these assets as `base`, `delta`, or `tombstone`.
Insert/base spans use `base`; updates use `delta`; deletes publish only a
`tombstone` typed-row asset. Retained-payload reconstruction and direct typed
int64 scans resolve latest-visible identity from the typed-row base/delta/
tombstone lineage, then read the matching typed-column part by row index for the
winning non-deleted generation. For nullable typed-column fields, reconstruction
must preserve source-document intent: present/non-null rows write the declared
path and value, explicit-null rows write the declared path with JSON null, and
missing/default rows leave the declared path absent from the retained-payload
reconstruction. Reopen/recovery uses the manifest refs and existing typed asset
manager paths; typed-column refs participate in reachability and rewrite/GC
eligibility as durable typed-storage assets.

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
`float32` payloads. `adjacency_list` typed-column data is stored as uncompressed
raw little-endian `uint32` payloads with exactly `adjacency_degree` elements per
row. The `typedcolumn` image builder keeps all sections aligned to
8-byte boundaries, which is sufficient for `float32` and `uint32` mappedresource
direct views. Readers must validate lifetime, range, length, endian mode, and
alignment through #1736 `mappedresource` handles before exposing direct typed
views; misaligned direct views fall back to heap/scratch decode or fail closed.

## Query, Predicate, and Allocation Boundary

The explicit typed-column int64 predicate scan supports only non-nullable int64
`typed_column_part` fields today. If the requested typed-column field is
nullable, or if nullable metadata is observed on the direct typed-column int64
path, the scan fails closed with `ErrColumnQueryPlanUnsupported`; it must not
fall back to full-document reconstruction/materialization, and it must not treat
null or missing rows as integer zero. The int64 and string prepared paths consume
the semantic capability matrix during prepare. Broader optimizer routing, string
predicate expansion, aggregate integration, and vector/adjacency nullable scans
are deferred to follow-up issues.

Direct typed-column predicate paths must preserve hot-path allocation discipline
and should actively remove existing avoidable allocations or obvious local
overhead in the touched path when that cleanup is bounded and testable. Use
typed-column sections, decoder scratch, setup-time decoder/metadata construction,
direct validated views, and pre-sized output buffers rather than per-row maps,
wrappers, interface values, closures, or string conversions. Nullable/missing
codec, scan, and reconstruction merge benchmarks must time the core typed-column
hot loop separately from public document materialization and target 0 allocs/op
after setup. Touched inner loops must be measurably no worse, and preferably
better, on `B/op` and `allocs/op`; if benchmarks or allocation profiles expose
allocations in touched functions, the PR must fix them or explicitly call out why
they are out of scope with a linked follow-up recommendation. Any remaining
hot-path allocation requires baseline-versus-final `B/op`/`allocs/op` evidence
and an allocation profile/top that names and justifies the source or defers it
with rationale. Future typed-column format/schema changes must state whether
they preserve 0-alloc/near-0-alloc decode and scan paths or introduce an explicit
benchmarked fallback. These allocation targets do not relax checksum, lifetime,
schema, or fail-closed validation.

## Boundary

The only production `TreeDB/collections` file that imports
`TreeDB/internal/typedcolumn` is the adapter seam. Publication/reopen logic calls
through that seam. Query/vector search integration remains deferred to later
issues `#1757`/`#1782`/`#1790`; this path publishes fixed-dimension
`float32_vector` and fixed-degree `adjacency_list` values but does not switch
native vector graph search to typed-column parts or add SIMD acceleration.
