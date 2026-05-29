# Typed-Column Adapter and Durable Vector/Adjacency Publication (#1754/#1755/#1756/#1783)

Status: current implementation note for issues #1754, #1755, #1756, and #1783 under parent tracker #1744.
Typed-column schema/version evolution and migration policy is defined in
`typed-column-schema-evolution.md`. Logical capability status and admission
rules are defined in `typed-column-semantics.md`; physical layout/codec
capabilities are defined in `typed-column-layout-capabilities.md`; aligned
fixed-width direct-view certification rules are defined in
`typed-column-direct-view-alignment.md`.

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
| `int64` | represented | `typedcolumn.ColumnTypeInt64` delta-varint by default. Explicit `fixed_width_encoding: "little_endian"` on a non-null `typed_column_part` int64 column selects uncompressed `raw_int64` little-endian payload bytes. |
| `float32` | represented | Default compatibility layout remains a raw int64 column carrying `math.Float32bits` in the low 32 bits; these bits must not be treated as int64 ordering/sum/min/max/stats/pruning semantics or native direct-view evidence. Explicit `fixed_width_encoding: "little_endian"` on a non-null `typed_column_part` float32 column selects native uncompressed `raw_float32` little-endian IEEE-754 payload bytes. |
| `double` / `float64` | represented | Default compatibility layout remains a raw int64 column carrying `math.Float64bits`; these bits must not be treated as int64 ordering/sum/min/max/stats/pruning semantics or native direct-view evidence. Explicit `fixed_width_encoding: "little_endian"` on a non-null `typed_column_part` double column selects native uncompressed `raw_float64` little-endian IEEE-754 payload bytes. |
| `string` | represented | Low-cardinality uint32 codes plus typed-column dictionary section metadata; code order must not imply lexical range/prefix unless dictionary order and collation proof are supplied. |
| `float32_vector` | represented | Fixed-dimension row-major dense little-endian `float32` sections with `vector_dims` as elements per row; active typed-column direct-view candidate after certification/read-time checks. |
| `uint32_list` | represented | Generic non-null variable-width integer-list sections using `raw_uint32_offsets_list`: a `uint64` sentinel offsets substream (`rows+1`) plus flattened little-endian `uint32` values. Writer, owned fallback reader, and certified direct-view reader are generic and do not require adjacency semantics. |
| `adjacency_list` | represented for dense compatibility; legacy offsets-list compatibility reader | Empty `adjacency_layout` keeps fixed-degree row-major dense little-endian `uint32` sections with `adjacency_degree` as elements per row. `adjacency_layout: "uint32_offsets_list"` selects the #1915/#1916/#1901 variable-list compatibility path (`uint64` offsets plus `uint32` values) on the same value type for safe writer/fallback-reader publication and certified direct views. #1989 quarantines graph-specific storage integration; primary list storage is generic `uint32_list`. |

## `uint32_list` adapter naming boundary (#1984)

`typed-column-uint32-list-semantics.md` defines the generic logical primitive
that #1985 admits through the adapter. The preferred public compatibility
constant is `ColumnStoreValueUint32List` with string `uint32_list`; #1985 adds
that code vocabulary, adapter mapping, conformance tests, writer/fallback
reader/direct-view paths, and naming regression updates.

When admitted, `uint32_list` uses `raw_uint32_offsets_list` as the physical
encoding: a first-class offsets/size substream of little-endian `uint64` sentinel
offsets (`rows+1`, `offsets[0] == 0`) plus a flattened little-endian `uint32`
values substream. Offset/length-only adapter APIs may read and validate offsets
without decoding values, but full row-value reconstruction must validate the
values section and fail closed on missing, corrupt, mismatched, compressed,
nullable, or nested list assets. The current `ColumnStoreValueAdjacencyList` and
`adjacency_layout` selector remain legacy/consumer-specific compatibility, not
the generic primitive.

Nullable scalar adapter support uses `nullable_int64` as the carrier encoding
for bool, int64, float32, double, and low-cardinality string fields: explicit
JSON null maps to null bitmap rows, omitted paths map to default/missing bitmap
rows, and present values map to the encoded carrier payload (`0/1` bools,
int64s, float bit patterns, or string dictionary codes). Vector and adjacency
nullable/missing support remains staged/fail-closed; `adjacency_list`
`typed_column_part` owners using the dense compatibility layout must declare
positive `adjacency_degree` and each present row must contain exactly that many
uint32 neighbors. Offsets-list adjacency uses explicit `adjacency_layout:
"uint32_offsets_list"`; it must not be inferred from a missing degree and is
supported by the safe #1915 writer/fallback reader path plus the #1916 certified
primitive direct-view reader wired through the adapter in #1917. Column-graph
rebuild/search now publishes and consumes generic `uint32_list` vector-index
state; #1989 keeps graph-specific offsets-list adjacency sources compatibility-only
rather than the datastore target. Serialized typed-column images publish
offsets-list columns as one global
`row_count + 1` little-endian `uint64` offsets section plus one global flattened
`uint32` values section, even when the typed-column part has multiple codec
blocks/granules.

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

For inserts and updates with scalar, fixed-dimension vector, fixed-degree dense
adjacency, or current explicit offsets-list variable adjacency `typed_column_part`
owners, TreeDB writes:

- a compatibility `tcs1_part_image`/`TCPA` typed-row asset containing row IDs,
  tombstones, and any `typed_row_asset` owned fields;
- a `tcs1_typed_column_part` asset containing the authoritative scalar,
  fixed-dimension `float32_vector`, fixed-degree dense `adjacency_list`, and
  explicit offsets-list variable `adjacency_list` `typed_column_part` values for
  the same generation.

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
and `[]uint32` buffers. Offsets-list adapter reads use paired `[]uint64`
offsets and `[]uint32` values handles and classify mmap direct, heap-copy typed,
scratch decode, stale-handle, source-unsupported, absolute-offset-unaligned, and
actual-pointer-unaligned outcomes separately. Durable typed-column asset reads
use the typed asset read cache with `mappedresource.ClassTypedColumnAsset` when a
manager is supplied.

## Retained Payload Seam

`typedColumnAdapterRetainedPayloadSplitRestore` reuses the production
retained-payload split/restore helpers as an internal test seam. It does not alter
production retained-payload behavior.

## Fixed-Width Payload Safety (#1737/#1756)

When `fixed_width_encoding: "little_endian"` is selected on non-null scalar
`typed_column_part` fields, `int64`, `float32`, and `double` use native
uncompressed little-endian raw payload sections (`raw_int64`, `raw_float32`, and
`raw_float64`). Native scalar float payloads preserve IEEE-754 bits exactly,
including NaN payloads, non-canonical NaNs, infinities, finite extrema, and
`+0`/`-0`. Metadata/control records, nullable/default wrappers, compression
payloads, sortable keys, and physical row assets are outside this native scalar
payload rule.

`float32_vector` typed-column data is stored as uncompressed raw little-endian
`float32` payloads. Default/dense `adjacency_list` typed-column data is stored as
uncompressed raw little-endian `uint32` payloads with exactly
`adjacency_degree` elements per row. The #1915 offsets-list selector instead
uses `uint64` offsets plus flattened `uint32` values; #1916 can expose paired
primitive direct views after certification/read-time checks or decode through
safe owned fallback slices. The `typedcolumn` image builder keeps sections
relatively aligned; current
direct-view eligibility also requires absolute storage alignment
(`asset_ref.offset + section/block offset`) and actual Go pointer alignment at
view construction time. Readers must validate
lifetime, range, length, endian mode, absolute offset alignment, source, and
actual pointer alignment through #1736 `mappedresource` handles before exposing
mmap direct typed views; heap-copy typed views and scratch decodes are safe
fallbacks but not zero-copy speedup evidence. #1737 standardizes payload bytes
and helpers only; production unsafe reader rollout remains in downstream #1886
children.

## Query, Predicate, and Allocation Boundary

The explicit typed-column int64 predicate scan supports only non-nullable int64
`typed_column_part` fields today. If the requested typed-column field is
nullable, or if nullable metadata is observed on the direct typed-column int64
path, the scan fails closed with `ErrColumnQueryPlanUnsupported`; it must not
fall back to full-document reconstruction/materialization, and it must not treat
null or missing rows as integer zero. The int64 and string prepared paths consume
the semantic capability matrix during prepare; int64 prepared aggregate planning
also consumes the layout/codec capability contract so `delta_varint` remains a
streaming decode layout while explicit `raw_int64` uses a safe little-endian byte
reducer. Broader optimizer routing, string predicate expansion, aggregate
integration, #1849 zero-copy direct views, and vector/adjacency nullable scans
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

Production `TreeDB/collections` imports of `TreeDB/internal/typedcolumn` stay
limited to the adapter seam and scoped vector-graph source/reader seams.
Publication/reopen logic calls through those seams. Query/vector search
integration remains graph-owned by the vector-index issues. The active adapter
stack consumes certified typed-column `float32_vector` payloads and generic
`uint32_list` / `raw_uint32_offsets_list` variable-list payloads; physical
row-asset direct views and legacy dense adjacency direct views remain
fallback/deferred. This path publishes fixed-dimension `float32_vector`,
fixed-degree dense `adjacency_list`, compatibility offsets-list
`adjacency_list`, and primary generic `uint32_list` values. `column_graph`
rebuilds publish HNSW adjacency through vector-index state `uint32_list` assets;
#1989 quarantines old per-layer graph-source assets as compatibility-only. New
storage work must route through generic `uint32_list` typed-column assets and
vector-index state; see `typed-column-uint32-list-adjacency-quarantine.md`.
