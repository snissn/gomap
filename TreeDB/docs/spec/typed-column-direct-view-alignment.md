# Typed-Column Aligned Fixed-Width Direct-View Contract (#1893)

Status: pre-alpha safety contract, writer-certification target, and conformance
baseline for the #1886 stack. This document defines when TreeDB column-store
bytes may be exposed as typed Go slices without copying. Issue #1895 landed the
writer side for typed-column-part fixed-width payloads: aligned image sections,
deterministic segment-prefix padding, and writer-certified layout contracts for
the active fixed-width candidates. The #1737 payload phase added native
little-endian scalar float payload encodings and shared fixed-width helpers.
Issues #1896 and #1898 added reader consumption for generic typed-column paths
and the column-graph typed-column vector source. The final issue #1899 evidence
matrix and deferral closeout is recorded in
`typed-column-direct-view-closeout-1899.md`. The generic optimized-consumer tier
matrix in `typed-column-optimized-consumer-capabilities.md` now owns the
cross-type `mmap_direct`/fallback classification that graph-search admission
(#2044) and reusable direct-view certifiers (#2046) consume; the role-specific
graph-search prepared-view policy is in
`typed-column-graph-search-prepared-views.md`.

## Scope and non-goals

The active stack targets typed-column fixed-width scalar/vector payloads only:

| Value type | Active direct-view payload candidate | Notes |
| --- | --- | --- |
| `ColumnStoreValueInt64` | yes | raw non-null uncompressed typed-column payload, 8-byte little-endian `int64`. |
| `ColumnStoreValueFloat32` | yes, native scalar layout | 4-byte little-endian IEEE-754 bits. The current raw-`int64` float bit carrier is compatibility/fallback-only and must not certify native scalar float direct views. |
| `ColumnStoreValueDouble` | yes, native scalar layout | 8-byte little-endian IEEE-754 bits. The current raw-`int64` float bit carrier is compatibility/fallback-only and must not certify native scalar float direct views. |
| `ColumnStoreValueInt8`/`Uint8`/`Int16`/`Uint16`/`Int32`/`Uint32` | yes | non-null uncompressed matching raw primitive scalar payloads; multi-byte values are little-endian and int64-compatible stats/pruning may be published. |
| `ColumnStoreValueUint64` | yes, payload only | non-null uncompressed little-endian `uint64` payloads; direct-view payload certification is active, but int64-compatible stats/pruning are not. |
| `ColumnStoreValueFloat16`/`BFloat16` | yes, storage-only bits | non-null uncompressed raw little-endian `uint16` bit payloads. Bits are preserved exactly; numeric float fast paths are not implied. |
| `ColumnStoreValueUint8Vector`/`Int8Vector` | yes | row-major fixed-width 1-byte dense numeric vector payloads with positive `elements_per_row`. |
| `ColumnStoreValueUint16Vector`/`Int16Vector`/`Float16Vector`/`BFloat16Vector` | yes | row-major fixed-width 2-byte little-endian dense numeric vector payloads with positive `elements_per_row`; float16/bfloat16 are raw bit payloads. |
| `ColumnStoreValueUint32Vector`/`Int32Vector` | yes | row-major fixed-width 4-byte little-endian dense numeric vector payloads with positive `elements_per_row`; `uint32_vector` is distinct from `adjacency_list`. |
| `ColumnStoreValueUint64Vector`/`Int64Vector`/`Float64Vector` | yes | row-major fixed-width 8-byte little-endian dense numeric vector payloads with positive `elements_per_row`. |
| `ColumnStoreValueFloat32Vector` | yes | fixed-dim row-major little-endian `float32` payloads. |
| `ColumnStoreValueBool` | no | bitpack/RLE and future bool encodings remain fallback-only until separately specified. |
| `ColumnStoreValueString` | no | string values and dictionary string tables are not string direct-view payloads. Derived dictionary-code sidecar row-code payloads are a separate `uint32` sidecar direct-view format. |
| `ColumnStoreValueAdjacencyList` | legacy offsets-list compatibility | #1916 enables certified typed-column `raw_uint32_offsets_list` direct views (`uint64` offsets, `uint32` values) after #1915 writer/fallback-reader support, and #1917 wires that reader through the adapter. The graph-specific `column_graph` adjacency-source publication/consumption path is quarantined by #1989; primary HNSW adjacency now uses `uint32_list` vector-index state above the reusable offsets/value mechanics. Existing dense fixed-degree `raw_uint32_dense` and physical row-asset adjacency remain fallback/compatibility. |

Physical row assets are deferred/fallback-only for this stack and must remain
linked to #1897. Row-asset vector/adjacency/generic consumers must not be counted
as current-stack mmap direct-view evidence.

Derived dictionary-code and int64 value sidecars are outside the typed-column
declared-value matrix but now follow the same fixed-width safety vocabulary:
version-2 `tcs1_dictionary_codes` assets keep manifest-style metadata/dictionary
headers, then expose an exactly `rows * 4` little-endian `uint32` local-code
payload after zero padding to 4-byte alignment; version-2 `tcs1_int64_values`
assets keep manifest-style metadata headers, then expose an exactly `rows * 8`
little-endian `int64` value payload after zero padding to 8-byte alignment. The
sidecar writer pads both the asset payload (relative payload offset) and the
segment prefix (absolute `asset_ref.offset + payload_offset`) so normal mmap
reads can construct a `[]uint32` or `[]int64` view on native little-endian hosts;
readers still perform concrete pointer-alignment, length, row-count,
cardinality where applicable, and schema/ref/checksum checks before using the
view. Call sites are responsible for bounding the direct-view slice lifetime to
the raw mmap/cache bytes it aliases, with copy/fallback helpers available when a
caller needs owned data or the source is unsupported.

## Writer certification and padding (#1895)

New typed-column-part images include a `layout_contract` section. For the active
writer-certified columns above, `DirectViewCertified` is set only for raw,
non-null, non-default, uncompressed, little-endian fixed-width payload sections
whose contract records:

- logical value type and typedcolumn physical type/encoding;
- element size, required alignment, endian, length multiple, row count, and
  fixed elements per row for dense vector rows;
- section offset/length/checksum and every block payload offset/length;
- zero null/default counts and no null/default mask flags; and
- descriptor, manifest, row-count, and checksum identity matching the image.

The writer pads image sections with deterministic zero bytes, and typed-column
part segment writers prepend deterministic zero padding before direct-view
candidates when the current segment offset would make
`asset_ref.offset + section.offset` or `asset_ref.offset + block.payload_offset`
unaligned. Most active candidates require at most 8-byte absolute alignment.
Scalar-u8 vector-index `quantized_codes` images request 64-byte section and
segment placement so every row begins on a cache-line boundary; legacy 8-byte
images remain readable. Physical asset rewrite preserves 64-byte placement when
the source ref is 64-byte aligned, and reachability accounting recognizes the
deterministic zero prefix used for that placement.
Segment-prefix padding is not part of the asset ref payload/checksum, but it is
part of the segment file size and appender offset progression; tests assert the
bytes are zero and that multiple typed-column-part assets in the same segment
remain aligned.

`DirectViewCertified` remains false for bool bitpack/RLE, strings/dictionaries,
nullable/default wrappers, compressed payloads, variable-width delta layouts,
physical row assets, and legacy dense adjacency rows. It is true for the
explicit non-null, non-default, uncompressed `raw_uint32_offsets_list` adjacency
primitive only when both global sections satisfy the offsets/value contract below.
The adapter's internal `__treedb_primary_id` row-locator column is also not an
issue #1895 certified value column; only declared `ColumnStoreValue*`
typed-column-part fields may be active writer certification targets. Synthetic
or legacy refs that start at a misaligned segment offset must fail closed or use
fallback planning even when their image-local layout contract is otherwise
valid.

## Storage owner and consumer matrix

| Storage owner/path | Consumer path | Current classification |
| --- | --- | --- |
| `typed_column_part` | generic typed-column scalar/vector consumers | `int64`, native `float32`, native `double`, #1929 primitive scalars, and `float32_vector` are active little-endian candidates after certification and read-time checks. |
| `typed_column_part` | `column_graph` typed-column vector source | `float32_vector` is the active candidate. Other value types fallback or are inapplicable. |
| `typed_column_part` | vector-index state `uint32_list` consumer | `raw_uint32_offsets_list` is active for HNSW adjacency state with safe writer/fallback-reader plus certified primitive direct-view support. The `adjacency_list` selector remains compatibility-only. |
| `typed_column_part` | legacy column_graph adjacency sources or legacy dense adjacency | Graph-specific `raw_uint32_offsets_list` source assets may be validated/reopened for old pre-alpha records and explicit compatibility fixtures, but #1989 quarantines them and new graph builds must not publish them. Row-asset adjacency remains fallback compatibility. Legacy fixed-degree `raw_uint32_dense` remains fallback/deferred compatibility. |
| physical row asset | vector, adjacency, or generic row consumers | deferred/fallback-only; #1897 owns row-record alignment/padding; #1899 records this as a safe deferral, not current-stack mmap evidence. |

## Required payload byte order fixtures

Conformance fixtures must pin little-endian bytes for:

- scalar `int64` values;
- native scalar `float32` bits, including NaN payloads, non-canonical NaNs,
  infinities, min/max finite values, and `+0` versus `-0`;
- native scalar `double`/`float64` bits with the same raw-bit edge cases;
- #1929 primitive integer scalars (`int8`, `uint8`, `int16`, `uint16`,
  `int32`, `uint32`, `uint64`) and storage-only `float16`/`bfloat16` raw-bit
  payloads;
- dense row-major `float32_vector` payloads.

Wrong-endian fixtures must fail closed or fallback. Raw-`int64` float carriers
must remain bit-preserving compatibility layouts, not native scalar float direct
views.

## Safety checks and placement

A direct-view decision has two validation boundaries. Checks may move earlier in
later writer/reader PRs only when the same fail-closed behavior is preserved.

### Required immediately before unsafe view construction

- Actual Go pointer alignment for the concrete byte slice.
- Exact byte length and element count for the requested view.
- Host endian compatibility.
- Handle/lifetime/released-state validation.
- Source classification: `mmap_direct_view` requires a mapped source. A
  heap-copy typed view can be safe when the lifetime is owned, but it is a
  fallback and must not be counted as zero-copy mmap speedup evidence.

### Hoistable to asset/block certification

- Logical value type, physical type, encoding, and compression/null/default
  exclusion.
- Row count, fixed dimensions/degree, section and block bounds, payload length
  multiple, and manifest identity.
- Checksum/integrity when the read-integrity policy requires it.
- Absolute storage alignment: `asset_ref.offset + section.offset` and
  `asset_ref.offset + block.payload_offset` must satisfy the required alignment.
  Relative section-local alignment is insufficient because an aligned image can
  be appended at an unaligned segment offset. For #1895 typed-column-part
  writers, the segment/appender layer supplies deterministic zero prefix padding
  before active direct-view candidates so newly written assets satisfy this rule.

### `raw_uint32_offsets_list` offsets/value mechanics (#1914, #1989 quarantine)

The #1901 path implemented an offsets-list extension of
`ColumnStoreValueAdjacencyList` selected by the explicit metadata selector
`adjacency_layout: "uint32_offsets_list"` plus the internal encoding
`raw_uint32_offsets_list`, not a new public value type and not an accidental
missing `adjacency_degree` dense row. Issue #1989 keeps that graph-specific
selector compatibility-only: the primary path is the physical split offsets/value
mechanics behind generic `uint32_list` vector-index state. Its physical shape is:

```text
offsets []uint64  // row_count + 1, little-endian
values  []uint32  // flattened uint32 values, little-endian
```

The image format records one canonical column-wide offsets section and one
column-wide values section per offsets-list column. Multi-block typed-column
parts keep block-local payloads internally, but serialized images publish global
offsets (exactly `row_count + 1`) plus global flattened values so each section
has a single checksum/range identity. The direct-view contract records and
validates the offsets and values sections separately:

| Section | Element type | Element size | Absolute alignment | Length rule |
| --- | --- | ---: | ---: | --- |
| offsets | `uint64` | 8 bytes | 8 bytes | exactly `row_count + 1` elements. |
| values | `uint32` | 4 bytes | 4 bytes | exactly `offsets[row_count]` elements. |

Validation is split by layer:

- primitive certification/read validation checks `offsets` length exactly
  `row_count + 1`, `offsets[0] == 0`, monotonic offsets, final offset exactly
  equal to the `uint32` value count, exact byte lengths for both sections,
  little-endian identity, Go `int` range before slicing, section checksums and
  bounds, no null/default/compression wrappers, and mappedresource lifetime;
- absolute alignment is checked separately for
  `asset_ref.offset + offsets_section.offset` (8-byte `uint64` alignment) and
  `asset_ref.offset + values_section.offset` (4-byte `uint32` alignment);
- actual Go pointer alignment is still checked immediately before exposing each
  unsafe typed view;
- graph-level validation remains graph-owned: neighbor ordinal bounds, layer
  semantics, deleted-row rejection, row identity, candidate ordering, and score
  correctness are not primitive checks.

See `typed-column-uint32-list-adjacency-quarantine.md` for the #1989 inventory of
what to keep, remove, or quarantine. See
`typed-column-uint32-list-semantics.md` for the #1984 logical `uint32_list`
semantics, validation invariants, and length-only offsets behavior.

Empty lists are represented by equal adjacent offsets. V1 direct views are only
for non-null, non-default, uncompressed offsets-list payloads. Fixed dense
`raw_uint32_dense` rows remain a distinct fallback/compatibility layout, and
physical row-asset adjacency direct views remain deferred to #1897.

### Fallback-only/deferred encodings

Bool bitpack/RLE, strings/dictionaries, nullable/default wrappers, compressed
payloads, variable-width varint/delta/double-delta layouts, physical row assets,
and legacy dense adjacency direct views are fallback-only or deferred unless a
future issue adds a new explicit encoding and conformance row. The old
`adjacency_layout` compatibility row remains `raw_uint32_offsets_list`; issue
`#1915` enables the safe writer/fallback reader, issue `#1916` enables the
primitive direct-view reader, and issue `#1917` wires that reader through adapter
scans. Column-graph rebuild/search now uses vector-index state `uint32_list`
assets on the primary path; new storage work must not extend the graph-specific
source path as the target architecture.

## Old/non-certified behavior

Prepared direct-view paths require a writer-certified layout contract. Missing,
old, unsupported, or corrupt contracts fail closed with a pre-alpha rebuild error
or use an explicit fallback path. Generic image parsing may remain tolerant for
corruption tests/tooling, but optimized prepared paths must not silently trust
non-certified assets.

## Counter vocabulary

Later writer/reader PRs must use these stable counter names and reason buckets:

| Counter | Meaning |
| --- | --- |
| `mmap_direct_view` | zero-copy typed view from mapped storage after certification and read-time checks. |
| `offsets_mmap_direct_view` | zero-copy typed `uint64` offsets view for `raw_uint32_offsets_list` after offsets-section certification and read-time checks. |
| `values_mmap_direct_view` | zero-copy typed `uint32` values view for `raw_uint32_offsets_list` after values-section certification and read-time checks. |
| `heap_copy_typed_view` | safe typed view over owned heap bytes; fallback, not zero-copy evidence. |
| `offsets_heap_copy_typed_view` | safe typed offsets view over owned heap bytes; fallback, not zero-copy evidence. |
| `values_heap_copy_typed_view` | safe typed values view over owned heap bytes; fallback, not zero-copy evidence. |
| `scratch_decode` | decode into caller/session scratch. |
| `streaming_fallback` | streaming codec or byte-loop fallback. |
| `source_unsupported` | mappedresource handle source did not match the direct-view requirement. |
| `certification_failure` | manifest/layout/checksum/schema certification rejected direct view. |
| `absolute_offset_unaligned` | `asset_ref.offset + payload offset` failed alignment. |
| `actual_pointer_unaligned` | concrete Go byte-slice address failed alignment. |
| `stale_handle` | nil/released/out-of-lifetime handle rejected view construction. |
| `offsets_list_validation_failure` | `raw_uint32_offsets_list` shape validation failed (offset count, monotonicity, Go `int` range, value length, or offsets/values section identity). |
| per-reason fallback counts | map keyed by stable reason strings such as `wrong_endian`, `length_multiple_mismatch`, `row_count_mismatch`, `dimension_mismatch`, `nullable_default_wrapper`, `compressed`, and `direct_view_deferred`. |

## Baseline benchmark harness for later PRs

Issue #1916 claims primitive reader behavior only: certified direct-view prepare/open
classification and allocation-free per-row offsets-list iteration after prepare.
It does not claim a column_graph/search speedup. Later graph-consuming PRs
should run focused baselines and final measurements with exact branch/commit,
hardware, rows, dimensions, `ns/op`, ops/sec, `B/op`, `allocs/op`,
direct/fallback counters, padding bytes, storage bytes, mapped bytes, decoded
bytes, and hot-loop allocs.

Later graph/search PRs that claim adjacency search speedups must also define
permanent primitive microbenchmarks for fallback decode, direct-view
prepare/open, and per-row offsets-list iteration, then graph benchmarks for
serial no-doc, serial full-doc, serial exclude-embedding, parallel graph-only,
and parallel prepared-searcher document modes. Report
`adjacency_mmap_direct/search`, `adjacency_heap_copy_typed_view/search`,
`adjacency_scratch_decode/search`, offsets-list certification failures, padding
bytes for offsets and values sections, and CPU/allocation profile summaries
against latest-main baselines.

Suggested commands:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkTypedColumn(Int64PredicateAggregatePrepared|Vector|Adjacency|.*Direct|.*Fallback)|Benchmark(ColumnVectorGraphNativeSearchCosineV3|OpenVectorIndexSearcherColumnGraphNativeReaderV4)' \
  -benchmem -benchtime=500ms -count=5

GOWORK=off go test ./TreeDB/internal/typedcolumn \
  -run '^$' \
  -bench 'BenchmarkTypedColumnVectorDense(DirectView|Section)Scan|BenchmarkTypedColumnDenseFloat32Dot' \
  -benchmem -benchtime=500ms -count=5
```

Report heap-copy typed views separately from `mmap_direct_view`, and do not use
heap-copy views as primary direct-view speedup evidence.
