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
`typed-column-direct-view-closeout-1899.md`.

## Scope and non-goals

The active stack targets typed-column fixed-width scalar/vector payloads only:

| Value type | Active direct-view payload candidate | Notes |
| --- | --- | --- |
| `ColumnStoreValueInt64` | yes | raw non-null uncompressed typed-column payload, 8-byte little-endian `int64`. |
| `ColumnStoreValueFloat32` | yes, native scalar layout | 4-byte little-endian IEEE-754 bits. The current raw-`int64` float bit carrier is compatibility/fallback-only and must not certify native scalar float direct views. |
| `ColumnStoreValueDouble` | yes, native scalar layout | 8-byte little-endian IEEE-754 bits. The current raw-`int64` float bit carrier is compatibility/fallback-only and must not certify native scalar float direct views. |
| `ColumnStoreValueFloat32Vector` | yes | fixed-dim row-major little-endian `float32` payloads. |
| `ColumnStoreValueBool` | no | bitpack/RLE and future bool encodings remain fallback-only until separately specified. |
| `ColumnStoreValueString` | no | string values, dictionaries, and dictionary codes are not string direct-view payloads. |
| `ColumnStoreValueAdjacencyList` | v1 offsets-list primitive plus fallback compatibility | #1915 implements the explicit typed-column `raw_uint32_offsets_list` variable-list primitive (`uint64` offsets, `uint32` values) for safe writer/fallback-reader use. Existing dense fixed-degree `raw_uint32_dense` and physical row-asset adjacency remain fallback/compatibility; unsafe direct-view readers and graph search consumption are deferred to #1916+. |

Physical row assets are deferred/fallback-only for this stack and must remain
linked to #1897. Row-asset vector/adjacency/generic consumers must not be counted
as current-stack mmap direct-view evidence.

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
unaligned. Current active candidates require at most 8-byte absolute alignment.
Segment-prefix padding is not part of the asset ref payload/checksum, but it is
part of the segment file size and appender offset progression; tests assert the
bytes are zero and that multiple typed-column-part assets in the same segment
remain aligned.

`DirectViewCertified` remains false for bool bitpack/RLE, strings/dictionaries,
nullable/default wrappers, compressed payloads, variable-width delta layouts,
physical row assets, and adjacency direct views. The adapter's internal
`__treedb_primary_id` row-locator column is also not a #1895 certified value
column; only declared `ColumnStoreValue*` typed-column-part fields may be active
writer certification targets. Synthetic or legacy refs that start at a
misaligned segment offset must fail closed or use fallback planning even when
their image-local layout contract is otherwise valid.

## Storage owner and consumer matrix

| Storage owner/path | Consumer path | Current classification |
| --- | --- | --- |
| `typed_column_part` | generic typed-column scalar/vector consumers | `int64`, native `float32`, native `double`, and `float32_vector` are active little-endian candidates after certification and read-time checks. |
| `typed_column_part` | `column_graph` typed-column vector source | `float32_vector` is the active candidate. Other value types fallback or are inapplicable. |
| `typed_column_part` | `adjacency_list` consumers | `raw_uint32_offsets_list` is the selected #1901 v1 primitive and has safe writer/fallback-reader support; legacy dense fixed-degree `raw_uint32_dense` remains fallback/compatibility. |
| physical row asset | vector, adjacency, or generic row consumers | deferred/fallback-only; #1897 owns row-record alignment/padding; #1899 records this as a safe deferral, not current-stack mmap evidence. |

## Required payload byte order fixtures

Conformance fixtures must pin little-endian bytes for:

- scalar `int64` values;
- native scalar `float32` bits, including NaN payloads, non-canonical NaNs,
  infinities, min/max finite values, and `+0` versus `-0`;
- native scalar `double`/`float64` bits with the same raw-bit edge cases;
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

### `raw_uint32_offsets_list` adjacency primitive (#1914)

The #1901 v1 adjacency target is an extension of
`ColumnStoreValueAdjacencyList` selected by the explicit metadata selector
`adjacency_layout: "uint32_offsets_list"` plus the internal encoding
`raw_uint32_offsets_list`, not a new public value type and not an accidental
missing `adjacency_degree` dense row. Its physical shape is:

```text
offsets []uint64  // row_count + 1, little-endian
values  []uint32  // flattened adjacency values, little-endian
```

The direct-view contract records and validates the offsets and values sections
separately:

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

Empty lists are represented by equal adjacent offsets. V1 direct views are only
for non-null, non-default, uncompressed offsets-list payloads. Fixed dense
`raw_uint32_dense` rows remain a distinct fallback/compatibility layout, and
physical row-asset adjacency direct views remain deferred to #1897.

### Fallback-only/deferred encodings

Bool bitpack/RLE, strings/dictionaries, nullable/default wrappers, compressed
payloads, variable-width varint/delta/double-delta layouts, physical row assets,
and adjacency direct views are fallback-only or deferred unless a future issue
adds a new explicit encoding and conformance row. For adjacency, that explicit
row is `raw_uint32_offsets_list`; #1915 enables the safe writer/fallback reader,
while unsafe direct-view readers and graph search runtime remain deferred.

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
| `certification_failure` | manifest/layout/checksum/schema certification rejected direct view. |
| `absolute_offset_unaligned` | `asset_ref.offset + payload offset` failed alignment. |
| `actual_pointer_unaligned` | concrete Go byte-slice address failed alignment. |
| `stale_handle` | nil/released/out-of-lifetime handle rejected view construction. |
| `offsets_list_validation_failure` | `raw_uint32_offsets_list` shape validation failed (offset count, monotonicity, Go `int` range, value length, or offsets/values section identity). |
| per-reason fallback counts | map keyed by stable reason strings such as `wrong_endian`, `length_multiple_mismatch`, `row_count_mismatch`, `dimension_mismatch`, `nullable_default_wrapper`, `compressed`, and `direct_view_deferred`. |

## Baseline benchmark harness for later PRs

This PR does not claim a speedup. Later implementation PRs should run focused
baselines and final measurements with exact branch/commit, hardware, rows,
dimensions, `ns/op`, ops/sec, `B/op`, `allocs/op`, direct/fallback counters,
padding bytes, storage bytes, mapped bytes, decoded bytes, and hot-loop allocs.

Later #1915+ PRs that claim adjacency speedups must also define permanent
primitive microbenchmarks for fallback decode, direct-view prepare/open, and
per-row offsets-list iteration, then graph benchmarks for serial no-doc,
serial full-doc, serial exclude-embedding, parallel graph-only, and parallel
prepared-searcher document modes. Report `adjacency_mmap_direct/search`,
`adjacency_heap_copy_typed_view/search`, `adjacency_scratch_decode/search`,
offsets-list certification failures, padding bytes for offsets and values
sections, and CPU/allocation profile summaries against latest-main baselines.

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
