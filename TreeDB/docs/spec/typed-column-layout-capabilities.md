# Typed-Column Layout/Codec Capability Contracts (#1838)

Status: pre-alpha internal contract for typed-column prepare/read planning. Semantic
operation support remains defined in `typed-column-semantics.md`; this document
covers the physical layout/codec contract that decides whether a semantic
operation may use a direct view, safe fixed-width reducer, streaming decoder,
stats payload, or pruning metadata. The aligned direct-view safety contract and
counter vocabulary are specified in `typed-column-direct-view-alignment.md`. The
stats envelope and payload contract is specified in `typed-column-stats.md`;
durable pruning metadata is specified in `typed-column-pruning.md`.

The optimized-consumer tier matrix in
`typed-column-optimized-consumer-capabilities.md` classifies these layout facts
into `mmap_direct`, `heap_typed_view`, `scratch_decode`, `predicate_only`,
`generic_only`, and `unsupported/experimental` consumer contracts. The graph
role-specific prepared-view gate is in
`typed-column-graph-search-prepared-views.md`. This document remains the physical
layout/codec source of truth for whether those tiers can be certified.

The implementation lives in `TreeDB/internal/columnlayout`. Capability keys are
not just encodings. A key includes:

- collection logical value type;
- `typedcolumn.ColumnType` physical carrier;
- `typedcolumn.Encoding`;
- `typedcolumn.Compression`;
- nullable/default/dictionary wrappers;
- fixed-width element count for dense vector/adjacency layouts; or
- the explicit legacy adjacency layout selector/internal encoding for the
  `raw_uint32_offsets_list` compatibility path. #1989 quarantines that
  graph-specific integration; primary variable-list storage is generic
  `uint32_list`.

## Current reference layouts

| Logical type | Physical + encoding | Layout capability |
| --- | --- | --- |
| `int64` | `int64` + `delta_varint` | Variable-width streaming int64 reducer/range predicate plus value-row pruning metadata. No direct view. |
| `int64` | `int64` + `raw_int64` + `compression=none` | Optional explicit fixed-width little-endian layout. Safe byte-loop reducer/range predicate and value-row pruning metadata are allowed after row-count and length validation. Direct-view metadata is declared for future use, but #1849 owns zero-copy typed views. |
| `string` | `low_cardinality_code` + `low_cardinality_uint32` | Dictionary-code equality/group support. Lexical range/pruning is unsupported unless dictionary order and collation proof are present. |
| `bool` | `bool` + `bool_bitpack_rle` | Bool-specific counts/equality; scalar range remains unsupported by semantics. |
| `float32`/`double` | default compatibility `int64` + `raw_int64` bit-pattern carrier | Does not advertise int64 direct-view, numeric aggregate/range, stats, or pruning capabilities. |
| `float32` | `float32` + `raw_float32` + `compression=none` | Explicit `fixed_width_encoding: "little_endian"` native scalar payload. It is a fixed-width direct-view candidate for downstream certification/readers, preserves raw IEEE-754 bits, and does not yet enable float numeric aggregate/range/stats/pruning fast paths. |
| `double` | `float64` + `raw_float64` + `compression=none` | Explicit `fixed_width_encoding: "little_endian"` native scalar payload. It is a fixed-width direct-view candidate for downstream certification/readers, preserves raw IEEE-754 bits, and does not yet enable float numeric aggregate/range/stats/pruning fast paths. |
| `int8`/`uint8`/`int16`/`uint16`/`int32`/`uint32` | matching primitive scalar type + matching `raw_*` encoding + `compression=none` | Non-null fixed-width little-endian primitive integer payloads. Direct-view certification is allowed after normal writer/read validation. Int64-compatible stats, sum stats, and value-row pruning are advertised for these widths because every value preserves logical ordering when widened to `int64`. |
| `uint64` | `uint64` + `raw_uint64` + `compression=none` | Non-null fixed-width little-endian 64-bit unsigned payload. Direct-view certification is allowed, but int64-compatible stats/pruning/reducer certification is not advertised because values above `MaxInt64` need a native uint64 stats/pruning payload. |
| `float16`/`bfloat16` | `float16`/`bfloat16` + `raw_float16`/`raw_bfloat16` + `compression=none` | Non-null storage-only raw 16-bit bit payloads. Direct-view certification preserves bits exactly; numeric float aggregate/range/stats/pruning fast paths are not advertised. |
| `float32_vector` | `float32_vector` + `raw_float32_vector` | Fixed-width little-endian dense rows with explicit vector direct-payload, similarity, dot-product, and vector-metric capabilities. Scalar aggregate/range shortcuts are rejected. |
| `adjacency_list` | `adjacency_list` + `raw_uint32_dense` | Legacy fixed-width little-endian dense fallback/compatibility payload bytes. Direct-view certification remains deferred; this is not the generic `uint32_list` target. Graph traversal/metrics may use decoded payloads; scalar aggregate/range shortcuts are rejected. |
| `adjacency_list` | `adjacency_list` + `raw_uint32_offsets_list` | Legacy #1915/#1916 variable-list compatibility path selected by `adjacency_layout: "uint32_offsets_list"`: `uint64` offsets plus flattened `uint32` values. Safe writer/fallback-reader publication and certified direct-view readers are enabled through the adapter. #1989 quarantines the graph-specific `column_graph` source integration; primary list consumers should use generic `uint32_list`. |

## Target `uint32_list` layout contract (#1984)

The first-class `uint32_list` logical layout is specified in
`typed-column-uint32-list-semantics.md`. Its v1 physical layout is
`raw_uint32_offsets_list` with separate declared-column offsets and values
sections.

The offsets section is little-endian `uint64`, has exact byte length
`(rows+1)*8`, starts with sentinel `offsets[0] == 0`, is monotonic
non-decreasing, and all offsets must fit host `int`. The values section is
little-endian flattened `uint32`, has byte length divisible by 4, and the final
offset must equal `values_section_bytes/4` before values are exposed. Row `i` is
`values[offsets[i]:offsets[i+1]]`; equal adjacent offsets represent an empty
list.

Length-only APIs may certify the offsets substream independently from values
bytes. That certification is limited to row count, row lengths, monotonicity,
host-int bounds, and the required flattened value count; full value reads and
direct views still require values-section identity, checksum/read-integrity,
endian, length, alignment, and lifetime validation. Graph traversal capability is
not a layout capability of `uint32_list`; it belongs to vector-index/HNSW
consumer state.

Nullable/default wrappers expose null and default-mask dependencies separately
from carrier-value capabilities. Value predicates and aggregates over nullable
carriers require an explicit kernel that composes #1844 row selections with
null/default masks; otherwise they fallback/fail closed instead of treating
carrier zeros as values.

## Raw fixed-width scalar policy

The default `int64` `typed_column_part` publication remains `delta_varint` for
legacy/pre-alpha compatibility. Raw fixed-width int64 is selected only by an
explicit schema/config policy: `fixed_width_encoding: "little_endian"` on a
non-null `int64` column owned by `typed_column_part`. The schema hash includes
this field, so a raw-int64 schema and a legacy delta-varint schema are distinct.
If a catalog/schema is inconsistent with on-disk parts, readers reject the asset
with an explicit schema/layout mismatch rather than silently mixing layouts.

Raw fixed-width scalar validation requires:

- logical type, physical type, and encoding match exactly (`int64`/`raw_int64`,
  `int8`/`raw_int8`, `uint8`/`raw_uint8`, `int16`/`raw_int16`,
  `uint16`/`raw_uint16`, `int32`/`raw_int32`, `uint32`/`raw_uint32`,
  `uint64`/`raw_uint64`, `float16`/`raw_float16`, or
  `bfloat16`/`raw_bfloat16`);
- `compression=none`;
- non-null/non-default wrapper state;
- little-endian fixed-width elements (`1`, `2`, `4`, or `8` bytes depending on
  the primitive);
- raw bytes and stored bytes exactly `rows * element_width` for every block;
- payload bytes read equal the descriptor's stored length;
- normal typed-column asset checksum/read-integrity policy.

The prepared aggregate path consults semantic capabilities first and then layout
capabilities. For raw int64 and #1929 integer primitives up to `uint32`, current
int64-compatible physical stats/pruning certification uses safe little-endian
byte loops and widening; it does not use unsafe pointer casts. Primitive integer
sum/avg/min/max aggregate reducers remain semantically deferred until typedkernel
widening kernels are registered. Delta-varint continues to use the streaming
decode path. `uint64` is a direct-view payload candidate only; it does not
advertise stats/pruning/reducer certification until a native uint64 stats/pruning
payload exists. Raw int64 carriers for scalar floats
fail closed at both layers: they preserve bits for
reconstruction, but they are not direct int64 views and cannot certify int64
numeric reducers, stats, or pruning metadata. Native scalar float payloads are
selected only by explicit little-endian fixed-width metadata and use
`raw_float32`/`raw_float64`; #1929 `float16`/`bfloat16` use raw 16-bit bit
payloads. These layouts are payload/direct-view candidates, but float numeric
equality/range/aggregate/stats/pruning semantics remain deferred.

## Writer-certified layout contracts (#1895)

Newly written `typed_column_part` images include one `layout_contract` section in
addition to the descriptor and column-data sections. The contract is versioned
and writer-built before publication. It records the image/descriptor identity,
descriptor checksum, per-column logical type, physical typedcolumn type,
encoding, compression, section offsets and lengths, section checksums, block row
spans, payload offsets/lengths for contiguous fixed-width/scalar sections,
fixed-width element counts, element size/alignment/endian, null/default mask
presence and counts, dictionary section identity/order/collation fields, and
capability flags for direct-view, streaming reducer, stats, and pruning
shortcuts. `raw_uint32_offsets_list` uses separate global offsets/values section
identity instead of per-block contiguous payload offsets; its v1 block
payload-offset fields are empty and block descriptors retain row spans plus raw
byte counts for fallback reconstruction.

The writer validates the contract before returning the image. Fixed-width direct
view candidates must be uncompressed, little-endian, aligned by absolute storage
offset (`asset_ref.offset + section/block payload offset`), exact-length
(`rows * fixed_width_elements * element_size`) for every block, and tied to the
expected collection logical type. For the #1895 typed-column-part writer, the
segment/appender layer adds deterministic zero prefix padding before active
candidate assets so newly written refs satisfy the absolute-offset rule; image
padding remains deterministic and included in serialized-image accounting.
Missing or mismatched logical type metadata disables direct/streaming
certification instead of being treated as compatible. Variable-width or wrapper
layouts may certify streaming metadata, but nullable/default carrier values do
not become value-fast-path eligible just because mask metadata exists. String
dictionary contracts record dictionary identity; lexical ordering remains
unsupported unless an explicit dictionary order plus collation proof is present.

Prepared readers validate the contract once per immutable asset ref/session and
bind that validation to the normal asset checksum/read-integrity policy. Old or
manually constructed images without a `layout_contract` section fail closed in
prepared certification with a pre-alpha rebuild error. Generic image parsing is
kept tolerant enough for corruption tests and low-level tooling, but optimized
prepared paths require certification.

## Validation boundary and diagnostics

Current validation is at prepare plus payload-read boundaries:

1. manifest, descriptor, schema, section identity, row count, block lengths, and
   the writer-certified layout contract are validated while preparing
   caller-owned/session-owned state;
2. checksum/read-integrity policy is preserved for full asset, cached-verify, or
   benchmark-only skip-checksum modes;
3. certified raw fixed-width hot scans reuse the prepared plan and avoid repeated
   generic payload row-count/length validation; uncached or uncertified paths
   still validate before consuming bytes;
4. hot row loops consume concrete prepared plans and must not call generic
   capability interfaces per row.

Prepared diagnostics must distinguish `mmap_direct_view`,
`heap_copy_typed_view`, `scratch_decode`, `streaming_fallback`,
`source_unsupported`, `certification_failure`, `absolute_offset_unaligned`, `actual_pointer_unaligned`,
`stale_handle`, and per-reason fallback counts. Prepared int64 diagnostics also
include `DirectViewCertified`, `StreamingCertified`, `StatsCertified`,
`PruningCertified`, `CertificationFailures`, and `CertificationFailureReason`,
alongside mapped/heap/decoded/materialized byte counters. When durable stats are
consumed, block diagnostics distinguish
`StatsBlocks`/`StatsFullCoveredBlocks`/`StatsRows` from `BlocksDecoded` and
`Kernel*` counters; stats misses and unsupported selection shapes increment
`StatsFallbackBlocks`. Pruning metadata validation is prepare-time and emits
explicit fallback/validation diagnostics in the int64 prepared state. Benchmarks
report these as `direct_view_certified/op`, `streaming_certified/op`,
`stats_blocks/op`, `stats_fallback_blocks/op`, and `certification_failures/op`.

Stable layout fallback reason codes live in `TreeDB/internal/columnlayout`, for
example `layout_variable_width_no_direct_view`,
`layout_unsupported_compression`, `layout_raw_length_row_count_mismatch`,
`layout_dictionary_order_unproven`, `layout_float_bit_pattern_not_numeric`,
`layout_vector_scalar_unsupported`, `layout_adjacency_scalar_unsupported`,
`layout_adjacency_direct_view_deferred` for legacy dense adjacency,
`layout_adjacency_offsets_list_direct_view_deferred` for old/fallback diagnostics,
and `layout_adjacency_offsets_list_runtime_deferred` for graph/search runtime
consumption before a dedicated graph/search consumer wires the adapter primitive.

## Future extension rules

Writer-certified contracts move repeated validation out of prepared hot loops,
but readers must still fail closed on stale/corrupt/unsupported assets and must
honor checksum policy. Future fast decode/direct-view work (#1849) must use the
validated layout metadata and resource lifetimes rather than ad-hoc unsafe casts.

New layouts should declare unsupported operations explicitly. Examples:

- string/dictionary layouts must distinguish dictionary code order from lexical
  value order and require dictionary-order/collation proof before lexical range
  or pruning;
- float layouts must not inherit int64 bit-pattern ordering or sum semantics;
- primitive integer layouts must prove logical signedness/width before widening
  into int64-compatible reducers or pruning metadata, and `uint64` requires a
  native payload before stats/pruning can be enabled;
- bool layouts should expose bool-specific counts/equality rather than broad
  scalar range;
- vector layouts expose vector-specific direct payload, similarity/dot-product,
  and vector metrics; legacy dense adjacency direct payloads are deferred while
  adjacency traversal/metrics continue through decoded/fallback dense payloads;
  the legacy `raw_uint32_offsets_list` variable-list compatibility layout has
  `uint64` offsets and `uint32` values, with safe writer/fallback reader and
  certified direct-view mechanics available through the adapter. #1989
  quarantines column_graph graph-source consumption; primary runtime
  consumption uses generic `uint32_list` vector-index state. Vector/adjacency
  layouts reject scalar aggregate/range shortcuts unless a future issue
  implements and tests them through the shared substrate.
