# Typed-Column Layout/Codec Capability Contracts (#1838)

Status: pre-alpha internal contract for typed-column prepare/read planning. Semantic
operation support remains defined in `typed-column-semantics.md`; this document
covers the physical layout/codec contract that decides whether a semantic
operation may use a direct view, safe fixed-width reducer, streaming decoder,
stats payload, or pruning metadata.

The implementation lives in `TreeDB/internal/columnlayout`. Capability keys are
not just encodings. A key includes:

- collection logical value type;
- `typedcolumn.ColumnType` physical carrier;
- `typedcolumn.Encoding`;
- `typedcolumn.Compression`;
- nullable/default/dictionary wrappers;
- fixed-width element count for dense vector/adjacency layouts.

## Current reference layouts

| Logical type | Physical + encoding | Layout capability |
| --- | --- | --- |
| `int64` | `int64` + `delta_varint` | Variable-width streaming int64 reducer/range predicate. No direct view. |
| `int64` | `int64` + `raw_int64` + `compression=none` | Optional explicit fixed-width little-endian layout. Safe byte-loop reducer/range predicate is allowed after row-count and length validation. Direct-view metadata is declared for future use, but #1849 owns zero-copy typed views. |
| `string` | `low_cardinality_code` + `low_cardinality_uint32` | Dictionary-code equality/group support. Lexical range/pruning is unsupported unless dictionary order and collation proof are present. |
| `bool` | `bool` + `bool_bitpack_rle` | Bool-specific counts/equality; scalar range remains unsupported by semantics. |
| `float32`/`double` | current `int64` + `raw_int64` bit-pattern carrier | Does not advertise int64 numeric aggregate/range/pruning. Native float layouts must define NaN, signed-zero, infinity, endian, width, and stats rules before enabling numeric fast paths. |
| `float32_vector` | `float32_vector` + `raw_float32_vector` | Fixed-width little-endian dense rows with vector-specific capabilities. Scalar aggregate/range shortcuts are rejected. |
| `adjacency_list` | `adjacency_list` + `raw_uint32_dense` | Fixed-width little-endian dense rows with graph/adjacency-specific capabilities. Scalar aggregate/range shortcuts are rejected. |

Nullable/default wrappers expose null and default-mask dependencies separately
from carrier-value capabilities. Value predicates and aggregates over nullable
carriers require an explicit kernel that composes #1844 row selections with
null/default masks; otherwise they fallback/fail closed instead of treating
carrier zeros as values.

## Raw int64 policy

The default `int64` `typed_column_part` publication remains `delta_varint` for
legacy/pre-alpha compatibility. Raw fixed-width int64 is selected only by an
explicit schema/config policy: `fixed_width_encoding: "little_endian"` on a
non-null `int64` column owned by `typed_column_part`. The schema hash includes
this field, so a raw-int64 schema and a legacy delta-varint schema are distinct.
If a catalog/schema is inconsistent with on-disk parts, readers reject the asset
with an explicit schema/layout mismatch rather than silently mixing layouts.

Raw int64 validation requires:

- logical type `int64`, physical type `int64`, encoding `raw_int64`;
- `compression=none`;
- non-null/non-default wrapper state;
- little-endian 8-byte elements;
- raw bytes and stored bytes exactly `rows * 8` for every block;
- payload bytes read equal the descriptor's stored length;
- normal typed-column asset checksum/read-integrity policy.

The prepared int64 aggregate path consults semantic capabilities first and then
layout capabilities. For raw int64 it uses a safe little-endian byte-loop reducer
and predicate cursor; it does not use unsafe pointer casts or expose zero-copy
views. Delta-varint continues to use the streaming decode path.

## Writer-certified layout contracts (#1850)

Newly written `typed_column_part` images include one `layout_contract` section in
addition to the descriptor and column-data sections. The contract is versioned
and writer-built before publication. It records the image/descriptor identity,
descriptor checksum, per-column logical type (when supplied by the collection
adapter), physical typedcolumn type, encoding, compression, section offsets and
lengths, section checksums, block row spans, payload offsets/lengths, fixed-width
element counts, element size/alignment/endian, null/default mask presence and
counts, dictionary section identity/order/collation fields, and capability flags
for direct-view, streaming reducer, stats, and pruning shortcuts.

The writer validates the contract before returning the image. Fixed-width direct
view candidates must be uncompressed, little-endian, aligned, and exact-length
(`rows * fixed_width_elements * element_size`) for every block. Variable-width
or wrapper layouts may certify streaming metadata, but nullable/default carrier
values do not become value-fast-path eligible just because mask metadata exists.
String dictionary contracts record dictionary identity; lexical ordering remains
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

Prepared int64 diagnostics include `DirectViewCertified`, `StreamingCertified`,
`StatsCertified`, `PruningCertified`, `CertificationFailures`, and
`CertificationFailureReason`, alongside mapped/heap/decoded/materialized byte
counters. Benchmarks report these as `direct_view_certified/op`,
`streaming_certified/op`, and `certification_failures/op`.

Stable layout fallback reason codes live in `TreeDB/internal/columnlayout`, for
example `layout_variable_width_no_direct_view`,
`layout_unsupported_compression`, `layout_raw_length_row_count_mismatch`,
`layout_dictionary_order_unproven`, `layout_float_bit_pattern_not_numeric`,
`layout_vector_scalar_unsupported`, and
`layout_adjacency_scalar_unsupported`.

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
- bool layouts should expose bool-specific counts/equality rather than broad
  scalar range;
- vector and adjacency layouts should expose vector/graph-specific capabilities
  and reject scalar aggregate/range shortcuts unless a future issue implements
  and tests them through the shared substrate.
