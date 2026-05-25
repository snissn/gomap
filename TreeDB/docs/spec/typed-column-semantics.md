# Typed-Column Semantic Capability Matrix (#1843)

Status: internal contract for current TreeDB typed-column planning. Capability
resolution is a prepare-time decision; row/block hot loops dispatch to concrete
scan/reducer code and must not call generic semantic interfaces per row.

The shared model lives in `TreeDB/internal/columnsemantics` and separates:

- collection logical value type (`bool`, `int64`, `float32`, `double`, `string`,
  `float32_vector`, `adjacency_list`);
- `typedcolumn.ColumnType` physical carrier;
- `typedcolumn.Encoding` layout/codec;
- operation capability status: `supported`, `unsupported`, or `fallback` with a
  stable reason code.

## Current logical-to-physical publication matrix

| Logical type | Current typedcolumn type | Encoding/layout | Scalar range/aggregate stance |
| --- | --- | --- | --- |
| `bool` | `bool` | `bool_bitpack_rle` | equality/counts supported; broad scalar range is unsupported (`bool_range_unsupported`). |
| `int64` | `int64` | `delta_varint` by default; explicit `fixed_width_encoding: "little_endian"` selects uncompressed `raw_int64`; double-delta remains a valid typedcolumn encoding but is not the adapter default | equality, ordered range, count rows/non-null, sum/avg/min/max, min/max stats, sum stats, equality pruning, and ordered-range pruning are supported for non-null int64 semantics. Durable sum/count stats are gated by the stats envelope in `typed-column-stats.md`; durable value-row pruning metadata is gated by `typed-column-pruning.md`. Physical reducer/direct-view/pruning eligibility is additionally gated by `typed-column-layout-capabilities.md`. |
| `float32` | `int64` | `raw_int64` carrying `math.Float32bits` | raw bit patterns do **not** provide int64 ordered range, sum, min/max, stats, pruning, or direct scalar value semantics (`float_raw_int64_bit_pattern`). Native float semantics require a future float layout and NaN/signed-zero/infinity rules. |
| `double` | `int64` | `raw_int64` carrying `math.Float64bits` | same raw-bit restriction as `float32`. |
| `string` | `low_cardinality_code` | `low_cardinality_uint32` plus dictionary metadata | dictionary equality/in-list/group-by are supported. Lexical range/prefix/pruning are unsupported unless dictionary order and collation identity are explicitly proven (`dictionary_order_unproven`, `dictionary_collation_unproven`). |
| `float32_vector` | `float32_vector` | `raw_float32_vector` | count rows/non-null supported for the non-null carrier; vector-specific capabilities are explicit/deferred. Scalar equality/range/sum/min/max/stats/pruning shortcuts are rejected (`vector_scalar_operation_unsupported`). |
| `adjacency_list` | `adjacency_list` | `raw_uint32_dense` | count rows/non-null supported for the non-null carrier; graph/vector-specific capabilities are explicit/deferred. Scalar shortcuts are rejected (`adjacency_scalar_operation_unsupported`). |

Nullable scalar adapter support uses `nullable_int64` as a carrier. The semantic
matrix treats the `nullable_int64` encoding itself as a nullable/default carrier,
even if a caller forgets to set a nullable flag; a descriptor that marks a field
nullable while advertising another encoding is rejected as a physical/encoding
mismatch. It distinguishes count rows, count non-null/null predicates, and value
aggregate semantics: count/null operations are supported, while value predicates
or aggregates require explicit null/default filtering and are reported as
`fallback` with
`nullable_carrier_value_semantics` or
`nullable_carrier_aggregate_semantics` unless a concrete kernel has opted in.

Current aggregate result semantics are explicit in the matrix result metadata:
row counts and non-null counts return checked `int64` counts; int64 `sum` returns
an `int64` result with checked overflow; int64 `avg` returns a `float64` quotient
after checked int64 sum/count accumulation; int64 `min`/`max` compare and return
signed int64 logical values; bool counts return int64 false/true/null buckets;
and dictionary group-by keys are dictionary string values bound to a stable
dictionary identity.

## Row selection and mask composition (#1844)

Typed-column kernels consume block-local row selections from
`TreeDB/internal/typedcolumn` rather than reimplementing mask semantics per type.
The internal `RowSelection` model represents:

- empty selections;
- all rows;
- one contiguous range;
- multiple sorted ranges;
- bitmap selections;
- sparse row indexes.

Selections expose count, iteration, range extraction, and shape diagnostics
without forcing row-id materialization for all/range forms. Composition is
fail-closed: predicate and visibility masks are intersected, while delete,
null, and default masks exclude rows from value semantics. Mismatched row domains
return an empty selection plus an error. Scratch-backed `Into` helpers make reuse
explicit and avoid hidden global caches.

Multi-column execution contracts are descriptors, not a planner. They bind
predicate/measure/projection/visibility/null/default roles to row spans,
optional immutable asset identity, and section dependency kinds such as values,
dictionaries, offsets, null/default masks, visibility, stats, pruning metadata,
vector payloads, and adjacency payloads. Row-span or asset-generation mismatches
must fail closed before a hot loop consumes multiple columns.

## Durable pruning metadata (#1841)

Typed-column pruning metadata is generic at the envelope layer and type-specific
at the payload layer. The first payload is `int64_value_rows_v1`, a non-null
int64 `(value,row)` index that advertises `pruning.equality` and
`pruning.ordered_range`. Prepared planning validates the envelope and payload,
then produces block-local `RowSelection` candidates that compose with visibility
and null/default masks. Missing metadata is a safe fallback; corrupt or stale
metadata fails closed. See `typed-column-pruning.md` for the payload contract.

## Durable stats envelope (#1840)

Typed-column stats are generic at the envelope layer and type-specific at the
payload layer. The envelope binds part id, column name, physical type, encoding,
row count, block count, null/default/visible/value counts, advertised logical
operations, advertised selection shapes, payload length, and payload checksum.
A payload may be consumed only when both the semantic capability matrix and the
layout contract support the requested operation and the envelope advertises the
requested selection shape. Invalid present stats fail closed; absent stats are a
safe fallback to decoding.

The first payload is non-null `int64` block/part count+sum+min/max. It answers
prepared count/sum/avg aggregate blocks only for all rows or full-covered blocks
(no partial/random/sparse selection and no visibility mutation mask). Sum uses
checked int64 overflow semantics; overflowing block sums are marked not
stats-answerable and fall back to the checked reducer.

Future bool, string/dictionary, float, vector, or adjacency stats must add their
own payload semantics rather than reusing int64 carrier meanings. Dictionary and
string range stats require dictionary-order plus collation proof. Float stats
require native float layout plus NaN/signed-zero/infinity and precision rules.
Vector/adjacency stats must be vector/graph-specific metadata, not scalar
aggregate shortcuts.

## typedcolumn coverage

The matrix covers every current `typedcolumn.ColumnType`:

- `int64`
- `low_cardinality_code`
- `bool`
- `float32_vector`
- `adjacency_list`

and every current `typedcolumn.Encoding`:

- `raw_int64`
- `delta_varint`
- `double_delta_varint`
- `nullable_int64`
- `bool_bitpack_rle`
- `low_cardinality_uint32`
- `raw_float32_vector`
- `raw_uint32_dense`

## Future scalar numeric-width admission rules

There are no public scalar `int32`, `int16`, `uint32`, `uint64`, decimal, or
similar typed-storage logical value types today. Before adding one, the type
must be admitted through this matrix with conformance tests that define:

- logical signedness and width independently of physical carrier bytes;
- comparison/range semantics over logical values, not reused carrier ordering;
- aggregate result type and overflow/widening policy;
- stats payload semantics (native-width, widened, saturating, checked, or
  unsupported);
- layout/codec compatibility without losing original logical width/signedness;
- explicit proof before any int64 kernel or pruning metadata is reused.

Benchmarks are required only if capability checks move into block/run hot paths;
current int64/string adapter consumption resolves semantic and layout
capabilities during prepare and records the matrix resolution phase as
`prepare`.
