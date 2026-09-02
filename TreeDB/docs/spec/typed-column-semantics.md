# Typed-Column Semantic Capability Matrix (#1843)

Status: internal contract for current TreeDB typed-column planning. Capability
resolution is a prepare-time decision; row/block hot loops dispatch to concrete
scan/reducer code and must not call generic semantic interfaces per row.

Optimized-consumer tier classification for the same logical/physical pairs lives
in `typed-column-optimized-consumer-capabilities.md`; semantic support here does
not by itself admit a type to graph-search healthy paths.

The shared model lives in `TreeDB/internal/columnsemantics` and separates:

- collection logical value type (`bool`, `int64`, `float32`, `double`, `string`,
  primitive scalar `int8`/`uint8`/`int16`/`uint16`/`int32`/`uint32`/`uint64`/
  `float16`/`bfloat16`, `float32_vector`, `uint32_list`, `bytes`, and
  `adjacency_list`);
- `typedcolumn.ColumnType` physical carrier;
- `typedcolumn.Encoding` layout/codec;
- operation capability status: `supported`, `unsupported`, or `fallback` with a
  stable reason code.

## Current logical-to-physical publication matrix

| Logical type | Current typedcolumn type | Encoding/layout | Scalar range/aggregate stance |
| --- | --- | --- | --- |
| `bool` | `bool` | `bool_bitpack_rle` | equality/counts supported; broad scalar range is unsupported (`bool_range_unsupported`). |
| `int64` | `int64` | `delta_varint` by default; explicit `fixed_width_encoding: "little_endian"` selects uncompressed `raw_int64`; double-delta remains a valid typedcolumn encoding but is not the adapter default | equality, ordered range, count rows/non-null, sum/avg/min/max, min/max stats, sum stats, equality pruning, and ordered-range pruning are supported for non-null int64 semantics. Durable sum/count stats are gated by the stats envelope in `typed-column-stats.md`; durable value-row pruning metadata is gated by `typed-column-pruning.md`. Physical reducer/direct-view/pruning eligibility is additionally gated by `typed-column-layout-capabilities.md` and `typed-column-direct-view-alignment.md`. |
| `float32` | default `int64` carrier; native `float32` when `fixed_width_encoding: "little_endian"` is selected | compatibility `raw_int64` carrying `math.Float32bits`; native `raw_float32` little-endian IEEE-754 bits | raw int64 bit patterns do **not** provide int64 ordered range, sum, avg, min/max, stats, pruning, or direct scalar value semantics (`float_raw_int64_bit_pattern`). Native `raw_float32` is a bit-preserving direct scalar payload candidate; equality/range/numeric aggregate/stats/pruning semantics remain explicit fallback/unsupported until the scalar float type-family work defines NaN, signed-zero, infinity, and accumulation policy. |
| `double` | default `int64` carrier; native `float64` when `fixed_width_encoding: "little_endian"` is selected | compatibility `raw_int64` carrying `math.Float64bits`; native `raw_float64` little-endian IEEE-754 bits | same raw-bit restriction as `float32`; native `raw_float64` is a bit-preserving direct scalar payload candidate without enabling numeric float fast paths yet. |
| `string` | `low_cardinality_code` | `low_cardinality_uint32` plus dictionary metadata | dictionary equality/in-list/group-by are supported. Lexical range/prefix/pruning are unsupported unless dictionary order and collation identity are explicitly proven (`dictionary_order_unproven`, `dictionary_collation_unproven`). |
| `int8`/`uint8`/`int16`/`uint16`/`int32`/`uint32` | matching fixed-width scalar type | `raw_int8`/`raw_uint8`/`raw_int16`/`raw_uint16`/`raw_int32`/`raw_uint32` | non-null primitive integer scalars with logical signedness/width preserved. Equality, ordered range, count, direct scalar value carrier, int64-compatible stats, and value-row pruning are supported only when the physical type and raw little-endian encoding match the logical type. Sum/avg/min/max aggregate kernels are deferred until primitive typedkernel widening reducers are registered. |
| `uint64` | `uint64` | `raw_uint64` | non-null raw little-endian 64-bit unsigned payload. Direct scalar value carrier and logical count/value semantics are admitted, but int64-compatible stats/pruning payloads are not advertised because values above `MaxInt64` cannot be represented without changing payload format. |
| `float16`/`bfloat16` | `float16`/`bfloat16` | `raw_float16`/`raw_bfloat16` | storage-only raw 16-bit bit payloads. Bits are preserved exactly, including NaN payloads, infinities, and signed zero; numeric equality/range/aggregate/stats/pruning semantics are fallback/unsupported until an explicit 16-bit float policy lands. |
| `float32_vector` | `float32_vector` | `raw_float32_vector` | count rows/non-null plus vector direct-payload, similarity, dot-product, and vector-metric capabilities are explicit prepare-time entries for specialized vector kernels. Scalar equality/range/sum/min/max/stats/pruning shortcuts are rejected (`vector_scalar_operation_unsupported`). |
| `uint32_list` | `uint32_list` | `raw_uint32_offsets_list` | count rows/non-null plus generic list length/payload direct-view capabilities are primitive shape semantics only. The primitive does not imply HNSW adjacency, graph ordinals, or scalar numeric range/aggregate semantics; scalar range/sum/min/max/stats/pruning shortcuts are rejected (`uint32_list_scalar_operation_unsupported`). |
| `bytes` | `bytes` | `raw_bytes_offsets` | count rows/non-null plus opaque byte-payload direct-view capabilities are primitive shape semantics only. Bytes are not strings: no UTF-8, collation, dictionary, lexical range, scalar compare, or text predicate semantics are implied; scalar operations are rejected (`bytes_scalar_operation_unsupported`). |
| `adjacency_list` | `adjacency_list` | legacy `raw_uint32_dense`; legacy `raw_uint32_offsets_list` compatibility path | count rows/non-null plus adjacency graph traversal and adjacency-metric semantics are graph-specific entries. Direct payload views are compatibility/fallback-only on this logical type. The offsets-list path is an explicit `ColumnStoreValueAdjacencyList` variable-list selector (`adjacency_layout: "uint32_offsets_list"`) plus internal encoding with `uint64` offsets and `uint32` values, not fixed padded rows. #1989 quarantines that graph-specific logical integration; first-class `uint32_list` semantics keep primitive shape validation separate from graph ordinal/layer/search validation. Scalar range/sum/min/max/stats/pruning shortcuts are rejected (`adjacency_scalar_operation_unsupported`). |

## First-class `uint32_list` semantic contract (#1984)

`TreeDB/docs/spec/typed-column-uint32-list-semantics.md` is the canonical
semantic contract for the generic integer-list primitive. `uint32_list` is the
canonical logical type name; `uint32[]` and conceptual `Array(UInt32)` are aliases
for reasoning and documentation only. The v1 primitive is non-null and
uint32-focused: every row has one list value, empty rows are valid, null/missing
lists are out of scope, and compressed, nullable, nested, shared-offset,
`int32_list`, and `uint64_list` variants are deferred.

The physical encoding for v1 is `raw_uint32_offsets_list`, not a graph type. It
stores a first-class offsets/size substream and a flattened values substream:
`offsets []uint64` little-endian with length `rows+1`, `offsets[0] == 0`, and
`values []uint32` little-endian, where row `i` is
`values[offsets[i]:offsets[i+1]]`. Validation requires exact offsets byte length
`(rows+1)*8`, monotonic host-int-bounded offsets, values bytes divisible by 4,
and final offset equal to the flattened value count.

Offset/length metadata may be validated independently for length-only APIs. Such
paths can prove row lengths and the required value count from offsets, and can
compare the final offset to values-section length metadata when available, but
they must not claim value checksum/integrity or direct-view eligibility until the
values substream is validated. HNSW adjacency is a consumer above this primitive;
neighbor ordinal bounds, graph layers, deleted rows, and traversal semantics are
not primitive `uint32_list` checks. `adjacency_list` remains legacy and
consumer-specific compatibility until downstream issues replace or isolate it.

## First-class `bytes` semantic contract (#2010)

`bytes` is the generic opaque binary primitive. It exists for exact byte payloads
and deliberately does not reuse `string`/dictionary/text semantics. The v1
primitive is non-null and one-dimensional: every row has exactly one byte slice,
empty byte slices are valid, and null/default/nested byte-list variants are
unsupported until explicitly designed.

The physical encoding is `raw_bytes_offsets`: `offsets []uint64` little-endian
with length `rows+1`, `offsets[0] == 0`, and a `values []byte` substream holding
the exact concatenation of row payloads. Row `i` spans
`values[offsets[i]:offsets[i+1]]`. Validation requires exact offsets byte
length, monotonic host-int-bounded offsets, and final offset equal to the values
byte length. The values substream is opaque and may contain NUL bytes,
non-UTF-8 sequences, and any other byte values without normalization.
Vector-index returned document IDs use this primitive as `document_ids` state;
legacy graph row ID bytes are compatibility fallback records, not the healthy
returned-ID source.

## Scalar float fail-closed policy

Default scalar `float32` and `double` typed-column storage remains a
preservation carrier, not a numeric float layout. The adapter stores raw
IEEE-754 bit patterns inside an `int64` physical column so reconstruction can
round-trip values, but those carrier integers are never a proof of float-domain
ordering, equality, direct-view eligibility, or arithmetic semantics. When a
non-null `typed_column_part` scalar float explicitly selects
`fixed_width_encoding: "little_endian"`, the payload uses native raw
`raw_float32`/`raw_float64` bytes and preserves IEEE-754 bits exactly, including
NaN payloads and signed zero. That native layout is a payload/direct-view
candidate for downstream certification/readers, not a float-domain numeric
semantics implementation.

Semantic admission is:

- `count_rows` and `count_non_null`: supported as checked `int64` counts because
  they only count rows and do not inspect float bit ordering or arithmetic;
- equality, inequality, and in-list: fallback only. A future implementation must
  define whether equality is bit-exact or IEEE numeric equality, how NaN payloads
  and quiet/signaling NaNs compare, and whether `+0` and `-0` are equal;
- ordered range, min, max, sum, avg, stats min/max/sum, equality/range pruning:
  unsupported for raw bit carriers with `float_raw_int64_bit_pattern` and still
  deferred for native raw float layouts until scalar float numeric policy lands;
- direct scalar value carriers: unsupported for raw int64 float carriers; native
  raw float layouts may expose bit-preserving scalar payload candidates after
  the downstream direct-view safety checks in `typed-column-direct-view-alignment.md`;
- nullable/default float carriers use the same nullable/default wrapper contract
  as other `nullable_int64` carriers: count/null operations may be supported, but
  value predicates and value aggregates fallback until a kernel explicitly
  composes value semantics with null/default masks. Native nullable scalar float
  encodings are not part of the #1737 payload phase.

Required future float policy before enabling native float-domain stats, pruning,
or reducers:

- NaN: define accepted encodings, payload preservation, equality behavior,
  ordering bucket, and aggregate propagation/ignore rules;
- signed zero: define equality and min/max/range placement for `+0` and `-0`;
- infinities: define ordering, range inclusion, min/max behavior, and sum/avg
  overflow/invalid behavior;
- precision/accumulation: define result type (`float32`, `double`, widened, or
  compensated), rounding mode, overflow/underflow behavior, and deterministic
  accumulation order.

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
aggregate shortcuts. Current vector entries cover direct payload access,
similarity scoring, dot-product kernels, and vector metrics; current adjacency
entries cover direct adjacency payload access, graph traversal/neighborhood
planning, and adjacency metrics. These entries do not admit scalar range,
sum/avg, min, max, int64 stats, or int64 pruning semantics.

## typedcolumn coverage

The matrix covers every current `typedcolumn.ColumnType`:

- `int64`
- `low_cardinality_code`
- `bool`
- `float32`
- `float64`
- `int8`
- `uint8`
- `int16`
- `uint16`
- `int32`
- `uint32`
- `uint64`
- `float16`
- `bfloat16`
- `float32_vector`
- `uint32_list`
- `bytes`
- `adjacency_list`

and every current `typedcolumn.Encoding`:

- `raw_int64`
- `delta_varint`
- `double_delta_varint`
- `nullable_int64`
- `bool_bitpack_rle`
- `low_cardinality_uint32`
- `raw_float32_vector`
- `raw_float32`
- `raw_float64`
- `raw_uint32_dense`
- `raw_uint32_offsets_list`
- `raw_bytes_offsets`
- `raw_int8`
- `raw_uint8`
- `raw_int16`
- `raw_uint16`
- `raw_int32`
- `raw_uint32`
- `raw_uint64`
- `raw_float16`
- `raw_bfloat16`

## Future scalar numeric-width admission rules

The #1929 primitive scalar set covers only non-null fixed-width integer widths
needed by quantized side arrays plus storage-only raw 16-bit float-bit payloads.
Future scalar additions (for example signed `int64`-incompatible widths,
`int64`-sized unsigned stats, decimals, nullable primitive scalars, compressed
primitive scalars, or arithmetic float16/bfloat16 semantics) must be admitted
through this matrix with conformance tests that define:

- logical signedness and width independently of physical carrier bytes;
- comparison/range semantics over logical values, not reused carrier ordering;
- aggregate result type and overflow/widening policy;
- stats payload semantics (native-width, widened, saturating, checked, or
  unsupported);
- layout/codec compatibility without losing original logical width/signedness;
- explicit proof before any int64 kernel or pruning metadata is reused.

Benchmarks are required only if capability checks move into block/run hot paths;
current adapter consumption resolves semantic and layout capabilities during
prepare and records the matrix resolution phase as `prepare`.
