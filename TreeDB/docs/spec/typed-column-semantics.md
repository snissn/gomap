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
| `int64` | `int64` | `delta_varint` by adapter; raw/double-delta are valid typedcolumn encodings | equality, ordered range, count rows/non-null, sum/avg/min/max, min/max stats, and ordered-range pruning are supported for non-null int64 semantics. |
| `float32` | `int64` | `raw_int64` carrying `math.Float32bits` | raw bit patterns do **not** provide int64 ordered range, sum, min/max, stats, pruning, or direct scalar value semantics (`float_raw_int64_bit_pattern`). Native float semantics require a future float layout and NaN/signed-zero/infinity rules. |
| `double` | `int64` | `raw_int64` carrying `math.Float64bits` | same raw-bit restriction as `float32`. |
| `string` | `low_cardinality_code` | `low_cardinality_uint32` plus dictionary metadata | dictionary equality/in-list/group-by are supported. Lexical range/prefix/pruning are unsupported unless dictionary order and collation identity are explicitly proven (`dictionary_order_unproven`, `dictionary_collation_unproven`). |
| `float32_vector` | `float32_vector` | `raw_float32_vector` | count rows supported; vector-specific capabilities are explicit/deferred. Scalar equality/range/sum/min/max/stats/pruning shortcuts are rejected (`vector_scalar_operation_unsupported`). |
| `adjacency_list` | `adjacency_list` | `raw_uint32_dense` | count rows supported; graph/vector-specific capabilities are explicit/deferred. Scalar shortcuts are rejected (`adjacency_scalar_operation_unsupported`). |

Nullable scalar adapter support uses `nullable_int64` as a carrier. The semantic
matrix distinguishes count rows, count non-null/null predicates, and value
aggregate semantics: count/null operations are supported, while value predicates
or aggregates require explicit null/default filtering and are reported as
`fallback` with `nullable_carrier_value_semantics` or
`nullable_carrier_aggregate_semantics` unless a concrete kernel has opted in.

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
similar column-store value types today. Before adding one, the type must be
admitted through this matrix with conformance tests that define:

- logical signedness and width independently of physical carrier bytes;
- comparison/range semantics over logical values, not reused carrier ordering;
- aggregate result type and overflow/widening policy;
- stats payload semantics (native-width, widened, saturating, checked, or
  unsupported);
- layout/codec compatibility without losing original logical width/signedness;
- explicit proof before any int64 kernel or pruning metadata is reused.

Benchmarks are required only if capability checks move into block/run hot paths;
current int64/string adapter consumption resolves capabilities during prepare.
