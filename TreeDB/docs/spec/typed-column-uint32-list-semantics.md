# Typed-Column `uint32_list` Semantics (#1984)

Status: semantic contract for the #1982 typed-column integer-list stack. This
spec defines the first-class logical list type and the validation vocabulary that
Issue #1985 must implement. It does **not** add a new runtime value type, writer,
reader, vector-index manifest, or HNSW search integration.

## Vocabulary

| Term | Meaning |
| --- | --- |
| `uint32_list` | Canonical TreeDB logical typed-column value type for a non-null variable-width list of `uint32` values. |
| `uint32[]` | Conceptual user-facing alias for `uint32_list`; it is spelling guidance, not a separate v1 type. |
| `Array(UInt32)` | ClickHouse-style reference model: a logical generic array/list type separated from offsets/sizes and element storage. |
| `raw_uint32_offsets_list` | TreeDB v1 physical encoding for `uint32_list`; it is not a logical graph or adjacency type. |
| offsets substream | First-class declared column-offset/size substream containing little-endian `uint64` offsets. |
| values substream | First-class declared column-values substream containing flattened little-endian `uint32` values. |
| `adjacency_list` | Legacy/consumer-specific compatibility type for current graph adjacency data; not the generic primitive. |

HNSW adjacency may consume `uint32_list` assets, but neighbor ordinal bounds,
layer ownership, graph traversal, deleted-row handling, and vector-index state
identity are consumer semantics above the typed-column list primitive.

## Logical v1 semantics

- `uint32_list` is generic datastore typed-column storage, independent of HNSW,
  `column_graph`, or graph-adjacency terminology.
- Every row has exactly one list value. The v1 contract is non-null: null lists,
  missing lists, default lists, and nullable wrappers are out of scope.
- Empty lists are valid. They are represented by equal adjacent offsets
  (`offsets[i] == offsets[i+1]`).
- Element values are unsigned 32-bit integers. The primitive does not assign
  graph meaning, sortedness, uniqueness, or row/ordinal validity to elements.
- v1 does not define `int32_list`, `uint64_list`, nested lists, shared-offset
  parallel arrays, compressed list sections, or nullable list sections. Those are
  explicit follow-ups.

## Physical v1 encoding: `raw_uint32_offsets_list`

TreeDB follows the ClickHouse `Array(T)` lesson by keeping a generic logical
array/list type separate from offsets/sizes and flattened element storage. The
TreeDB v1 physical convention is explicit sentinel offsets instead of
ClickHouse's one cumulative end offset per row:

```text
rows        = number of typed-column rows
offsets     = []uint64, little-endian, length rows+1
values      = []uint32, little-endian, flattened row values
offsets[0]  = 0
row i       = values[offsets[i]:offsets[i+1]]
```

The offsets section and values section are separate typed-column image sections
with independent section identity and checksums. Their on-disk scalar encodings
are little-endian: `uint64` for offsets and `uint32` for values.

## Validation invariants

A conforming v1 `raw_uint32_offsets_list` asset must satisfy all of the
following before row slicing or direct-view exposure:

1. `rows >= 0` and `rows + 1` fits the host Go `int`.
2. The offsets section byte length is exactly `(rows+1)*8`.
3. The values section byte length is a multiple of 4.
4. Decoded offsets length is exactly `rows+1`.
5. `offsets[0] == 0`.
6. Offsets are monotonic non-decreasing.
7. Every offset, including the final offset, fits the host Go `int`.
8. The final offset equals the flattened value count (`len(values)` or
   `values_section_bytes/4`).
9. Row `i` is valid only when `0 <= i < rows`; its length is
   `offsets[i+1] - offsets[i]` and may be zero.
10. Full values reads must validate the values section identity, byte length,
    checksum/read-integrity policy, and little-endian `uint32` element decoding
    before exposing elements.

Primitive validation stops at shape and element-byte validity. It must not check
HNSW neighbor ordinal ranges, graph layers, symmetry, tombstones, or traversal
reachability; those checks belong to vector-index/HNSW consumers.

## Length-only and offset-only behavior

The offsets substream is first-class metadata and can be validated/read without
opening or decoding the flattened values bytes when an API needs length-only
behavior. Offset-only validation can prove the row count, offset byte shape,
`offsets[0]`, monotonicity, host-int bounds, row lengths, and the required final
flattened value count.

If trusted values-section metadata is available, an offset-only path may also
check `offsets[rows] == values_section_bytes/4` without decoding values. It must
not claim value element integrity, value checksum verification, or value direct
view eligibility until the values substream is validated under the normal read
integrity policy. A missing values substream is therefore acceptable only for
APIs that explicitly request lengths/offsets; full row-value APIs fail closed.

## Compatibility naming strategy

The public compatibility name for the generic logical type is
`ColumnStoreValueUint32List` with documented string `uint32_list`. Issue #1985
adds that Go constant, adapter admission, the code vocabulary row, and
round-trip/direct-view/fallback validation without requiring
`ColumnStoreValueAdjacencyList` semantics.

`ColumnStoreValueAdjacencyList`, `adjacency_layout`, and existing
`column_graph` adjacency-source schema strings remain legacy/consumer-specific
compatibility. They must not be extended as the generic list primitive.
