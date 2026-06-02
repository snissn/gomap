# Typed-column fixed-byte and packed-code layout contract (#1931)

Status: #1931 durable typed-column integration. The fixed-byte and packed-code
contracts are part of typed-column part images, layout certification, typed
storage schema metadata, and collection `typed_column_part` publication.

## Scope

Issue `#1931` owns fixed-byte and packed-code rows for quantized assets:

- `fixed_bytes` / `byte_vector`: row-major opaque byte payloads with a fixed
  `bytes_per_row`.
- `packed_bit_vector`: one bit per logical element.
- `packed_uint2_vector`: two bits per logical element.
- `packed_uint4_vector`: four bits per logical element.
- `uint64_word_vector` is a compatible access/view mode over row bytes, not a
  separate sidecar format.

Issue `#1931` does not implement scalar/BRQ/PQ scoring and does not replace the dense
numeric vector types owned by `#1930`.

## Durable names and encodings

Typed-column part images use these logical/physical names:

| Public value type | Typed-column column type | Raw encoding |
| --- | --- | --- |
| `byte_vector` | `fixed_bytes` | `raw_fixed_bytes` |
| `packed_bit_vector` | `packed_bit_vector` | `raw_packed_bit_vector` |
| `packed_uint2_vector` | `packed_uint2_vector` | `raw_packed_uint2_vector` |
| `packed_uint4_vector` | `packed_uint4_vector` | `raw_packed_uint4_vector` |

The part descriptor stores `fixed_width_elements` and `bits_per_element`.
`fixed_bytes` requires `bits_per_element=0`; packed types require
`bits_per_element` to match the type (`1`, `2`, or `4`).

## Metadata

Packed rows require metadata for both logical and physical shape:

| Field | Meaning |
| --- | --- |
| `elements_per_row` | Number of values/dimensions in each row. |
| `bits_per_element` | `1`, `2`, or `4`. |
| `logical_bits_per_row` | `elements_per_row * bits_per_element`. |
| `bytes_per_row` | `ceil(logical_bits_per_row / 8)`. |
| `physical_words_per_row` | `ceil(bytes_per_row / 8)` for scratch word access. |

Fixed bytes require `bytes_per_row`; payload length is exactly
`row_count * bytes_per_row`.

## Bit and byte order

Packed code rows use LSB0 order within each byte: the first logical element is
stored in the least significant bit(s) of byte 0. For multi-bit elements, element
order advances through low bits first. For example:

- 1-bit values `[1,0,1,1,0,0,1,0,1,1]` encode as bytes `4d 03`.
- 2-bit values `[0,1,2,3,1]` encode as bytes `e4 01`.
- 4-bit values `[0x0a,0x0b,0x0c]` encode as bytes `ba 0c`.

Word views decode row bytes as little-endian `uint64` words. A byte row
`01 02 03 04 05 06 07 08` therefore views as word
`0x0807060504030201`.

## Padding and integrity

Unused high bits in the final physical byte after `logical_bits_per_row` are
padding and must be zero. Writers zero them. Read/open validation fails closed on
non-zero padding, section-length mismatch, row-width mismatch, unsupported
`bits_per_element`, or inconsistent logical/physical metadata.

When `bytes_per_row` is not a multiple of 8, scratch word access zero-fills bytes
after the physical row in the final partial word. This word padding is an access
rule; it is not additional on-disk payload.

## Access and lifetime

Row byte access returns `[]byte` slices that alias immutable typed-column payload
bytes and must not allocate per row. Returned slices are valid only for the
lifetime of the backing typed-column image/mappedresource handle.

Word access may return direct `[]uint64` only when all of the following hold:

- host is little-endian;
- row length is a multiple of 8 bytes;
- row start pointer is aligned for `uint64`;
- the caller/lifetime contract keeps the backing bytes alive.

Otherwise callers use an explicit scratch `[]uint64`; hot scorer-shaped loops
must preallocate scratch large enough for `physical_words_per_row` to avoid
per-candidate allocation.

## Implementation baseline

`TreeDB/internal/typedcolumn/packed_code.go` provides:

- `FixedBytesRows` with no-allocation `Row` and scratch/direct `RowWords`.
- `PackedUintRows` with no-allocation `RowBytes`, `Element`, padding validation,
  and scratch/direct `RowWords`.
- `EncodePackedUintRows`, `PackUintRow`, `UnpackUintRow`, and shape helpers for
  row bytes/payload bytes/word counts.

Collection `typed_column_part` publication admits `byte_vector`,
`packed_bit_vector`, `packed_uint2_vector`, and `packed_uint4_vector` with
checkpoint/reopen coverage. Scoring/query-mode consumption remains downstream
work for #1932/#1926.
