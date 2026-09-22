# BSON-Ordered Scalar Index Key Codec v2

Status: adopted for BSON collection scalar indexes by issues #4062 and #4063.
Existing typed-v1 indexes are unchanged. Metadata version 6 admits one through
four ordered scalar components, each ascending or descending; arrays in a
compound component fail closed. Pre-alpha format evolution rejects older
metadata versions rather than providing migration scaffolding.

## Goals

The v2 scalar component is a versioned, bytewise-sortable encoding for the BSON
scalar types currently selected for the next index generation. Byte comparison
of ascending components matches the targeted MongoDB/BSON scalar ordering,
including numeric equality across Int32, Int64, Double, and Decimal128. Components are
self-delimiting so they can be concatenated for compound keys. The document-ID
suffix is encoded separately and never changes scalar comparison.

## Header and BSON type order

An ascending component begins with marker `0xb2`. A descending component is the
bitwise complement of the complete ascending component and therefore begins
with `0x4d`.

The second byte is the ordered type tag:

| BSON value | Tag |
|---|---:|
| missing field | `0x08` |
| null | `0x10` |
| numeric | `0x20` |
| UTF-8 string | `0x30` |
| ObjectID | `0x70` |
| bool | `0x80` |
| DateTime | `0x90` |
| Timestamp | `0xa0` |

Missing and null are deliberately distinct. Unsupported values, including
embedded documents, arrays, binary, MinKey, and MaxKey, fail closed.

## Numeric normalization

All finite Int32, Int64, Double, and Decimal128 values normalize to an exact
base-10 coefficient and exponent. Trailing coefficient zeroes are removed.
Cross-type equal numbers therefore produce identical bytes.

Numeric classes are ordered as:

```text
NaN < -Infinity < negative finite < zero < positive finite < +Infinity
```

Their class bytes are `0x08`, `0x10`, `0x20`, `0x30`, `0x40`, and `0x50`
respectively.

A finite magnitude stores:

1. a class byte (`0x20` negative or `0x40` positive);
2. the adjusted decimal exponent `exponent + digits - 1` as a signed 16-bit
   big-endian value with the sign bit flipped;
3. decimal digits packed as `(digit + 1)` nibbles followed by a zero terminator
   nibble.

Negative finite exponent and digit bytes are complemented to reverse magnitude
order. Negative zero normalizes to the shared zero encoding. Decimal128 and
Double infinities and NaNs share canonical class encodings. Finite Doubles are
converted from their exact binary value rather than through rounded display
text, so exact Decimal128 `0.1` sorts before binary Double `0.1`.

## Variable and fixed-width payloads

Strings preserve UTF-8 byte order. A zero byte is escaped as `00 ff` and the
string terminator is `00 00`. ObjectID stores 12 raw bytes. Bool stores `00` or
`01`. DateTime stores signed milliseconds with the sign bit flipped. Timestamp
stores seconds then ordinal as two unsigned big-endian 32-bit words.

Malformed raw BSON, invalid UTF-8, non-canonical numeric coefficients,
unterminated components, invalid fixed-width payloads, and components exceeding
1 MiB are rejected. String and document-ID escape growth is counted before
append, so embedded NUL bytes cannot cross that bound through expansion.
Finite numeric coefficients are limited to 2,048 decimal digits and normalized
decimal exponents to the inclusive range `[-10000, 10000]`; decoding applies
the same limits to corrupted or hand-built components.

## Descending and compound keys

Descending encoding complements every byte of one validated ascending
component. Complementing again restores the exact ascending bytes. Because each
component is self-delimiting, ascending or descending components can be
concatenated without field separators.

`bsonIndexEntryKeyV2` appends a separately marked, zero-escaped, terminated
document-ID suffix after one or more exact scalar components. The suffix provides stable
per-document uniqueness without becoming part of scalar ordering. Its encoded
form has its own 1 MiB bound, enforced before escape expansion is appended.
The suffix marker, escaped bytes, and terminator always remain uncomplemented,
including after a descending scalar component. The complement rule ends at the
scalar component boundary, and document-ID decoding is identical in both sort
directions.

The direct collection compound-range primitive requires a positive result
limit. In addition to emitting at most that many document IDs, it examines at
most `limit * 64` physical entries while merging buffered and persisted overlay
runs. Tombstones and shadowed duplicate entries count against this bound; when
either bound is reached the primitive returns `truncated=true`.

The standalone Mongo gateway consumes this primitive only for canonical
top-level conjunctions: an equality (or bounded `$in` fanout) prefix followed
by at most one range component, with remaining predicates rechecked after
materialization. It allocates one global result/work budget across all `$in`
prefixes and rejects on truncation rather than returning a partial result. A
requested sort is direct only when the remaining index components match its
declared directions or their complete reverse; document IDs remain the stable
final tie order. Missing, malformed, incompatible, or unsupported hints reject
from catalog metadata before any index or document read. Routed reads reject
rather than falling back locally.

## Durability and compatibility boundary

Golden vectors and reopen tests freeze the byte format. Collection metadata
explicitly selects v2; decoders and range-bound builders select that metadata,
never an entry-byte heuristic. A v2 decoder has no implicit fallback and no
decoder for another generation. Corrupt/truncated components or ID suffixes,
or metadata that selects v2 for a non-BSON collection, fail closed. Existing
typed-v1 indexes remain readable and writable through their current code.
