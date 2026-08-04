# BSON-Ordered Scalar Index Key Codec v2

Status: staged substrate for issue #4061. Collection-index adoption and migration
belong to #4062 and later issues. Existing typed-v1 indexes are unchanged.

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
-Infinity < negative finite < zero < positive finite < +Infinity < NaN
```

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
document-ID suffix after an exact scalar component. The suffix provides stable
per-document uniqueness without becoming part of scalar ordering. Its encoded
form has its own 1 MiB bound, enforced before escape expansion is appended.

## Durability and compatibility boundary

Golden vectors and reopen tests freeze the byte format. The codec has no
implicit fallback and no decoder for another generation. Existing typed-v1
indexes remain readable and writable through their current code. #4062 must
introduce explicit format selection and migration policy before v2 is used by
collection indexes.
