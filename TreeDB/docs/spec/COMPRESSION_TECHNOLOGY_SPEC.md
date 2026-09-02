# ClickHouse Compression Technology Specification

Status: reference, non-normative for TreeDB on-disk/API contracts.

Provenance:

- Upstream repository: `https://github.com/ClickHouse/ClickHouse`
- Upstream commit: `ad347dbafb074ccf13790b5045b25708a975fb77`
  (`https://github.com/ClickHouse/ClickHouse/commit/ad347dbafb074ccf13790b5045b25708a975fb77`)
- License file:
  `https://github.com/ClickHouse/ClickHouse/blob/ad347dbafb074ccf13790b5045b25708a975fb77/LICENSE`
- License/attribution: ClickHouse source at that snapshot is copyright
  `2016-2026 ClickHouse, Inc.` and licensed under the Apache License 2.0.

This document is TreeDB-authored reference material derived from reading that
external ClickHouse source snapshot. It is not a vendored copy of ClickHouse
source and is non-normative for ClickHouse itself.

This document specifies the compression technology present in that external
ClickHouse source tree. It covers the ClickHouse-owned internal block format,
codecs, selection rules, read/write behavior, and adjacent compression
mechanisms used by table storage, protocols, formats, temporary files,
in-memory columns, and text indexes.

Production persistent column-store collection writes are blocked until the
user-command WAL gate links to green evidence for typed command frames,
`AppliedCommandLSN` publication, collection command replay, catalog barriers,
external-file reachability, backup/restore, and read-only recovery-required
behavior. Before that gate, column-store work is limited to docs, benchmarks,
pure codecs, filters/search packages, and isolated encode/decode tests that do
not publish persistent collection roots. Persistent column-store APIs, column
part descriptor roots, secondary indexes pointing at column-store rows,
column-file external refs in published roots, and crash/reopen safety claims for
column-store writes are blocked.

External libraries such as LZ4, ZSTD, zlib, Brotli, XZ, bzip2, Snappy,
Arrow, ORC, and Parquet are treated as delegated implementations. For those
codecs this spec defines the ClickHouse framing, parameters, validation, and
integration points rather than re-specifying the upstream algorithm.

## 1. Compression Planes

ClickHouse uses several related but distinct compression systems:

1. Internal native compressed blocks. These are used for MergeTree data
   files, the native TCP protocol, internal HTTP `compress=1` streams,
   temporary spill files, and the `clickhouse compressor` tool.
2. Column codec pipelines. SQL `CODEC(...)`, table defaults, server
   defaults, and TTL recompression select the internal block codec pipeline.
3. External stream compression. File, URL, object storage, HTTP
   `Content-Encoding`, and many formats use standard stream or container
   compression chosen by extension, setting, or header.
4. In-memory column compression. `ColumnCompressed` uses raw LZ4 to shrink
   column objects in memory without the native block frame.
5. Serialization-level compression. Sparse serialization stores only
   non-default values and offsets. Text indexes have their own posting-list
   bitpacking codec.

These systems must not be mixed by accident. A ClickHouse native `.bin`
block is not an LZ4 frame, even when the codec inside the block is LZ4.

## 2. Native Compressed Block Format

All native compressed blocks have this physical layout:

```text
full_block :=
    checksum_low64  UInt64 little-endian
    checksum_high64 UInt64 little-endian
    codec_block

codec_block :=
    method              UInt8
    compressed_size     UInt32 little-endian
    uncompressed_size   UInt32 little-endian
    payload             byte[compressed_size - 9]
```

Constants:

- `COMPRESSED_BLOCK_HEADER_SIZE = 9`.
- The checksum is `CityHash128(codec_block)`. It does not include the
  checksum bytes themselves.
- `compressed_size` includes the 9-byte codec header.
- Reader-side maximum compressed block size is `0x40000000` bytes, which is
  1 GiB.
- Native readers reject zero compressed size, zero uncompressed size, and
  compressed size smaller than the 9-byte header.
- All integer fields in native frames are little-endian.

Registered method bytes:

| Byte | Name | Notes |
|---:|---|---|
| `0x02` | `NONE` | Payload is raw data. |
| `0x82` | `LZ4` | Also used by `LZ4HC`; the block format does not preserve HC level. |
| `0x90` | `ZSTD` | ZSTD level/window are not stored in the block. |
| `0x91` | `Multiple` | Pipeline wrapper. |
| `0x92` | `Delta` | Numeric delta transform. |
| `0x93` | `T64` | Integer range bit/byte transposition. |
| `0x94` | `DoubleDelta` | Time-series delta-of-delta codec. |
| `0x95` | `Gorilla` | Time-series XOR codec. |
| `0x96` | `AES_128_GCM_SIV` | Encryption postprocessor. |
| `0x97` | `AES_256_GCM_SIV` | Encryption postprocessor. |
| `0x98` | `FPC` | Floating-point predictor codec. |
| `0x99` | removed `DeflateQpl` | Reserved. Do not reuse. |
| `0x9a` | `GCD` | Integer common-divisor transform. |
| `0x9b` | removed `ZSTD_QPL` | Reserved. Do not reuse. |
| `0x9c` | `ALP` | Experimental floating-point codec. |

Native block compression pseudocode:

```text
compress_native_block(source, codec):
    reserve 16 bytes for checksum
    method = codec.method_byte
    payload = codec.do_compress(source)
    codec_block = method || uint32_le(len(payload) + 9) ||
                  uint32_le(len(source)) || payload
    checksum = CityHash128(codec_block)
    return uint64_le(checksum.low64) || uint64_le(checksum.high64) ||
           codec_block
```

Native block decompression pseudocode:

```text
decompress_native_block(bytes, expected_codec = null, allow_different = false):
    read checksum_low64, checksum_high64
    read method, compressed_size, uncompressed_size
    validate compressed_size >= 9, compressed_size <= 1 GiB
    codec_block = next compressed_size bytes starting at method
    validate CityHash128(codec_block) == checksum
    codec = expected_codec
    if codec is null or (allow_different and codec.method_byte != method):
        codec = CompressionCodecFactory.get_by_method(method)
    validate codec.method_byte == method
    dest = allocate uncompressed_size bytes unless NONE can alias payload
    actual = codec.do_decompress(payload, dest, uncompressed_size)
    validate actual == uncompressed_size
    return dest
```

The checksum diagnostic path for blocks under 1 MiB attempts single-bit flips
in the codec block and checksum to produce more useful corruption messages.
That diagnostic is not part of the storage format.

## 3. Codec Interface Contract

`ICompressionCodec` owns a single codec frame:

- `compress(source, source_size, dest)` writes the method byte, calls the
  codec-specific compressor into `dest + 9`, writes compressed and
  uncompressed sizes, and returns the total codec-frame size.
- `decompress(source, source_size, dest, expected_size)` checks the method
  byte, reads the expected decompressed size from the frame, calls the
  codec-specific decompressor, and verifies the returned byte count.
- `getMaxCompressedDataSize(uncompressed_size)` must return a safe upper
  bound for the complete codec frame, including the 9-byte header.
- `getAdditionalSizeAtTheEndOfBuffer()` lets codecs such as LZ4 ask callers
  to provide safe trailing bytes for optimized decompression.
- `isCompression`, `isGenericCompression`, `isEncryption`,
  `isFloatingPointTimeSeriesCodec`, `isDeltaCompression`, `isExperimental`,
  and `isNone` drive validation and selection decisions.

If `setExternalDataFlag` is set, decompression failures are reported as
external-data failures instead of internal corruption.

## 4. Codec Factory and SQL Codec Pipelines

`CompressionCodecFactory` registers all native codecs and makes `LZ4` the
source-level default codec. Cloud deployments may configure a different
default outside this code path.

SQL `CODEC(...)` is parsed as an AST:

- `CODEC(Default)` aliases to the current default codec passed to the
  factory.
- A single codec returns that codec.
- Multiple codecs return `CompressionCodecMultiple` unless generic-only
  filtering removes stages.
- If a caller passes `only_generic = true`, non-generic codecs are discarded.
  If nothing remains, `NONE` is returned.
- Type-dependent codecs infer omitted width parameters from the column type.

Validation rules:

- `NONE` cannot be combined with other codecs unless suspicious codecs are
  explicitly allowed.
- A pipeline with no real compression and no `NONE` is suspicious unless the
  stages are all encryption.
- Encryption codecs must be the final stages.
- Generic compression must be the last non-encryption stage.
- Floating-point time-series codecs on non-float columns are suspicious.
- `Delta` before a floating-point time-series codec is suspicious because the
  time-series codecs already perform delta-like modeling.
- `ALP` is experimental and requires `allow_experimental_codecs`.

Because raw block headers only store method bytes, codec metadata that affects
encoding choices is stored in part metadata or query settings, not in the
block itself. Examples: `LZ4HC` level, `ZSTD` level/window, and inferred
specialized codec widths.

## 5. Codec Specifications

### 5.1 NONE

Method byte: `0x02`.

Payload:

```text
payload := source bytes
```

Compression copies source bytes to the payload. Decompression validates that
the payload size equals the expected uncompressed size and copies bytes back.
The codec reports `isCompression = false`, `isGenericCompression = false`,
and `isNone = true`.

Use cases:

- Explicitly disable compression.
- Keep native framing/checksum behavior while preserving raw payload bytes.

### 5.2 LZ4 and LZ4HC

Method byte: `0x82`.

Payload:

```text
payload := raw upstream LZ4 block data
```

Implementation contract:

- Reserve `LZ4_COMPRESSBOUND(source_size)` payload bytes.
- `LZ4` compresses with `LZ4_compress_default`.
- `LZ4HC(level)` compresses with `LZ4_compress_HC`.
- Both use the same method byte and same decompressor. A block compressed
  with `LZ4HC` is detected later as `LZ4` if only the raw method byte is
  available.
- Decompression uses ClickHouse's optimized LZ4 decoder rather than the
  generic library decoder.

The optimized decoder uses wide copies, overlap-copy specializations,
SSSE3/NEON paths where available, and safe trailing-byte requirements to
reduce branches and copying. It is still decoding the LZ4 block format, not a
ClickHouse-specific LZ4 variant.

Use cases:

- Default general-purpose internal compression.
- Low CPU overhead.
- Native protocol compression when `network_compression_method = LZ4` or
  `LZ4HC`.

### 5.3 ZSTD

Method byte: `0x90`.

Payload:

```text
payload := upstream ZSTD compressed frame
```

Parameters:

- `ZSTD(level)`
- `ZSTD(level, window_log)`

Implementation contract:

- Validate `level <= ZSTD_maxCLevel`.
- `window_log = 0` means the ZSTD library default.
- If a nonzero window log is used, enable long-distance matching and set
  `ZSTD_c_windowLog`.
- Reserve `ZSTD_compressBound(source_size)` payload bytes.
- Compress with `ZSTD_compress2`.
- Decompress with `ZSTD_decompress` into the known uncompressed size.

The level and window log are not stored in each native block. They are part
of the codec description used while writing.

Use cases:

- Better ratio than LZ4 at higher CPU cost.
- Common for marks, primary key files, output formats, and configured large
  parts.

### 5.4 Multiple

Method byte: `0x91`.

Payload:

```text
payload :=
    codec_count UInt8
    method[codec_count] UInt8
    final_inner_frame bytes
```

Compression algorithm:

1. Require at least two nested codecs at construction time. The factory
   normally avoids `Multiple` for a single stage.
2. Write `codec_count`.
3. Write the method byte for each nested codec in pipeline order.
4. Starting with the original source bytes, call each nested codec's
   `compress` method in order. Each nested stage produces a complete
   9-byte-header codec frame.
5. Store the final nested frame after the method list.
6. The outer `Multiple` frame's `uncompressed_size` is the size of the
   original source.

Decompression algorithm:

1. Read `codec_count` and method bytes.
2. The remaining bytes are the output of the last nested stage.
3. For each method in reverse order, construct that codec by method byte and
   decompress one complete inner codec frame.
4. Validate the final byte count against the outer frame's uncompressed size.

`Multiple` uses a single-byte codec count, so a native pipeline cannot exceed
255 encoded method entries. Real pipelines should be far shorter.

### 5.5 Delta

Method byte: `0x92`.

Parameters:

- `Delta(bytes)` where `bytes` is `1`, `2`, `4`, or `8`.
- If omitted, infer from fixed-size column type where possible, otherwise
  default to `1`.

Payload:

```text
payload :=
    data_bytes_size UInt8
    bytes_to_skip   UInt8
    prefix          byte[bytes_to_skip]
    deltas          byte[(source_size - bytes_to_skip)]
```

`bytes_to_skip = source_size % data_bytes_size`. The prefix bytes are copied
verbatim. The remaining bytes are interpreted as little-endian unsigned
integers of `data_bytes_size`.

Compression:

```text
prev = 0
for each value curr in aligned_values:
    delta = curr - prev       // unsigned modulo arithmetic
    write_le(delta)
    prev = curr
```

Decompression:

```text
prev = 0
for each encoded delta:
    curr = prev + delta       // unsigned modulo arithmetic
    write_le(curr)
    prev = curr
```

`Delta` is a transform, not a generic compressor. It should normally be
followed by a generic codec such as `ZSTD` or `LZ4`.

### 5.6 DoubleDelta

Method byte: `0x94`.

Parameters:

- `DoubleDelta(bytes)` where `bytes` is `1`, `2`, `4`, or `8`.
- If omitted, infer from numeric type where possible, otherwise default to
  `1`.

Payload:

```text
payload :=
    data_bytes_size UInt8
    bytes_to_skip   UInt8
    prefix          byte[bytes_to_skip]
    item_count      UInt32 little-endian
    first_value     UInt[data_bytes_size] little-endian, if item_count >= 1
    first_delta     UInt[data_bytes_size] little-endian, if item_count >= 2
    bitstream       bytes
```

The prefix/alignment rule is the same as `Delta`. Values are treated as
unsigned fixed-width integers so overflow is well-defined modulo the type
width. Signed comparison is only used to select compact encodings for a
nonzero double delta.

For item `i >= 2`:

```text
delta        = value[i] - value[i - 1]
double_delta = delta - previous_delta
```

Bit writing uses `BitWriter`: write the low bits of the requested value into
the high end of the pending bit buffer, read back most-significant bit first,
and zero-pad the final byte.

Double-delta code table:

| Condition | Prefix | Payload bits |
|---|---:|---:|
| `double_delta == 0` | `0` | none |
| `-63 < dd < 64` | `10` | sign bit + 6 bits of `abs(dd) - 1` |
| `-255 < dd < 256` | `110` | sign bit + 8 bits of `abs(dd) - 1` |
| `-2047 < dd < 2048` | `1110` | sign bit + 11 bits of `abs(dd) - 1` |
| fits signed 32-bit range | `11110` | sign bit + 31 bits of `abs(dd) - 1` |
| otherwise | `11111` | sign bit + 63 bits of `abs(dd) - 1` |

The decoder peeks the next byte, uses the high five bits as an index into a
32-entry lookup table, skips the prefix bits, reads the payload bits, rebuilds
the signed double delta, then reconstructs:

```text
delta = previous_delta + double_delta
value = previous_value + delta
```

Best fit: monotonic integer time series with constant or near-constant stride,
for example timestamps.

### 5.7 GCD

Method byte: `0x9a`.

Parameters:

- No explicit argument.
- Width is inferred from Int, UInt, Decimal, Date, and DateTime-like types.
  Supported widths are `1`, `2`, `4`, `8`, `16`, and `32` bytes.
- Default width is `1`.

Payload:

```text
payload :=
    data_bytes_size UInt8
    bytes_to_skip   UInt8
    prefix          byte[bytes_to_skip]
    gcd_value       UInt[data_bytes_size] little-endian
    body            bytes
```

Compression:

1. Copy unaligned prefix bytes.
2. Compute the greatest common divisor of all aligned values. Stop early if
   it becomes `1`.
3. Write the GCD.
4. If `gcd` is `0` or `1`, copy the original aligned values unchanged.
5. Otherwise, write each value divided by `gcd`.

For widths up to 8 bytes, division uses `libdivide`. For `UInt128` and
`UInt256`, ClickHouse uses direct division.

Decompression:

1. Read GCD.
2. If `gcd` is `0` or `1`, copy aligned values.
3. Otherwise, multiply each stored quotient by `gcd`.

`GCD` is a transform. It is most effective before a generic codec when values
share a large divisor.

### 5.8 T64

Method byte: `0x93`.

Parameters:

- `T64()` or `T64('byte')` selects byte-transpose mode.
- `T64('bit')` selects bit-transpose mode.
- The type-specific width and signedness are inferred from the column type.

Allowed logical types include fixed-width integer-like types, enums, dates,
times, decimals up to 64 bits, and IPv4. Unsupported types fail validation.

Payload starts with a cookie:

```text
cookie UInt8
```

Cookie layout:

- Low 7 bits: type magic number.
- High bit: variant, `0` for byte mode and `1` for bit mode.

The remaining payload includes:

```text
bytes_to_skip UInt8
prefix        byte[bytes_to_skip]
min_value     UInt64 little-endian
max_value     UInt64 little-endian
matrix_data   bytes
```

Compression:

1. Copy the unaligned prefix where `bytes_to_skip = source_size % sizeof(T)`.
2. Scan aligned values to find `min` and `max`.
3. Compute the number of valuable bits:
   - Unsigned values use the bit length of `min XOR max`.
   - Signed values crossing zero keep enough magnitude bits plus a sign bit.
4. If the valuable bit count is zero, store no matrix data. Decompression
   fills all values with `min`.
5. Process values in groups of 64, including a final partial group.
6. Build a 64-by-64 logical bit matrix from the values.
7. Crop invariant upper bits.
8. In byte mode, transpose by bytes and bit-transpose only the partial last
   byte. In bit mode, fully bit-transpose.
9. Write `valuable_bits * sizeof(UInt64)` bytes per full 64-value group,
   with equivalent cropped data for the final group.

Decompression reverses the transposition and restores cropped upper bits.
For signed ranges crossing zero, the sign bit determines whether upper bits
come from the min-side or max-side pattern.

Byte mode favors speed. Bit mode can feed more regular data to ZSTD but costs
more CPU.

### 5.9 Gorilla

Method byte: `0x95`.

Parameters:

- `Gorilla(bytes)` where `bytes` is `1`, `2`, `4`, or `8`.
- If omitted, infer from fixed-size contiguous column type where possible,
  otherwise default to `1`.

Payload:

```text
payload :=
    data_bytes_size UInt8
    bytes_to_skip   UInt8
    prefix          byte[bytes_to_skip]
    item_count      UInt32 little-endian
    first_value     UInt[data_bytes_size] little-endian
    bitstream       bytes
```

For each following value:

```text
xor_diff = current XOR previous
```

Bitstream encoding:

- If `xor_diff == 0`, write `0`.
- Else compute leading zero bits, trailing zero bits, and significant data
  width.
- If a previous nonzero XOR window exists and the current nonzero bits fit
  inside it, write `10` and then the significant bits shifted right by the
  previous trailing-zero count.
- Otherwise write `11`, then the leading-zero count, then the significant
  data width, then the significant bits. Update the remembered XOR window.

The number of bits used to write a data width is:

- 4 for 1-byte values.
- 5 for 2-byte values.
- 6 for 4-byte values.
- 7 for 8-byte values.

The leading-zero count uses one fewer bit than the data-width field because a
nonzero XOR always has at least one significant bit.

Best fit: slowly changing floating or gauge-like values. It is marked as a
floating-point time-series codec for validation, although the implementation
operates over fixed-width unsigned representations.

### 5.10 FPC

Method byte: `0x98`.

Parameters:

- Supports `Float32` and `Float64`.
- `FPC(level)` where `level` is `1..28`; default is `12`.
- Predictor table size is `2^level`.

Payload header:

```text
float_width       UInt8   // 4 or 8
compression_level UInt8
```

FPC uses two predictors:

- FCM predicts from a table keyed by a rolling hash of previous values.
- DFCM predicts `table[hash] + previous_value` and stores deltas in its
  table.

Hash updates:

- `UInt64` FCM: `hash = ((hash << 6) XOR (value >> 48)) & mask`.
- `UInt32` FCM: `hash = ((hash << 1) XOR (value >> 22)) & mask`.
- `UInt64` DFCM: `hash = ((hash << 2) XOR (delta >> 40)) & mask`.
- `UInt32` DFCM: `hash = ((hash << 4) XOR (delta >> 23)) & mask`.

Compression loop:

1. Interpret floats as unsigned integer bit patterns.
2. For each value, compute both predictions.
3. XOR actual value with each prediction.
4. Choose the predictor whose residual has more leading zero bits.
5. Update both predictors with the actual value or delta.
6. Encode values in pairs. One header byte stores two 4-bit descriptors:
   high nibble for the first value, low nibble for the second.

Nibble format:

- Bit 3 marks DFCM if set; FCM if clear.
- Low 3 bits encode the number of leading zero bytes in the residual.
- For 8-byte floats, encoded counts `>= 4` are shifted so the descriptor can
  represent the possible byte counts used by this implementation.

After the header byte, write only the non-leading-zero residual bytes for the
two values. Chunks are processed in groups of 64 values. The final chunk is
padded to an even number of values internally, but decompression returns only
the requested uncompressed byte count.

Best fit: floating-point sequences with predictable bit patterns.

### 5.11 ALP

Method byte: `0x9c`.

Status: experimental. Requires `allow_experimental_codecs`.

Parameters:

- Supports `Float32` and `Float64`.
- No SQL arguments.
- Float width is inferred from the column type.

Codec header:

```text
meta              UInt8   // low 4 bits version = 1, bit 4 variant = 0
float_width       UInt8   // 4 or 8
block_float_count UInt16 little-endian // must be 1024
```

ALP splits input into blocks of up to 1024 floats. Each block is either stored
raw or encoded.

Decimal integerization:

```text
encoded = round(value * 10^exponent * 10^-fraction)
decoded = encoded * 10^fraction * 10^-exponent
```

A value is encodable only if:

- It is finite.
- It is not negative zero.
- The encoded integer is in range.
- Decoding produces exactly the original floating-point value.

Rounding uses the ALP magic-constant technique:

- Float64 uses `2^51 + 2^52`.
- Float32 uses `2^22 + 2^23`.

Parameter selection:

1. Take 8 samples of up to 32 floats across the column.
2. Brute-force valid `(exponent, fraction)` pairs where
   `0 <= fraction <= exponent < EXPONENT_COUNT`.
3. `EXPONENT_COUNT` is 19 for Float64 and 10 for Float32.
4. Estimate encoded size for every pair.
5. Sort candidates by occurrence count descending, then exponent and
   fraction.
6. Keep the top 5 candidates.
7. For each block, sample up to 32 values, test candidates, choose the
   estimated smallest, and stop after two consecutive worse candidates.

Encoded block layout:

```text
exponent             UInt8
fraction             UInt8
exception_count      UInt16 little-endian
bits                 UInt8
frame_of_reference   Int64 little-endian
ffor_payload         byte[bits * 1024 / 8]
exceptions           repeated exception_count times
```

Exception layout:

```text
index UInt16 little-endian
raw_float byte[float_width]
```

Encoding:

1. Convert floats to Int64 using the chosen decimal parameters.
2. For exceptions, store the raw float and replace the encoded slot with the
   frame-of-reference value.
3. For a partial final block, fill missing positions up to 1024 with the
   frame-of-reference value.
4. Let `frame_of_reference` be the block minimum.
5. Store deltas from the frame-of-reference with fixed-frame-of-reference
   bitpacking.
6. `bits` is the bit width of the maximum delta; zero means all values equal.

Raw block layout:

```text
marker UInt8 = 255
raw_float_bytes
```

Use raw blocks when the encoded block would not be smaller than
`raw_size + 1`.

FFOR details:

- Values are normalized by subtracting the frame-of-reference.
- A fixed bit width is used for all 1024 positions.
- The implementation uses generated dispatch tables, strided iteration, and
  64-byte-aligned arrays for FastLanes-compatible performance.

### 5.12 AES-GCM-SIV Encryption Codecs

Method bytes:

- `0x96` for `AES_128_GCM_SIV`.
- `0x97` for `AES_256_GCM_SIV`.

These are encryption postprocessors, not compression. They must appear at the
end of a codec pipeline because encrypted data is not compressible.

Configuration:

- Keys are loaded from the `encryption_codecs` server config.
- AES-128 keys are 16 bytes; AES-256 keys are 32 bytes.
- Keys can be literal or hex.
- `current_key_id` is required when multiple keys or nonzero key ids are
  present.
- Nonce is either empty, meaning implicit 12 zero bytes, or a configured
  12-byte nonce.

Payload:

```text
key_id     VarUInt
nonce_flag UInt8
nonce      byte[12] if nonce_flag == 1
ciphertext byte[uncompressed_size]
tag        byte[16]
```

`nonce_flag == 0` means use the implicit zero nonce and no nonce bytes follow.
This deterministic design preserves deduplication for equal compressed
blocks, but equal plaintext blocks under the same key and nonce produce equal
ciphertext.

Decompression reads the key id and nonce, validates that the encrypted body
has exactly `uncompressed_size + 16` bytes, then authenticates and decrypts
with OpenSSL EVP. s390x uses AES-GCM compatibility paths.

## 6. Native Read and Write Buffers

`CompressedWriteBuffer` accumulates uncompressed bytes until it flushes a
block. If the downstream buffer has enough available space, it writes the
checksum and codec frame directly into the downstream buffer. Otherwise it
uses a temporary compressed buffer. Changing the codec with `setCodec`
flushes any pending data first.

`ParallelCompressedWriteBuffer` uses a thread pool. Buffers receive sequence
numbers, compression tasks run concurrently, and writes are emitted in
sequence order. It can still direct-write when no earlier buffer is pending
and the destination has enough capacity.

`CompressedReadBufferBase` reads checksum plus header, validates sizes and
checksum, chooses the codec by method byte when needed, and decompresses into
an owned uncompressed buffer. If the codec is `NONE`, it can point the
uncompressed view directly at the payload and avoid a copy.

`CompressedReadBuffer` and `CompressedReadBufferFromFile` can decompress a
large block directly into the user's target buffer when the full block fits
the request. Seekable readers store a compressed-file offset plus an offset
inside the decompressed block.

`CachedCompressedReadBuffer` caches decompressed blocks by `(path, file_pos)`
using `UncompressedCache`. A cached entry records the decompressed bytes,
additional tail bytes, and compressed size.

## 7. MergeTree Compression Selection

MergeTree column data compression is selected in this priority order:

1. Column-level `CODEC(...)` in the table declaration.
2. Table setting `default_compression_codec`.
3. Server config `<compression>` selector.
4. Factory default `LZ4`.

The server `<compression>` selector is an ordered list of `<case>` elements.
Each case can contain:

- `min_part_size`
- `min_part_size_ratio`
- `method`
- optional `level`

The last matching case wins. If nothing matches, the factory default is used.

Write-time decisions:

- New inserts call `chooseCompressionCodec(0, 0)`, so they use `LZ4` unless
  server config has a zero-threshold matching case or table/column settings
  override it.
- Merges and full-part mutations call `getCompressionCodecForPart`.
- If a recompression TTL is due, the TTL codec wins.
- Otherwise the server selector sees the part's compressed size and
  `part_size / total_active_size`.
- Some partial mutation paths that hardlink data keep the source part's
  default codec.

TTL recompression:

- `TTL ... RECOMPRESS CODEC(...)` is parsed into a codec AST and validated by
  the compression factory.
- Merge logic compares the next TTL codec against the current part default
  codec by AST string.
- `merge_with_recompression_ttl_timeout` defaults to 4 hours.

Part metadata:

- Writers persist `default_compression_codec.txt` containing `CODEC(...)` for
  the part default.
- Readers load that file when present.
- If absent or invalid, ClickHouse attempts to infer a codec from the first
  non-empty data file for a column without an explicit codec.
- If inference fails, it falls back to the factory default.

Substreams:

- Specialized codecs are not allowed on substreams such as `NullMap`,
  `ArraySizes`, `StringSizes`, `DictionaryIndexes`, and `SparseOffsets`.
- These streams use `only_generic = true`; non-generic stages are stripped.
- This prevents transforms that require real column values from being applied
  to structural metadata streams.

Block sizing:

- Global defaults are `min_compress_block_size = 65536` and
  `max_compress_block_size = 1048576`.
- MergeTree settings can override both values.
- `max_compress_block_size` controls the uncompressed buffer capacity for a
  compressed stream.
- `min_compress_block_size` controls when writers flush at a mark boundary:
  after writing a granule, if enough uncompressed bytes have accumulated, the
  stream is finalized and the next mark starts in a new compressed block.

Wide parts:

- Each column substream has its own data stream.
- A mark records `(offset_in_compressed_file, offset_in_decompressed_block)`.
- Checksums record compressed size, uncompressed size, and hashes.

Compact parts:

- Data is stored in a single `data.bin`.
- Writers create one compressed stream per distinct codec hash and map
  substreams to those streams.
- When switching streams, the previous stream is flushed so streams do not
  overwrite one another in the shared file.
- If `compress_per_column_in_compact_parts = true`, each column in a granule
  starts a new compressed block. This improves column skipping.
- If false, all columns in a granule can share a block. This may improve
  ratio but increases read amplification when only some columns are needed.

Marks and primary key:

- `compress_marks = true` by default.
- `compress_primary_key = true` by default.
- Default codecs for both are `ZSTD(3)`.
- Their default block sizes are 65536 bytes.

Sparse serialization:

- `ratio_of_defaults_for_sparse_serialization` defaults to `0.9375`.
- If the sampled default-row ratio is greater than the threshold, the column
  may use sparse serialization.
- A ratio `>= 1.0` disables sparse serialization.
- Sampling tracks row count and default count, using a default-row search
  sample ratio of 0.1.

Sparse column layout:

- `ColumnSparse` stores a values column containing a default value at position
  0 plus all non-default values.
- It stores a sorted UInt64 offsets column containing positions of non-default
  rows.
- It stores the full logical row count.

Sparse stream layout:

- `SparseOffsets` stores varint-encoded counts of default values before each
  non-default value.
- `SparseElements` stores nested non-default values.
- The final group count of a serialize call ORs
  `END_OF_GRANULE_FLAG = 1ULL << 62`, allowing independent granule reads to
  know about trailing defaults.
- Nullable sparse columns avoid writing a full null-map stream; sparse null
  maps can be reconstructed from offsets.

## 8. Other Storage Engines and Internal Uses

The Log engine supports per-column `CODEC(...)` metadata and otherwise uses
the default LZ4 native compressed buffer.

StripeLog uses a default LZ4 compressed buffer for data.

Set, Join, Memory, KeeperMap, backups, distributed async insert queues, and
other internal persisted or temporary block paths use native compressed
buffers where they need compact durable byte streams. Most do not expose a
full per-column codec selection surface.

Temporary spill files use `temporary_files_codec`, defaulting to `LZ4` when
the setting is empty. Sorting, joins, and other spill users pass that codec
into temporary file writers. `TemporaryDataBuffer` records compressed and
uncompressed statistics.

## 9. Native Protocol and HTTP Compression

Native TCP protocol:

- Compression is negotiated by a protocol compression flag.
- Allowed network methods are `NONE`, `ZSTD`, `LZ4`, and `LZ4HC`.
- Specialized column codecs are intentionally excluded from arbitrary network
  compression.
- `network_zstd_compression_level` applies when the method is `ZSTD`.
- The client defaults to no compression for localhost and compression for
  remote connections.
- The server may read compressed streams with `allow_different_codecs = true`
  because distributed and precompressed blocks can contain varying codec
  bytes in one stream.

HTTP:

- Query parameter `compress=1` enables ClickHouse native compressed response
  blocks. This is not represented as HTTP `Content-Encoding`.
- Query parameter `decompress=1` tells the server to read the request body as
  ClickHouse native compressed blocks.
- `http_native_compression_disable_checksumming_on_decompress` can disable
  checksum validation for internal compressed POST data.
- If `enable_http_compression` is enabled, `Accept-Encoding` is used for
  standard HTTP response compression and `Content-Encoding` is set.
- Request bodies are wrapped by `Content-Encoding` using the external
  compression method selector.

HTTP `Accept-Encoding` preference order:

```text
zstd, br, lz4, snappy, gzip, deflate, xz, bz2, none
```

## 10. External Stream Compression

`CompressionMethod` covers recognizable external formats, independent of the
native block frame:

- `none`
- `gzip` / `gz`
- `deflate`
- `brotli` / `br`
- `lzma` / `xz`
- `zstd` / `zst`
- `lz4`
- `bz2`
- `snappy`

Selection:

- If the hint is empty or `auto`, choose by file extension.
- If `auto` has no recognized extension, choose `none`.
- Unknown explicit hints throw.

Level validation:

- ZSTD levels: `1..22`.
- LZ4 levels: `1..12`.
- Other supported stream levels: `1..9`.

Read wrappers exist for zlib/gzip, Brotli when built, XZ, ZSTD with
`zstd_window_log_max`, LZ4, bzip2 when built, and Hadoop Snappy when built.

Write wrappers exist for zlib/gzip, Brotli, XZ, ZSTD, LZ4, and bzip2. Snappy
write is not supported by this generic wrapper; Parquet and ORC can still use
Snappy through their own libraries.

File, URL, object storage, HDFS, Hive, Iceberg, DeltaLake, and related
storage/table-function paths feed their path and explicit setting into this
selector.

Format settings:

- `output_format_compression_level` defaults to `3`.
- `output_format_compression_zstd_window_log` can enable long-window ZSTD for
  output streams.
- Parquet output compression defaults to `zstd`; supported values include
  `snappy`, `lz4`, `brotli`, `zstd`, `gzip`, and `none`.
- Arrow output defaults to `lz4_frame`; supported values include
  `lz4_frame`, `zstd`, and `none`.
- ORC output defaults to `zstd`; supported values include `lz4`, `snappy`,
  `zlib`, `zstd`, and `none`.
- ORC compression block size defaults to 262144.

## 11. In-Memory Column Compression

`ColumnCompressed` is a lazy in-memory wrapper around compressed column data.
It is not a native compressed block and has no ClickHouse 16-byte checksum or
9-byte codec header.

Implementation:

- Compress with raw `LZ4_compress_default`.
- Decompress with raw `LZ4_decompress_safe`.
- Store enough metadata to know the original column type and byte size.
- Support only `decompress`, `size`, `byteSize`, and `allocatedBytes`.
  Other column operations throw until the column is decompressed.

Admission rule:

- If `force_compression = false`, accept compression only when compressed
  data is at least 50 percent smaller:
  `compressed_size * 2 <= original_size`.
- If forced, accept only if compressed data is no larger than the original.
- Otherwise return null and keep the original column.

Many column types recursively compress subcolumns, including strings,
vectors, decimals, fixed strings, nullable, arrays, tuples, sparse, variants,
dynamic, maps, objects, and replicated columns.

Typical users include hash join build-side blocks and query result cache
entries.

## 12. Text Index Posting List Compression

Text indexes use posting-list codecs separate from native block compression.

Registered posting-list codecs:

- `none`
- `bitpacking`

Bitpacking assumptions and layout:

- Row ids in a posting list are strictly increasing.
- Logical posting-list segments are split by `postings_list_block_size`,
  normalized up to a multiple of the physical `BLOCK_SIZE`.
- Each segment has varint header fields:
  codec type, payload byte size, cardinality, and first row id.
- Segment metadata records offsets and row-id ranges.

Encoding:

1. Set `previous_row_id = first_row_id`.
2. Convert row ids to gaps:
   `gap = row_id - previous_row_id`.
   The first gap is zero.
3. Split gaps into physical fixed-size blocks.
4. For each block, compute the maximum bit width needed for any gap.
5. Write one byte `max_bits`.
6. Bit-pack all gaps in that block using `max_bits`.

Decoding:

1. Read `max_bits` for the physical block.
2. Bit-unpack gaps.
3. Reconstruct row ids with an inclusive scan using the previous absolute row
   id.

This codec is optimized for monotonically increasing document or row-id
lists.

## 13. Implementation Rules and Tips

Use native compression only through `CompressedWriteBuffer`,
`CompressedReadBuffer`, or `ICompressionCodec` unless implementing a new
codec. That keeps checksum handling, sizes, direct-write optimization, and
buffer-tail requirements correct.

Use upstream APIs for delegated codecs:

- LZ4 native blocks store raw LZ4 block payloads, not LZ4 frame files.
- ZSTD native blocks store ZSTD compressed payloads inside ClickHouse frames.
- External files use their corresponding stream/frame wrappers, not native
  block framing.

When adding a codec:

1. Assign a never-before-used method byte. Do not reuse removed QPL bytes.
2. Implement `getMaxCompressedDataSize` as a safe bound for the complete
   codec frame.
3. Encode all codec-specific metadata in the payload unless it is guaranteed
   by part metadata or construction context.
4. Make decompression validate source size, destination size, item counts,
   and any bitstream invariants before writing beyond buffers.
5. Decide whether the codec is generic, specialized, encryption, time-series,
   delta, experimental, or none. These flags affect SQL validation and
   substream filtering.
6. Register the codec in the factory and system-codecs metadata.
7. Add compatibility tests for exact bytes if the format is ClickHouse-owned.

Pipeline ordering:

- Put transforms such as `Delta` and `GCD` before generic compression.
- Put generic compression before encryption.
- Do not put specialized transforms on structural substreams.
- Prefer `T64(...), ZSTD` for integer ranges that differ only in low bits.
- Prefer `DoubleDelta, ZSTD` for near-linear integer time series.
- Prefer `Gorilla`, `FPC`, or `ALP` for float time series, depending on data
  shape and experimental-setting availability.
- Prefer plain `LZ4` where CPU budget matters more than ratio.
- Prefer `ZSTD` or configured higher-ratio codecs for cold, large, or
  recompressed parts.

Operational notes:

- Native block inspection can reconstruct method bytes but cannot recover
  all original SQL codec parameters.
- `clickhouse compressor --stat` can inspect native compressed streams.
- External tools such as `lz4` cannot decompress native `.bin` files because
  of ClickHouse checksum and header framing.
- For seekable native files, keep both compressed-file offsets and offsets
  within decompressed blocks.
- For cached reads, cache decompressed blocks by compressed-file position.
- For HTTP, distinguish `compress=1` native compression from
  `Content-Encoding` stream compression.

## 14. Source Map

Primary native compression files:

- `src/Compression/CompressionInfo.h`
- `src/Compression/ICompressionCodec.h`
- `src/Compression/ICompressionCodec.cpp`
- `src/Compression/CompressionFactory.cpp`
- `src/Compression/CompressionFactoryAdditions.cpp`
- `src/Compression/CompressedWriteBuffer.cpp`
- `src/Compression/CompressedReadBufferBase.cpp`
- `src/Compression/CompressedReadBuffer.cpp`
- `src/Compression/CompressedReadBufferFromFile.cpp`
- `src/Compression/CachedCompressedReadBuffer.cpp`
- `src/Compression/ParallelCompressedWriteBuffer.cpp`
- `src/Compression/getCompressionCodecForFile.cpp`

Native codecs:

- `src/Compression/CompressionCodecNone.cpp`
- `src/Compression/CompressionCodecLZ4.cpp`
- `src/Compression/LZ4_decompress_faster.cpp`
- `src/Compression/CompressionCodecZSTD.cpp`
- `src/Compression/CompressionCodecMultiple.cpp`
- `src/Compression/CompressionCodecDelta.cpp`
- `src/Compression/CompressionCodecDoubleDelta.cpp`
- `src/Compression/CompressionCodecGCD.cpp`
- `src/Compression/CompressionCodecT64.cpp`
- `src/Compression/CompressionCodecGorilla.cpp`
- `src/Compression/CompressionCodecFPC.cpp`
- `src/Compression/CompressionCodecALP.cpp`
- `src/Compression/CompressionCodecEncrypted.cpp`
- `src/Compression/FFOR.h`
- `src/IO/BitHelpers.h`

Selection and storage integration:

- `src/Storages/CompressionCodecSelector.h`
- `src/Interpreters/Context.cpp`
- `src/Storages/MergeTree/MergeTreeDataWriter.cpp`
- `src/Storages/MergeTree/MergeTreeData.cpp`
- `src/Storages/MergeTree/MergeTask.cpp`
- `src/Storages/MergeTree/MutateTask.cpp`
- `src/Storages/MergeTree/IMergeTreeDataPartWriter.cpp`
- `src/Storages/MergeTree/MergeTreeDataPartWriterWide.cpp`
- `src/Storages/MergeTree/MergeTreeDataPartWriterCompact.cpp`
- `src/Storages/MergeTree/MergeTreeWriterStream.cpp`
- `src/Storages/MergeTree/MergedBlockOutputStream.cpp`
- `src/Storages/MergeTree/IMergeTreeDataPart.cpp`
- `src/Storages/MergeTree/MergeTreeSettings.cpp`
- `src/Storages/TTLDescription.cpp`
- `src/DataTypes/Serializations/ISerialization.cpp`
- `src/DataTypes/Serializations/SerializationInfo.cpp`
- `src/DataTypes/Serializations/SerializationSparse.cpp`
- `src/Columns/ColumnSparse.cpp`

Other compression systems:

- `src/IO/CompressionMethod.h`
- `src/IO/CompressionMethod.cpp`
- `src/Server/HTTPHandler.cpp`
- `src/Client/Connection.cpp`
- `src/Server/TCPHandler.cpp`
- `src/Interpreters/TemporaryDataOnDisk.cpp`
- `src/Columns/ColumnCompressed.cpp`
- `src/Storages/MergeTree/IPostingListCodec.cpp`
- `src/Storages/MergeTree/MergeTreeIndexTextPostingListCodec.cpp`
- `src/Storages/MergeTree/BitpackingBlockCodec.h`
- `programs/compressor/Compressor.cpp`
