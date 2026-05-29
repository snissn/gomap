# TreeDB Storage Format

This document defines TreeDB's durable on-disk formats and local frame formats.
The native client/server wire protocol is owned by
`TreeDB/docs/spec/native-wire-protocol.md`.

TreeDB is pre-alpha; format compatibility between versions is not guaranteed.
That disclaimer does not permit fail-open handling of acknowledged durable
writes. Once a directory advertises a required storage feature such as
`command_wal_v1`, unsupported binaries must fail closed instead of serving,
cleaning, compacting, or rewriting the directory. Typed-column image,
descriptor, manifest, and schema evolution follows the fail-closed policy in
`typed-column-schema-evolution.md`.

## 1. Top-Level Storage Objects

A TreeDB deployment uses:

- `index.db` (paged B+Tree index and metadata),
- commit-log segments under `wal/commit-l*.log`; future user-command WAL
  frames extend this same segment family rather than creating a second WAL file
  class,
- value-log segments under `value_vlog/value-l*.log`,
- optional split outer-leaf value-log segments under `leaf_vlog/value-l*.log`
  when `IndexOuterLeavesInValueLog` is enabled,
- typed asset manager segments under
  `column_assets/<namespace>/assets/segments/segment-*.tca` for production
  typed-storage physical assets (`column_assets` remains the compatibility
  directory name),
- optional side-store DBs (`dictdb`, `templatedb`) using their own `index.db` files.

The old collection root-delta WAL storage class (`wal/collection-l*.log`,
`collection_wal_v1`) is deprecated before becoming the active committed format.
It is retained in `collection-wal-durability-plan.md` as historical design
context. The active target is the user-command WAL: command frames ordered by
`LSN`, checkpointed by durable `AppliedLSN`, and defined in
`user-command-wal.md`. Exact command-frame bytes, checksums, segment metadata,
cleanup records, and golden encodings must be maintained here and mapped to
tests in `TreeDB/docs/spec/verification.md` as each milestone lands.

The operator restorable file set, live backup barrier, and restore validation
procedure are defined in `TreeDB/docs/spec/backup-restore.md`. A live
filesystem-level copy without that barrier is unsupported once command-WAL
external refs can exist.

## 2. Index Page Basics

### 2.1 Fixed page size

- `PageSize = 4096` bytes.

### 2.2 Page header

All pages begin with a 16-byte header:

```text
u64 PageID
u32 Checksum   // CRC-32/IEEE over page with checksum bytes zeroed
u16 Flags      // low bits: page type; high bits: encoding flags
u16 Count      // entry count
```

Page types (`Flags` low bits):

- `0x01`: meta page
- `0x02`: freelist page
- `0x03`: internal page
- `0x04`: leaf page

### 2.3 Checksum

- Checksum algorithm: CRC-32/IEEE.
- Verification may be cached unless `VerifyOnRead` forces every-read checks.

## 3. Meta Page Body

Meta page payload (after 16-byte header) encodes:

```text
u64 CommitSeq
u64 UserRootPageID
u64 SystemRootPageID
u64 FreelistHeadID
u64 TotalPages
u32 ActiveSlabID      // legacy/reserved field name in MetaPageBody
u64 ActiveSlabTail    // legacy/reserved field name in MetaPageBody
u64 LastCommitHeight  // reserved
```

Notes:

- Field names intentionally match `page.MetaPageBody.ActiveSlabID` and
  `page.MetaPageBody.ActiveSlabTail` for binary compatibility.
- Current TreeDB value storage uses persistent value-log segments and `ValuePtr` references.

## 3.1 Command WAL Meta Extension

Command-WAL V1 meta pages extend the 60-byte body above with an in-page marker
and the applied command stream boundary:

```text
body offset 60 / page offset 76:
[8]byte CommandWALV1Marker = "TMETAW1\x00"

body offset 68 / page offset 84:
u64 AppliedCommandLSN
```

The `command_wal_v1` meta body size is 76 bytes. Bytes after offset 76 are
reserved and must be written as zero until assigned by a later required feature.
`AppliedCommandLSN` is the physical on-disk field for the logical `AppliedLSN`
command stream boundary. Alternating meta-page selection must choose roots and
`AppliedCommandLSN` from the same meta page candidate.

The marker is checksummed with the selected meta page. A decoder must treat
`AppliedCommandLSN` as zero unless the marker is present in that same page body.
`format.json`, manifests, stats, or any other sidecar file must not decide
whether a meta page's `AppliedCommandLSN` bytes are authoritative.

Rules:

- New `command_wal_v1` directories start with `AppliedCommandLSN=0`.
- Updating `AppliedCommandLSN` without selecting the roots that contain those
  command effects is invalid.
- Selecting roots that contain command effects without the matching
  `AppliedCommandLSN` is invalid for durable root publish/checkpoint state.
- Required feature validation must fail closed before full `command_wal_v1`
  execution is enabled if a command-WAL directory is opened by code that decodes
  only the 60-byte pre-command-WAL meta body.
- `format.json` must use version 3 or newer when `required_features` contains
  `command_wal_v1`; putting required features in version 2 is invalid because
  older binaries would ignore unknown JSON fields and fail open.
- PR2 must add meta-page tests covering `AppliedCommandLSN` encode/decode,
  in-page marker gating for legacy/reserved bytes, alternating meta pages,
  old/new tuple selection, and checksum validation over the extended body.

## 3.2 Collection Document Payloads

Collection document payload encodings are defined separately in
`TreeDB/docs/spec/collections-document-formats.md`. In particular,
template-v1 collections store compact `TD1D` primary documents and persist the
template ID map in the collection-local `<collection>/templates` ordered root.

## 4. Value Pointer Encoding (`page.ValuePtr`)

Base struct:

```text
u64 Offset
u32 Length
u32 FileID
```

Semantics:

- `FileID` identifies the value-log segment.
- `Offset` points to `record_start + 4` (immediately after record CRC field).
- `Length` stores record length hint plus packed flags.

### 4.1 FileID layout

`FileID` uses a marker bit and packed segment id:

- bit 31 set => value-log pointer (`IsValueLogFileID=true`),
- remaining 31 bits encode `(lane, seq)`:
  - lane bits: 8
  - sequence bits: 23

Limits:

- max lane id: 255
- max seq per lane: 8,388,607

### 4.2 `Length` bit packing

Flags:

- compressed marker bit: `0x80000000`
- grouped marker bit: `0x40000000`

Grouped pointer sub-index bits:

- low portion: bits `29..26`
- extra bit: bit `31` contributes one sub-index bit
- high portion: bits `25..24`
- total sub-index range: `0..127`

Record-length hint:

- grouped pointers keep a best-effort 24-bit record-length hint.
- max encodable grouped record length hint: `0x00ffffff`.
- if record is larger, hint is set to zero and reader uses record header length fields.

### 4.3 Packed on-disk `ValuePtr`

Some leaf encodings optionally store packed pointer payloads:

```text
u32 Offset32
u32 Length
u32 FileID
```

- Packed size: 12 bytes.
- Requires offset to fit in `u32`.

## 5. Leaf Page Encodings

All leaf pages use slotted-page mechanics (header + directory + payload heap).

Leaf encoding flags in page header:

- `0x8000`: leaf prefix compression enabled
- `0x4000`: leaf columnar encoding enabled
- `0x2000`: prefix v2 compact header enabled
- `0x1000`: packed `ValuePtr` payload enabled
- `0x0400`: columnar v2 layout enabled

Entry flags in payload (node flags) include inline/pointer/tombstone semantics.

### 5.1 Plain leaf entry

```text
u16 KeyLen
u32 ValueLen        // ignored for pointer entries
u8  EntryFlags
bytes Key
bytes Value | ValuePtr(16) | PackedValuePtr(12)
```

### 5.2 Prefix-compressed leaf v1

```text
u16 SharedPrefixLen
u16 SuffixLen
u32 ValueLen
u8  EntryFlags
bytes KeySuffix
bytes Value | Pointer
```

### 5.3 Prefix-compressed leaf v2

```text
u8 SharedPrefixLen8
u8 SuffixLen8
u8 EntryFlags
(optional) u16 SharedPrefixLen16 + u16 SuffixLen16 when both 8-bit lengths are 0xFF
(optional) uvarint ValueLen for inline non-tombstone values
bytes KeySuffix
bytes Value | Pointer
```

Notes:

- Pointer/tombstone entries omit inline value length field.
- Restart interval for prefix reconstruction: 16 entries.

### 5.4 Columnar leaf v2 (non-prefix)

```text
u16 KeyOff[Count]
u16 ValOff[Count]
u8  Flags[Count]
bytes ValueBlob
bytes KeyBlob
```

### 5.5 Columnar + prefix v2

```text
u16 KeyOff[Count]
u16 ValOff[Count]
u8  Flags[Count]
u16 PrefixLen[Count]
bytes ValueBlob
bytes KeySuffixBlob
```

Keys reconstruct using previous key prefix within restart blocks.

## 6. Internal Page Encodings

Internal encoding flags:

- `0x0800`: base-delta enabled
- `0x0200`: delta width `u16` (otherwise `u32`)
- `0x0100`: exact fence bounds persisted

### 6.1 Plain internal entry

```text
u16 KeyLen
u64 ChildPageID
bytes Key
```

### 6.2 Base-delta internal entry

```text
u16 SuffixLen
u16|u32 ChildDelta
bytes KeySuffix
```

With footer payload:

```text
bytes lowFence
bytes highFence
bytes sharedPrefix
u16 lowLen
u16 highLen
u16 prefixLen
u64 baseChildID
```

Child page id reconstructs as `baseChildID + ChildDelta`.

### 6.3 Leaf-Log Child Refs (IndexOuterLeavesInValueLog)

When `Options.IndexOuterLeavesInValueLog` is enabled, B+Tree leaf pages are
stored as persistent value-log records instead of pager pages in `index.db`.
Internal pages still live in `index.db`.

Internal pages that point at leaf-log records use an explicit child-ref entry
layout:

```text
u16 keyLen
u32 fileID
u64 offset
u32 recordLengthHint
u16 subIndex
bytes key
```

Notes:

- Roots remain normal pager page IDs; a single leaf-log leaf is represented by a
  one-child internal root page.
- `recordLengthHint` is best-effort. A zero hint means readers should consult
  the value-log record header.
- `subIndex` identifies the leaf page within a grouped value-log frame.
- Current builders keep leaf-log children on internal pages that contain only
  leaf-log child refs. Base-delta page-child encoding is disabled for these
  pages.

## 7. Value-Log Record Format

Each value-log record is:

```text
u32 CRC32
u8  Version         // currently 1
u8  Flags           // bit0 = grouped record
u16 Reserved
u64 RID             // 0 for grouped container record
u32 ValueLen
bytes Payload[ValueLen]
```

`Offset` in `ValuePtr` points to header byte after CRC (`Version`).

### 7.1 Grouped payload frame format

When grouped flag is set, payload starts with frame header:

```text
u8  FrameVersion    // currently 1
u8  FrameFlags      // bit0 = compressed
u8  K               // 1..128
u8  Reserved        // block codec id for compressed block frames with dictID=0
u64 DictID
u64 RID[K]
u32 Offsets[K+1]
bytes FramePayload
```

- `Offsets` are monotonic and define raw value slices.
- If `FrameFlags` indicates compression, frame payload is decoded first.
- `DictID` selects dictionary for dict-compressed payloads.

### 7.2 Compact split-leaf payload format

When TreeDB writes outer leaf pages into the split `leaf_vlog` directory, it
may store them in a compact canonical payload format instead of persisting the
entire raw `4096`-byte page image. This format is used only for split
`leaf_vlog` segments, not for generic lane-255 value-log files in `value_vlog`.

Compact payload layout:

```text
u8  Magic[8]        // 8a 4c 46 50 47 01 91 3c
u16 PrefixLenLE
u16 SuffixLenLE
bytes Prefix[PrefixLen]
bytes Suffix[SuffixLen]
```

Semantics:

- `Prefix` is the live byte range from the start of the page through the end of
  the top metadata/directory region.
- `Suffix` is the live byte range from the first heap byte through the end of
  the page.
- The bytes between them are the free gap of the slotted page and are omitted
  from the stored payload.

Reconstruction rules:

- decoder allocates a full `4096`-byte page buffer,
- copies `Prefix` at the start,
- copies `Suffix` at the end,
- zero-fills the omitted middle gap.

Encoding rules:

- only valid leaf pages may use this compact format,
- the canonical compact encoding zero-fills the omitted gap and recomputes the
  page checksum before storing `Prefix` and `Suffix`,
- if compact encoding is not smaller than the raw page, TreeDB stores the raw
  page payload instead.

### 7.3 Typed Asset Manager, TCPA Typed-Row Assets, and Typed-Column Parts

Production typed-storage physical data is stored in typed asset manager segments
under the compatibility `column_assets` directory. These assets are
value-log-shaped durable payloads, but they are not ordinary row `value_vlog`
values and they are not split-leaf `leaf_vlog` records. Manifest/control roots
can live in B-tree/root metadata; typed-row payloads, typed-column part payloads,
and derived accelerator payloads such as marks, dictionaries, locators, and
aggregate metadata belong under the isolated typed asset manager namespace.

A collection namespace such as `events/column-assets` maps to:

```text
Dir/maindb/column_assets/events/column-assets/
  assets/segments/segment-000001.tca
  assets/indexes/
  prepared/
  quarantine/
  tmp/
```

Segment file names use `segment-%06d.tca`. Durable manifest part records
store typed `ColumnAssetRef` values containing kind, namespace, generation,
part id, segment file id, offset, length, and checksum. Part records also carry
a `part_role` lifecycle value: `base`, `delta`, or `tombstone`. Part record
version 4 appends a SortKey trailer after `part_role`: `u64 column_count`, then
for each column a manifest string column name and manifest string direction
(currently empty/ascending only). Version 1/2/3 part records have no SortKey
trailer. Only `tcs1_typed_column_part` records may publish a non-empty SortKey;
readers and rewrite tooling must preserve or skip this trailer by version.
Current part refs may use `tcs1_part_image` for compatibility typed-row/TCPA
assets or `tcs1_typed_column_part` for sectioned typed-column payloads,
including scalar columns plus vector/list/adjacency payload sections described
below.
`base` parts are complete insert/base spans, `delta` parts carry update rows
layered over the older visible set, and `tombstone` parts are typed-row delete
assets with no matching typed-column payload. GC/rewrite must enumerate these
refs from manifest/control roots and snapshots; it must not scan row documents
to discover typed-storage assets.

Compatibility typed-row physical part payloads use the `TCPA` envelope:

```text
u32      Magic = "TCPA"
u16      Version
string   Collection
string   Namespace
u64      Generation
u64      PartID
u64      AppliedCommandLSN
string   Operation        // insert, update, or delete
u64      SchemaHash
u64      ColumnCount
u64      RowCount
columns  declared column descriptors
rows     versioned row payloads
```

Version 2 row payloads are:

```text
bytes    RowID
bool     Deleted
values   declared column values when Deleted=false
```

Insert/update rows must have `Deleted=false` and exactly one value per declared
`typed_row_asset` column in the row asset. Delete/tombstone rows must have
`Deleted=true` and zero column values. For layouts with `typed_column_part`
owners, a `TCPA` row asset is still published for row IDs/tombstones and any
row-owned fields; the matching `tcs1_typed_column_part` for the same non-delete
generation contains authoritative scalar, fixed-dimension `float32_vector`, and
non-null variable-width `uint32_list` typed-column values keyed by row index.
Latest-visible readers resolve document identity from the typed-row
row/tombstone assets first, then read the typed-column part for the winning
non-deleted generation+row. Readers validate namespace, generation, part id,
schema hash, declared column descriptors, length, role, operation, and checksum
before accepting an asset ref.

Typed-column part descriptor column type codes are currently:

| Code | Type string | Notes |
| ---: | --- | --- |
| 1 | `int64` | Signed integer scalar and default float bit-pattern carrier. |
| 2 | `low_cardinality_code` | String dictionary code carrier. |
| 3 | `bool` | Boolean bitpack/RLE carrier. |
| 4 | `float32_vector` | Fixed-dimension dense little-endian `float32` rows. |
| 5 | `adjacency_list` | Legacy/consumer-specific dense or explicit offsets-list adjacency compatibility. |
| 6 | `float32` | Native raw little-endian IEEE-754 scalar. |
| 7 | `float64` | Native raw little-endian IEEE-754 scalar. |
| 8 | `uint32_list` | Generic non-null offsets/value list primitive added by #1985. |

`uint32_list` descriptors must use `raw_uint32_offsets_list` encoding,
`fixed_width_elements=0`, and uncompressed split offsets/value sections. Readers
must fail closed on unknown type codes rather than guessing a payload shape.

Version 1 row payloads omitted the `Deleted` flag and represented only live
insert/update rows:

```text
bytes    RowID
values   declared column values
```

M12C and later decoders may read version 1 as `Deleted=false` for pre-v2 assets.
Writers emit version 2.

Dictionary-code derived sidecars referenced by
`ColumnAssetRef.Kind = tcs1_dictionary_codes` use asset magic `TCDC`. Version 2
keeps the manifest-style big-endian header, collection/namespace/generation,
schema, column identity, dictionary strings, cardinality, and row-count fields,
but the row-code payload is no longer a manifest `uint32` stream. Writers add
deterministic zero padding after the dictionary strings until the row payload is
4-byte aligned, then emit exactly `row_count * 4` bytes of little-endian `uint32`
local dictionary codes. Segment writers also prefix-pad dictionary-code assets so
`asset_ref.offset + payload_offset` is 4-byte aligned for mmap direct-view
consumers. Readers fail closed on non-zero payload padding, payload-length or
row-count mismatch, absolute misalignment, codes outside dictionary cardinality,
checksum mismatch when requested, or unsupported versions. Version 1
big-endian/manifest row-code payloads are intentionally rejected by current
pre-alpha readers; rebuild old DB directories instead of migrating in place.

Dense int64 value derived sidecars referenced by
`ColumnAssetRef.Kind = tcs1_int64_values` use asset magic `TCI8`. Version 2
keeps the manifest-style big-endian header, collection/namespace/generation,
schema, column identity, column index, and row-count fields, then adds
zero padding until the row-value payload is 8-byte aligned. Writers then emit
exactly `row_count * 8` bytes of little-endian two's-complement `int64` values.
Segment writers prefix-pad int64 value assets so `asset_ref.offset +
payload_offset` is 8-byte aligned for mmap direct-view consumers. Readers fail
closed on non-zero payload padding, payload-length or row-count mismatch,
absolute misalignment, checksum mismatch when requested, schema/ref/column
mismatch, or unsupported versions. Version 1 big-endian/manifest row-value
payloads are intentionally rejected by current pre-alpha readers; rebuild old DB
directories instead of migrating in place.

Sectioned typed-column part payloads are `TreeDB/internal/typedcolumn` part
images referenced by `ColumnAssetRef.Kind = tcs1_typed_column_part`. When a
collection SortKey is fully owned by `typed_column_part` and uses supported
ascending non-null bool/int64/string columns, the typed-column image descriptor
SortKey and the v4 manifest part SortKey trailer must match exactly. String
SortKey columns rely on part-local dictionary codes only when those codes are
assigned in logical bytewise-ascending order and the dictionary metadata certifies
that collation. Mixed-owner SortKeys fall back to the synthetic
`__treedb_primary_id` order and publish no typed-column SortKey trailer;
typed-column-owned unsupported, nullable, or descending SortKeys fail closed.
The durable Issue `#1755` scalar path represents bool, int64, float32,
double/float64, and
string fields. Int64 typed-column fields use `delta_varint` by default; a
non-null scalar `typed_column_part` field that explicitly sets
`fixed_width_encoding: "little_endian"` uses an uncompressed native raw
little-endian payload: `raw_int64` for `int64` (`rows * 8` bytes),
`raw_float32` for `float32` (`rows * 4` IEEE-754 bits), or `raw_float64` for
`double`/`float64` (`rows * 8` IEEE-754 bits). Native scalar float payloads
preserve raw bits exactly, including NaN payloads and signed zero. The legacy
raw-`int64` float bit-pattern carrier remains a compatibility/fallback layout
when native fixed-width encoding is not selected and must not be treated as a
native scalar float direct-view payload. Issue `#1756` adds fixed-dimension
`float32_vector` fields as uncompressed row-major little-endian dense `float32`
sections whose element count per row is `vector_dims`. Issue `#1783` adds
fixed-degree `adjacency_list` fields as uncompressed row-major little-endian
dense `uint32` sections whose element count per row is `adjacency_degree`;
that dense layout remains fallback/compatibility. Issue #1914 selected the #1901
variable-list compatibility path as an explicit `ColumnStoreValueAdjacencyList`
layout extension selected by `adjacency_layout: "uint32_offsets_list"` and the
internal encoding `raw_uint32_offsets_list`. Issue #1989 quarantines that
consumer-specific selector; the primary `uint32_list` path uses the reusable
physical mechanics:

```text
offsets []uint64  // row_count + 1, little-endian
values  []uint32  // flattened uint32 values, little-endian
```

The serialized image stores one canonical column-wide offsets section and one
column-wide values section per offsets-list column. For multi-block parts, block
payloads may use block-local offsets internally, but the image writer publishes a
single global `row_count + 1` offsets array by dropping duplicate block starts and
adding cumulative value bases; readers reconstruct block-local fallback payloads
from those global sections. The offsets-list mechanics validate exact offsets
count, `offsets[0] == 0`, monotonic offsets, final offset equal to the value
count, exact offsets/value byte lengths, Go `int` range before slicing,
little-endian identity, and separate section metadata/checksums for offsets
(8-byte elements) and values (4-byte elements). #1915 adds the safe writer and
fallback reader into owned Go slices; #1916 adds certified direct-view readers
for paired offsets/value handles, and #1917 wires that variable adjacency reader
through typed-column adapters. #1918 recorded durable `column_graph` layer-0
adjacency sources as `raw_uint32_offsets_list` typed-column assets during
physical graph rebuilds, and later graph-source work extended manifests to record
per-layer sources. Those `column_graph` source records are legacy compatibility,
not the target datastore primitive; current primary adjacency uses `uint32_list`
vector-index state. #1984 defines `uint32_list` semantics in
`typed-column-uint32-list-semantics.md`, #1985 adds the generic runtime
primitive implementation, and #1986/#1988 own vector-index state/search
consumption.

The `column_graph` manifest keeps the row graph asset ref as the canonical graph
asset. The legacy all-layer source metadata is an optional compatibility
manifest trailer with magic `TCGL` and version `1`: it records `layer_count`,
`source_count`, and then one `TCGA` v1 source record per layer in ascending layer
order. Each source
record binds the source schema/column name, value type/encoding, layer number,
source schema hash, row count, value count, offsets/value/padding byte
accounting, source `tcs1_typed_column_part` ref, base-manifest identity, graph
schema hash, and graph-asset identity. `source_count` must equal `layer_count`;
layer `i` must have `Layer=i`; layer 0 is also exposed through the legacy
optional layer-0 field for older readers. Empty rows and layers are represented
by equal adjacent offsets in the per-layer offsets array. Old graph manifests
without the trailer remain row-asset fallback readable. New graph builds leave
these `TCGA`/`TCGL` fields empty and publish typed-column `uint32_list`
vector-index state instead. Do not add new storage features to this
`TCGA`/`TCGL` compatibility path.

Issue #1986 adds a separate vector-index state control record under
`\x06vector-index-state/v1/index/<index_name>` with magic `TVIS` and version
`2` (`1` is still accepted for pre-alpha compatibility). The record stores
index identity, row count, base manifest identity, expected adjacency layer
count, and typed-column asset refs by logical type plus physical encoding. Its
asset roles include adjacency (`uint32_list` over
`raw_uint32_offsets_list`), inverse norms (`float32` over `raw_float32`),
optional normalized vectors (`float32_vector` over `raw_float32_vector`), and
future row/document refs. The active manifest checksum includes the control
record, but the record's base checksum excludes vector-index derived records so
stale-state checks compare against authoritative collection data. See
`vector-index-state-manifest.md` for validation and fail-closed rules.

As of the #1895 pre-alpha format update, newly written `typed_column_part` images
carry a writer-built `layout_contract` section. The contract may mark only raw
non-null uncompressed `raw_int64`, native `raw_float32`, native `raw_float64`,
fixed-dimension `raw_float32_vector`, and explicit `raw_uint32_offsets_list`
typed-column payload sections as `DirectViewCertified`; the adapter-internal
`__treedb_primary_id` row-locator column is not a declared-value direct-view
certification target. The contract records section/block offsets, lengths,
checksums, element size, endian, length multiple, row count, fixed elements per
row, and null/default exclusion. For `raw_uint32_offsets_list`, the contract
records global offsets/value section identity and leaves generic per-block
combined payload offsets empty because the two sections are discontiguous. Image
padding bytes are deterministic zero bytes
and are included in serialized-image byte accounting. When a typed-column-part
asset contains an active direct-view-certified candidate, the column asset segment
writer/appender also emits deterministic zero prefix padding as needed so the
absolute storage addresses (`asset_ref.offset + section/block payload offset`)
satisfy the declared alignment; this segment prefix padding is outside the asset
payload/checksum but is part of segment file size and appender offset accounting.
Old or manually constructed typed-column assets without a valid layout contract,
or refs whose absolute offsets are misaligned, fail closed in certified/prepared
paths. TreeDB is pre-alpha, so rebuilding old DB directories is preferred over
on-disk migration scaffolding for this format change.

Nullable scalar typed-column support uses nullable int64 carrier granules for
bool, int64, float32, double/float64, and low-cardinality string fields. A
nullable scalar column uses the `nullable_int64` encoding. Each granule payload
contains a fixed header, the encoded non-null/non-default carrier values, and
two row-aligned bitmaps:

- the null bitmap marks rows whose JSON path was present with an explicit
  `null`; these rows have no stored int64 payload value and reconstruct as
  explicit JSON null;
- the default/missing bitmap marks rows whose declared path was omitted from the
  source document; these rows have no stored int64 payload value and reconstruct
  by omitting that path from the retained-payload document; and
- rows with neither bit set are present/non-null and consume one encoded carrier
  payload value in row order (`0/1` bools, int64s, float bit patterns, or string
  dictionary codes).

Null and default/missing bits are mutually exclusive. Granule metadata stores
`NullCount` and `DefaultCount`; the two counts must be non-negative and must not
exceed the row count (`DefaultCount <= Rows-NullCount`). Decoders must fail
closed on invalid count metadata, truncated or incorrectly-sized bitmaps, rows
marked both null and default/missing, or stored-value underflow/overflow. Min/max
metadata, when present, covers only stored present/non-null carrier values; null
and default/missing rows contribute no value, and all-null/all-missing blocks
omit min/max. Future native nullable scalar encodings may reuse the same
explicit-null versus missing bitmap model only after their per-type payload
format is specified.

Nullable/missing typed-column codecs are allocation-budgeted hot paths and carry
a positive optimization expectation, not only a no-regression gate. When changing
encoding, decode, scan, or reconstruction merge loops, implementations should
actively remove existing avoidable allocations and obvious local overhead in the
same touched path when the cleanup is bounded, testable, and evidenced. These
loops must use compact bitmaps/default metadata plus caller-owned scratch and
must target 0 allocs/op after setup when benchmarking the core typed-column loop
separately from document materialization. Touched inner loops must be measurably
no worse, and preferably better, on `B/op` and `allocs/op`. Implementations must
not add per-row heap wrappers, maps, interface values, closures, or string/byte
conversions in these loops; if benchmarks or profiles expose allocations in
touched functions, the PR must fix them or explicitly list why they are out of
scope with a linked follow-up recommendation. Any remaining allocation requires
baseline-versus-final `B/op` and `allocs/op` evidence plus allocation profile/top
evidence before it is accepted or explicitly deferred. Checksum, lifetime,
schema, null/missing, and fail-closed validation must not be weakened to meet the
allocation budget.

Production `float32_vector`, `uint32_list`, and `adjacency_list`
nullable/missing support remains staged and fail-closed. Authoritative
`uint32_list` typed-column fields are non-null in v1 and reject adjacency-degree
or adjacency-layout selectors; empty lists are represented by equal adjacent
offsets. Authoritative dense `adjacency_list` typed-column fields must be
non-nullable, must declare positive `adjacency_degree`, and must fail closed when
any source row length, schema descriptor, or asset payload length disagrees with
that fixed degree. The adjacency offsets-list selector is also non-nullable, must
not declare `adjacency_degree`, and uses the #1915/#1916 concrete encoding for
safe publication/reopen/fallback reconstruction and adapter direct reads.

## 8. Commit-Log Segment Format

Commit-log file is a sequence of segments.

Segment envelope:

```text
u32 LengthField      // high bit = compressed flag, remaining bits = payload length
u32 CRC32(payload_stored)
bytes PayloadStored[Length]
```

If compressed flag is set:

```text
u32 RawLen
bytes ZstdCompressedRawPayload
```

Compression is only kept when it is a strict size win.

### 8.1 Legacy Pre-Command-WAL Raw Commit Batch Payload Format

This is the current pre-command-WAL raw payload format. It is not a compatibility
target for `command_wal_v1`. When command WAL lands, raw key/value writes are
encoded as `RawKVBatch` command frames and this payload may be removed from
normal open/recovery code.

```text
u8  Version          // currently 1
u32 RecordCount
Record[RecordCount]
```

Record format:

```text
u8  Op               // 0=set RID, 1=set inline, 2=delete
u16 KeyLen
u32 ValueLen
u64 RID
u64 Seq
bytes Key[KeyLen]
bytes Value[ValueLen]
```

Validation rules:

- `OpSetRID`: `RID != 0`, `ValueLen == 0`.
- `OpSetInline`: `RID == 0`.
- `OpDelete`: `RID == 0`, `ValueLen == 0`.

## 9. Command WAL Typed Frame Target

The active target for collection and catalog durability is the user-command WAL
defined in `user-command-wal.md`. It extends the existing commit-log segment
family instead of defining a new collection WAL file class. Typed command WAL
frames must live in `wal/commit-l<lane>-<seq>.log` as the only WAL payload
format once `command_wal_v1` is enabled.

The current raw commit-log record schema is superseded, not retained as a
compatibility payload. Raw KV writes become `RawKVBatch` command frames. New
collection and catalog commands must use typed command payloads inside the
shared commit-log frame stream; they must not be encoded as physical root deltas
and must not create `wal/collection-l*.log` files.

The commit-log physical segment header remains unchanged:

```text
u32 StoredLenAndFlags
u32 StoredCRC32
bytes StoredPayload[StoredLenAndFlags & lenMask]
```

`StoredPayload`, after optional existing zstd decompression, is a command frame:

```text
bytes[4] Magic              // "TCW1"
u16      Version            // 1
u16      MinReaderVersion   // reader must support at least this version
u16      CommandKind
u16      Scope
u64      FeatureFlags       // low 32 bits are critical in PR1
u64      LSN
u64      CatalogEpoch
u64      SchemaEpoch
u64      BaseAppliedLSN
u16      PayloadFormat
u16      ReservedZero
u32      PayloadLen
u32      ExternalRefsLen
u32      PreconditionsLen
u32      ResultAssertionsLen
bytes[32] PayloadSHA256
bytes Payload[PayloadLen]
bytes ExternalRefs[ExternalRefsLen]
bytes Preconditions[PreconditionsLen]
bytes ResultAssertions[ResultAssertionsLen]
```

Current command kinds:

| Value | Kind | Scope | Payload format | Status |
|---:|---|---|---|---|
| 1 | `RawKVBatch` | raw KV | `RawKVBatchV1` | typed raw key/value command batch |
| 100 | `CollectionInsertBatchByID` | collection | `CollectionInsertBatchByIDV1` | deterministic collection insert/upsert-by-id batch |
| 101 | `CollectionDeleteBatchByID` | collection | `CollectionDeleteBatchByIDV1` | deterministic collection delete-by-id batch |
| 102 | `CollectionUpdateBatchByID` | collection | `CollectionUpdateBatchByIDV1` | deterministic collection update/replace-by-id batch |
| 103 | `CollectionRebuildVectorIndex` | collection | `CollectionRebuildVectorIndexV1` | deterministic collection vector-index rebuild command |
| 200 | `CatalogCreateCollection` | catalog | `CatalogCreateCollectionV1` | deterministic catalog create-collection command; old placeholder name is an alias only |

Current payload format IDs:

| Value | Payload format |
|---:|---|
| 1 | `RawKVBatchV1` |
| 2 | `NativeWireDeterministic` |
| 3 | `CollectionInsertBatchByIDV1` |
| 4 | `CollectionDeleteBatchByIDV1` |
| 5 | `CollectionUpdateBatchByIDV1` |
| 6 | `CatalogCreateCollectionV1` |
| 7 | `CollectionRebuildVectorIndexV1` |

`RawKVBatchV1` payload:

```text
u16 Version        // 1
u32 OpCount
Op[OpCount]

Op:
u8  Op             // 1=set, 2=delete
u32 KeyLen
u32 ValueLen
bytes Key[KeyLen]
bytes Value[ValueLen]
```

A `RawKVBatch` command frame is one atomic command: one frame, one `LSN`, and
all contained operations decode as one batch. Delete operations require
`ValueLen=0`.

Writers may use compact all-zero set payload variants when every operation is a
set with the same non-empty zero-filled value length:

```text
u16 Version        // 2
u32 OpCount
u32 ValueLen
ZeroOp[OpCount]

ZeroOp:
u32 KeyLen
bytes Key[KeyLen]
```

Version 3 is the same compact zero-set payload with a narrower per-key length
field and is valid only when every key length fits in `u16`:

```text
u16 Version        // 3
u32 OpCount
u32 ValueLen
ZeroOp[OpCount]

ZeroOp:
u16 KeyLen
bytes Key[KeyLen]
```

Readers expand version 2 and version 3 entries to ordinary `RawKVBatch` set
operations with a zero-filled `Value[ValueLen]`; the command frame still carries
payload format `RawKVBatchV1`.

`CatalogCreateCollectionV1` payload:

```text
u16 Version        // 1
u32 CollectionNameLen
u32 MetadataLen
bytes CollectionName[CollectionNameLen]
bytes Metadata[MetadataLen] // canonical collection metadata JSON
```

The payload name and decoded metadata name must match. Replay is idempotent only
when an existing catalog entry has identical normalized metadata; incompatible
metadata fails closed before advancing `AppliedCommandLSN`.

Column-enabled collection metadata is stored inside the canonical collection
metadata JSON under `options.column_store`. It is production-facing
control-plane state, not a sidecar hint. Current normalized fields are:

- `enabled`: column storage is enabled for the collection.
- `columns`, `sort_key`, and `aggregate_metadata`: declared projection schema,
  analytical ordering, and aggregate metadata definitions.
- `retained_payload` and `reconstruction`: how non-column row bytes and column
  values reconstruct full documents. The current default retained-payload policy
  is `non-column`.
- `asset_manager`: the typed column asset manager. Current production metadata
  requires `kind="value-log"` and an isolated namespace.
- `manifest_root`: descriptor for the collection system root that owns the
  active column manifest identity record. The root name must be
  `<collection>/column/manifest`, and its storage policy must match
  `control_root_storage_policy`.
- `active_manifest`: published column manifest identity
  `{generation, format, version, checksum}`. Current format is `tcs1`, version
  `1`.
- `recovery_authoritative_manifest` and
  `recovery_authoritative_applied_command_lsn`: the manifest generation and
  command stream boundary considered safe for recovery. When `active_manifest`
  is present, the recovery-authoritative identity must also be present and must
  match it until a later format explicitly supports split active/recovery
  generations; the applied command LSN must also be present and non-zero.
- `profile_support`: current production default is `durable-only`.
  `benchmark-relaxed` is permitted only for explicit benchmark/experimental
  use under relaxed durability modes.
- `locator`: current default strategy is `side-index`.
- `schema_hash`: normalized hash of stable column schema/config fields used for
  cache identity invalidation. Manifest generation and recovery LSN are not
  schema-hash inputs.

Issue `#1753` added `TreeDB/internal/typedcolumn` as the transplanted
`experiments/colgranule` typed-column data plane. Issues `#1754`/`#1755` connect
it to production collection metadata for opt-in scalar `typed_column_part`
owners; issue `#1756` adds fixed-dimension `float32_vector` dense sections. The
transplant and adapter boundaries are documented in `typed-column-transplant.md`
and `typed-column-adapter.md`; closeout evidence and #1736 COW-maintenance
handoff facts are recorded in `typed-storage-closeout-1758.md`.

Readers must fail closed for a column-enabled collection when:

- active manifest metadata is missing required recovery-authoritative metadata,
- active and recovery-authoritative identities disagree,
- the manifest root descriptor does not match the collection system root name/policy,
- manifest identity format/version/checksum fields are invalid,
- the recovery-authoritative applied command LSN is zero while an active manifest
  is present, or
- a durable-only column collection is opened under a relaxed durability mode.

For typed-column parts specifically, readers and maintenance planners must reject
unsupported image versions, descriptor versions, manifest identity versions,
schema-hash drift, field-owner/value-type mismatches, `vector_dims` and
fixed-width layout mismatches, and kind/generation/part/checksum/range mismatches
from headers, descriptors, manifest identities, or refs whenever possible. They
must fail closed before full payload decode or per-row allocation when those
compact records already prove the format unsupported. Rebuild benchmark and
experiment directories rather than relying on implicit migrations during the
pre-alpha period; future migration tooling requirements are owned by
`typed-column-schema-evolution.md`.

`CollectionInsertBatchByIDV1` payload:

```text
u16 Version        // 1
u32 CollectionLen
u32 DocumentCount
bytes Collection[CollectionLen]
Document[DocumentCount]

Document:
u32 IDLen
u32 DocumentLen
bytes ID[IDLen]
bytes Document[DocumentLen]
```

`CollectionUpdateBatchByIDV1` uses the same canonical payload layout as
`CollectionInsertBatchByIDV1`; each document is the final accepted replacement
for the listed ID after user callbacks or declarative updates have resolved.

`CollectionDeleteBatchByIDV1` payload:

```text
u16 Version        // 1
u32 CollectionLen
u32 IDCount
bytes Collection[CollectionLen]
ID[IDCount]

ID:
u32 IDLen
bytes ID[IDLen]
```

Collection batch payloads require a non-empty collection name and non-empty
document IDs. Encoders canonicalize entries by strictly increasing document ID
before writing the payload, and decoders reject duplicate or out-of-order IDs.

`CollectionRebuildVectorIndexV1` payload:

```text
u16 Version        // 1
u32 CollectionLen
u32 IndexNameLen
bytes Collection[CollectionLen]
bytes IndexName[IndexNameLen]
```

The collection and index names must be non-empty. The command payload names the
logical rebuild request only; it does not carry vector graph bytes, physical root
deltas, or a vector-only sidecar file. Normal execution and replay re-enter the
collection vector-index rebuild path for the named index. For explicit
`column_graph` indexes, that path rebuilds vector, inverse-norm, and row-asset
adjacency data into physical column assets, publishes HNSW adjacency as
`uint32_list` vector-index state, and records vector-index control identity in
the `TVIS` state record. Old adjacency-source refs are #1989-quarantined
compatibility. Current graph manifests may still contain row graph refs and
legacy layer-source trailer refs for compatibility; new derived-state refs belong
in vector-index state. Replay outcomes that are
defined no-ops, such as a strategy/config drift status that no longer requires a
physical rebuild, must still publish a no-op command-WAL boundary and advance
`AppliedCommandLSN`. Corrupt payloads, unsupported payload versions, and
undefined replay outcomes fail closed before advancing `AppliedCommandLSN`.

`ExternalRefs`, `Preconditions`, and `ResultAssertions` are length-delimited
sections so PR1 can harden framing before replay uses them. The PR1 external-ref
section starts with `u32 Count`; each ref is:

```text
u16 Class          // 1=value-log, 2=leaf-log, 3=payload file
u16 Flags
u32 PathLen
u64 FileID
u64 Offset
u64 Length
bytes[32] Digest
bytes Path[PathLen]
```

Precondition and result-assertion sections each start with `u32 Count`; every
entry is:

```text
u16 Type
u16 ReservedZero
u32 PayloadLen
bytes Payload[PayloadLen]
```

Readers must fail closed on:

- old raw `commitlog.Record` payloads when reading as command WAL;
- unsupported required frame versions;
- unknown command kinds;
- unknown critical flags;
- malformed section lengths before allocating section-owned objects;
- payload digest mismatch;
- corrupt complete physical segment CRC;
- duplicate command `LSN` during segment scan.

Activation must begin from a clean WAL state or an explicit rebuild. The command
WAL implementation does not need to replay old raw batch segments in command WAL
directories.

`AppliedLSN` is the durable checkpoint proof for typed command frames. It is
metadata for the same commit-log sequence stream, not a collection-specific
applied marker. Recovery skips typed command frames with `LSN <= AppliedLSN`
and replays complete frames with higher LSNs through the deterministic command
executor before serving reads.

The V1 physical storage target for `AppliedLSN` is the in-page-marked meta-page
field named `AppliedCommandLSN`, encoded by the command-WAL meta extension in
Section 3.1 at body offset 68 / page offset 84. It must be selected atomically
with the roots that contain the corresponding command effects. PR1 may document
a blocking reason to revisit this before PR2 starts, but storage-format
implementation must not proceed with both meta-page and system-root storage as
live options. A sidecar cleanup file, manifest, stats record, format-config
marker, or post-commit maintenance record is not authoritative state for
recovery.

The deprecated collection root-delta WAL format (`collection_wal_v1`,
`wal/collection-l*.log`, `WALLSN`, `CollectionSeq`, and collection root-delta
frames) is not an active storage target. Its detailed design is preserved only
in `collection-wal-durability-plan.md` as historical analysis for external-ref,
crash-recovery, checkpoint, and fail-closed risks.

## 10. File Naming Conventions

Current canonical names:

- commit log: `wal/commit-l<lane>-<seq>.log`
- value log: `value_vlog/value-l<lane>-<seq>.log`
- split leaf log: `leaf_vlog/value-l<lane>-<seq>.log`

Recovery parser may accept historical value-log and commit-log file names before
`command_wal_v1` activation. Once command WAL is enabled, command frames use the
shared `commit-l<lane>-<seq>.log` segment family and old raw batch payloads are
unsupported.

## 11. Storage Compaction Lifecycle

`DB.CompactStorage` is the canonical online storage compaction entry point. It
does not introduce a new on-disk format; it coordinates the existing storage
objects above into one lifecycle:

1. establish a durable checkpoint boundary,
2. rewrite live value-log records into new `value_vlog` segments,
3. run reachability-based value-log GC,
4. pack live split outer-leaf pages into new `leaf_vlog` generations,
5. run leaf-generation GC,
6. vacuum/rewrite `index.db`,
7. run settle GC passes,
8. delete untracked zero-byte `value_vlog` segment files,
9. audit the remaining storage debt.

Applied compaction is serialized with other backend maintenance for the full
multi-phase sequence. Planning mode reports debt without mutating storage and is
safe for read-only opens.

In cached mode, public maintenance wrappers checkpoint first, protect cached
value-log paths, reserve rewrite RIDs from the live cached allocator, and
reconcile cached split value-log writers after backend maintenance so later
writes advance past backend-created `value_vlog`/`leaf_vlog` segments instead of
reusing segment file names.
