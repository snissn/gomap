# TreeDB Storage Format

This document defines TreeDB's durable on-disk formats and local frame formats.
The native client/server wire protocol is owned by
`TreeDB/docs/spec/native-wire-protocol.md`.

TreeDB is pre-alpha; format compatibility between versions is not guaranteed.
That disclaimer does not permit fail-open handling of acknowledged durable
writes. Once a directory advertises a required storage feature such as
`collection_wal_v1`, unsupported binaries must fail closed instead of serving,
cleaning, compacting, or rewriting the directory.

## 1. Top-Level Storage Objects

A TreeDB deployment uses:

- `index.db` (paged B+Tree index and metadata),
- commit-log segments under `wal/commit-l*.log`,
- collection WAL segments under `wal/collection-l*.log` once
  `collection_wal_v1` is enabled,
- value-log segments under `value_vlog/value-l*.log`,
- optional split outer-leaf value-log segments under `leaf_vlog/value-l*.log`
  when `IndexOuterLeavesInValueLog` is enabled,
- optional collection vector-index snapshot files under
  `vector_indexes/<collection>/<index>/`,
- optional side-store DBs (`dictdb`, `templatedb`) using their own `index.db` files.

Collection WAL is a target storage class, not yet a current committed on-disk
byte format. This document owns the reserved file class name
`wal/collection-l<lane>-<seq>.log` plus the target local frame format once the
M1 gate in `collection-wal-durability-plan.md` lands. Until then, the WAL plan
owns the target logical transaction semantics. When M1 lands, exact frame
bytes, checksums, commit markers, segment metadata, cleanup records, and golden
encodings must be maintained here and mapped to tests in
`TreeDB/docs/spec/verification.md`.

Collection WAL records use per-collection `CollectionSeq` dependency ordering,
global `WALLSN` append positions only for scan/cleanup accounting, side-ref
validation, and durable cleanup metadata before missing collection WAL segments
can be treated as safely cleaned. `WALLSN` is not a replay skip key; recovery
skips only by durable per-collection applied sequence watermarks.

The operator restorable file set, live backup barrier, and restore validation
procedure are defined in `TreeDB/docs/spec/backup-restore.md`. A live
filesystem-level copy without that barrier is unsupported once collection WAL
side refs can exist.

## 2. Index Page Basics

### 2.1 Fixed page size

- `PageSize = 4096` bytes.

### 2.2 Page header

All pages begin with a 16-byte header:

```text
u64 PageID
u32 Checksum   // CRC32C over page with checksum bytes zeroed
u16 Flags      // low bits: page type; high bits: encoding flags
u16 Count      // entry count
```

Page types (`Flags` low bits):

- `0x01`: meta page
- `0x02`: freelist page
- `0x03`: internal page
- `0x04`: leaf page

### 2.3 Checksum

- Checksum algorithm: CRC32C (Castagnoli).
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

## 3.1 Collection Document Payloads

Collection document payload encodings are defined separately in
`TreeDB/docs/spec/collections-document-formats.md`. In particular,
template-v1 collections store compact `TD1D` primary documents and persist the
template ID map in the collection-local `<collection>/templates` ordered root.

## 3.2 Collection Vector-Index Snapshot Files

Collection vector indexes are derived indexes. Canonical documents and
embeddings remain in collection primary storage; vector-index snapshot files are
persistent rebuild accelerators and search accelerators, not the source of
truth for collection rows. A missing, incomplete, corrupt, unsupported, or stale
vector-index snapshot MUST NOT make the collection unreadable. Callers loading
the index MUST fall back to exact collection search or rebuild the index.

Current vector-index snapshots are JSON files under:

```text
<dbDir>/vector_indexes/<collection>/<index>/
  manifest.json
  epoch-00000000000000000000/
    meta.json
    nodes.json
    edges.json
    tombstones.json
    docmap.json
```

`<collection>` and `<index>` are path components validated before use. They
must be non-empty, must not be `.` or `..`, and must not contain `/` or `\`.
Epoch directory names are `epoch-%020d`. Temporary save directories use the
`.tmp-epoch-...` prefix and must not be treated as published snapshots.

The current manifest format version is `1`. `manifest.json` contains:

| Field | Meaning |
|---|---|
| `format_version` | required vector snapshot manifest version, currently `1` |
| `collection` | collection name scope |
| `index_name` | vector index name scope |
| `epoch` | monotonic snapshot epoch; normally a nanosecond timestamp advanced past existing epochs |
| `epoch_dir` | relative epoch directory containing the payload files |
| `dims` | vector dimensionality, or zero for an empty index |
| `metric` | distance metric requested by the index |
| `encoding` | vector payload encoding, currently float32 or int8 quantized |
| `m`, `ef_construction`, `ef_search` | HNSW-style index parameters |
| `max_level` | maximum graph level stored in the snapshot |
| `node_count`, `live_doc_count`, `deleted_doc_count` | manifest counts checked against epoch payload files |
| `created_at_unix` | manifest creation time |
| `collection_commit_seq` | backend commit sequence captured at save time |
| `collection_system_root` | system-root page id captured at save time |
| `collection_primary_root` | collection primary-root id captured at save time |
| `files` | file manifest entries for the epoch payload files |

Each `files` entry contains `name`, `size`, and `sha256`. File names must be
plain file names with no path separators. Loaders MUST verify every listed
file's size and SHA-256 digest before decoding it.

Epoch payload files are JSON:

- `meta.json`: index name, field, metric, encoding, dimensions, HNSW
  parameters, rebuild policy, entry node, and max level.
- `nodes.json`: node records. Document IDs are lowercase hex-encoded
  collection primary keys so arbitrary binary IDs round-trip losslessly. Float32
  snapshots store `vector`; int8 snapshots store `quantized` plus
  `quant_scale`.
- `edges.json`: graph adjacency records keyed by node id and layer.
- `tombstones.json`: deleted node ids.
- `docmap.json`: map from hex document id to current live node id.

Loaders MUST validate manifest scope and index options before trusting the
epoch. Required checks include format version, collection name, index name,
metric, encoding, dimensions when the caller requested a fixed dimension,
positive HNSW parameters, non-negative counts, a safe epoch directory, and a
non-empty file list.

After ordinary TreeDB recovery has selected the current backend roots, a vector
snapshot load MUST refresh the collection root state and compare the manifest
freshness marker with the current collection marker. A manifest with missing
freshness fields (`collection_commit_seq == 0` or `collection_system_root ==
0`) is rejected with exact fallback. A manifest whose commit sequence, system
root, or primary root differs from the current marker is rejected as stale.

After payload decode, loaders MUST validate manifest counts against decoded
payloads, reject tombstones outside the node range, require each node's
`deleted` flag to agree with the tombstone file, validate vector dimensions and
finite/usable vector values, validate document-id hex, and reject edges that
reference invalid node ids or layers. Any validation failure returns a
non-loaded status with an exact-fallback reason instead of installing the
snapshot.

Snapshot publication is epoch-then-manifest:

1. write all payload files under a temporary epoch directory;
2. fsync each payload file and the temporary epoch directory;
3. rename the temporary epoch directory to its final `epoch-%020d` name;
4. fsync the vector index directory;
5. write and fsync `.manifest.tmp`;
6. rename `.manifest.tmp` to `manifest.json`;
7. fsync the vector index directory again.

The manifest points to the active epoch. Older epoch directories are retained
until `PruneOldSnapshots` removes them. Pruning MUST preserve the manifest's
active epoch and any requested newest retained epochs, must ignore temporary
directories, and must fsync the vector index directory after deleting epochs.

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
u32 CRC32C
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

## 8. Commit-Log Segment Format

Commit-log file is a sequence of segments.

Segment envelope:

```text
u32 LengthField      // high bit = compressed flag, remaining bits = payload length
u32 CRC32C(payload_stored)
bytes PayloadStored[Length]
```

If compressed flag is set:

```text
u32 RawLen
bytes ZstdCompressedRawPayload
```

Compression is only kept when it is a strict size win.

### 8.1 Commit batch payload format

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

## 9. Collection WAL v1 Segment and Frame Format

This is the target local frame format for the M1 collection WAL gate. It is not
current runtime behavior until `collection_wal_v1` is accepted and advertised.
Collection WAL is a separate file class from the key/value commit log. It must
not be decoded with the commit-log `Record` schema, and cached commit-log RID
skip behavior must not apply to complete collection WAL transactions.

All multi-byte integers in collection WAL are little-endian. Decoders must
reject nonzero reserved fields. Length caps are checked before allocation.
Unknown required versions, critical sections, critical side-ref classes, and
compression codecs fail closed before replay, cleanup, or serving.

### 9.1 Segment File Header

Every `collection-l<lane>-<seq>.log` file begins with:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic bytes `54 44 42 43 57 41 4c 01` (`TDBCWAL\x01`) |
| 8 | 2 | `SegmentHeaderLen`, currently `64` |
| 10 | 2 | `SegmentFormatVersion`, currently `1` |
| 12 | 2 | `MinReaderVersion`, currently `1` |
| 14 | 2 | `SegmentFlags` |
| 16 | 4 | `HeaderCRC32C` over bytes `[0,64)`, with this field zeroed |
| 20 | 4 | `FileClass`, `1 = collection_wal` |
| 24 | 4 | `Lane` |
| 28 | 8 | `SegmentSeq` |
| 36 | 8 | `FirstWALLSN`, or `0` while unsealed |
| 44 | 8 | `LastWALLSN`, or `0` while unsealed |
| 52 | 12 | reserved zero |

### 9.2 Transaction Frame

After the segment header, the file contains zero or more frames:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic bytes `54 44 42 43 57 54 58 01` (`TDBCWTX\x01`) |
| 8 | 2 | `FrameHeaderLen`, currently `96` |
| 10 | 2 | `FrameFormatVersion`, currently `1` |
| 12 | 2 | `MinReaderVersion` |
| 14 | 2 | `RecordType`: `1 = transaction`, `2 = segment_metadata`, `3 = cleanup_record` |
| 16 | 4 | `FrameFlags`: bit 0 compressed, bit 1 commit trailer required |
| 20 | 2 | `CompressionCodec`: `0 = none`, `1 = zstd` |
| 22 | 2 | `SectionTableVersion`, currently `1` |
| 24 | 8 | `WALLSN` |
| 32 | 16 | `CollectionUID`, zero only for non-transaction record types |
| 48 | 8 | `CollectionSeq`, zero only for non-transaction record types |
| 56 | 8 | `StoredPayloadLen` |
| 64 | 8 | `RawPayloadLen` |
| 72 | 4 | `HeaderCRC32C` over `[0,96)`, with this field zeroed |
| 76 | 4 | `StoredPayloadCRC32C` |
| 80 | 4 | `ReplayDigestCRC32C` for transaction records, zero otherwise |
| 84 | 4 | `RequiredFeatureBitsLow` |
| 88 | 8 | reserved zero |

Compression is not encoded by stealing bits from the length field. A compressed
frame records both `CompressionCodec` and `RawPayloadLen`. Unknown compression
codec on a required record fails closed. The frame header must be integrity
checked before payload allocation, and `StoredPayloadLen`/`RawPayloadLen` must
respect the fixed collection WAL transaction limits. In v1, both
`StoredPayloadLen` and the encoded `CollectionWALTransaction` are capped at
16,777,216 bytes. This cap is hard and non-disableable during recovery.

The frame is committed only if the header, stored payload, and commit trailer
are present and valid. The commit trailer is:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic bytes `54 44 42 43 57 43 4d 01` (`TDBCWCM\x01`) |
| 8 | 4 | `TrailerLen`, currently `16` |
| 12 | 4 | `WholeFrameCRC32C` over frame header, stored payload, and trailer bytes `[0,12)` with all CRC fields already populated |

A short read of the header, stored payload, or trailer is an incomplete tail only
when it occurs in the terminal active segment and no later non-cleaned
collection WAL segment exists. Otherwise it is hard corruption.

### 9.3 Transaction Payload Header

A v1 transaction payload begins with a fixed header followed by a section table.
The first root-delta byte is not before the fixed header and section table.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | `PayloadMagic` `54 44 42 43 57 50 31 01` (`TDBCWP1\x01`) |
| 8 | 2 | `TransactionVersion`, currently `1` |
| 10 | 2 | `FixedHeaderLen`, currently `288` |
| 12 | 4 | `TransactionFlags` |
| 16 | 16 | `CollectionUID` |
| 32 | 8 | `CollectionGeneration` |
| 40 | 8 | `CollectionSeq` |
| 48 | 8 | `DependsOnCollectionSeq` |
| 56 | 8 | `CatalogEpoch` |
| 64 | 8 | `SchemaEpoch` |
| 72 | 8 | `SchemaVersion` |
| 80 | 8 | `BaseCommitSeq` |
| 88 | 8 | `BaseSystemRootID` |
| 96 | 32 | `BaseCatalogDigest` |
| 128 | 32 | `CatalogDigest` |
| 160 | 32 | `LogicalCatalogDigest` |
| 192 | 32 | `LocalReplayCatalogDigest` |
| 224 | 4 | `MutationClass` |
| 228 | 4 | `RootDeltaCount` |
| 232 | 4 | `SideRefCount` |
| 236 | 4 | `DescriptorOpCount` |
| 240 | 4 | `SectionCount` |
| 244 | 4 | `FixedHeaderCRC32C` over `[0,288)`, with this field zeroed |
| 248 | 40 | reserved zero |

V1 readers must validate in this order: segment magic, segment header
length/version/min-reader, segment header CRC, frame magic, frame header
length/version/min-reader, length caps and overflow before allocation, frame
header CRC, payload CRC, commit trailer, transaction fixed-header CRC, section
CRCs, replay digest, and side-ref closure. No transaction-controlled string,
slice, map, side-ref, root-delta, or decompression allocation may occur before
the frame and transaction checksums pass. Unknown required versions, unknown
critical sections, unknown replay-critical fields, unknown required features,
unknown v1 `RefClass` values, and unknown compression codecs fail closed.

V1 bounds are:

| Field / structure | Bound |
|---|---:|
| Encoded transaction and outer frame payload | 16 MiB |
| Segment size | 64 MiB default, 1 GiB absolute max |
| Root deltas / mutated roots | 64 |
| Inline root-delta bytes per transaction | 4 MiB |
| Inline root-delta bytes per root | 1 MiB |
| Root-delta payload side ref | 64 MiB |
| Decoded root-delta entries per transaction | 262,144 |
| Delta key / document ID | 16 KiB |
| Inline delta value | 1 MiB |
| Side refs per transaction | 16,384 |
| Descriptor / system delta ops | 1,024 |
| `RelativePath` | 512 bytes, 16 components, 128 bytes per component |
| Compressed decoded bytes | 64 MiB |
| Recovery heap budget | 128 MiB per DB open worker |

The section table immediately follows the fixed header. Each section table entry
is:

| Size | Field |
|---:|---|
| 2 | `SectionType` |
| 2 | `SectionVersion` |
| 4 | `SectionFlags`: bit 0 critical, bit 1 replay-critical |
| 8 | `SectionOffset` from start of payload |
| 8 | `SectionLength` |
| 4 | `SectionCRC32C` |
| 4 | reserved zero |

Known v1 section types are:

- `1 = root_delta_table`
- `2 = side_ref_table`
- `3 = system_delta_template`
- `4 = descriptor_ops`
- `5 = stats`
- `6 = unknown_preserved`

Unknown critical sections make recovery fail closed. Unknown noncritical
sections may be skipped only when not replay-critical and not referenced by any
known section. Encoders and quarantine/metadata-rewrite tools must preserve
unknown sections byte-for-byte unless the format explicitly proves they are
discardable.

Root deltas are encoded only inside `root_delta_table`; the first byte of the
first root delta is the byte at `SectionOffset` for that section.

### 9.4 Root Delta and Side-Ref Section Constraints

Root-delta sections are deterministic byte sequences. A root-delta table must
carry stable `RootRef` identity before any root-local entry payload:
`CollectionUID`, `RootUID`, `RootKind`, optional `IndexUID`, optional
`ColumnDescriptorUID`, `BaseRootID`, `BaseRootGeneration`,
`BaseRootDescriptorEpoch`, `BaseRootDescriptorDigest`, storage policy,
`RootDeltaOrdinal`, key comparator identity, delta encoding version,
`IncludeDeletedOnColdBuild`, entry count, encoded byte length, and delta digest.
`RootName` may be present only as diagnostic text. Entries must encode
operation kind, `EntryOrdinal`, key length, value length, and either inline
value bytes, stable `ValuePtr` bytes, or a `RootDeltaPayload` side-ref index.
Tombstones are first-class operations.

Side refs encode `RefClass`, `ClassVersion`, `Critical`, `FileID`, `Offset`,
`Length`, `ChecksumKind`, `Checksum`, and optional advisory `RelativePath`.
Known v1 `RefClass` values are `1=ValueLogRecord`, `2=LeafLogRecord`,
`3=RootDeltaPayload`, `4=ColumnManifest`, `5=ColumnSubstreamFile`,
`6=ColumnFilterFile`, `7=ColumnDeleteBitmapFile`,
`8=ColumnDictionaryFile`, and `9=ColumnMetadataFile`; `0` is invalid. Any
unknown `RefClass` in a complete v1 transaction is fatal. Future optional refs
may be ignored only if no replay-critical section references them, canonical
embedded side-ref derivation proves they are not root-reachable, and the
transaction feature bits explicitly allow skipping them. Cleanup, GC, rewrite,
and quarantine must treat unknown refs from complete unwatermarked transactions
as protected until the transaction is watermarked and checkpointed or an
operator performs a destructive rebuild.

`RelativePath` is advisory validation data only. It must use `/` separators and
reject NUL, empty path, `.`, `..`, repeated separators, absolute POSIX paths,
Windows drive paths, UNC paths, backslashes, components longer than 128 bytes,
more than 16 components, and total bytes over 512. Cleanup and quarantine must
resolve deletion targets only through `(RefClass, RefID/FileID)` and the trusted
file-class registry, never through WAL-provided path strings.

### 9.5 Required Storage Features and Compatibility Gates

Collection WAL is a required storage feature once any WAL-on collection write
can be acknowledged before its root group is checkpointed.

A directory with collection WAL enabled must contain all of:

1. `format.json` with an incompatible `Version` that older binaries reject and
   `required_features` containing `collection_wal_v1`;
2. a recovered system-root feature key
   `treedb/storage-format/required-features/collection_wal_v1 = true`;
3. collection WAL segment headers carrying `FileClass = collection_wal` and
   `MinReaderVersion <= reader_version`.

`format.json` is the early-open/downgrade gate before pager recovery. The
system root is the authoritative recovered feature state. WAL headers are the
decoder gate. The current meta page is not a feature gate unless a future
meta-page format explicitly adds feature bits.

A binary that does not support every required feature must fail before serving,
checkpointing, compacting, vacuuming, cleaning WAL, or rewriting side files.
`IgnoreFormatConfig` and similar runtime overrides must not bypass required
on-disk features; destructive rebuild tooling must be explicit.

Downgrade from a directory with `collection_wal_v1` is unsupported and must be
detected. Missing or inconsistent gates are recovery errors, except during an
explicit migration state defined below.

### 9.6 Collection WAL Migration States

The system root and `format.json` must agree on one of these states:

| State | Allowed files | Open behavior |
|---|---|---|
| `NoCollectionWAL` | no `collection-l*.log`; no collection WAL cleanup metadata | old behavior |
| `CollectionWALSupportedButDisabled` | no committed collection WAL required for recovery | open allowed; writes may choose WAL-off behavior |
| `CollectionWALRequiredPreparing` | feature gate durable; admission closed for upgrade; no acknowledged WAL-on collection writes until finalized | only upgrading binary may open read-write |
| `CollectionWALRequired` | committed collection WAL may exist and must be recovered | unsupported binaries fail; supported binaries replay or fail closed |
| `CollectionWALRecoveryRequired` | complete unapplied collection WAL exists | read-write open must recover; read-only open must fail unless an explicit replay overlay exists |
| `CollectionWALCleanupPending` | segments may be deleted only according to durable cleanup records | cleanup resumes idempotently |

Activation from `NoCollectionWAL` to `CollectionWALRequired` must:

1. close collection write admission;
2. checkpoint existing roots and current commit WAL;
3. durably write the new `format.json` gate and fsync its directory;
4. durably commit the system-root feature state;
5. fsync the database directory containing the new markers;
6. continue only with a binary that supports collection WAL;
7. acknowledge WAL-on collection writes only after the final state is durable.

Crash during activation must reopen into either the old state, a preparing state
that cannot acknowledge WAL-on collection writes, or the final required state.
Any other mixture is a recovery error until an admin repair or destructive
rebuild path is invoked.

### 9.7 Collection Descriptor Record

Every persisted collection descriptor must include:

| Field | Meaning |
|---|---|
| `CollectionUID uuid128` | durable identity; never derived from name |
| `CollectionGeneration uint64` | increments on drop/recreate; rename preserves UID |
| `CatalogEpoch uint64` | increments on catalog metadata changes such as rename |
| `SchemaEpoch uint64` | increments on mutation-planning, schema, index, template, or column descriptor changes |
| `SchemaVersion uint64` | logical schema version if distinct from epoch |
| `LogicalCatalogDigest bytes32` | deterministic digest of logical catalog descriptor for native/Raft guards |
| `LocalReplayCatalogDigest bytes32` | digest of local replay descriptor; may include physical root IDs |
| `RootDescriptors []RootDescriptorV1` | stable root descriptors keyed by `RootUID`, not root name |
| `IndexDescriptors []IndexDescriptorV1` | stable index descriptors keyed by `IndexUID`, not index name |
| `CollectionName` | diagnostic only; not replay identity |
| `DroppedAtEpoch optional uint64` | tombstone for deleted collections |
| `DropCollectionSeq optional uint64` | sequence/log position for drop tombstone |
| `HighestAppliedSeqAtDrop optional uint64` | applied sequence known at drop time |

Recovery must reject an unapplied collection WAL transaction when the recovered
descriptor for its `CollectionUID` is absent, tombstoned, or has mismatching
generation, catalog epoch, schema epoch, catalog digest, root descriptor epoch,
root descriptor digest, index UID/digest, or column descriptor generation,
unless an explicit migration record maps the old descriptor to a new one.

`RootDescriptorV1` fields:

| Field | Meaning |
|---|---|
| `RootUID uuid128` | stable root identity |
| `OwnerCollectionUID uuid128` | owning collection |
| `RootKind enum` | primary, template, index-state, secondary, delete, locator, filter, column descriptor, etc. |
| `LogicalName string` | diagnostic/API display only |
| `IndexUID uuid128 optional` | secondary/index-owned root owner |
| `ColumnDescriptorUID uuid128 optional` | column-owned root owner |
| `RootGeneration uint64` | increments when the root descriptor is replaced |
| `RootDescriptorEpoch uint64` | monotonic descriptor version |
| `RootID uint64` | local physical root id |
| `StoragePolicy uint8` | root storage policy |
| `DescriptorDigest bytes32` | digest over canonical descriptor bytes |

`IndexDescriptorV1` fields:

| Field | Meaning |
|---|---|
| `IndexUID uuid128` | stable index identity |
| `OwnerCollectionUID uuid128` | owning collection |
| `Name string` | API/display name |
| `IndexGeneration uint64` | increments on same-name recreate |
| `CreatedAtSchemaEpoch uint64` | schema epoch at creation |
| `DroppedAtSchemaEpoch optional uint64` | tombstone epoch |
| `DefinitionDigest bytes32` | digest over name, field path, type, uniqueness, multikey, storage policy, collation/normalization, and owning schema epoch |
| `FieldPath canonical` | canonical field path |
| `ValueType enum` | indexed value type |
| `Unique bool` | uniqueness flag |
| `MultiKey bool` | multikey flag |
| `StoragePolicy uint8` | secondary root storage policy |
| `SecondaryRootUID uuid128` | root identity for the secondary index |

Dropping an index tombstones the `IndexUID`. Recreating the same display name
creates a new `IndexUID`, generation, root UID, and definition digest.

### 9.8 Durable Manifest Writes and Cleanup Metadata

Feature gates, segment metadata, cleanup records, and migration-state manifests
must be written with a crash-durable manifest protocol:

1. write a temp file in the target directory;
2. fsync the temp file;
3. rename to the final path;
4. fsync the parent directory;
5. for segment deletion, unlink the segment and fsync the WAL directory;
6. record cleanup state transitions so crashes during cleanup are idempotent.

The generic atomic-rename helper is not sufficient unless it performs the fsync
steps above. Startup accepts a missing collection WAL segment only when durable
cleanup metadata proves the whole segment was safely cleaned.

### 9.9 Collection WAL Decoder Outcomes

A collection WAL reader must report one of these outcomes for each frame or
segment boundary:

| Outcome | Meaning |
|---|---|
| `CompleteValid` | complete frame, supported required version, checksums valid |
| `CompleteCorrupt` | complete frame with bad checksum, digest, impossible structure, unknown required field, or side-ref closure failure |
| `TerminalIncompleteTail` | incomplete frame in terminal active segment with no later non-cleaned segment |
| `NonTerminalShortRead` | short read before a later frame, in a sealed segment, or before a later non-cleaned segment |
| `UnsupportedVersion` | complete frame or segment requires a newer reader |
| `UnsupportedSkippableRecord` | record is explicitly noncritical and skippable |
| `MissingSegment` | segment is absent and no durable cleanup metadata covers every transaction it contained |
| `DuplicateWALLSN` | duplicate global append location appears in complete frames |
| `DuplicateCollectionSeq` | duplicate `(CollectionUID, CollectionSeq)` appears in complete frames |
| `MaliciousLength` | length exceeds cap or overflows before allocation |
| `MixedVersionSegment` | segment mixes versions without an explicit migration reader |

Only `TerminalIncompleteTail` in the active terminal segment may be ignored.
`CompleteCorrupt`, `NonTerminalShortRead`, `UnsupportedVersion`,
`MissingSegment`, `DuplicateWALLSN`, `DuplicateCollectionSeq`, and
`MaliciousLength` fail database open or quarantine the affected collection
before roots can be published. Missing transaction `N` blocks `N+1` for the
same `CollectionUID`.

### 9.10 File Classes and Layout

Collection WAL files are recognized only by the exact name pattern
`collection-l<lane>-<seq>.log` and only in the main DB WAL directory.

Root layout:

- main DB: `<root>/maindb`
- collection WAL: `<root>/maindb/wal/collection-l<lane>-<seq>.log`
- commit WAL: `<root>/maindb/wal/commit-l<lane>-<seq>.log`
- value log: `<root>/maindb/value_vlog/value-l<lane>-<seq>.log`
- leaf log: `<root>/maindb/leaf_vlog/value-l<lane>-<seq>.log`
- collection vector-index snapshots:
  `<root>/maindb/vector_indexes/<collection>/<index>/manifest.json` plus
  active epoch directories
- side stores: `<root>/dictdb`, `<root>/templatedb`

Flat layout:

- main DB: `<dir>`
- collection WAL: `<dir>/wal/collection-l<lane>-<seq>.log`
- collection vector-index snapshots:
  `<dir>/vector_indexes/<collection>/<index>/manifest.json` plus active epoch
  directories
- side stores disabled unless explicitly migrated to root layout

`dictdb` and `templatedb` must not contain collection WAL files. Legacy `wal-*`
and `vlog-*` names are never collection WAL. Unknown `.log` files in a WAL
directory are unsupported future files and must not be deleted or ignored when a
required feature gate is present.

Collection WAL and collection side-file classes require safe local-file
resolution. Open must fail before collection WAL recovery if the DB root,
`maindb`, WAL directory, value-log directory, leaf-log directory, or collection
side-file class root is a symlink, not owned by the effective DB user or
configured DB owner, group-writable, world-writable, or not a directory.
Collection WAL writers/readers/cleanup must use directory-fd based no-follow
resolution where available, create new mutable files with exclusive create,
`fstat` every opened file, require regular files under the expected class root,
and require `nlink == 1` for new mutable WAL/side files. Cleanup, quarantine,
and rewrite must unlink or rename only registry-resolved file names under the
class-root fd; WAL-provided `RelativePath` can only validate, never select, a
target.

### 9.11 Allocator Recovery

The next `WALLSN` must be recovered from durable segment metadata plus every
complete frame. The next `CollectionSeq` for a collection must be recovered from
the collection descriptor, applied watermark, and every complete pending WAL
transaction for that `CollectionUID`. Duplicate `WALLSN` or duplicate
`(CollectionUID, CollectionSeq)` is fatal unless covered by an explicit legacy
quarantine path. Gaps in `CollectionSeq` block later transactions for the same
collection.

## 10. File Naming Conventions

Current canonical names:

- commit log: `wal/commit-l<lane>-<seq>.log`
- collection WAL: `wal/collection-l<lane>-<seq>.log`
- value log: `value_vlog/value-l<lane>-<seq>.log`
- split leaf log: `leaf_vlog/value-l<lane>-<seq>.log`

Recovery parser also accepts legacy names (`commit-`, `value-`, `wal-`, `vlog-`) for backward compatibility during pre-alpha evolution.
Legacy names are never collection WAL.

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
