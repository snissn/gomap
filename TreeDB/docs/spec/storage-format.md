# TreeDB Storage Format

This document defines TreeDB's durable on-disk formats and local frame formats.
The native client/server wire protocol is owned by
`TreeDB/docs/spec/native-wire-protocol.md`.

TreeDB is pre-alpha; format compatibility between versions is not guaranteed.
That disclaimer does not permit fail-open handling of acknowledged durable
writes. Once a directory advertises a required storage feature such as
`command_wal_v1`, unsupported binaries must fail closed instead of serving,
cleaning, compacting, or rewriting the directory.

## 1. Top-Level Storage Objects

A TreeDB deployment uses:

- `index.db` (paged B+Tree index and metadata),
- commit-log segments under `wal/commit-l*.log`; future user-command WAL
  frames extend this same segment family rather than creating a second WAL file
  class,
- value-log segments under `value_vlog/value-l*.log`,
- optional split outer-leaf value-log segments under `leaf_vlog/value-l*.log`
  when `IndexOuterLeavesInValueLog` is enabled,
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

Canonical collection metadata JSON is the normalized `collectionMetaDisk`
payload:

```json
{
  "version": 3,
  "name": "collection",
  "options": {},
  "indexes": [],
  "vector_indexes": []
}
```

Version 3 adds first-class `vector_indexes` entries to the metadata envelope.
Each vector index definition stores `name`, `field`, `metric`, `dimensions`,
optional HNSW search/build settings (`m`, `ef_construction`, `ef_search`),
`encoding`, and `schema_generation`. Decoders must reject metadata whose
`version` differs from the implementation's current collection metadata
version instead of silently accepting older layouts with missing vector-index
state.

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

Readers must fail closed for a column-enabled collection when active manifest
metadata is missing required recovery-authoritative metadata, when active and
recovery-authoritative identities disagree, when the manifest root descriptor
does not match the collection system root name/policy, when manifest identity
format/version/checksum fields are invalid, when the recovery-authoritative
applied command LSN is zero while an active manifest is present, or when a
durable-only column collection is opened under a relaxed durability mode.

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
