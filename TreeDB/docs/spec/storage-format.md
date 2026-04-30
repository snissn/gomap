# TreeDB Storage Format

This document defines the current on-disk and wire formats used by TreeDB.

TreeDB is pre-alpha; format compatibility between versions is not guaranteed.

## 1. Top-Level Storage Objects

A TreeDB deployment uses:

- `index.db` (paged B+Tree index and metadata),
- value-log segments (`value-l*.log`),
- optional split outer-leaf value-log segments under `leaf_vlog/value-l*.log`
  when `IndexOuterLeavesInValueLog` is enabled,
- commit-log segments (`commit-l*.log`),
- optional side-store DBs (`dictdb`, `templatedb`) using their own `index.db` files.

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

## 9. File Naming Conventions

Current canonical names:

- commit log: `commit-l<lane>-<seq>.log`
- value log: `value-l<lane>-<seq>.log`
- split leaf log: `leaf_vlog/value-l<lane>-<seq>.log`

Recovery parser also accepts legacy names (`commit-`, `value-`, `wal-`, `vlog-`) for backward compatibility during pre-alpha evolution.
