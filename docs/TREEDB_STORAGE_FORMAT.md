# TreeDB Storage Format (Index + Value Log)

This document is the canonical reference for TreeDB’s on-disk layout and the
**persistent value log** design (no slabs).

TreeDB is **pre-alpha**: on-disk formats may change without backward-compatibility guarantees.
It is acceptable for new binaries to fail to open old DB directories (and vice versa).

Operator-facing behavior is covered by:
- `docs/contracts/DURABILITY.md`
- `docs/TREEDB_WRITE_PATHS.md`
- `docs/TREEDB_RECOVERY.md`

## TL;DR

- `Options.Dir` is a *root* directory containing:
  - `Dir/maindb/`: main database (index + journal + value log)
  - `Dir/dictdb/`: dictionary store (for value-log compression)
- Large values can be stored out-of-line in `Dir/maindb/wal/` and referenced by `page.ValuePtr` pointers stored in the B+Tree.
- The value log is **persistent storage**: pointers are valid long-term; segments are deleted only when unreachable (GC) or after rewrite/compaction.

## Directory layout

TreeDB creates/manages two sub-databases under the root directory:

### Main DB (`Dir/maindb/`)

- `Dir/maindb/index.db`: memory-mapped pager file containing B+Tree pages + metadata.
- `Dir/maindb/wal/`: journal (redo) + value-log segments.
  - journal segments: `commit-l<lane>-<seq>.log`
  - value-log segments: `value-l<lane>-<seq>.log`
- `Dir/maindb/LOCK`: cross-process exclusive-open lock for the main DB.

### Dictionary store (`Dir/dictdb/`)

- `Dir/dictdb/index.db`: dictionary metadata store (internal).
- `Dir/dictdb/LOCK`: exclusive-open lock for the dictionary store.

The dictionary store is used to persist trained dictionary bytes so value-log
compressed frames can be decoded after restart.

## Value pointers (`page.ValuePtr`)

TreeDB leaf entries either store an inline value or a pointer (`node.FlagPointer`)
to an out-of-line value-log record.

`page.ValuePtr` is a fixed-size pointer stored in the index:

- `FileID`: identifies the value-log segment file (see `page.IsValueLogFileID`).
- `Offset`: byte offset **to the record header immediately after the CRC** (i.e. record start + 4).
- `Length`: the record length **excluding** the CRC (and may contain pointer flags for grouped frames).

Grouped records (frames with `k>1`) embed a sub-record index inside `Length`
(see `page.ValuePtrMarkGrouped` / `page.ValuePtrIsGrouped`).

### Leaf entry encoding (index leaf pages)

TreeDB stores B+Tree pages in `Dir/maindb/index.db` using a fixed 4096-byte
slotted-page layout:
- a 16-byte header (`page.PageHeader`) at the start,
- a directory of `Count` uint16 offsets growing up from byte 16,
- entry payload bytes growing down from the end of the page.

For `PageTypeLeaf`, the page-header `Flags` field stores the leaf encoding mode
in high bits (in addition to the low-bit page type).

Leaf-encoding flags (current):
- `0x8000`: leaf prefix-compressed
- `0x4000`: leaf columnar
- `0x2000`: leaf prefix v2 (only valid when prefix-compressed)
- `0x1000`: leaf packed ValuePtr (pointer payload uses 12B packed encoding)

`leaf columnar` and `leaf prefix-compressed` can be combined. When both flags
are set, leaf entries use a combined columnar+prefix encoding (see below).

#### Plain leaf entries (no prefix compression, non-columnar)

```text
[ u16 KeyLen ]
[ u32 ValueLen ]  ignored for pointer entries
[ u8  Flags ]
[ Key bytes (KeyLen) ]
[ Inline value bytes (ValueLen) | ValuePtr (16 bytes) | PackedValuePtr (12 bytes) ]
```

#### Prefix-compressed leaf entries (v1)

When prefix compression is enabled and `leaf prefix v2` is **not** set:

```text
[ u16 SharedPrefixLen ]
[ u16 SuffixLen ]
[ u32 ValueLen ]  ignored for pointer entries
[ u8  Flags ]
[ Key suffix bytes (SuffixLen) ]
[ Inline value bytes (ValueLen) | ValuePtr (16 bytes) | PackedValuePtr (12 bytes) ]
```

Keys are reconstructed within restart blocks: every Nth entry is a restart
(`SharedPrefixLen=0`), and non-restart keys copy `SharedPrefixLen` bytes from
the previous key, then append the suffix bytes.

#### Prefix-compressed leaf entries (v2 compact header)

When prefix compression is enabled and `leaf prefix v2` is set:

```text
[ u8 SharedPrefixLen8 ]
[ u8 SuffixLen8 ]
[ u8 Flags ]
[ optional: u16 SharedPrefixLen16 | u16 SuffixLen16 ]  if both 8-bit lengths are 0xFF
[ optional: uvarint ValueLen ]  only for inline, non-tombstone entries
[ Key suffix bytes (SuffixLen) ]
[ Inline value bytes (ValueLen) | ValuePtr (16 bytes) | PackedValuePtr (12 bytes) ]
```

Notes:
- For pointer/tombstone entries, `ValueLen` is omitted to reduce leaf payload.
- Tombstone entries store no value bytes.
- Restart points follow the same fixed interval as v1 (see `TreeDB/node` for the
  current restart interval).

#### Columnar leaf entries (non-prefix)

When `leaf columnar` is enabled and `leaf prefix-compressed` is **not** set:

```text
[ u16 KeyLen ]
[ u32 ValueLen ]  ignored for pointer entries
[ u8  Flags ]
[ u16 KeyOff ]  offset from entry start to key bytes
[ u16 ValOff ]  offset from entry start to value bytes / pointer bytes
[ Key bytes (KeyLen) ]  at entryStart+KeyOff
[ Inline value bytes (ValueLen) | ValuePtr (16 bytes) | PackedValuePtr (12 bytes) ]  at entryStart+ValOff
```

Notes:
- Key/value bytes may be ordered arbitrarily within the entry; offsets are the
  source of truth.
- Current TreeDB encoder writes value bytes **before** key bytes to improve
  cache locality for key-only seeks (search touches the key region without
  pulling inline values into cache).

#### Columnar + prefix-compressed leaf entries (v2)

When both `leaf columnar` and `leaf prefix-compressed` are set (and `leaf prefix v2` is set):

```text
[ u8 SharedPrefixLen8 ]
[ u8 SuffixLen8 ]
[ u8 Flags ]
[ optional: u16 SharedPrefixLen16 | u16 SuffixLen16 ]  if both 8-bit lengths are 0xFF
[ optional: uvarint ValueLen ]  only for inline, non-tombstone entries
[ u16 KeyOff ]  offset from entry start to key suffix bytes
[ u16 ValOff ]  offset from entry start to value bytes / pointer bytes
[ Key suffix bytes (SuffixLen) ]  at entryStart+KeyOff
[ Inline value bytes (ValueLen) | ValuePtr (16 bytes) | PackedValuePtr (12 bytes) ]  at entryStart+ValOff
```

Notes:
- Keys reconstruct the same way as prefix v2: within restart blocks, each entry
  copies `SharedPrefixLen` bytes from the previous full key, then appends the
  suffix bytes.
- Offsets allow value bytes to be placed away from key bytes; current TreeDB
  encoder writes value bytes **before** key suffix bytes.
- When `leaf packed ValuePtr` is set, pointer entries use the packed encoding:
  `Offset32 (u32 LE) | Length (u32 LE) | FileID (u32 LE)`. This requires
  value-log segment offsets stay within `u32` (cached mode enforces this when
  `Options.IndexPackedValuePtr` is enabled).

### Internal entry encoding (index internal pages)

Internal pages use the same 4096-byte slotted-page layout as leaves (header +
directory offsets + heap payload).

For `PageTypeInternal`, the page-header `Flags` field stores the internal
encoding mode in high bits (in addition to the low-bit page type).

Internal-encoding flags (current):
- `0x0800`: internal base-delta (separator keys prefix-coded; child IDs stored as base + delta)
- `0x0400`: internal base-delta `delta16` (when set, `ChildDelta` is encoded as `u16` instead of `u32`)

#### Plain internal entries (no base-delta)

```text
[ u16 KeyLen ]
[ u64 ChildPageID ]
[ Key bytes (KeyLen) ]
```

#### Base-delta internal entries (with prefix coding)

Each entry stores a key **suffix** and a child-ID **delta**:

```text
[ u16 SuffixLen ]
[ u16 ChildDelta ]   if `delta16` flag is set
[ u32 ChildDelta ]   otherwise
[ Key suffix bytes (SuffixLen) ]
```

The page stores a footer at the end containing the shared key prefix and the
base child ID:

```text
[ prefix bytes (prefixLen) ]
[ u16 prefixLen ]
[ u64 baseChildID ]
```

The full separator key for an entry is `prefix || suffix`. `prefixLen` may be
`0` (no prefix bytes stored).

## Value-log record format (`TreeDB/internal/valuelog`)

Value-log segments are append-only. Each record is:

```text
[ u32 CRC32C ]  little-endian; CRC32C(Castagnoli) of (header_without_crc || payload)
[ u8  Version ]
[ u8  Flags   ]  bit0: grouped record
[ u16 Reserved ]
[ u64 RID     ]  record id (0 for grouped records)
[ u32 ValueLen]  payload byte length
[ payload bytes (ValueLen) ]
```

Payload interpretation:

- **Non-grouped record** (`Flags & recordFlagGrouped == 0`)
  - payload is the raw value bytes
  - `RID` must be non-zero
- **Grouped record** (`Flags & recordFlagGrouped != 0`)
  - payload is a *frame* containing up to `k` logical records (each with its own RID)
  - record header `RID` is `0`

### Frame format (grouped payload)

The grouped payload begins with a frame header, followed by:
- `k` record IDs (`uint64`)
- `k+1` offsets (`uint32`) into the **decoded** payload
- the frame payload bytes (raw concatenation or zstd-compressed)

See:
- `TreeDB/internal/valuelog/valuelog.go` (`EncodeFrame` / `DecodeFrame`)
- `docs/TREEDB_VALUELOG_AUTOTUNE.md` (operator-facing compression/autotune behavior)

## Lifecycle: persistence, GC, rewrite

### Persistence (key invariant)

The value log is **not** an ephemeral write-ahead log:
- pointers stored in the index are expected to remain valid across restarts,
- old segments are not truncated “because they’re old”.

### GC: delete fully-unreferenced segments

TreeDB can delete value-log segments that are completely unreachable:

- API: `(*treedb.DB).ValueLogGC(ctx, treedb.ValueLogGCOptions)`
- Implementation: `TreeDB/db/vlog_gc.go`

Behavior:
- scans both the user tree and system tree for value-log pointers,
- computes the set of referenced segments,
- deletes segments that are:
  - not referenced,
  - not the currently-active segment per lane,
  - not pinned by active snapshots.

In cached mode, `ValueLogGC` checkpoints first to ensure memtables/journal state
is reflected in the backend before pointer scanning.

### Rewrite: compact partially-dead segments (offline)

GC only removes segments that are *fully* unreferenced. To reclaim space from
segments that contain a mix of live and dead records, TreeDB provides an offline
rewrite:

- API: `treedb.ValueLogRewriteOffline(treedb.Options{Dir: ...})`
- Implementation: `TreeDB/db/vlog_rewrite.go`

This:
- rewrites live referenced values into new value-log segments,
- rewrites the user+system indexes to reference the new pointers,
- swaps `Dir/maindb/index.db` to the new index via a crash-safe protocol,
- removes old value-log segments once the swap is complete.

Constraints:
- offline operation (DB must be closed; it acquires `Dir/maindb/LOCK`)
- requires a clean commitlog (no pending journal segments)

## Read integrity controls

Value-log records are checksummed (CRC32C). Reads can be configured via:

- `Options.ValueLog.ReadIntegrity` (see `docs/TREEDB_WRITE_PATHS.md` for migration mapping)

Skipping checksums trades integrity for throughput and should be treated as an
explicit “unsafe mode”.
