# TreeDB Storage Format (Index + Value Log)

This is a supporting storage-format explainer.

For the canonical TreeDB spec, see:
- `TreeDB/docs/spec/README.md`
- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/docs/spec/value-log-lifecycle.md`

TreeDB is **pre-alpha**: on-disk formats may change without backward-compatibility guarantees.
It is acceptable for new binaries to fail to open old DB directories (and vice versa).

Operator-facing behavior is covered by:
- `docs/contracts/DURABILITY.md`
- `docs/TREEDB_WRITE_PATHS.md`
- `docs/TREEDB_RECOVERY.md`

## TL;DR

- `Options.Dir` is a *root* directory containing:
  - `Dir/maindb/`: main database (`index.db`, `wal`, `value_vlog`, optional `leaf_vlog`, and `column_assets`)
  - `Dir/dictdb/`: dictionary store (for value-log compression)
- Large values can be stored out-of-line in `Dir/maindb/value_vlog/` and referenced by `page.ValuePtr` pointers stored in the B+Tree.
- The value log is **persistent storage**: pointers are valid long-term; segments are deleted only when unreachable (GC) or after rewrite/compaction.
- Typed-storage physical assets are **value-log-shaped typed assets**, not generic row `value_vlog` payloads. Production typed-row assets and opt-in scalar/vector typed-column parts live under the isolated typed asset manager namespace.
- Typed-storage closeout evidence, naming-audit classification, and the #1736 COW-maintenance handoff are recorded in `TreeDB/docs/spec/typed-storage-closeout-1758.md`.

## Directory layout

TreeDB creates/manages two sub-databases under the root directory:

### Main DB (`Dir/maindb/`)

- `Dir/maindb/index.db`: memory-mapped pager file containing B+Tree pages + metadata.
- `Dir/maindb/wal/`: redo journal segments named `commit-l<lane>-<seq>.log`.
- `Dir/maindb/value_vlog/`: persistent large-value segments named `value-l<lane>-<seq>.log`.
- `Dir/maindb/leaf_vlog/`: optional persistent outer-leaf generation segments named `value-l<lane>-<seq>.log`.
- `Dir/maindb/column_assets/`: compatibility directory name for the isolated typed asset manager root used by typed-storage physical assets.
- `Dir/maindb/LOCK`: cross-process exclusive-open lock for the main DB.

### Dictionary store (`Dir/dictdb/`)

- `Dir/dictdb/index.db`: dictionary metadata store (internal).
- `Dir/dictdb/LOCK`: exclusive-open lock for the dictionary store.

The dictionary store is used to persist trained dictionary bytes so value-log
compressed frames can be decoded after restart.

### Typed-storage assets (`column_assets/`)

Typed-storage assets use an isolated value-log-shaped manager instead of ordinary
row value-log entries. The `column_assets` directory name is compatibility
metadata. A collection namespace such as `events/column-assets` maps to:

```text
Dir/maindb/column_assets/events/column-assets/
  assets/segments/segment-000001.tca
  assets/indexes/
  prepared/
  quarantine/
  tmp/
```

Column manifest records stored in B-tree/root metadata hold durable
`ColumnAssetRef` values: kind, namespace, generation, part id, segment file id,
offset, length, and checksum. `tcs1_part_image` refs identify compatibility
`TCPA` typed-row assets; `tcs1_typed_column_part` refs identify sectioned
scalar and fixed-dimension vector typed-column parts. `TCMP` manifest part
records version 3 also persist multipart roles: `base`, `delta`, and
`tombstone`. Readers fail closed on malformed key/ref/checksum/role/operation
combinations; typed-column part refs must pair with the same generation's
row-locator `TCPA` asset and row count, while delete/tombstone generations have
only the row-locator/tombstone part. GC/rewrite must enumerate those refs from
manifest/control roots, not by scanning row documents.

The typed-row physical part payload uses the versioned `TCPA` envelope:

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
rows     row id + deleted flag + optional declared column values
```

Generic row payload versions store each row as length-prefixed row id bytes,
deleted flag, and optional declared values. Insert and update rows must have
`deleted=false` and one declared value per `typed_row_asset` column in the row
asset. Each declared value stores its type, null bit, and present bit; the
present bit distinguishes an omitted nullable JSON path from an explicit JSON
`null` so retained-payload reconstruction remains lossless. Delete/tombstone
rows must have `deleted=true` and no column values.

Version 7 may be used when the row asset has zero row-owned columns and all row
ids in the part have the same byte width. After the column descriptors it stores
a fixed-id row encoding marker, the id width, then contiguous row id bytes.
The deleted/tombstone state is derived from the asset operation, so insert/update
parts are all live rows and delete parts are all tombstones.

For layouts with `typed_column_part` owners, a `TCPA` row asset is still
published for row IDs/tombstones and row-owned fields; the matching
`tcs1_typed_column_part` for the same generation contains authoritative scalar
and fixed-dimension `float32_vector` typed-column values keyed by row index. The
current typed-column publication matrix is bool, int64, float32, double/float64,
string, #1929 non-null primitive scalars (int8/uint8/int16/uint16/int32/uint32/
uint64 plus storage-only float16/bfloat16 raw bits), and fixed-dimension float32
vectors; unsupported nullable primitive scalars and production adjacency sections
fail closed until later typed-storage issues.
Readers validate namespace, generation, part id, schema hash, column
descriptors, length, and checksum before accepting an asset ref.

## Value pointers (`page.ValuePtr`)

TreeDB leaf entries either store an inline value or a pointer (`node.FlagPointer`)
to an out-of-line value-log record.

`page.ValuePtr` is a fixed-size pointer stored in the index:

- `FileID`: identifies the value-log segment file (see `page.IsValueLogFileID`).
- `Offset`: byte offset **to the record header immediately after the CRC** (i.e. record start + 4).
- `Length`: a packed field that contains pointer flags and an optional record-length hint.

Grouped records (frames with `k>1`) embed a sub-record index inside `Length`
(see `page.ValuePtrMarkGrouped` / `page.ValuePtrIsGrouped`).

### `ValuePtr.Length` bit layout (grouped pointers)

When the grouped flag is set, the `Length` field packs:

- **Grouped flag**: bit 30 (`0x4000_0000`)
- **Sub-index** (encoded range 0–255) within the grouped value-log record:
  - bits 29..26: sub-index bits 3..0
  - bit 31: sub-index bit 4
  - bits 25..24: sub-index bits 6..5
  - bit 23: sub-index bit 7
- **Optional record-length hint**: low 23 bits (bits 22..0), storing the record length
  excluding CRC (`HeaderSize-4 + ValueLen`) when it fits in 23 bits.

Notes:
- The record-length hint is **best-effort**: if the record is larger than the
  representable range, TreeDB stores `0` and readers rely on the value-log record
  header’s `ValueLen` instead.
- Compression is encoded in the value-log record header (frame flags), not in
  `ValuePtr.Length`.

### Outer-leaf blocks (`TOL2`)

TreeDB may store *outer-leaf blocks* in the value log. These blocks bundle one
or more sorted key entries into one value-log record payload and support
optional compression (snappy/lz4) plus prefix-compressed keys with restart
points.

Each block uses the `TOL2` envelope:

```text
bytes[4] Magic            // "TOL2"
u8       Version          // 1=single KV, 2=multi-KV, 3=typed entries
u8       Codec            // 0=raw, 1=snappy, 2=lz4
u16      RestartInterval  // restart interval for prefix-compressed keys
u16/u32  Version-specific metadata
u32      Checksum         // CRC-32/IEEE(header-with-zero-checksum || encoded-payload)
bytes    EncodedPayload
```

- Version 1 stores one `{key,value}` pair.
- Version 2 stores multiple sorted `{key,value}` pairs with prefix-compressed
  keys plus a restart-offset table/trailer.
- Version 3 stores multiple sorted `{key,kind,value}` entries where `kind`
  selects either:
  - inline bytes, or
  - a nested `page.ValuePtr` blob reference for very large values.

Lookup resolves `{pointer,key}` by decoding one block and finding the requested
key inside that block (binary search with a small linear-scan fallback).

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
- `0x0400`: leaf columnar v2 (dense key/value columns; see below)

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

#### Columnar leaf entries (non-prefix, v2)

When `leaf columnar` is enabled, `leaf prefix-compressed` is **not** set, and
`leaf columnar v2` is set:

```text
Directory (Count * u16): KeyOff[i]   offset from page start to key i
ValOff column (Count * u16): ValOff[i]  offset from page start to value/pointer i
Flags column (Count * u8): Flags[i]

Value blob: concatenated values/pointers for entries in key order
Key blob: concatenated keys for entries in key order
```

Notes:
- Key/value bytes are laid out as **separate blobs** so key-only seeks/searches
  touch only the key column + key blob.
- Key/value lengths are derived from adjacent offsets:
  - `KeyLen[i] = KeyOff[i+1]-KeyOff[i]` (last key ends at page end)
  - `ValLen[i] = ValOff[i+1]-ValOff[i]` (last value ends at `KeyOff[0]`)
- Pointer entries store `ValuePtr` (16B) or `PackedValuePtr` (12B).

#### Columnar + prefix-compressed leaf entries (v2)

When both `leaf columnar` and `leaf prefix-compressed` are set (and `leaf prefix v2` is set):

```text
KeyOff column (Count * u16): KeyOff[i]      offset from page start to key suffix i
ValOff column (Count * u16): ValOff[i]      offset from page start to value/pointer i
Flags column (Count * u8): Flags[i]
PrefixLen column (Count * u16): PrefixLen[i]

Value blob: concatenated values/pointers for entries in key order
Key-suffix blob: concatenated key suffixes for entries in key order
```

Notes:
- Keys reconstruct with restart blocks (fixed interval): each entry copies
  `PrefixLen[i]` bytes from the previous full key, then appends suffix bytes.
- Restart entries are enforced with `PrefixLen=0`.
- Offsets derive lengths from adjacent rows:
  - `KeySuffixLen[i] = KeyOff[i+1]-KeyOff[i]` (last suffix ends at page end)
  - `ValLen[i] = ValOff[i+1]-ValOff[i]` (last value ends at `KeyOff[0]`)
- Values and key suffixes are physically separated so key-only search/seek paths
  avoid touching value payload bytes.
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
- `0x0800`: internal base-delta enabled
- `0x0200`: internal base-delta uses `u16` child deltas (otherwise `u32`)
- `0x0100`: exact subtree fence bounds persisted (`low`/`high`)

#### Plain internal entries (no base-delta)

```text
[ u16 KeyLen ]
[ u64 ChildPageID ]
[ Key bytes (KeyLen) ]
```

#### Base-delta internal entries (with prefix coding)

Each entry stores a key **suffix** and a child-ID **delta** (`u16` or `u32`,
selected per page):

```text
[ u16 SuffixLen ]
[ ChildDelta ]       childID = baseChildID + ChildDelta
[ Key suffix bytes (SuffixLen) ]
```

The page stores a footer payload at the end containing optional exact subtree
fence bounds plus the shared key prefix, followed by a fixed tail:

```text
[ low fence bytes (lowLen) ]
[ high fence bytes (highLen) ]
[ prefix bytes (prefixLen) ]
[ u16 lowLen ]
[ u16 highLen ]
[ u16 prefixLen ]
[ u64 baseChildID ]
```

The full separator key for an entry is `prefix || suffix`. `prefixLen` may be
`0` (no prefix bytes stored). Fence semantics are `low` inclusive and `high`
exclusive; an empty `high` means unbounded (e.g. root upper bound).

## Value-log record format (`TreeDB/internal/valuelog`)

Value-log segments are append-only. Each record is:

```text
[ u32 CRC32 ]  little-endian; CRC-32/IEEE of (header_without_crc || payload)
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

### Recommended full compaction

For a fully compacted storage footprint, use:

```sh
treemap compact <db-dir> -rw
```

or:

```go
stats, err := db.CompactStorage(ctx, treedb.CompactStorageOptions{
    Mode: treedb.CompactStorageFull,
})
```

This is the user-facing contract. It coordinates value-log rewrite, value-log
GC, leaf-generation packing, leaf-generation GC, index vacuum, and zero-byte
`value_vlog` cleanup. If online index vacuum is unsupported on the current
platform, that phase is explicitly reported as skipped and the other storage
domains are still compacted. The lower-level operations below are advanced
internals for debugging and maintenance schedulers.

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

### Rewrite: compact partially-dead value segments

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

Value-log records are checksummed (CRC-32/IEEE). Reads can be configured via:

- `Options.ValueLog.ReadIntegrity` (see `docs/TREEDB_WRITE_PATHS.md` for migration mapping)

Skipping checksums trades integrity for throughput and should be treated as an
explicit “unsafe mode”.
