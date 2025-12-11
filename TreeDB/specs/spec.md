# TreeDB Design Specification v2.4 (Updated with RFCs)

## 1\. System Overview

**TreeDB** is an embedded, persistent key-value store tailored for the Cosmos SDK workload (IAVL+ Tree). It prioritizes high write throughput, efficient range scans, and low memory overhead during block commits.

### Key Architectural Features

  * **Hybrid Storage:** Values are stored in an append-only log (**Slabs**). Keys are stored in a Memory-Mapped B+Tree (**Index**).
  * **Safety:** Uses Copy-On-Write (COW) for snapshot isolation, redundant superblocks for crash recovery, and CRC32C checksums for data integrity.
  * **Concurrency:** Supports Single-Writer / Multi-Reader (SWMR) with lock-free reads via MVCC (Multi-Version Concurrency Control) semantics using Epochs and Reference Counting.

## 2\. File Architecture

The database resides in a single directory containing two types of files.

### 2.1 The Value Log ("Slabs")

  * **Purpose:** Stores large data payloads (Contract code, large metadata).
  * **Filenames:** `data-0000.slab`, `data-0001.slab`, etc.
  * **Format:** Append-only record stream (Section 2.1.1).
  * **Rotation:** When the active file exceeds `4GB`, it is closed (becoming read-only) and a new file is created.

#### 2.1.1 Slab Record Format (Compaction-Readable)

To allow compaction to iterate slabs without scanning the B+Tree, large values are written as self-describing records:

```text
[ Slab Record ]
+-------------------------------+
| CRC32C       (4 bytes)        |  CRC32C (Castagnoli) of (KeyLen..ValueBytes)
| KeyLen       (2 bytes)        |  uint16, max 64KiB
| ValueLen     (4 bytes)        |  uint32
| KeyBytes     (KeyLen bytes)   |
| ValueBytes   (ValueLen bytes) |
+-------------------------------+
```

Rules:

  * **Checksum:** Uses **CRC32C (Castagnoli)** for hardware acceleration (SSE4.2/ARMv8).
  * Only values stored out-of-line (i.e., `len(value) > InlineThreshold`) are appended as slab records.
  * The `ValuePtr.Offset` points to the beginning of `KeyLen` (i.e., immediately after CRC32C), so the compactor can parse forward easily and readers can bounds-check safely.
  * On read, the record CRC32C is verified before returning data.

### 2.2 The Index File ("The Pager")

  * **Purpose:** Stores the B+Tree structure, the Freelist, and redundant Superblocks (Meta).

  * **Filename:** `index.db`

  * **Access Pattern:** **Chunked Memory Map**.

      * The file is logically a contiguous array of Pages.
      * Physically, the Go runtime maps it in **Configurable Chunks** (default: 256MB; max: 1GB).
      * **Growth:** To expand, we `Truncate` the file and `Mmap` *only* the new chunk. Old chunks remain valid to prevent segfaults in active readers.
      * **Safety Protocol:**
          * `Truncate` operations are strictly limited to file expansion. Shrinking the file while mapped is forbidden to prevent `SIGBUS` panics.
          * `Unmap` operations must verify zero active references before releasing a chunk.

#### 2.2.1 Durability Ordering for Mmap Writes (Required)

Because `index.db` is memory-mapped, TreeDB defines an explicit persistence boundary:

  * **Non-sync writes (`Set`, `Batch.Write`)**: may return after OS page cache has been dirtied; durability is not guaranteed until a later fsync.

  * **Sync writes (`SetSync`, `Batch.WriteSync`)**: return only after:

    1.  **Strict Ordering:** The active slab data must be durable (`fdatasync`/`fsync` on active slab) **BEFORE** any meta page updates are written.
    2.  The committed superblock is durable (`msync` dirty index pages as needed + `fdatasync`/`fsync` on `index.db`).

Implementation note: the exact combination of `msync` and `fdatasync` is OS-dependent; the contract is that `*Sync` means the new root is recoverable after a power loss.

## 3\. Data Structures

### 3.1 Global Constants

  * **Page Size:** `4096 bytes`
  * **Inline Threshold:** `256 bytes` (Default. Values `<=` this size are stored directly in the tree).
    *Note:* This is configurable; deployments may choose lower values for blob-heavy workloads.
  * **Max Key Size:** Variable (handled via Slotted Pages), typically \< 1KB.

### 3.2 The Value Pointer

Used to point to data stored in the Slabs.

**On-disk encoding is fixed-width 16 bytes** (do not rely on Go struct packing):

```text
ValuePtr (16 bytes)
+-------------------------+
| FileID   (2 bytes)      |
| Reserved (2 bytes)      |  // Must be zero; reserved for future use
| Offset   (8 bytes)      |
| Length   (4 bytes)      |
+-------------------------+
```

Corresponding Go type (in-memory):

```go
type ValuePtr struct {
    FileID    uint16
    _reserved uint16
    Offset    uint64
    Length    uint32
} // 16 bytes in serialized form
```

### 3.3 Page Layout

The `index.db` file is an array of 4KB pages.

#### Common Page Header (16 Bytes)

Every page starts with this header.

```text
[  Page Header  ]
+--------------------------+
| Checksum (4 bytes)       | <--- CRC32C (Castagnoli) of the page body.
| PageID   (8 bytes)       | <--- Self-referential ID for sanity checking.
| Flags    (2 bytes)       | <--- Node Type (Meta/Free/Int/Leaf).
| Count    (2 bytes)       | <--- Number of items in this page.
+--------------------------+
```

**Implementation Note:** To maximize throughput, deserialization of headers and records from memory-mapped slices should use `unsafe.Pointer` casting rather than `binary.Read`, ensuring zero-copy parsing.

#### Page Types

  * **0x01 Meta:** Superblocks (two pages: Page 0 and Page 1).
  * **0x02 Freelist:** Stores IDs of dead pages available for reuse.
  * **0x03 Internal:** B+Tree Branch nodes.
  * **0x04 Leaf:** B+Tree Data nodes.

#### The Meta Pages (Page 0 and Page 1)

TreeDB uses **redundant superblocks** to survive torn writes during commit.

Each Meta Page stores:

  * `CommitSeq` (8b): Monotonic commit counter.
  * `RootPageID` (8b): Pointer to the current B+Tree root.
  * `FreelistHeadID` (8b): Pointer to the head of the On-Disk Freelist.
  * `TotalPages` (8b): Total file size in pages.
  * `LastCommitHeight` (8b): Optional: the application’s height/sequence (may equal `CommitSeq`).
  * `SlabStats` (Map): Tracks per-slab statistics (optional; see Section 3.4 and 5.4).

**Recovery Rule (Open):**

  * Read both meta pages, verify CRC32C, choose the page with the **highest valid `CommitSeq`**.

#### The Freelist Page (Type 0x02)

Used to recycle space within `index.db`.

  * **Header:** Standard Header.
  * **NextPageID (8b):** Pointer to the next Freelist page (forming a linked list).
  * **Body:** Array of `PageID (uint64)`.

### 3.4 The Slab Manager

The `SlabManager` controls access to physical `.slab` files. It mediates between the Reader (needing stability) and the Compactor (needing cleanup).

```go
// SlabFile represents a single physical file on disk.
type SlabFile struct {
    FileID   uint16
    Handle   *os.File
    Mmap     []byte        // The read-only memory map

    // Safety Primitives
    RefCount atomic.Int64 // Number of active Snapshots referencing this file
    IsZombie atomic.Bool  // True if the file has been removed from the active set

    // Compaction Stats
    DeadBytes atomic.Uint64
    TotalBytes uint64 // Immutable once sealed; tracked for trigger decisions
}

// SlabSet is an immutable list of SlabFiles active at a specific point in time.
type SlabSet struct {
    Files map[uint16]*SlabFile
}
```

**Publication Rule (RCU / Atomic Swap):**

  * The current `SlabSet` is stored behind an atomic pointer.
  * Snapshot acquisition reads the pointer once (acquire semantics) and pins referenced slabs by incrementing their `RefCount`.

## 4\. Node Layouts (Slotted Pages)

We use **Slotted Pages** to support variable-length keys while maintaining binary-search speed.

### 4.1 Internal Node Layout

  * **Directory (Top):** Array of `Offsets (uint16)` growing downward.
  * **Heap (Bottom):** Data growing upward.
  * **Entry Format:** `[Child PageID (8b)] [KeyBytes]`
  * **Search:** Binary search the Directory to find the Child pointer.

### 4.2 Leaf Node Layout

Leaf nodes store the actual key-value data. To support $O(1)$ bidirectional iteration (Forward and Reverse) required by Cosmos SDK, leaf nodes form a **Doubly Linked List**.

  * **Header (32 Bytes):**

      * Standard Header (16 bytes)
      * `NextLeafID` (8 bytes): Pointer to the right sibling. `0` if last.
      * `PrevLeafID` (8 bytes): Pointer to the left sibling. `0` if first.

  * **Directory (Top):** Array of `Offsets (uint16)`.

  * **Heap (Bottom):** Stores keys and values (or pointers).

**Visual Layout:**

```text
[  Standard Header (16b)  ]
[   NextLeafID (8b)       ]
[   PrevLeafID (8b)       ]
[  ... Directory ...      ]
          ...
[  ... Heap Data ...      ]
```

**Leaf Update Logic (Copy-On-Write):**
When a Leaf Node is modified or split:

1.  **The Target Leaf** is duplicated/allocated (New PageID).

2.  **The Previous Leaf's** `NextLeafID` must be updated to point to the New PageID.

3.  **The Next Leaf's** `PrevLeafID` must be updated to point to the New PageID.

      * *Note:* In a standard COW B+Tree, updating neighbors can cause write amplification. TreeDB’s **Batch Zipper** reconstructs contiguous ranges of leaves in memory, so link stitching is only required at the **left-most** and **right-most** boundaries of the rewritten range.

## 5\. Core Algorithms

### 5.1 Batch Write (The "Zipper" Merge)

Writes are batched to ensure atomicity and reduce I/O.

**Phase 1: Buffer & Pre-Write (Memory Pressure Fix)**

1.  Iterate over `Put(Key, Val)` operations.

2.  **If `len(Val) > InlineThreshold`:**

      * **Immediately append** a slab record (Section 2.1.1) to the active `.slab` file.
      * Calculate `ValuePtr`.
      * Store `(Key -> ValuePtr)` in the in-memory Batch Map.
      * *Note:* If the batch fails, these bytes in the slab are wasted but harmless.

3.  **If `len(Val) <= InlineThreshold`:**

      * Store `(Key -> Val)` literals in the in-memory Batch Map.

**Phase 2: Recursive Merge**

1.  Sort the Batch Keys.

2.  Recursively descend the B+Tree (COW).

3.  **Leaf Node:**

      * Allocate `NewPage`.
      * Merge `OldLeaf` items + `Batch` items into `NewPage`.
      * Perform splitting if `NewPage` \> 4KB.

4.  **Internal Node:** Update child pointers to point to new pages.

**Phase 3: Atomic Commit (Redundant Superblock)**

1.  We now have a `NewRootPageID`.

2.  **If Sync=true:** `fdatasync` the active slab file **MUST complete** before any meta page update.

3.  Ensure index pages backing the new root are persisted as required by the durability contract (Section 2.2.1).

4.  **Write the inactive Meta Page** (Page 0 or Page 1):

      * `CommitSeq = OldCommitSeq + 1`
      * `RootPageID = NewRootPageID`
      * `FreelistHeadID = CurrentFreelistHead`
      * `TotalPages = CurrentTotalPages`

5.  **If Sync=true:** persist the meta update (Section 2.2.1).

6.  **Retire Old Pages:** Add all replaced `OldPageIDs` to the **Graveyard** (See 5.2).

### 5.2 Page & Slab Lifecycle (The Graveyard & Snapshots)

To support concurrent readers without locks, we cannot immediately overwrite old pages or delete old slab files.

#### 5.2.1 Snapshot Acquisition

To ensure safe reading of both Pages and Slabs, every Read operation (including `Get` and `Iterator`) must first acquire a **Snapshot**.

1.  **Capture State:** Read the current `SlabSet` pointer (atomic load).

2.  **Increment Refs:**

      * Iterate through all `*SlabFile` pointers in the captured set.
      * Atomically increment `SlabFile.RefCount` for each.

3.  **Release State:** When `Snapshot.Close()` or `Iterator.Close()` is called:

      * Decrement `RefCount` for each slab in the snapshot.
      * If `RefCount == 0` AND `IsZombie == true`, physically close and remove the file from disk.

#### 5.2.2 The Graveyard (Pages)

  * **In-Memory Graveyard:** `map[CommitSeq][]PageID`. When a commit occurs at `CommitSeq N`, all replaced pages are added here.

  * **The Reader Hold:** Readers (Snapshots and Iterators) pin a `CommitSeq` while traversing pages. A pinned CommitSeq is a **hard safety boundary** for reclamation decisions.

  * **Snapshot Safety Invariant (MANDATORY):** **No page that is reachable from any pinned root (including via leaf `NextLeafID` / `PrevLeafID` chains) may be reclaimed, reused, or placed on the On-Disk Freelist until all Snapshots and Iterators that could reach that page have been closed.**

  * **Reader Liveness (TTL) Policy (Updated):**

      * TTL is used **only** to bound garbage collection delays.
      * A Snapshot/Iterator is **not invalidated** solely due to time passing.
      * TTL expiration **does not permit** reclamation of pages that are reachable from any pinned root.
      * TTL may be used only for diagnostics, metrics, or optional future policies that explicitly fail iterators with a deterministic error.
      * TTL **must not** cause iterators to observe fewer historical pages, miss keys, break leaf traversal, observe reused PageIDs, or return logically inconsistent views.

  * **The Pruner:** Moves `PageID`s from Graveyard to the **On-Disk Freelist** only when safe under the hold policy and the Snapshot Safety Invariant.

### 5.3 Read Path & Iterator

#### 1\. Standard Get(Key)

  * Traverse B+Tree.

  * **Checksum:** On every `ReadPage`, verify CRC32C.

  * **Fetch:**

      * **Inlined:** Allocate a new byte slice and copy data from the Page.

      * **Pointer:**

        1.  Extract `FileID`, `Offset`, `Length` from `ValuePtr`.
        2.  Lookup the `*SlabFile` from the Snapshot’s `SlabSet`.
        3.  Bounds-check `Offset + Length` and parse the slab record (Section 2.1.1).
        4.  Verify record CRC32C.
        5.  Copy `ValueBytes` into a newly allocated slice.

      * *Safety Note:* Never return a direct slice into the mmap region.

#### 2\. Iterator Semantics (Cosmos Compatibility)

TreeDB iterators must match cosmos-db semantics:

  * `Iterator(start,end)` yields keys in ascending order over `[start,end)`.
  * `ReverseIterator(start,end)` yields keys in descending order over `[start,end)`.
  * Both are advanced by calling `Next()`.
  * `nil` start/end represent unbounded domains.
  * Returned `Key()`/`Value()` must not alias internal memory.
  * End-of-iteration is represented by `Valid() == false` with `Error() == nil`.

#### 3\. Forward Iterator (Internal Mechanics)

  * **Initialization:** Seek to `startKey`.

  * **Next():**

      * Increment index in current Leaf Directory.
      * If index \>= `Count`: Load `NextLeafID`.
      * If `NextLeafID == 0`: Stop (EOF).

#### 4\. Reverse Iterator (Internal Mechanics)

  * **Initialization:**

      * Seek to `endKey`.
      * If `endKey` is exclusive and matches a key exactly, move one step back.
      * If `endKey` is nil, Seek to the **Last Key** of the **Right-Most Leaf**.

  * **Next():**

      * Decrement index in current Leaf Directory.
      * If index \< 0: Load `PrevLeafID`.
      * If `PrevLeafID == 0`: Stop (SOF).

  * **Concurrency:** Holds a reader hold on `CommitSeq` to protect page traversal.

### 5.4 Concurrent Slab Compaction (The "Move-and-CAS" Protocol)

Compaction runs concurrently with high-throughput writes and reads. To prevent races (e.g., “Resurrection”), compaction relies on **Optimistic Concurrency Control** via a `CompareAndSwap` operation on the B+Tree index.

#### 5.4.1 Compaction Lifecycle

**Phase 1: Candidate Selection**

  * Select a “Cold Slab” where `DeadBytes / TotalBytes > 0.5`.
  * Allocate a “Target Slab” (`active-compaction.slab`).

**Phase 2: Optimistic Copy (The "Ghost" Write)**

The compactor iterates sequentially through slab records (Section 2.1.1). For each record `(Key, OldPtr)`:

1.  **Dead Hints Optimization:** Consult a "Dead Hint" mechanism (e.g., bitmap or tombstone log) if available to skip known-dead records without B+Tree lookups.
2.  **Liveness Check:** If no hint, verify via B+Tree: If `Tree.Get(Key) != OldPtr`, skip.
3.  **Copy:** Append the value to the Target Slab, producing `NewPtr`.
4.  **Record Intent:** Add `Key -> (OldPtr, NewPtr)` to a local `CompactionBatch`.

**Phase 3: Atomic Commit (CAS)**

Once `CompactionBatch` reaches a threshold (e.g., 4MB):

1.  **Serialize with the Single Writer:** The Compaction CAS operation **MUST** execute under the **same global write lock** used by `Batch.Write` to prevent interleaving user updates.

2.  For each entry:

      * `CompareAndSwap(Key, OldPtr, NewPtr)`
      * On mismatch: abort the update for that key (waste is acceptable).

#### 5.4.2 Zombie Transition (Ref-Counted Deletion)

Once the Cold Slab is fully processed:

1.  Atomically remove Cold Slab from the active `SlabSet` and add Target Slab.

2.  Mark Cold Slab `IsZombie = true`.

3.  Attempt deletion:

      * If `RefCount == 0`: close + remove immediately.
      * Else: defer to the last Snapshot release.

### 5.5 Race Condition Analysis & Safety Proofs

#### 5.5.1 Race: The "Resurrection" Write

CAS prevents a compactor from overwriting a newer user write: if the index no longer points at `OldPtr`, `CompareAndSwap` fails.

#### 5.5.2 Race: The "Vanishing" Slab (Reader vs. Deleter)

Ref-counted deletion ensures that slabs remain present while any Snapshot holds them.

#### 5.5.3 Race: The "Torn" Compaction Commit

A crash during CAS can leave a mix of pointers across slabs; this is safe because the B+Tree is authoritative and both slabs remain loadable after restart.

### 5.6 Adaptive Inline Threshold (Option A: Telemetry + Feedback Control)

TreeDB may optionally run an adaptive controller to automatically tune `InlineThreshold` based on observed index pressure and slab/compaction pressure.

#### 5.6.1 Goals

  * Avoid B+Tree fanout collapse and excess leaf splitting caused by inlining too much value data.
  * Avoid excessive slab bandwidth, dead-byte accumulation, and compaction storms caused by pushing too much out-of-line.
  * Keep behavior stable and predictable under Cosmos workloads (IAVL-heavy medium values vs. blob-heavy workloads).

#### 5.6.2 Operating Rules (MANDATORY)

  * **Threshold is latched per commit:** At the start of each `Batch.Write*()` call, TreeDB reads the current `InlineThreshold` into a local variable and uses that value consistently for the entire commit.

  * **Hard caps:**

      * `InlineHardMin = 64 bytes`
      * `InlineHardMax = 2048 bytes`

  * **Very large values are always out-of-line:** if `len(value) > InlineHardMax`, they must be stored in slabs regardless of any adaptive decision.

#### 5.6.3 Telemetry (EWMA)

TreeDB maintains exponentially-weighted moving averages (EWMA) for the following metrics. All are computed at commit boundaries.

**Index-side telemetry**

  * `leaf_fill_avg`: average fraction of leaf bytes used after zipper merge (0..1).
  * `split_rate`: leaf splits per commit (or per 10k inserted/updated keys; implementation-defined).
  * `index_write_bytes`: approximate bytes dirtied/written in `index.db` per commit.

**Slab/compaction telemetry**

  * `slab_write_bytes`: bytes appended to slabs per commit (including record overhead).
  * `slab_dead_ratio`: `sum(DeadBytes) / sum(TotalBytes)` over slabs in the active set (or over all slabs; implementation-defined).
  * `compaction_io_bps`: observed compaction IO bandwidth (bytes/sec), EWMA.

EWMA parameters:

  * `ewma_alpha` is configurable; defaults should correspond to a half-life of \~1–5 minutes of commits (implementation-defined).

#### 5.6.4 Pressure Functions

TreeDB computes two pressures each evaluation interval:

```text
index_pressure =
    w1 * max(0, leaf_fill_avg - leaf_fill_target)
  + w2 * max(0, split_rate - split_rate_target)
  + w3 * max(0, index_write_bytes_per_op - index_write_target)

slab_pressure  =
    v1 * max(0, slab_dead_ratio - slab_dead_target)
  + v2 * max(0, compaction_io_bps - compaction_io_target)
  + v3 * max(0, slab_write_bytes_per_op - slab_write_target)
```

Defaults (recommended, configurable):

  * `leaf_fill_target = 0.85`
  * `split_rate_target = 0` (or a small tolerance; workload-dependent)
  * `slab_dead_target = 0.35`
  * `compaction_io_target` is deployment-specific (e.g., rate limiter value)
  * `index_write_target` and `slab_write_target` are deployment-specific and may be disabled by setting weights to zero.

Weights:

  * `w1,w2,w3,v1,v2,v3` are configurable. Default deployments may start with `w1,w2,v1,v3 > 0` and others at 0.

#### 5.6.5 Update Rule (Bounded Step Controller)

Every `K` commits (default: `K=100`), TreeDB updates `InlineThreshold`:

```text
delta = clamp( alpha * (slab_pressure - index_pressure), -step, +step )
InlineThreshold = clamp(InlineThreshold + delta, InlineHardMin, InlineHardMax)
```

Defaults (recommended, configurable):

  * `step = 64 bytes`
  * `alpha` chosen such that typical adjustments are one `step` per evaluation under sustained pressure.
  * `InlineThreshold` initializes to `256 bytes` unless configured otherwise.

Interpretation:

  * If `index_pressure` dominates, `delta` becomes negative and the threshold decreases, pushing more values out-of-line.
  * If `slab_pressure` dominates, `delta` becomes positive and the threshold increases, inlining more values.

#### 5.6.6 Observability

The following `Stats()` keys should be exported when adaptive tuning is enabled:

  * `treedb.inline_threshold.current`
  * `treedb.inline_threshold.hard_min`
  * `treedb.inline_threshold.hard_max`
  * `treedb.inline_threshold.leaf_fill_avg`
  * `treedb.inline_threshold.split_rate`
  * `treedb.inline_threshold.slab_dead_ratio`
  * `treedb.inline_threshold.slab_write_bytes`
  * `treedb.inline_threshold.compaction_io_bps`

## 6\. Go Interface Definition

```go
package goslab

import "github.com/cosmos/cosmos-db" // Implied dependency

// DB implements the cosmos-db.DB interface
type DB struct {
    // ... internal fields (pager, slabManager, meta) ...
}

// Open initializes the store
func Open(opts Options) (*DB, error)

// --- cosmos-db.DB Interface Implementation ---

// Get returns the value for a key, or nil if not found.
func (db *DB) Get(key []byte) ([]byte, error)

// Has checks if a key exists.
func (db *DB) Has(key []byte) (bool, error)

// Set sets the value for a key. (Immediate write - internally creates a mini-batch)
func (db *DB) Set(key, value []byte) error

// SetSync sets the value and flushes to disk immediately.
func (db *DB) SetSync(key, value []byte) error

// Delete removes a key.
func (db *DB) Delete(key []byte) error

// DeleteSync removes a key and flushes to disk.
func (db *DB) DeleteSync(key []byte) error

// Iterator returns an iterator over a domain of keys in ascending order.
// Start is inclusive, End is exclusive.
func (db *DB) Iterator(start, end []byte) (db.Iterator, error)

// ReverseIterator returns an iterator over a domain of keys in descending order.
// Start is inclusive, End is exclusive.
func (db *DB) ReverseIterator(start, end []byte) (db.Iterator, error)

// Close closes the database.
func (db *DB) Close() error

// NewBatch creates a batch for atomic updates.
func (db *DB) NewBatch() db.Batch

// NewBatchWithSize creates a batch with a size hint for internal buffering.
func (db *DB) NewBatchWithSize(size int) db.Batch

// Print prints the internal tree structure (for debugging).
func (db *DB) Print() error

// Stats returns a map of property strings to values.
// Mandatory keys:
//  - "cosmos.db.type": "treedb"
//  - "treedb.pages.total": Total pages in index.db
//  - "treedb.slabs.active_id": ID of the current write slab (from SlabManager)
//  - "treedb.slabs.zombies": Count of zombie slabs awaiting cleanup
func (db *DB) Stats() map[string]string

// --- Extended Management Interface ---

// Compact triggers a manual compaction cycle.
// Useful for node operators during maintenance windows.
func (db *DB) Compact() error

// --- Internal Interfaces for CAS Support ---

// Tree interface required for Safe Compaction
type Tree interface {
    Get(key []byte) (ValuePtr, error)
    Set(key []byte, ptr ValuePtr) error

    // CompareAndSwap atomically sets 'key' to 'newPtr' ONLY IF the current value equals 'oldPtr'.
    // Returns true if swapped, false if mismatch.
    // MUST execute under the same write lock as Batch.Write.
    CompareAndSwap(key []byte, oldPtr, newPtr ValuePtr) (bool, error)
}

// --- Batch Implementation ---

type Batch struct {
    db   *DB
    ops  map[string]operation
    size int
}

func (b *Batch) Set(key, value []byte) error
func (b *Batch) Delete(key []byte) error
func (b *Batch) Write() error
func (b *Batch) WriteSync() error
func (b *Batch) Close() error
func (b *Batch) GetByteSize() (int, error) // Required by Cosmos
```

