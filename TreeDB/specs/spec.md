# TreeDB Design Specification v2.7 (Final Polish)

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
  * **Rotation:** When the active file exceeds `4GB`:
     1. `fsync` the active file.
     2. Close (seal) the active file.
     3. Create new slab.
     4. **Directory Sync:** `fsync` the parent directory to persist the new file entry.

#### 2.1.1 Slab Record Format (Compaction-Readable)

To allow compaction to iterate slabs without scanning the B+Tree, large values are written as self-describing records:

```text
[ Slab Record ]
+-------------------------------+
| CRC32C        (4 bytes)       |  CRC32C (Castagnoli) of (KeyLen..ValueBytes)
| KeyLen        (2 bytes)       |  uint16, max 64KiB
| ValueLen      (4 bytes)       |  uint32
| KeyBytes      (KeyLen bytes)  |
| ValueBytes    (ValueLen bytes)|
+-------------------------------+
```

Rules:

  * **Checksum:** Uses **CRC32C (Castagnoli)** for hardware acceleration (SSE4.2/ARMv8).
  * Only values stored out-of-line (i.e., `len(value) > InlineThreshold`) are appended as slab records.
  * The `ValuePtr.Offset` points to the beginning of `KeyLen` (i.e., immediately after CRC32C), so the compactor can parse forward easily and readers can bounds-check safely.
  * **Definition of Length:** `ValuePtr.Length` is defined as `2 (KeyLen) + 4 (ValueLen) + len(Key) + len(Value)`. This ensures that `Offset + Length` precisely covers the data protected by the CRC.
  * On read, the record CRC32C is verified before returning data.
  * **Approved performance optimizations (write path):**
      * Implementations MAY use vectored writes (e.g., `pwritev`) and pooled header buffers when appending large records. The on-disk bytes MUST remain identical to the layout above and CRC coverage MUST be unchanged; a portable fallback to `WriteAt` is required.
      * Implementations MAY use a bounded, sequential buffered writer (optionally with `O_APPEND`) for the active slab. Buffers MUST be memory-bounded, MUST flush all pending bytes before any commit publishes new roots, and MUST flush then `fdatasync` for `*Sync` commits **before** the index/meta durability boundary. Concurrent random `WriteAt` on the active slab is forbidden while buffered append is enabled.

### 2.2 The Index File ("The Pager")

  * **Purpose:** Stores the B+Tree structure, the Freelist, and redundant Superblocks (Meta).
  * **Filename:** `index.db`
  * **Access Pattern:** **Chunked Memory Map**.
      * The file is logically a contiguous array of Pages.
      * Physically, the Go runtime maps it in **Configurable Chunks** (default: 256MB).
      * **Alignment Invariant:** `ChunkSize` MUST be an exact multiple of the **Page Size** (4KB). The Pager MUST ensure that no Page physically crosses a chunk boundary.
      * **Growth:** To expand, we strictly **pre-allocate disk space** (e.g., via `fallocate`) *before* extending the mmap. Old chunks remain valid to prevent segfaults in active readers.
      * **Safety Protocol:**
          * **No Shrinking:** Physical file truncation (shrinking) is FORBIDDEN while the process is running.
          * **Fail-Stop:** `recover()` cannot catch `SIGBUS` in Go. Hardware I/O errors on mmap regions will crash the node; truncation faults must be prevented via bounds checks.
          * `Unmap` operations must verify zero active references before releasing a chunk.

#### 2.2.1 Durability Ordering for Mmap Writes (Required)

Because `index.db` is memory-mapped, TreeDB defines an explicit persistence boundary:

  * **Non-sync writes (`Set`, `Batch.Write`)**: may return after OS page cache has been dirtied; durability is not guaranteed until a later fsync.
  * **Sync writes (`SetSync`, `Batch.WriteSync`)**: return only after:
    1.  **Strict Ordering:** The active slab data must be durable (`fdatasync`/`fsync` on active slab) **BEFORE** any meta page updates are written.
    2.  The committed superblock is durable (`msync` dirty index pages as needed + `fdatasync`/`fsync` on `index.db`).

Implementation note: the exact combination of `msync` and `fdatasync` is OS-dependent; the contract is that `*Sync` means the new root is recoverable after a power loss.

#### 2.2.2 Approved Pager Optimizations & Internal APIs (Performance Sprint)

The following pager-level optimizations are permitted provided their invariants hold:

  * **Preallocation cache:** The Pager MAY cache the last preallocated file size (`preallocSize`) to skip redundant `Stat`/`Truncate` calls. The cached size is advisory: if a later stat observes the file smaller than cached, the Pager MUST refresh or fail fast to avoid mapping beyond EOF (SIGBUS risk). Alignment to chunk boundaries and OS page size MUST be preserved.
  * **Commit-level growth estimate:** The writer MAY batch/guesstimate required growth once per commit. Estimates MUST be capped to a small multiple of `ChunkSize` (≤2–4 chunks) and correctness MUST NOT depend on the estimate; under-shoots must fall back to on-demand growth.
  * **Optional skip-zeroing:** Skipping zeroing of freshly file-extended pages is allowed only as an explicit opt-in. It MAY apply only to pages that extend the file (never freelist-reused pages). All bytes covered by the page body CRC MUST still be deterministically initialized by the tree/pager before publish. Tests/debug builds SHOULD keep zeroing enabled.
  * **Zero-copy internal reads:** Internal packages MAY use pinned page views to avoid PageSize copies:
      * `ReadPage(pid)` continues to return a safe copy.
      * `ReadPageRef(pid)` returns a lifetime-bounded view that pins the underlying chunk. Callers MUST treat `PageRef.Bytes()` as read-only and MUST still verify the per-page body CRC before semantic use.
      * Every `PageRef` MUST be released (`defer ref.Release()`); `Pager.Close()` MUST account for outstanding refs.
  * **Direct mmap writes for new pages:** The Pager MAY expose `WithMutablePage(pid, fn)` (or equivalent) that yields a mutable view only for pages newly allocated in the current commit. Using it on old/retired pages MUST be prevented by API shape or debug assertions. Old page CRC MUST be verified before cloning, and new page body CRC MUST be recomputed before commit.
  * **Freelist in-place pop/append:** Freelist operations MAY mutate freelist pages in place to avoid allocations, but MUST verify freelist-page CRC before reading/modifying and MUST update CRC after each mutation while preserving the `NextPageID` chain invariants.

## 3\. Data Structures

### 3.1 Global Constants

  * **Page Size:** `4096 bytes`
  * **Inline Threshold:** `256 bytes` (Default).
      * **Rationale:** Chosen to ensure standard IAVL Tree Nodes (~150 bytes) are stored inline. This prevents a "Double-IO" penalty (Index Read + Slab Read) for tree traversals.
  * **Max Key Size:** Variable (handled via Slotted Pages), typically \< 1KB.

### 3.2 The Value Pointer

Used to point to data stored in the Slabs.

**Updated Layout (Alignment):** Fields are reordered to ensure the `uint64 Offset` falls on an 8-byte boundary. This prevents compiler padding and safe-guards against alignment faults on ARM.

**On-disk encoding (16 bytes):**

```text
ValuePtr (16 bytes)
+-------------------------+
| Offset   (8 bytes)      |  // 8-byte aligned
| Length   (4 bytes)      |
| FileID   (4 bytes)      |  // uint32 (expanded)
+-------------------------+
```

Corresponding Go type (in-memory):

```go
type ValuePtr struct {
    Offset    uint64 // Bytes 0-7
    Length    uint32 // Bytes 8-11
    FileID    uint32 // Bytes 12-15
} // 16 bytes, naturally aligned
```

### 3.3 Page Layout

The `index.db` file is an array of 4KB pages.

#### Common Page Header (16 Bytes)

Every page starts with this header.

```text
[  Page Header  ]
+--------------------------+
| PageID   (8 bytes)       | <--- Self-referential ID (Aligned).
| Checksum (4 bytes)       | <--- CRC32C (Castagnoli) of the page body.
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
  * `UserRootPageID` (8b): Pointer to the User Data B+Tree root.
  * `SystemRootPageID` (8b): Pointer to the Internal Metadata B+Tree root.
  * `FreelistHeadID` (8b): Pointer to the head of the On-Disk Freelist.
  * `TotalPages` (8b): Total file size in pages.
  * `ActiveSlabID` (4b): The FileID of the slab that was active at the time of commit.
  * `ActiveSlabTail` (8b): The byte offset of the valid end-of-data for the active slab. Used to truncate torn tail records during crash recovery.
  * `LastCommitHeight` (8b): Optional: the application’s height/sequence (may equal `CommitSeq`).

**Removed:** `SlabStats` are no longer stored in the fixed-size Meta Page (overflow risk). They are stored as **Internal Metadata Keys** (Section 3.5).

**Recovery Rule (Open):**

1.  Read both meta pages, verify CRC32C, choose the page with the **highest valid `CommitSeq`**.
2.  **Slab Repair:** Open `ActiveSlabID`. Truncate it to `ActiveSlabTail`.
3.  **Orphan Cleanup:** Scan data directory. Any slab file with `ID > ActiveSlabID` is a "Ghost Slab" (created but not committed) and MUST be deleted.

#### The Freelist Page (Type 0x02)

Used to recycle space within `index.db`.

  * **Header:** Standard Header.
  * **NextPageID (8b):** Pointer to the next Freelist page (forming a linked list).
  * **Body:** Array of `PageID (uint64)`.

### 3.5 Internal Metadata Keyspace (Namespace Isolation)

#### 3.5.1 Namespace Isolation (Dual Roots)
TreeDB maintains two distinct B+Trees rooted in the Superblock:
1. **User Tree:** Stores **encoded user keys** as `0x01 | userKey` internally. Public APIs and iterators MUST strip the `0x01` prefix on output.
2. **System Tree:** Stores internal metadata (Slab Stats, Configs) in a disjoint keyspace starting with `0x00`.
This encoding is canonical (not implementation-defined) to match Cosmos iterator expectations and to keep raw-tree dumps unambiguous.

#### 3.5.2 Slab Stats Keys
  * **Key:** `0x00 | "slab" | uint32(FileID)`
  * **Value:** `[DeadBytes (8b)] [TotalBytes (8b)]`

### 3.4 The Slab Manager

The `SlabManager` controls access to physical `.slab` files. It mediates between the Reader (needing stability) and the Compactor (needing cleanup).

```go
// SlabFile represents a single physical file on disk.
type SlabFile struct {
    FileID   uint32
    Handle   *os.File
    // Slabs are accessed via syscall.Pread (Random Read).
    // We DO NOT mmap slabs to avoid SIGBUS on truncation and excessive virtual memory usage.

    // Safety Primitives
    RefCount atomic.Int64 // Number of active Snapshots referencing this file
    IsZombie atomic.Bool  // True if the file has been removed from the active set
}

// SlabSet is an immutable list of SlabFiles active at a specific point in time.
type SlabSet struct {
    Files map[uint32]*SlabFile
}
```

**Publication Rule (RCU / Atomic Swap):**

  * The current `DBState` is stored behind an atomic pointer.
  * Writers publish a new `DBState` (with updated `SlabSet`) via a single atomic swap.
  * Snapshot acquisition loads the `DBState` once (acquire semantics) and pins referenced slabs by incrementing their `RefCount`.

## 4\. Node Layouts (Slotted Pages)

We use **Slotted Pages** to support variable-length keys while maintaining binary-search speed.

### 4.1 Internal Node Layout

  * **Directory (Top):** Array of `Offsets (uint16)` growing downward.
  * **Heap (Bottom):** Data growing upward.
  * **Entry Format:** `[Child PageID (8b)] [KeyBytes]`
  * **Search:** Binary search the Directory to find the Child pointer.

### 4.2 Leaf Node Layout

Leaf nodes store the actual key-value data.

**Critical Architecture Decision (Write Amplification):**
Unlike v2.3, Leaf Nodes **do not** store persistent sibling pointers (`NextLeafID`/`PrevLeafID`). In a COW architecture, updating a sibling pointer modifies the neighbor's PageID, triggering a cascading rewrite of all preceding leaves ($O(N)$ write cost). Instead, TreeDB uses **Cursor Stacks** (Section 5.3) for bidirectional iteration.

  * **Header (16 Bytes):** Standard Header (Same as Internal Nodes).
  * **Directory (Top):** Array of `Offsets (uint16)`.
  * **Heap (Bottom):** Stores keys and values (or pointers).

#### 4.2.1 Leaf Entry Format

To support variable keys and hybrid value storage (Inline vs Pointer), leaf entries in the heap are encoded as follows:

```text
[ Leaf Entry ]
+-------------------------+
| KeyLen   (2 bytes)      | uint16
| ValueLen (4 bytes)      | uint32
| Flags    (1 byte)       | 0x00=Inline, 0x01=Pointer, 0x02=Tombstone
| Key      (KeyLen bytes) |
| Value    (Variable)     | If Inline: ValueLen bytes.
|                         | If Pointer: 16-byte ValuePtr (ValueLen ignored).
+-------------------------+
```

## 5\. Core Algorithms

### 5.1 Batch Write (The "Zipper" Merge)

Writes are batched to ensure atomicity and reduce I/O.

**Phase 1: Buffer & Pre-Write (Memory Pressure Fix)**

1.  Iterate over `Put(Key, Val)` operations.
2.  **If `len(Val) > InlineThreshold`:**
      * **Append** a slab record (Section 2.1.1) to the active `.slab` file.
        Implementations MAY buffer records and flush them sequentially per commit to reduce syscall overhead. Buffers MUST be memory-bounded: implementations MAY flush in bounded chunks when the buffer grows large, while still preserving append order and returning correct `ValuePtr` offsets into the final on-disk stream.
      * Calculate `ValuePtr`.
      * Store `(Key -> ValuePtr)` in the in-memory Batch Map.
      * *Note:* If the batch fails, these bytes in the slab are wasted but harmless.
3.  **If `len(Val) <= InlineThreshold`:**
      * Store `(Key -> Val)` literals in the in-memory Batch Map.

**Phase 2: Recursive Merge**

1.  Sort the Batch Keys.
2.  Recursively descend the B+Tree (COW).
      * **Implementation note (performance):** Writers SHOULD clone pages along the search path using zero-copy views (`ReadPageRef`) and write new pages directly into mmap via `WithMutablePage` where available, applying slotted-page `Set/Delete` in place on the clones. Full zipper merge remains the canonical algorithm for large batches; incremental updates are an allowed optimization that preserves identical on-disk layout and COW semantics.
3.  **Leaf Node:**
      * Allocate `NewPage`.
      * Merge `OldLeaf` items + `Batch` items into `NewPage`.
      * Perform splitting if `NewPage` \> 4KB.
4.  **Internal Node:** Update child pointers to point to new pages.
5.  **Stats Update:** Update internal metadata keys (`0x00` prefix) for any modified slab stats.
      * **Optimization:** Dead-byte accounting for overwritten pointer values SHOULD reuse the leaf entry observed during the merge/update rather than issuing a separate `Get` for each key.

**Phase 3: Atomic Commit (Redundant Superblock)**

1.  We now have a `NewRootPageID`.
2.  **If Sync=true:** `fdatasync` the active slab file **MUST complete** before any meta page update.
3.  Ensure index pages backing the new root are persisted as required by the durability contract (Section 2.2.1).
4.  **Write the inactive Meta Page** (Page 0 or Page 1):
      * `CommitSeq = OldCommitSeq + 1`
      * `RootPageID = NewRootPageID`
      * `FreelistHeadID = CurrentFreelistHead`
      * `TotalPages = CurrentTotalPages`
      * `ActiveSlabTail = ActiveSlab.CurrentOffset` (for crash recovery)
5.  **If Sync=true:** persist the meta update (Section 2.2.1).
6.  **Retire Old Pages:** Add all replaced `OldPageIDs` to the **Graveyard** (See 5.2).

### 5.2 Page & Slab Lifecycle (The Graveyard & Snapshots)

To support concurrent readers without locks, we cannot immediately overwrite old pages or delete old slab files.

#### 5.2.1 Snapshot Acquisition

To ensure safe reading of both Pages and Slabs, every Read operation (including `Get` and `Iterator`) must first acquire a **Snapshot**.

##### 5.2.1.1 The DBState (Atomic View)
To guarantee consistent reads, TreeDB maintains a global atomic pointer to an immutable `DBState` struct:
  * `CommitSeq` (uint64)
  * `RootPageID` (uint64)
  * `SlabSet` (*SlabSet)

##### 5.2.1.2 Snapshot Acquisition
1.  **Capture State:** Atomically load the `DBState` pointer once (Acquire Semantics).
2.  **Pin Sequence:** Register `DBState.CommitSeq` in the Reader Registry (affects `MinPinnedSeq`).
3.  **Pin Slabs:** Iterate `DBState.SlabSet` and increment `RefCount` for each file.
4.  **Traverse:** The Snapshot MUST use `DBState.RootPageID` for all lookups.
5.  **Release State:** When `Snapshot.Close()` or `Iterator.Close()` is called:
      * Decrement `RefCount` for each slab in the snapshot.
      * If `RefCount == 0` AND `IsZombie == true`, physically close and remove the file from disk.

#### 5.2.2 The Graveyard (Pages)

  * **In-Memory Graveyard:** `map[CommitSeq][]PageID`. When a commit occurs at `CommitSeq N`, all replaced pages are added here.
  * **Reader Registry (MinPinnedSeq):** TreeDB maintains an atomic counter or lock-protected set of all active Snapshot `CommitSeq`s. The **MinPinnedSeq** is the lowest sequence number currently in use by any reader.
  * **KeepRecent Policy:** A configurable setting (e.g., `KeepRecent=100`) defining how many historical versions must be retained regardless of active readers.
  * **Snapshot Safety Invariant (MANDATORY):** **No page that is reachable from any pinned root may be reclaimed, reused, or placed on the On-Disk Freelist until all Snapshots and Iterators that could reach that page have been closed.**
  * **The Pruner:** Moves `PageID`s from Graveyard to the **On-Disk Freelist** only when:
      * `RetiredAtSeq < MinPinnedSeq` **AND**
      * `RetiredAtSeq < (CurrentSeq - KeepRecent)`

### 5.3 Read Path & Iterator (Cursor Stack)

TreeDB implements iterators using a **Stateful Cursor Stack** to enable bidirectional traversal without persistent sibling pointers.

#### 1\. Standard Get(Key)

  * Traverse B+Tree.
  * **Checksum:** On every `ReadPage`/`ReadPageRef`, verify CRC32C before semantic use.
  * **Performance (allowed):** Internal point reads MAY descend using a zero-allocation `searchView(encKey)` that uses `ReadPageRef` for pinned views and in-place binary search. Any key/value slice decoded as a view into pinned pages MUST NOT escape the pin; public `Get` still returns copies and strips the user-key prefix.
  * **Fetch:**
      * **Inlined:** Allocate a new byte slice and copy data from the Page.
      * **Pointer:**
        1.  Extract `Offset`, `Length`, `FileID` from `ValuePtr`.
        2.  Lookup the `*SlabFile` from the Snapshot’s `SlabSet`.
        3.  Bounds-check `Offset + Length` and parse the slab record (Section 2.1.1).
        4.  Verify record CRC32C.
        5.  Copy `ValueBytes` into a newly allocated slice.
      * **Mmap Safety Protocol (CRITICAL):**
          * **No Direct Slices:** The `Page` struct returned by the Pager MUST NOT contain standard Go slices (`[]byte`) pointing to the mmap to avoid GC scanning of off-heap memory.
          * **Safe Access:** Use `unsafe.Pointer` casting or `uintptr` arithmetic.
          * **Bounds Enforcement:** The Pager MUST verify `Offset + PageSize` is within the mapped chunk/file before returning a pointer. `SIGBUS` cannot be recovered from; it must be prevented.



#### 2\. Iterator Data Structure

The Iterator holds a slice of `CursorItem` objects representing the path from Root to the current position.

```go
type CursorItem struct {
    PageID  uint64
    Ref     pager.PageRef // Pinned mmap view; must be released on pop/Close.
    Node    *Page         // Parsed view backed by Ref.Bytes().
    Index   int    // The current index within the node's directory
}
```

#### 2.1 Cosmos Iterator Contract (Strict)
To comply with `cosmos-db`, the Iterator must adhere to these rules:
  * **Safety Copies:** `Key()` and `Value()` MUST return newly allocated copies safe for modification by the caller.
  * **Key Encoding:** Iterators MUST strip the internal user-key prefix (`0x01`) before returning keys.
  * **Panic on Invalid:** `Next()` MUST panic if `Valid()` is false.
  * **Invalid Domain:** If `start >= end`, the iterator is immediately Invalid.
  * **Tombstones:** Tombstones encountered during iteration must be skipped (treated as non-existent).

#### 3\. Forward Iterator Logic (`Next`)

  * **Initialization:** Perform a standard B+Tree search for `startKey`. Push every node visited onto the `Stack`.
  * **Advance:**
    1.  Look at the `Top` item of the Stack (the Leaf).
    2.  Increment `Top.Index`.
    3.  **If `Top.Index < Top.Node.Count`**: The next key is in the same leaf. Return it.
    4.  **If `Top.Index >= Top.Node.Count`** (Leaf Exhausted):
          * Pop the Leaf from the Stack.
          * **Recursion:** Look at the *new* `Top` (the Parent). Increment `Parent.Index`.
          * **If Parent Exhausted:** Pop again, repeat up to Root.
          * **If Root Exhausted:** Stop (EOF).
          * **If Valid Branch Found:**
              * Get `ChildPageID` at the new `Index`.
              * Push Child to Stack.
              * **Drill Down:** Continuously follow index `0` (left-most child) until a Leaf is reached.
              * Return Key at `Leaf.Index = 0`.

#### 4\. Reverse Iterator Logic (`Next` / `Prev` Semantics)

  * **Initialization (Cosmos Semantics):**
      * The domain is `[start, end)`.
      * **Step 1 (Seek):**
          * If `end` is `nil`: seek to the **right-most leaf**, last item (largest key).
          * If `end` is provided: seek to the first key `>= end`.
      * **Step 2 (Adjust):**
          * If the seek found a key `>= end` (or hit EOF), move the cursor **backward** one item.
          * *Rationale:* positions the cursor at the largest key strictly less than `end`.
      * **Step 3 (Verify):**
          * If cursor is invalid or `Key < start`: iterator is empty.
  * **Advance:**
    1.  Look at the `Top` item of the Stack.
    2.  Decrement `Top.Index`.
    3.  **If `Top.Index >= 0`**: The previous key is in the same leaf. Return it.
    4.  **If `Top.Index < 0`** (Leaf Exhausted):
          * Pop the Leaf from the Stack.
          * **Recursion:** Look at the *new* `Top` (the Parent). Decrement `Parent.Index`.
          * **If Parent Exhausted:** Pop again, repeat up to Root.
          * **If Root Exhausted:** Stop (SOF).
          * **If Valid Branch Found:**
              * Get `ChildPageID` at the new `Index`.
              * Push Child to Stack.
              * **Drill Down:** Continuously follow index `Count - 1` (right-most child) until a Leaf is reached.
              * Return Key at `Leaf.Index = Leaf.Count - 1`.

### 5.4 Concurrent Slab Compaction (The "Move-and-Micro-Batch" Protocol)

Compaction runs concurrently with high-throughput writes and reads. To prevent races (e.g., “Resurrection”), compaction performs an **optimistic copy** followed by **micro-batch locking**: under the global write lock it verifies the current pointer still matches `OldPtr` before applying `NewPtr`.

#### 5.4.1 Compaction Lifecycle

**Phase 1: Candidate Selection**

  * Iterate **internal metadata keys** (`0x00` prefix) to find slabs where `DeadBytes / TotalBytes > 0.5`.
  * Allocate a “Target Slab” (`active-compaction.slab`).

**Phase 2: Optimistic Copy (The "Ghost" Write)**

The compactor iterates sequentially through slab records (Section 2.1.1). For each record `(Key, OldPtr)`:

1.  **Dead Hints Optimization:** Consult a "Dead Hint" mechanism (e.g., bitmap or tombstone log) if available to skip known-dead records without B+Tree lookups.
2.  **Liveness Check:** If no hint, verify via B+Tree: If `Tree.Get(Key) != OldPtr`, skip.
3.  **Copy:** Append the value to the Target Slab, producing `NewPtr`.
4.  **Record Intent:** Add `Key -> (OldPtr, NewPtr)` to a local `CompactionBatch`.

**Phase 3: Atomic Commit (Micro-Batch Locking)**

Instead of a `CompareAndSwap` interface, the compactor uses the standard global write lock.

1.  **Preparation:** Hold a buffer of `(Key, OldPtr, NewPtr)` pairs.
2.  **Execution (Loop):**
      * Acquire Global Write Lock.
      * **Verify & Apply:** For each item in the micro-batch:
          * `CurrentPtr = Tree.Get(Key)`
          * If `CurrentPtr == OldPtr`: `Tree.Set(Key, NewPtr)`
          * Else: skip (user updated concurrently).
      * Release Global Write Lock.
      * **Yield:** Sleep briefly to avoid starving user writes.

#### 5.4.2 Zombie Transition (Ref-Counted Deletion)

Once the Cold Slab is fully processed:

1.  Atomically remove Cold Slab from the active `SlabSet` and add Target Slab.
2.  Mark Cold Slab `IsZombie = true`.
3.  Attempt deletion:
      * If `RefCount == 0`: close + remove immediately.
      * Else: defer to the last Snapshot release.

### 5.5 Race Condition Analysis & Safety Proofs

#### 5.5.1 Race: The "Resurrection" Write

Verification under the write lock prevents resurrection: if the index no longer points at `OldPtr`, the compactor skips the update and the user write wins.

#### 5.5.2 Race: The "Vanishing" Slab (Reader vs. Deleter)

Ref-counted deletion ensures that slabs remain present while any Snapshot holds them.

#### 5.5.3 Race: The "Torn" Compaction Commit

A crash during micro-batch updates can leave a mix of pointers across slabs; this is safe because the B+Tree is authoritative and both slabs remain loadable after restart.

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

  * `w1,w2,w3` (Index Weights) should generally be **2x-3x higher** than `v1,v2,v3` (Slab Weights) for IAVL workloads.
  * *Rationale:* In IAVL, "Index Health" (Fanout) is critical for read performance. "Slab Health" (Dead Bytes) is secondary and can be managed by aggressive background compaction. We must avoid polluting the Index with high-churn data.

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
  * `treedb.inline_threshold.index_write_bytes`
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
// Contract: Errors on nil key. Returns nil (no error) if key does not exist.
// Receiver name 'tdb' used to avoid shadowing imported 'db' package.
func (tdb *DB) Get(key []byte) ([]byte, error)

// Has checks if a key exists.
func (tdb *DB) Has(key []byte) (bool, error)

// Set sets the value for a key.
// Contract: Errors on nil key or nil value. Empty keys (`[]byte{}`) are allowed.
// Implementations MAY use a single-op fast path that bypasses Batch allocation/sorting,
// but MUST preserve identical atomicity and durability semantics.
// Fast paths MUST acquire the same global writer lock used by Batch commits and compaction micro-batches.
func (tdb *DB) Set(key, value []byte) error

// SetSync sets the value and flushes to disk immediately.
func (tdb *DB) SetSync(key, value []byte) error

// Delete removes a key.
// Implementations MAY share the same single-op fast path as Set.
func (tdb *DB) Delete(key []byte) error

// DeleteSync removes a key and flushes to disk.
func (tdb *DB) DeleteSync(key []byte) error

// Iterator returns an iterator over a domain of keys in ascending order.
// Start is inclusive, End is exclusive.
// nil start/end represent unbounded domains.
func (tdb *DB) Iterator(start, end []byte) (db.Iterator, error)

// ReverseIterator returns an iterator over a domain of keys in descending order.
// Start is inclusive, End is exclusive.
func (tdb *DB) ReverseIterator(start, end []byte) (db.Iterator, error)

// Close closes the database.
func (tdb *DB) Close() error

// NewBatch creates a batch for atomic updates.
func (tdb *DB) NewBatch() db.Batch

// NewBatchWithSize creates a batch with a size hint for internal buffering.
func (tdb *DB) NewBatchWithSize(size int) db.Batch

// Print prints the internal tree structure (for debugging).
func (tdb *DB) Print() error

// Stats returns a map of property strings to values.
// Mandatory keys:
//  - "cosmos.db.type": "treedb"
//  - "treedb.pages.total": Total pages in index.db
//  - "treedb.slabs.active_id": ID of the current write slab (from SlabManager)
//  - "treedb.slabs.zombies": Count of zombie slabs awaiting cleanup
func (tdb *DB) Stats() map[string]string

// --- Extended Management Interface ---

// Compact triggers a manual compaction cycle.
// Useful for node operators during maintenance windows.
func (tdb *DB) Compact() error

// --- Internal Interfaces for Compaction Support ---

// Tree interface required for compaction verify-set
type Tree interface {
    // Get returns the raw leaf data (Inline, Pointer, or Tombstone).
    Get(key []byte) (LeafEntry, error)
    
    // Set updates the leaf directly with a specific pointer/inline value.
    // It returns the previous LeafEntry (or nil if absent) to allow dead-byte accounting
    // without an extra tree lookup.
    // MUST execute under the same write lock as Batch.Write.
    Set(key []byte, val LeafEntry) (old *LeafEntry, err error)
}

// --- Batch Implementation ---

type Batch struct {
    db       *DB
    ops      map[string]operation
    byteSize int // Tracks cumulative key+value size for gas.
}

func (b *Batch) Set(key, value []byte) error
func (b *Batch) Delete(key []byte) error
// Write flushes the batch to the DB. The batch is closed after this call.
func (b *Batch) Write() error
// WriteSync flushes and syncs to disk. The batch is closed after this call.
func (b *Batch) WriteSync() error
func (b *Batch) Close() error
// GetByteSize returns the approximate memory cost (Keys + Values + Overhead).
func (b *Batch) GetByteSize() (int, error) {
    return b.byteSize, nil
}
```
