# TreeDB Design Specification v2.3 (Concurrent Compaction)

## 1\. System Overview

**TreeDB** is an embedded, persistent key-value store tailored for the Cosmos SDK workload (IAVL+ Tree). It prioritizes high write throughput, efficient range scans, and low memory overhead during block commits.

### Key Architectural Features

  * **Hybrid Storage:** Values are stored in an append-only log (**Slabs**). Keys are stored in a Memory-Mapped B+Tree (**Index**).
  * **Safety:** Uses Copy-On-Write (COW) for snapshot isolation and CRC32 checksums for data integrity.
  * **Concurrency:** Supports Single-Writer / Multi-Reader (SWMR) with lock-free reads via MVCC (Multi-Version Concurrency Control) semantics using Epochs and Reference Counting.

## 2\. File Architecture

The database resides in a single directory containing two types of files.

### 2.1 The Value Log ("Slabs")

  * **Purpose:** Stores large data payloads (Contract code, large metadata).
  * **Filenames:** `data-0000.slab`, `data-0001.slab`, etc.
  * **Format:** Append-only byte sequence.
  * **Rotation:** When the active file exceeds `4GB`, it is closed (becoming read-only) and a new file is created.

### 2.2 The Index File ("The Pager")

  * **Purpose:** Stores the B+Tree structure and the Freelist.
  * **Filename:** `index.db`
  * **Access Pattern:** **Chunked Memory Map**.
      * The file is logically a contiguous array of Pages.
      * Physically, the Go runtime maps it in **1GB Chunks**.
      * **Growth:** To expand, we `Truncate` the file and `Mmap` *only* the new chunk. Old chunks remain valid to prevent segfaults in active readers.

## 3\. Data Structures

### 3.1 Global Constants

  * **Page Size:** `4096 bytes`
  * **Inline Threshold:** `64 bytes` (Values $\le$ this size are stored directly in the tree).
  * **Max Key Size:** Variable (handled via Slotted Pages), typically \< 1KB.

### 3.2 The Value Pointer

Used to point to data stored in the Slabs.

```go
type ValuePtr struct {
    FileID uint16 // 2 bytes: Supports 65k slab files
    Offset uint64 // 8 bytes: Offset within the slab
    Length uint32 // 4 bytes: Length of the data
} // Total: 14 bytes
```

### 3.3 Page Layout

The `index.db` file is an array of 4KB pages.

#### Common Page Header (16 Bytes)

Every page starts with this header.

```text
[  Page Header  ]
+--------------------------+
| Checksum (4 bytes)       | <--- CRC32 (IEEE) of the page body. Verified on read.
| PageID   (8 bytes)       | <--- Self-referential ID for sanity checking.
| Flags    (2 bytes)       | <--- Node Type (Meta/Free/Int/Leaf).
| Count    (2 bytes)       | <--- Number of items in this page.
+--------------------------+
```

#### Page Types

  * **0x01 Meta:** The Superblock (Page 0).
  * **0x02 Freelist:** Stores IDs of dead pages available for reuse.
  * **0x03 Internal:** B+Tree Branch nodes.
  * **0x04 Leaf:** B+Tree Data nodes.

#### The Meta Page (Page 0)

  * `RootPageID` (8b): Pointer to the current B+Tree root.
  * `FreelistHeadID` (8b): Pointer to the head of the On-Disk Freelist.
  * `TotalPages` (8b): Total file size in pages.
  * `LastCommitSeq` (8b): The Sequence (Height) of the last commit.
  * `SlabStats` (Map): Tracks dead bytes per slab file for compaction triggers.

#### The Freelist Page (Type 0x02)

Used to recycle space within `index.db`.

  * **Header:** Standard Header.
  * **NextPageID (8b):** Pointer to the next Freelist page (forming a linked list).
  * **Body:** Array of `PageID (uint64)`.

### 3.4 The Slab Manager (NEW)

The `SlabManager` controls access to physical `.slab` files. It mediates between the Reader (needing stability) and the Compactor (needing cleanup).

```go
// SlabFile represents a single physical file on disk.
type SlabFile struct {
    FileID   uint16
    Handle   *os.File
    Mmap     []byte       // The read-only memory map

    // Safety Primitives (Updated for Race Safety)
    RefCount atomic.Int64 // Number of active Snapshots referencing this file
    IsZombie atomic.Bool  // True if the file has been "deleted" by compaction
    
    // Compaction Stats
    DeadBytes atomic.Uint64
}

// SlabSet is an immutable list of SlabFiles active at a specific point in time.
type SlabSet struct {
    Files    map[uint16]*SlabFile
}
```

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
[   NextLeafID (8b)       ] <--- NEW: Forward Link
[   PrevLeafID (8b)       ] <--- NEW: Backward Link
[  ... Directory ...      ]
          ...
[  ... Heap Data ...      ]
```

**Leaf Update Logic (Copy-On-Write):**
When a Leaf Node is modified or split:

1.  **The Target Leaf** is duplicated/allocated (New PageID).
2.  **The Previous Leaf's** `NextLeafID` must be updated to point to the New PageID.
3.  **The Next Leaf's** `PrevLeafID` must be updated to point to the New PageID.
      * *Note:* In a standard COW B+Tree, updating neighbors can cause write amplification (cascading updates). However, since TreeDB uses the **Batch Zipper** (Section 5.1), we reconstruct contiguous ranges of leaves in memory. We only need to perform the "Link Stitching" on the **left-most** and **right-most** boundaries of the batch.

## 5\. Core Algorithms

### 5.1 Batch Write (The "Zipper" Merge)

Writes are batched to ensure atomicity and reduce I/O.

**Phase 1: Buffer & Pre-Write (Memory Pressure Fix)**

1.  Iterate over `Put(Key, Val)` operations.
2.  **If `len(Val) > 64`:**
      * **Immediately append** `Val` to the active `.slab` file.
      * Calculate `ValuePtr`.
      * Store `(Key -> ValuePtr)` in the in-memory Batch Map.
      * *Note:* If the batch fails, these bytes in the Slab are wasted (leaked) but harmless.
3.  **If `len(Val) <= 64`:**
      * Store `(Key -> Val)` literals in the in-memory Batch Map.

**Phase 2: Recursive Merge**

1.  Sort the Batch Keys.
2.  Recursively descend the B+Tree (COW).
3.  **Leaf Node:**
      * Allocate `NewPage` (See Section 5.3).
      * Merge `OldLeaf` items + `Batch` items into `NewPage`.
      * Perform splitting if `NewPage` \> 4KB.
4.  **Internal Node:** Update child pointers to point to new pages.

**Phase 3: Atomic Commit**

1.  We now have a `NewRootPageID`.
2.  **Fsync** the Slab file (if `Sync=true`).
3.  Update **Page 0 (Meta)**:
      * `RootPageID = NewRootPageID`
      * `FreelistHeadID = CurrentFreelistHead`
4.  **Retire Old Pages:** Add all replaced `OldPageIDs` to the **Graveyard** (See 5.2).

### 5.2 Page & Slab Lifecycle (The Graveyard & Snapshots)

To support concurrent readers without locks, we cannot immediately overwrite old pages or delete old slab files.

#### 5.2.1 Snapshot Acquisition (NEW)

To ensure safe reading of both Pages and Slabs, every Read operation (including `Get` and `Iterator`) must first acquire a **Snapshot**.

1.  **Capture State:** The Snapshot captures the `CurrentSlabSet` from the `SlabManager`.
2.  **Increment Refs:**
      * Iterate through all `*SlabFile` pointers in the captured `CurrentSlabSet`.
      * Atomically increment `SlabFile.RefCount` for each.
3.  **Release State:** When `Snapshot.Close()` or `Iterator.Close()` is called:
      * Iterate through the captured `SlabFile` pointers.
      * Atomically decrement `SlabFile.RefCount`.
      * **Trigger Cleanup:** If `RefCount == 0` AND `IsZombie == true`, physically close and remove the file from disk.

#### 5.2.2 The Graveyard (Pages)

  * **In-Memory Graveyard:** `map[Sequence][]PageID`. When a Batch commits at Sequence `N`, all "Old Pages" replaced during that commit are added here.
  * **The Reader Hold:** Readers acquire a hold on a Sequence. `MinActiveSequence` is the lowest sequence currently being read.
  * **Reader Liveness (TTL):** To prevent crashed readers from pinning the `MinActiveSequence` indefinitely (causing file bloat), Snapshots enforce a **TTL (e.g., 10 mins)**. If exceeded, the hold is ignored, and the reader receives `ErrSnapshotExpired`.
  * **The Pruner:** Moves `PageID`s from Graveyard to the **On-Disk Freelist** only when `Sequence < MinActiveSequence`.

### 5.3 Read Path & Iterator

#### 1\. Standard Get(Key)

  * Traverse B+Tree.
  * **Checksum:** On every `ReadPage`, verify CRC32.
  * **Fetch:**
      * **Inlined:** Allocate a new byte slice and copy data from the Page.
      * **Pointer:**
        1.  Extract `FileID`, `Offset`, `Length` from `ValuePtr`.
        2.  **Lookup:** Retrieve the `*SlabFile` from the **Snapshot's local SlabSet** (not the global one).
        3.  **Bounds Check:** Verify `Offset + Length <= len(SlabFile.Mmap)`.
        4.  **Copy:** `val = make([]byte, Length)` -\> `copy(val, SlabFile.Mmap[Offset:])`.
      * *Safety Note:* Never return a direct slice into the Mmap region.

#### 2\. Forward Iterator

  * **Initialization:** Seek to `startKey`.
  * **Next():**
      * Increment index in current Leaf Directory.
      * If index \>= `Count`: Load `NextLeafID`.
      * If `NextLeafID == 0`: Stop (EOF).

#### 3\. Reverse Iterator

Used heavily by Cosmos SDK for finding the latest versions or traversing time-ordered queues.

  * **Initialization:**
      * Seek to `endKey`.
      * If `endKey` is exclusive and matches a key exactly, move one step back.
      * If `endKey` is nil, Seek to the **Last Key** of the **Right-Most Leaf** in the tree.
  * **Prev():**
      * Decrement index in current Leaf Directory.
      * If index \< 0: Load `PrevLeafID`.
      * If `PrevLeafID == 0`: Stop (SOF).
  * **Concurrency:** Holds a "Reader Hold" on the Sequence to protect page traversal.

### 5.4 Concurrent Slab Compaction (The "Move-and-CAS" Protocol)

Compaction is the most dangerous operation in the database. It runs concurrently with high-throughput writes and reads. To prevent race conditions (e.g., the "Resurrection Bug" where an old value overwrites a new one), compaction relies on **Optimistic Concurrency Control** via a `CompareAndSwap` operation on the B+Tree index.

#### 5.4.1 The Compaction Lifecycle

**Phase 1: Candidate Selection**

  * The system identifies a "Cold Slab" where `DeadBytes / TotalBytes > 0.5`.
  * A new "Target Slab" is allocated (`active-compaction.slab`).

**Phase 2: Optimistic Copy (The "Ghost" Write)**
The compactor iterates sequentially through the **Cold Slab**. For every `ValuePtr` (FileID, Offset, Len) found:

1.  **Liveness Check (Read):** Query the B+Tree for the corresponding Key.
      * If `Tree.Get(Key) != CurrentValuePtr`: The value has already been updated or deleted by the user. **Skip.**
      * If `Tree.Get(Key) == CurrentValuePtr`: The value is still live.
2.  **Copy Data:** Read the bytes from the **Cold Slab** and append them to the **Target Slab**.
3.  **Record Intent:** Store the mapping `Key -> (OldPtr, NewPtr)` in a local `CompactionBatch`.
      * *Note:* At this stage, the `NewPtr` is valid on disk but **invisible** to readers.

**Phase 3: The Atomic Commit (CAS)**
Once the `CompactionBatch` reaches a size threshold (e.g., 4MB), we commit it to the B+Tree.

1.  **Acquire Writer Lock:** Determine the commit ordering relative to standard user batches.
2.  **Execute CAS Loop:**
    For every entry in the `CompactionBatch`:
      * **Check:** Does `Tree.Get(Key)` still equal `OldPtr`?
      * **If YES:** Update `Tree.Set(Key, NewPtr)`. (Success: Value moved).
      * **If NO:** The user updated `Key` while we were copying. **Abort update.**
          * *Result:* The data we copied to `Target Slab` is now "dead on arrival". This is acceptable waste. It will be cleaned up in the *next* compaction cycle of the `Target Slab`.
3.  **Release Lock.**

#### 5.4.2 The Zombie Transition (Ref-Counted Deletion)

Once the Cold Slab has been fully processed:

1.  **Global Swap:** The `SlabManager` atomically removes the **Cold Slab** from the `ActiveSet` and adds the **Target Slab**.
2.  **Mark Zombie:** The **Cold Slab** is marked `IsZombie = true`.
3.  **Cleanup Trigger:**
      * The Compactor calls `TryDelete(ColdSlab)`.
      * The function checks `Slab.RefCount`.
          * **If RefCount == 0:** Close file handle and `os.Remove()`.
          * **If RefCount \> 0:** The file remains on disk. It is added to a `ZombieList`.
4.  **Deferred Cleanup:**
      * Every time a Reader releases a Snapshot (`Close()`), it decrements the RefCount of all slabs in that snapshot.
      * If the decrement transitions a Slab's RefCount to 0 **AND** it is in the `ZombieList`, the Reader thread triggers the `os.Remove()`.

### 5.5 Race Condition Analysis & Safety Proofs

This section defines how TreeDB guarantees strict serializability despite background file shuffling.

#### 5.5.1 Race: The "Resurrection" Write

  * **Scenario:**
    1.  Compactor reads `Key A` (val=1) from Slab 1.
    2.  Compactor copies `val=1` to Slab 2.
    3.  **User** calls `Set(Key A, val=2)`. This updates the Index to point to `Slab 3`.
    4.  Compactor tries to update Index for `Key A` to point to `Slab 2`.
  * **Resolution:**
    The **Compare-And-Swap (CAS)** in Phase 3 fails.
    The Compactor compares its `OldPtr` (Slab 1) with the current Index state (Slab 3). They mismatch. The Compactor aborts the update. `Key A` correctly remains `val=2`.

#### 5.5.2 Race: The "Vanishing" Slab (Reader vs. Deleter)

  * **Scenario:**
    1.  Reader A acquires Snapshot at `T=1`. It holds a pointer to `Slab 1`.
    2.  Compactor finishes processing `Slab 1` at `T=2`.
    3.  Compactor attempts to delete `Slab 1`.
    4.  Reader A tries to read a value from `Slab 1`.
  * **Resolution:**
    When Reader A acquired the snapshot, it performed `atomic.AddInt64(&Slab1.RefCount, 1)`.
    When Compactor finishes, it marks `Slab1.IsZombie = true` but checks `RefCount`. Since RefCount is at least 1, **physical deletion is skipped**.
    Reader A continues to read from the open file handle.
    When Reader A calls `Close()`, it performs `atomic.AddInt64(&Slab1.RefCount, -1)`. The count hits 0. The Reader's defer handler detects `IsZombie` and deletes the file.

#### 5.5.3 Race: The "Torn" Compaction Commit

  * **Scenario:** System crashes mid-way through the Compactor's `CAS Loop`. Half the keys point to `OldSlab`, half point to `NewSlab`.
  * **Resolution:**
    Both `OldSlab` and `NewSlab` are valid files on disk. The B+Tree is the source of truth. Upon restart:
    1.  The B+Tree loads successfully.
    2.  Keys pointing to `OldSlab` work fine.
    3.  Keys pointing to `NewSlab` work fine.
    4.  **Recovery:** The `SlabManager` scans the directory. It loads both slabs. The compaction simply resumes (or restarts) later. No data is lost; only disk space efficiency is temporarily reduced.

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

// Print prints the internal tree structure (for debugging).
func (db *DB) Print() error

// Stats returns a map of property strings to values.
// Mandatory keys:
//  - "cosmos.db.type": "treedb"
//  - "treedb.pages.total": Total pages in index.db
//  - "treedb.slabs.active_id": ID of the current write slab (from SlabManager)
//  - "treedb.slabs.zombies": Count of zombie slabs awaiting cleanup
func (db *DB) Stats() map[string]string

// --- Internal Interfaces for CAS Support ---

// Tree interface required for Safe Compaction
type Tree interface {
    Get(key []byte) (ValuePtr, error)
    Set(key []byte, ptr ValuePtr) error
    
    // CompareAndSwap is required for safe compaction.
    // It atomically sets 'key' to 'newPtr' ONLY IF the current value equals 'oldPtr'.
    // Returns true if swapped, false if mismatch.
    CompareAndSwap(key []byte, oldPtr, newPtr ValuePtr) (bool, error)
}

// --- Batch Implementation ---

type Batch struct {
    db      *DB
    ops     map[string]operation 
    size    int
}

func (b *Batch) Set(key, value []byte) error
func (b *Batch) Delete(key []byte) error
func (b *Batch) Write() error
func (b *Batch) WriteSync() error
func (b *Batch) Close() error
func (b *Batch) GetByteSize() (int, error) // Required by Cosmos
```

