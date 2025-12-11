# TreeDB Design Specification v2.1

## 1\. System Overview

**TreeDB** is an embedded, persistent key-value store tailored for the Cosmos SDK workload (IAVL+ Tree). It prioritizes high write throughput, efficient range scans, and low memory overhead during block commits.

### Key Architectural Features

  * **Hybrid Storage:** Values are stored in an append-only log (**Slabs**). Keys are stored in a Memory-Mapped B+Tree (**Index**).
  * **Safety:** Uses Copy-On-Write (COW) for snapshot isolation and CRC32 checksums for data integrity.
  * **Concurrency:** Supports Single-Writer / Multi-Reader (SWMR) with lock-free reads via MVCC (Multi-Version Concurrency Control) semantics using Epochs.

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
4.  **Retire Old Pages:** Add all replaced `OldPageIDs` to the **Graveyard** (See 5.3).

### 5.2 Page Lifecycle & Safety (The Graveyard)

To support concurrent readers without locks, we cannot immediately overwrite old pages.

**1. The In-Memory Graveyard**

  * A structure in RAM: `map[Sequence][]PageID`.
  * When a Batch commits at Sequence `N`, all "Old Pages" replaced during that commit are added to `Graveyard[N]`.

**2. The Reader Hold**

  * Readers (Iterators) acquire a "Hold" on a specific Sequence.
  * `MinActiveSequence` = The lowest sequence currently being read by any iterator.

**3. The Pruner (Moving to Disk)**

  * A background process (or triggered at Commit start).
  * Finds all `Graveyard[S]` where `S < MinActiveSequence` (and `S < PruningKeepRecent`).
  * Moves these PageIDs from the RAM Graveyard to the **On-Disk Freelist** (Type 0x02 pages inside `index.db`).

**4. The Allocator**

  * When `NewPage()` is requested:
    1.  Check **On-Disk Freelist**.
    2.  If not empty, pop a PageID and overwrite it.
    3.  If empty, append a new page to the end of `index.db`.

### 5.3 Read Path & Iterator

#### 1\. Standard Get(Key)

  * Traverse B+Tree.
  * **Checksum:** On every `ReadPage`, verify CRC32.
  * **Fetch:**
      * **Inlined:** **Allocate a new byte slice** and copy data from the Page. Return the copy.
      * **Pointer:** Read from `SlabManager`. **Allocate a new byte slice** and copy data from the Mmap region. Return the copy.
      * *Safety Note:* Never return a direct slice into the Mmap region, as file truncation/remaps can cause Segfaults in consumer code.

#### 2\. Forward Iterator

  * **Initialization:** Seek to `startKey`.
  * **Next():**
      * Increment index in current Leaf Directory.
      * If index \>= `Count`: Load `NextLeafID`.
      * If `NextLeafID == 0`: Stop (EOF).

#### 3\. Reverse Iterator (NEW)

Used heavily by Cosmos SDK for finding the latest versions or traversing time-ordered queues.

  * **Initialization:**
      * Seek to `endKey`.
      * If `endKey` is exclusive and matches a key exactly, move one step back.
      * If `endKey` is nil, Seek to the **Last Key** of the **Right-Most Leaf** in the tree.
  * **Prev():**
      * Decrement index in current Leaf Directory.
      * If index \< 0:
          * Load `PrevLeafID`.
          * If `PrevLeafID == 0`: Stop (SOF).
          * Load the new page.
          * Set index to `NewPage.Count - 1` (Last item in the new page).
  * **Concurrency:**
      * Like the Forward Iterator, the Reverse Iterator holds a "Reader Hold" on the Sequence number (Epoch) at the time of creation to prevent the `PrevLeafID` pages from being garbage collected by the Graveyard/Pruner.

### 5.4 Slab Compaction (Garbage Collection)

Instead of scanning the whole tree, we track "Liveness" per slab file to avoid "stop-the-world" pauses.

1.  **Metadata Tracking:**
      * In `Page 0` (Meta), we maintain a map: `SlabStats[FileID] { TotalBytes, DeadBytes }`.
      * When a key is **overwritten** or **deleted**, we look up the old `ValuePtr` (if not inlined) and increment `DeadBytes` for that `FileID`.
2.  **Trigger:**
      * When `DeadBytes / TotalBytes > 0.5` (50% fragmentation) for a specific `.slab` file.
3.  **Compaction Process (Background):**
      * Open `new.slab`.
      * Iterate the `old.slab` sequentially (fast scan).
      * **Verification:** For every value in `old.slab`, check the B+Tree. Does the current Index still point to this offset?
          * **Yes:** Copy value to `new.slab`. Update B+Tree to point to new location.
          * **No:** Discard (it was deleted/moved).
      * Delete `old.slab`.

## 6\. Go Interface Definition

```go
package goslab

import "github.com/cosmos/cosmos-db" // Implied dependency

// DB implements the cosmos-db.DB interface
type DB struct {
    // ... internal fields (pager, slabs, meta) ...
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
//  - "treedb.pages.free": Count of pages in the freelist
//  - "treedb.slabs.active_id": ID of the current write slab
//  - "treedb.slabs.total_size": Combined size of all slab files
func (db *DB) Stats() map[string]string

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
