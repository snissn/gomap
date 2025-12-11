# GoSlabDB Design Specification v2.1

## 1\. System Overview

**GoSlabDB** is an embedded, persistent key-value store tailored for the Cosmos SDK workload (IAVL+ Tree). It prioritizes high write throughput, efficient range scans, and low memory overhead during block commits.

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

  * **Header:** Standard Header + `NextLeafID (8b)` (Linked list for range scans).
  * **Directory (Top):** Array of `Offsets (uint16)`.
  * **Heap (Bottom):**
      * **Case A (Pointer):** `[KeyLen] [KeyBytes] [Flag=0x00] [ValuePtr (14b)]`
      * **Case B (Inlined):** `[KeyLen] [KeyBytes] [Flag=0x01] [ValLen] [ValueBytes]`

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

### 5.3 Read Path & Integrity

1.  **Get(Key):**

      * Traverse B+Tree.
      * **Checksum:** On every `ReadPage`, verify CRC32. If mismatch -\> Panic/Error.
      * **Leaf:** Search Key.
      * **Fetch:** If Inlined, return bytes. If Pointer, read from `SlabManager`.

2.  **Iterator:**

      * Holds a reference to the `RootPageID` at the time of creation.
      * Lazy-loads values from disk only when `Value()` is called.

### 5.4 Slab Garbage Collection

  * **Trigger:** User-defined (e.g., "Disk usage \> 80%").
  * **Method:**
    1.  Open a new `active.slab`.
    2.  Iterate the **entire B+Tree**.
    3.  If a key points to an "Old Slab", read the value and rewrite it to `active.slab`. Update the B+Tree with the new `ValuePtr`.
    4.  Once an Old Slab has zero references (ref-count tracked or post-scan), delete the file.

## 6\. Go Interface Definition

```go
package goslab

// Immutable configuration
type Options struct {
    DirPath          string
    InlineThreshold  int  // Default 64
    SyncOnCommit     bool // Default false
    Checksums        bool // Default true
}

type DB struct {
    pager    *Pager        // Chunked MMap & CRC32
    slabs    *SlabManager  // Append-only logs
    meta     *MetaPage     // Root & Freelist pointers
    graveyard *Graveyard   // In-Memory Epoch reclamation
}

// Opens the database
func Open(opts Options) (*DB, error)

// --- Write Path ---

func (db *DB) NewBatch() *Batch

type Batch struct {
    // Stores Ptrs for large values, Literals for small values
    ops map[string]operation 
}

func (b *Batch) Put(key, value []byte) error
func (b *Batch) Delete(key []byte) error
func (b *Batch) Commit() error 

// --- Read Path ---

func (db *DB) Get(key []byte) ([]byte, error)

// Range Scan
// Automatically acquires a "Reader Hold" on the current sequence
func (db *DB) Iterator(start, end []byte) *Iterator

type Iterator struct {
    // ...
}

func (it *Iterator) Valid() bool
func (it *Iterator) Next()
func (it *Iterator) Key() []byte
func (it *Iterator) Value() ([]byte, error) 
func (it *Iterator) Close() // Releases the "Reader Hold"

// --- Maintenance ---

// Marks versions older than `keepRecent` as safe for the Freelist
func (db *DB) Prune(keepRecent uint64) error
```
