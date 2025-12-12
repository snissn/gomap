# TreeDB Implementation & Testing Plan

This document outlines the step-by-step plan to implement **TreeDB**, a persistent key-value store for the Cosmos SDK, strictly adhering to `specs/spec.md` and `specs/test-spec.md`.

## Phase 1: Foundation - Storage Layer

**Goal:** Establish the physical file handling for Index (Pages) and Data (Slabs).

### 1.1 Page Architecture (`page`, `layout`)
- [ ] Define global constants (`PageSize=4096`, `InlineThreshold=256`).
- [ ] Implement `PageHeader` struct (16 bytes) with serialization/deserialization.
- [ ] Implement `ValuePtr` struct (16 bytes) with `Offset`, `Length`, `FileID`.
- [ ] Implement `CRC32C` helper functions.
- [ ] **Test:** Unit tests for Header/ValuePtr binary encoding and alignment.

### 1.2 The Pager (`pager`)
- [ ] Implement `Pager` struct managing `index.db`.
- [ ] Implement `Mmap` logic with chunking (default 256MB).
- [ ] Implement `Alloc(count)` to grow file and return new PageIDs.
- [ ] Implement `Read(pageID)` and `Write(pageID, data)`.
- [ ] Implement `Truncate` safety checks (grow only).
- [ ] **Test:** `pager_test.go` covering growth, boundary reads, and panic safety on unmap.

### 1.3 The Slab Manager (`slab`)
- [ ] Implement `SlabFile` struct (FileID, os.File, RefCount).
- [ ] Implement `SlabManager` to manage active/immutable slabs.
- [ ] Implement `Append(key, value)` logic (Record Format: CRC + Lens + Data).
- [ ] Implement `Read(valuePtr)` logic with CRC verification.
- [ ] Implement Rotation logic (4GB limit).
- [ ] **Test:** `slab_test.go` covering append/read round-trip, rotation, and corrupted record detection.

---

## Phase 2: B+Tree Structure & Nodes

**Goal:** Implement the in-memory representation and manipulation of B+Tree nodes.

### 2.1 Node Layout (Slotted Pages)
- [ ] Implement `Node` wrapper around raw Page data.
- [ ] Implement **Leaf Node** layout: Directory (offsets), Heap (Entries).
    - Entry Format: `KeyLen | ValLen | Flags | Key | Val/Ptr`.
- [ ] Implement **Internal Node** layout: Directory, Heap (Child Pointers).
- [ ] **Test:** `node_test.go` verifying entry insertion, space accounting, and binary search within a node.

### 2.2 Tree Operations (Single Threaded)
- [ ] Define `Tree` struct holding the `Pager` and `RootPageID`.
- [ ] Implement `Get(key)`: Binary search traversal.
- [ ] Implement `Set` helpers (basic node splitting logic basics, though full recursive write comes in Phase 3).
- [ ] **Test:** Manually construct a 2-level tree and verify `Get` works.

---

## Phase 3: The "Zipper" - Write Path & Commit

**Goal:** Implement the Batch Write, Copy-On-Write (COW), and Atomic Commit.

### 3.1 Batch & Pre-Write
- [ ] Implement `Batch` struct (`map[string]entry`).
- [ ] Implement **Phase 1**: Iterate batch, write large values (`> InlineThreshold`) to Active Slab, store `ValuePtr` in map.
- [ ] **Test:** Verify batch pre-write appends to slab and updates map with pointers.

### 3.2 Recursive Merge (COW)
- [ ] Implement **Phase 2**: `writeRecursive(node, batchKeys)`.
    - Clone node (COW).
    - Merge batch ops into node.
    - Handle Split (creating new siblings) and propagate up.
- [ ] Implement `Freelist` page handling (allocate from free vs end of file).
- [ ] **Test:** `zipper_test.go`: Insert keys causing splits, verify old pages remain untouched (COW).

### 3.3 Atomic Commit & Superblocks
- [ ] Implement `MetaPage` (Superblock) layout (Pages 0 & 1).
- [ ] Implement **Phase 3**:
    - `fsync` active slab.
    - Write new root to Index.
    - Update and write new `MetaPage` (Seq+1).
    - `fsync` Index.
- [ ] Implement `Open()` recovery: Read both metas, pick highest valid Seq, repair active slab tail.
- [ ] **Test:** `recovery_test.go`: Simulate torn writes, verify recovery chooses correct meta.

---

## Phase 4: Read Path, Snapshots & API

**Goal:** Implement safe concurrent reads and the public Cosmos API.

### 4.1 Snapshot Isolation
- [ ] Implement `DBState` atomic pointer (`CommitSeq`, `RootPageID`, `SlabSet`).
- [ ] Implement `AcquireSnapshot()` and `ReleaseSnapshot()`.
- [ ] Integrate Reference Counting on Slabs.

### 4.2 Cursors & Iterators
- [ ] Implement `CursorItem` and `CursorStack`.
- [ ] Implement `Iterator` (Forward): Logic to drill down left.
- [ ] Implement `ReverseIterator`: Logic to drill down right.
- [ ] **Test:** `iterator_test.go`: Full traversal, range queries, "Step" logic.

### 4.3 Public API
- [ ] Implement `DB` struct satisfying `cosmos-db` interface.
- [ ] Wire up `Get`, `Set`, `Delete`, `Iterator`, `Close`.
- [ ] Implement `NewBatch` and `NewBatchWithSize`.
- [ ] **Test:** `api_test.go`: Basic CRUD cycles.

---

## Phase 5: Lifecycle & Safety

**Goal:** Implement the Graveyard, Pruning, and Resource Management.

### 5.1 The Graveyard
- [ ] Track `RetiredPages` in `Batch` commit.
- [ ] Store retired pages in in-memory Graveyard (`CommitSeq -> []PageID`).

### 5.2 Reader Registry & Pruning
- [ ] Implement `ReaderRegistry` to track active `CommitSeq`s.
- [ ] Implement `MinPinnedSeq()` calculation.
- [ ] Implement `Prune()`:
    - Move pages from Graveyard to On-Disk Freelist if `RetiredSeq < MinPinnedSeq`.
    - Handle `KeepRecent` history window.
- [ ] **Test:** `lifecycle_test.go`: Verify pages are not reused while an Iterator is open.

### 5.3 Internal Metadata
- [ ] Implement "System Tree" (dual root in Superblock).
- [ ] Implement Internal Key encoding (`0x00 | ...`).
- [ ] Track Slab Stats (`DeadBytes`) in System Tree.

---

## Phase 6: Compaction & Advanced Features

**Goal:** Implement Slab Compaction and Adaptive Thresholds.

### 6.1 CAS & Tree Extensions
- [ ] Add `CompareAndSwap(key, old, new)` to `Tree`.
- [ ] Ensure CAS works under the same write lock as Batch.

### 6.2 Slab Compaction
- [ ] Implement `Compactor`:
    - Scan "System Tree" for high dead-byte slabs.
    - "Ghost Copy": Read old slab, append to "Target Slab", prepare CAS batch.
    - Execute CAS micro-batches.
- [ ] Implement Zombie Slab deletion (check RefCount).
- [ ] **Test:** `compaction_test.go`: Fill slab with updates, compact, verify space reclaimed.

### 6.3 Adaptive Inline Threshold (Optional/Advanced)
- [ ] Implement Telemetry (EWMA) for leaf fill, split rate, etc.
- [ ] Implement `Controller` to adjust `InlineThreshold` per commit.

---

## Phase 7: Verification & Hardening

**Goal:** comprehensive testing and failure simulation.

### 7.1 Fuzzing
- [ ] Implement `fuzz_test.go` using `gopter` or Go Fuzz.
- [ ] Model-based testing (compare against Go map).

### 7.2 Crash Simulation
- [ ] Create `cmd/stress`: A tool to run random ops.
- [ ] Script to `kill -9` stress tool and verify DB integrity on restart.

### 7.3 Cosmos Compliance
- [ ] Import `github.com/cosmos/cosmos-db` and run their compliance suite.

---

## Work Procedure

1.  **Read Context**: Before starting a phase, re-read relevant spec sections.
2.  **Implementation**: Write code + Unit Tests.
3.  **Verification**: Run tests.
4.  **Commit**: Git commit with clear message.
5.  **User Check**: Briefly pause for user feedback if major design decisions arise.
