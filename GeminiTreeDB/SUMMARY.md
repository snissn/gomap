# TreeDB Implementation Summary

## Status
**Complete.** All planned phases (1-7) have been implemented, verified, and stress-tested.

## Architecture

TreeDB is a **Hybrid Storage Engine** designed for the Cosmos SDK workload (IAVL+ Tree).

### 1. Storage Layer
- **Index (`index.db`):** 
  - Uses **Chunked Memory Mapping** (64KB chunks) to handle large files efficiently and safely.
  - Stores B+Tree Nodes (Internal and Leaf).
  - Pages are 4KB fixed size.
- **Value Log (`data-*.slab`):**
  - Large values (`> 256 bytes`) are stored in append-only slab files.
  - Slabs are rotated at 4GB.
  - Index stores `ValuePtr` (FileID, Offset, Length) instead of data.
  - This reduces write amplification and memory pressure during tree traversals.

### 2. Transaction Model
- **Single-Writer / Multi-Reader (SWMR):**
  - **Writes:** Serialized via a global lock (`db.mu`).
  - **Reads:** Lock-free Snapshot Isolation.
- **Atomic Commit:**
  - Uses **Copy-On-Write (COW)** ("The Zipper") to build a new tree version without mutating the old one.
  - Persists the new Root ID to a **Double-Buffered Meta Page** (Superblock).
  - Ensures durability with strictly ordered `fsync` calls (Slab -> Index -> Meta).

### 3. Lifecycle & Safety
- **Snapshot Isolation:**
  - Readers acquire a `Snapshot` which pins the `CommitSeq` and the active `SlabSet`.
  - Reference counting prevents deletion of active slab files.
- **Graveyard:**
  - Tracks pages retired by each commit.
  - **Pruner:** Reclaims pages only when they are no longer visible to any active Snapshot (based on `MinPinnedSeq`).
  - **Freelist:** Reclaimed pages are added to an on-disk freelist for reuse (Basic implementation).

### 4. Compaction
- **Ghost Copy:** Background compaction moves live records from old slabs to the active slab.
- **Atomic Switch:** `ApplyCompaction` updates the B+Tree pointers atomically, making the old slab dead.

## Compliance & Verification
- **Cosmos DB Interface:** Fully implemented (`Get`, `Set`, `Delete`, `Iterator`, `Batch`, etc.).
- **Tests:**
  - **Unit Tests:** Coverage for all packages (`page`, `node`, `tree`, `zipper`, `slab`, `db`).
  - **Fuzzing:** Model-based testing verifies strict consistency against a Go map.
  - **Crash Recovery:** Validated via `verify_crash.sh` (Stress -> Kill -9 -> Verify).
  - **Race Detection:** Passed `go test -race`.

## Known Limitations / Future Work
1.  **Freelist Optimization:**
    - The current `freelist.Allocator` is functional but basic. It supports allocating and freeing single pages.
    - Future work: Optimize for contiguous allocations if `Alloc(n)` is needed for larger nodes/blobs.
2.  **Slab Compaction Heuristics:**
    - The `Compactor` mechanism works (`CompactSlab`), but the *policy* to automatically select the best slab (based on Dead Bytes) is not fully wired up to a stats engine.
    - Future work: Implement the "System Tree" stats tracking fully to drive auto-compaction.
3.  **Adaptive Inline Threshold:**
    - Infrastructure allows variable threshold, but the EWMA controller to adjust it dynamically based on load is not implemented (currently static).
4.  **Leaf Node Fragmentation:**
    - Slotted pages do not currently auto-defragment their heap. High update churn on a single leaf might lead to `ErrNodeFull` slightly earlier than optimal. `Zipper` handles this by splitting, so correctness is preserved.

## Repository Structure
- `db/`: Core database logic and public API.
- `tree/`: B+Tree read path and Iterators.
- `zipper/`: B+Tree write path (COW).
- `node/`: Binary page layout.
- `page/`: Raw page/header handling.
- `pager/`: Memory-mapped file management.
- `slab/`: Value log management.
- `lifecycle/`: Graveyard and Reader Registry.
- `compaction/`: Slab compaction logic.
- `cmd/`: Tools (`stress`, `verify`).
