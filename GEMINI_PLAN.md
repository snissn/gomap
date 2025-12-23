# TreeDB Optimization Plan: Closing the Read Latency Gap

**Goal:** Eliminate the read latency regression (specifically `block_lookup` +6700% vs LevelDB) observed in high-churn workloads like `sstore-manytx`. Maintain our 2x write throughput advantage.

**Root Cause:**
1.  **Index Generation Thrashing:** High-frequency commits cause frequent creation/destruction of `mmap` views (`indexGen`), triggering expensive `munmap` syscalls and TLB shootdowns.
2.  **Safety Tax:** Backend `Get` currently forces a copy for safety, punishing large value reads (1MB block bodies).
3.  **Allocation Churn:** Iterators and Snapshots are allocated per-op on some hot paths.

## A. Generation Ghosting (Lifecycle Optimization)

**Objective:** Decouple "Logical Retirement" of an index generation from "Physical Unmap".

**Mechanism:**
1.  **Ghost Cache:** Maintain a `sync.Pool` or LRU-like list of "Retired but Mapped" `indexGen` objects.
2.  **Deferred Unmap:** When `releaseIndex` drops the refcount to zero:
    *   Do **not** call `pager.Close()` immediately.
    *   Instead, move the `indexGen` to a "Ghost List" with a timestamp.
3.  **Scavenger:** A background goroutine (or piggybacked on `Prune`) checks the Ghost List. Only unmap generations older than `GhostTTL` (e.g., 5 seconds) or if total mapped virtual memory exceeds a safety cap (e.g., 64GB).
4.  **Resurrection (Optional):** If a new Snapshot requests an older generation that is currently Ghosted, revive it? (Unlikely needed for Geth, which usually follows the tip).

**Files to Modify:**
*   `TreeDB/db/index_gen.go` (Add lifecycle states)
*   `TreeDB/db/index_gen_db.go` (Implement ghost/scavenge logic)
*   `TreeDB/db/db.go` (Wire up scavenger)

## B. True Zero-Copy for Snapshots

**Objective:** Enable `GetUnsafe` to return direct mmap pointers when safety is guaranteed by an explicit Snapshot.

**Mechanism:**
1.  **Explicit Context:** Currently, `GetUnsafe` on `DB` delegates to `Get` (Copy) because it can't guarantee the mmap won't disappear.
2.  **Snapshot Override:** `Snapshot.GetUnsafe` *knows* the generation is pinned. It should call `tree.GetUnsafe` which calls `slab.Read(ptr)` directly.
3.  **Slab Read Update:** Ensure `SlabManager.Read` allows bypassing the copy if the caller proves they hold a pin.
    *   Current `SlabFile.Read` uses `pread` fallback or mmap. The mmap path returns a slice. We need to ensure this path is robust.
    *   Refine `SlabFile.Read` to accept an `unsafe` flag.

**Files to Modify:**
*   `TreeDB/slab/slab.go` (Audit mmap path safety)
*   `TreeDB/tree/tree.go` (Pass-through flag)
*   `TreeDB/db/api.go` (`Snapshot.GetUnsafe` implementation)

## C. Bloom Filters for User Tree

**Objective:** Short-circuit `Has` and `Get` for non-existent keys (e.g. searching for block bodies that don't exist).

**Mechanism:**
1.  **Structure:** Add a Bloom Filter to the `DBState` or `indexGen`.
    *   *Option 1 (Simple):* Global Bloom for the current `index.db` generation. Rebuilt on open (fast if persisted), updated on write.
    *   *Option 2 (Persistent):* Block-based Bloom filters stored in `index.db` metadata pages? (Too complex for now).
    *   *Decision:* **Memtable-Only Bloom** first. The backend B-Tree is fast enough (log N). The regression is likely seeking/allocating iterators for keys that don't exist.
    *   Wait, the regression is `block_lookup` which reads *existing* blocks. Bloom won't help hit latency.
    *   *Pivot:* **Bloom is Lower Priority** than A/B/D given the profile. The profile showed overhead in `munmap` and `malloc`, not B-Tree search depth. **Defer C** until A/B/D are done.

## D. Iterator & Snapshot Pooling

**Objective:** Zero-allocation hot paths for `Get` and `Iterator`.

**Mechanism:**
1.  **Snapshot Pool:**
    *   `db.AcquireSnapshot()` checks a `sync.Pool`.
    *   `Snapshot.Close()` resets fields and returns it to the pool.
2.  **Iterator Pool:**
    *   `db.Iterator()` checks a pool.
    *   `Iterator` struct needs a `Reset()` method to clear stacks/slices without deallocating the backing arrays.
    *   `Iterator.Close()` returns it to the pool.
3.  **Safety:** Pooled objects must handle "Generation Mismatch" (e.g. if the DB advanced). `Reset` must clear references to old `indexGen` to allow unmapping.

**Files to Modify:**
*   `TreeDB/db/db.go` (Snapshot pooling)
*   `TreeDB/tree/iterator.go` (Iterator pooling and Reset)
*   `TreeDB/db/api.go` (Public API wiring)

## Implementation Order

1.  **Phase 1: Pooling (D)** - Lowest risk, immediate CPU/GC win.
2.  **Phase 2: Ghosting (A)** - Solves the `munmap` thrashing (the likely cause of the huge latency spike).
3.  **Phase 3: Zero-Copy (B)** - Optimizes the bandwidth for large values.

---
*Created by Gemini Agent*
