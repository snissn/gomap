# Verified Cache Optimization Plan

## 1. Problem Statement
In the current architecture, pages are accessed via memory-mapped I/O (mmap). Because the Pager is stateless regarding page content validity, every access to a page (even if repeated frequently) triggers a CRC32 Checksum verification.
*   **Cost:** ~4096 bytes hashed per node visit.
*   **Impact:** High CPU usage on read-heavy workloads (10-20% overhead), reduced throughput.

## 2. Proposed Solution
Implement a **Verified Bitset** (or "Verified Cache") in the Pager. This is a lightweight, volatile (RAM-only) data structure that tracks which pages have already been successfully verified since the database was opened.

### Architecture
*   **Data Structure:** A Bitset (`[]uint64` or `BitMap`).
    *   Each bit corresponds to a Page ID.
    *   Memory Overhead: ~32KB per 1GB of data (negligible).
*   **Lifecycle:**
    *   **Start:** Initialized to all zeros (unverified).
    *   **Read (Hit):** If `Bitset[PageID] == 1`, return data immediately (Zero CPU).
    *   **Read (Miss):** If `Bitset[PageID] == 0`, verify CRC. If valid, set `Bitset[PageID] = 1`.
    *   **Write/Reuse:** When a page is allocated from the Freelist or overwritten, `Bitset[PageID]` MUST be cleared to 0.

## 3. Implementation Steps

### A. Repository: `gomap-gemini/TreeDB`

1.  **Pager (`pager/pager.go`):**
    *   Add `verifiedBits []uint64` to `Pager` struct.
    *   Implement `IsVerified(pageID uint64) bool`.
    *   Implement `MarkVerified(pageID uint64)`.
    *   Implement `MarkUnverified(pageID uint64)`.
    *   Ensure thread safety (Atomic operations or Mutex). Given `Pager` uses `mu sync.RWMutex`, we can protect it there or use `sync/atomic` for high concurrency.
    *   Handle `Alloc` (resize bitset if needed).

2.  **Tree / Node Access (`tree/tree.go`, `db/db.go`):**
    *   Update `Tree.GetEntry` and `db.readMeta`:
        ```go
        if !pager.IsVerified(id) {
             if !node.VerifyChecksum() { return error }
             pager.MarkVerified(id)
        }
        ```

3.  **Invalidation (`freelist/allocator.go`, `pager/pager.go`):**
    *   Update `pager.GetForWrite(id)`: Explicitly call `MarkUnverified(id)`.
    *   Update `Alloc`: Ensure reused pages are cleared.

### B. Repository: `gomap/TreeDB` (Legacy/Reference)

1.  **Pager (`internal/pager/pager.go`):**
    *   Add equivalent Bitset logic.

2.  **Tree Access (`treedb.go`):**
    *   Locate the "Read Page" or "Get Node" logic.
    *   Inject the "Check Bit -> Verify -> Set Bit" logic.

3.  **Invalidation:**
    *   Check `internal/pager/freelist.go` (or wherever `Alloc` happens).
    *   Ensure invalidation on page reuse.

## 4. Safety Considerations
*   **Copy-On-Write:** Since pages are immutable once written (until freed), the cache is naturally coherent.
*   **Corruption:** The only risk is RAM corruption flipping a bit *after* verification. Standard ECC RAM mitigates this.
*   **Concurrency:** Bitset access must be thread-safe.

## 5. Next Steps (Phase 2)
*   Update `AGENTS.md` and `specs/spec.md` in both repositories to reflect this new architectural component.
