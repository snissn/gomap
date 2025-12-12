# Performance Design - TreeDB

## 1. Zipper: Reduce Allocations via Pooling and Zero-Copy
**Problem:** `zipper.writeRecursive` allocates a new page buffer and copies the old page buffer for every node visited, causing >50% of allocations.

**Solution:**
1.  **Buffer Pooling:** Use `sync.Pool` to reuse `[]byte` buffers for the *new* page data.
2.  **Zero-Copy Reads:** Use `pager.Get()` (direct mmap access) instead of `pager.ReadPage()` (copy) when reading the *old* node data.

**Design Details:**
*   Add `nodePool *sync.Pool` to `Zipper` struct.
*   Initialize `nodePool` in `New()` to return `make([]byte, page.PageSize)`.
*   In `writeRecursive`:
    *   Acquire `newData` from `z.nodePool.Get()`.
    *   Replace `z.pager.ReadPage(pageID)` with `z.pager.Get(pageID)` to get `oldData` (zero-copy).
    *   Perform merge operations (populating `newData`).
    *   After `z.pager.Write(newPageID, newData)`, the data is copied into the mmap (Verified: `Pager.Write` performs a copy).
    *   Return `newData` to the pool using `z.nodePool.Put(newData)`.
    *   **CRITICAL SAFETY REQUIREMENT:** The `splitKey` returned by `writeRecursive` (and its helpers `mergeLeaf`/`mergeInternal`) **MUST** be a deep copy. It cannot point into the `newData` buffer, because `newData` is returned to the pool and will be overwritten. Use `append([]byte(nil), key...)` to ensure isolation.

## 2. Pager: Targeted Msync
**Problem:** `pager.Sync()` calls `Msync` on *all* chunks, which is expensive (O(N) syscalls) and blocks if synchronous.

**Solution:** Track "dirty" chunks and only sync those.

**Design Details:**
*   Add `dirtyChunks map[int]struct{}` to `Pager` struct.
*   Initialize `dirtyChunks` in `Open`.
*   In `Pager.Write(pageID, data)` (holds `Lock`):
    *   Calculate `chunkIdx`.
    *   Set `p.dirtyChunks[chunkIdx] = struct{}{}`.
*   In `Pager.Sync()`:
    *   **Locking Strategy (Copy-and-Clear):**
        1.  Acquire `p.mu.Lock()` (Write Lock).
        2.  Extract keys from `dirtyChunks` into a local slice `toSync`.
        3.  Reset `dirtyChunks = make(map[int]struct{})`.
        4.  Release `p.mu.Unlock()`.
        5.  Acquire `p.mu.RLock()` (Read Lock) to ensure chunks aren't unmapped during sync.
        6.  Iterate `toSync` and call `unix.Msync` on each chunk.
        7.  Release `p.mu.RUnlock()`.
        8.  Call `p.file.Sync()` (metadata/resize).

## 3. Tree: Zero-Copy Read Path
**Problem:** `Tree.GetEntry` calls `pager.ReadPage` at every level, allocating and copying 4KB per level.

**Solution:** Use direct mmap access for traversal.

**Design Details:**
*   In `Tree.GetEntry(key)`:
    *   Replace `t.pager.ReadPage(currID)` with `t.pager.Get(currID)`.
    *   This returns a direct reference to the mmap region.
    *   `node.NewNode(data)` works the same.
*   In `Tree.Get(key)`:
    *   Call `t.GetEntry(key)`.
    *   The returned `entry.Value` points to mmap.
    *   **Safety:** We must COPY the value before returning it to the user.
    *   `return append([]byte(nil), entry.Value...), nil`.
    *   **Concurrency Note:** Since `Pager.Close()` unmaps memory, calling `Close()` concurrently with `Get()` will cause a crash (SIGSEGV). This is standard behavior for mmap-based DBs; the user must ensure `Close()` is not called while readers are active.

## Safety Sign-off
*   **Memory Safety:** 
    *   **Zipper:** `Pager.Write` copies data, so returning `newData` to the pool immediately is safe. `splitKey` deep copy is mandatory and enforced by design.
    *   **Tree:** Zero-copy logic in `Tree` requires explicit copies at the boundary (`Get`). Internal traversal is safe as long as `Close` is not called concurrently.
*   **Concurrency:** 
    *   `Pager.Sync` uses a split locking strategy. The race between unlocking (step 4) and re-locking (step 5) is benign because `Alloc` (the only other operation that modifies chunks list) only appends, preserving validity of indices in `toSync`.
    *   `Resize` (via `Alloc`) holds `Lock`, so it cannot occur during the critical sections of `Sync` or `Get`.
*   **Durability:** 
    *   Targeted `Msync` maintains the same guarantees as full `Msync` because `Write` (which dirties pages) is mutually exclusive with the `dirtyChunks` extraction in `Sync`. Any write happening *after* extraction will be caught by the *next* `Sync`.

**Status:** Approved for Implementation.
