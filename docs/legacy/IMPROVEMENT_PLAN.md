# Legacy: Gomap Improvement Plan

This document is historical. The active roadmap lives in `TODO.md` and `@AGENTS.md`.

This document outlines the plan to improve the quality, stability, and performance of the `gomap` library. We will follow a Test-Driven Development (TDD) and Benchmark-Driven Development approach.

## Phase 1: Fix Resource Leaks (Priority: High)
**Objective:** Prevent file descriptor and disk space leaks during the hash map resizing process.

*   **Issue:** The `resize()` function creates a new index file but fails to close and delete the old one.
*   **Strategy:**
    1.  **Reproduce:** Create a new test case `TestResizeLeak` in `gomap_test.go` that triggers multiple resizes and asserts that the number of open file descriptors does not grow proportionally.
    2.  **Fix:** Modify `replaceHashmap` in `resize.go` to:
        *   Close the old memory-mapped file.
        *   Close the old file handle.
        *   Remove the old file from disk.
    3.  **Verify:** Ensure `TestResizeLeak` passes.

## Phase 2: Architecture Cleanup - "Dead Code" Slab (Priority: Medium)
**Objective:** Remove confusing and unused code to improve maintainability.

*   **Reflection on Mmap Optimization:**
    *   *User Question:* "Is [mmap for slabs] able to land as an effective optimization?"
    *   *Analysis:* For an append-only log (like the slab file), standard buffered `write()` is typically faster and more robust than `mmap`. Mmap introduces overhead for page faults, requires file extension management (handling SIGBUS), and offers fewer guarantees about when data hits the disk compared to `write()` + `fsync()`. Mmap excels at random read access, but for sequential writes, the kernel's page cache optimization for `write()` is hard to beat.
    *   *Conclusion:* The current implementation creates an mmap (`slab`) but performs all I/O on a separate file handle (`slab-real`). This is confusing and wasteful. We should remove the mmap write path unless we intend to fully rewrite the storage engine to be a memory-mapped database (like LMDB), which is a larger scope.
*   **Strategy:**
    1.  Remove `h.slabMap` from the `Hashmap` struct.
    2.  Remove `openMmapSlab` logic that creates the unused `slab` file.
    3.  Simplify `initN` to only open `slab-real` for appending.

## Phase 3: Performance Optimization (Priority: Low/Future)
**Objective:** optimize batch writes.

*   **Current State:** `HashmapDistributed` does not expose a batch API. The internal `Hashmap.addManyKeys` uses an inefficient "goroutine per item" strategy (`ConcurrentMap`).
*   **Strategy:**
    1.  **Benchmark Baseline:**
        *   Create a benchmark in `benchmark/` that measures `Add` performance.
        *   *Note:* `HashmapDistributed` needs an `AddMany` method to properly benchmark batch performance.
    2.  **Implement Batch API:**
        *   Add `AddMany(items []Item)` to `HashmapDistributed`.
        *   Logic: Group items by shard index (`hash(key) % num_shards`) and call `shard.addManyKeys` for each group in parallel (one goroutine per shard).
    3.  **Optimize Internal Logic:**
        *   Refactor `Hashmap.addManyKeys` to remove the per-item goroutine spawn. Since the shard is locked during this operation, simple sequential insertion (or batched with a fixed-size worker pool) will be significantly faster and reduce scheduler pressure.
    4.  **Verify:** Run benchmarks to demonstrate throughput improvement.

## Phase 4: Code Quality & Go Idioms
**Objective:** Improve library stability.

*   **Issue:** Library uses `panic` and `log.Fatal` for recoverable errors.
*   **Strategy:**
    *   Update `Hashmap` methods to return `error`.
    *   Update `HashmapDistributed` to propagate these errors.
    *   Add tests for failure conditions (e.g., read-only filesystem).

## Phase 5: Micro-Optimizations (Priority: Low)
**Objective:** Reduce garbage collection pressure and syscall overhead.

*   **Issue:** `encodeuint64` allocates a new `[]byte` on every call. `unmarshalItemFromSlab` performs two syscalls per read.
*   **Strategy:**
    *   **Zero-Allocation Encoding:** Rewrite `encodeuint64` to write directly into the destination buffer or use `binary.LittleEndian.PutUint64`.
    *   **Syscall Reduction:**
        *   In `unmarshalItemFromSlab`, perform a single optimistic read (e.g., 64 bytes) that covers the header and small keys/values. Only read again if the data exceeds the buffer.
        *   Buffer writes in `addSlab` using `bufio.Writer` or explicit batching.
    *   **Hashing:** Evaluate switching from `fnv1` to `xxhash` for better performance and collision resistance.

---
**Next Step:** Await user approval to begin Phase 1.
