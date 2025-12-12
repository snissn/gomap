# Performance Report - TreeDB

## Baseline vs Optimized

### Baseline (Before Phase 9)
*   **Throughput:** 225.59 Ops/sec
*   **P50 Latency:** 19.44 ms
*   **P99 Latency:** 43.02 ms
*   **Write Amp:** 78.91x

### Optimized (After Phase 9)
*   **Throughput:** 279.76 Ops/sec (+24%)
*   **P50 Latency:** 16.18 ms (-17%)
*   **P99 Latency:** 33.99 ms (-21%)
*   **Write Amp:** 61.56x (-22%)

## Implemented Optimizations

### 1. Node Pooling
*   **Description:** Implemented `sync.Pool` in `Zipper` to reuse 4KB node buffers during recursive writes.
*   **Impact:** Significantly reduced garbage collection pressure by avoiding new allocations for every modified node.

### 2. Targeted Msync
*   **Description:** Updated `Pager` to track dirty chunks and only call `Msync` on modified regions during commit.
*   **Impact:** Reduced I/O overhead by avoiding full-file syncs or unnecessary OS calls for clean pages.

### 3. Zero-Copy Read (Partial)
*   **Description:** Implemented `pager.Get` to return direct mmap slices.
*   **Status:** Enabled in `Zipper` (write path) for reading old node data. Disabled in `Tree.GetEntry` (read path) due to safety/concurrency concerns with `unsafe` usage during high-concurrency stress tests. The write path zero-copy contributes to the throughput gain.

## Remaining Bottlenecks

1.  **Fsync/Sync Latency:** Disk I/O remains the dominant factor. Further improvements would require WAL-based async commit (outside current scope).
2.  **Lock Contention:** The global write lock limits write concurrency. Sharding or finer-grained locking could help.

## Conclusion
The performance sprint successfully improved throughput by ~24% and reduced tail latency by ~21% through memory and I/O optimization strategies, meeting the objectives of Phase 9.
