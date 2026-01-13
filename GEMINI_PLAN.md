# TreeDB Optimization Sprint Plan: Unified Slab/WAL Architecture

**Objective:** Maximize write throughput and storage efficiency by unifying the Vlog/Slab lifecycle into a single "Data Segment" that is written once, compressed, and promoted to permanent storage.

## Phase 1: Shared Compression Infrastructure
**Goal:** Decouple compression logic from `slab` package to allow the unified data writer to use it.
- [ ] **Refactor:** Move `compressionTrainer` and `CompressionConfig` to `TreeDB/internal/compression`.
- [ ] **Refactor:** Update `slab` package to use the new `internal/compression` package.
- [ ] **Refactor:** Move `compressionMetrics` to `internal/compression`.

## Phase 2: Metadata WAL & Compression
**Goal:** Create a lightweight "Metadata Log" that stores structure (`Seq`, `Op`, `Key`, `Pointer`) while the heavy payload lives in the Data Segment. This replaces the traditional "full data" WAL.
- [ ] **Update WAL Writer:** Add `Compression` option to `wal.Options`.
- [ ] **Segment Compression:** In `wal.writeSegment`, compress the payload (Snappy or ZSTD) before writing.
- [ ] **Update WAL Reader:** Decompress segments transparently in `readSegment`.
- [ ] **Benchmark:** Verify throughput gains from smaller, compressed metadata writes.

## Phase 3: Asynchronous Unified Writer
**Goal:** Implement the "Active Data Segment" writer—essentially a Slab writer that supports async buffering and compression.
- [ ] **Unified Writer:** Create `SlabWriter` (or `DataSegmentWriter`) that writes the standard Slab format (`[Key][Value]`).
- [ ] **Async Buffering:** Implement double-buffering (e.g., 4MB userspace buffers) to hide `syscall.Write` latency.
- [ ] **Background Flush:** Dedicated goroutine to flush full buffers to disk.
- [ ] **Sync Barrier:** Ensure `Sync()` waits for buffers to flush.

## Phase 4: Zero-Copy Adoption (The "Holy Grail")
**Goal:** Implement the promotion lifecycle where an Active Data Segment becomes a Read-Only Slab via simple file rename, eliminating the secondary write.
- [ ] **Split Write Path:**
    *   **Data:** Write compressed `[Key][Value]` to Active Data Segment (Async Writer).
    *   **Metadata:** Write `[Seq][Op][Key][Ptr]` to Metadata WAL.
- [ ] **Adoption Logic:** Implement `PromoteToSlab(activePath)` in `SlabManager`:
    *   Close the active file.
    *   Rename `active-N.dat` -> `data-N.slab`.
    *   Update `SlabManager` file registry (O(1) update) to point to the new read-only path.
- [ ] **Integration:** Update `DB.finalizeCommit` (or flush logic) to trigger promotion instead of rewriting values from memtable.

## Execution Order
1.  **Phase 1 (Refactor)** - Prerequisite for shared compression.
2.  **Phase 2 (Metadata WAL)** - Independent win; sets stage for removing data from WAL.
3.  **Phase 3 (Async Writer)** - High-impact throughput win; serves as the "Active Segment" writer.
4.  **Phase 4 (Adoption)** - The final integration that connects the Async Writer to the Slab lifecycle.