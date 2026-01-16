# Sprint: Slab Async Writes & Vlog Dictionary Compression

**Goal**: Further optimize TreeDB write throughput by decoupling disk I/O from the append path and extending dictionary compression to the Value Log.

## Phase 1: Shared Compression Infrastructure
- [ ] **Refactor `compressionTrainer`**: Move from `TreeDB/slab` to a shared internal package (e.g., `TreeDB/internal/compression`) so it can be used by both Slab and Vlog managers.
- [ ] **Unified Sampling**: Allow both Slab and Vlog writes to contribute samples to the shared background trainer.

## Phase 2: Vlog Dictionary Compression (Slab V2 Style)
- [ ] **Vlog Metadata Records**: Implement support for injection of `TypeDictionary` records into the Value Log stream.
- [ ] **Active Dictionary Tracking**: Update `VlogManager` to track the current dictionary per segment.
- [ ] **Compressed Vlog Reads**: Update `page.ValuePtr` and `VlogManager.Read` to handle dictionary-compressed value log records.
- [ ] **Validation**: Verify compression ratios on Celestia mainnet traces (targeting 40-60% Vlog reduction).

## Phase 3: Asynchronous/Buffered Slab Writes
- [ ] **`SlabWriter` Implementation**: Manage a userspace double-buffer (e.g., 4MB-8MB) for active slab writes.
- [ ] **Background Writer**: Dedicated goroutine to flush full buffers using large, amortized `write` syscalls.
- [ ] **Durable Tail Tracking**: Implement a mechanism to ensure `Sync()` blocks until all buffered data is committed.
- [ ] **Error Propagation**: Ensure background write errors are correctly surfaced to the next foreground `Append` call.

## Phase 4: Performance & Bottleneck Re-Analysis
- [ ] **Benchmark Re-run**: Execute `BenchmarkWriteSpeedComparison` and `BenchmarkTraceReplayTimeline` to measure the impact of buffered writes on syscall overhead.
- [ ] **Profile Analysis**: Confirm that `syscall.syscall` is effectively hidden or amortized by the buffering logic.
