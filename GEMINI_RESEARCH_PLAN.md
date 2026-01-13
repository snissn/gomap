# Gemini Research Plan: Slab & WAL Optimizations

**Date:** 2026-01-13
**Branch:** `feature/slab-optimizations`
**Objective:** Optimize TreeDB write throughput and storage efficiency by reducing WAL/Slab duplication, enabling compression in the WAL, and implementing asynchronous slab writes.

## 1. Baseline Performance Analysis

**Benchmark:** `BenchmarkWriteParallelCachedRotationStress`
**Profile Findings:**
- **Bottleneck:** `appendWAL` / `appendWALInline` accounts for ~21% of CPU time.
- **System Calls:** `syscall.syscall` is ~3%, suggesting that while I/O is a factor, the blocking nature of the write path and memory copying (`runtime.memmove` ~10%) are significant.
- **Contention:** High `runtime.usleep` (~60%) indicates threads are frequently waiting, likely due to lock contention in `SlabManager` or `WAL`.

## 2. Research Areas

### 2.1. Vlog/WAL Compression (User Point 2)
**Goal:** Reduce WAL size and I/O bandwidth by compressing values in the Vlog/WAL.
**Hypothesis:** Using the same dictionary-based compression as the Slabs will significantly reduce WAL write volume.
**Implementation Plan:**
- Refactor `compressionTrainer` to be accessible by `vlog` package (move to `internal/compression`).
- Update `vlog.Writer` to accept a compression configuration.
- Modify `vlog` file format to support a "Compressed" flag (e.g., in the `Op` byte).
- **Metric:** Measure WAL size reduction and `appendWAL` CPU usage.

### 2.2. Zero-Copy Flush / Slab Adoption (User Point 1)
**Goal:** Eliminate write amplification where data is written to WAL/Vlog (for durability) and then copied to Slab (for persistence).
**Hypothesis:** If the Vlog file format is compatible with the Slab format, we can "adopt" or "promote" a flushed Vlog segment to become a Slab, avoiding the copy.
**Challenges:**
- **Format Mismatch:** Vlog records include `Seq` (8 bytes) and `Op` (1 byte). Slab records do not.
- **Ordering:** Vlog is append-only by arrival. Slabs are typically sorted or structured by flush logic (though `AppendMany` is just append).
- **Proposal:** Research if we can align the formats or implement a "Raw Slab" mode for Vlog that allows direct promotion.

### 2.3. Asynchronous Slab Writes (User Point 3)
**Goal:** Decouple the `SlabManager.Append` latency from disk I/O.
**Plan (per `AGENTS_SLAB_VLOG_ASYNC.md`):**
- Implement `SlabWriter` with a userspace double-buffer (e.g., 4MB-8MB).
- Create a background goroutine to flush full buffers.
- Implement "Durable Tail Tracking" to ensure `Sync()` waits for buffers to flush.
- **Benefit:** Hides `syscall.write` latency from the hot path.

## 3. Immediate Action Items

1.  **Refactor Compression:** Move `compressionTrainer` to `TreeDB/internal/compression`.
2.  **Prototype Vlog Compression:** Add compression to `vlog` writer.
3.  **Prototype Async Writer:** Implement a simple buffered writer for Slabs.

## 4. Verification

- Rerun `BenchmarkWriteParallelCachedRotationStress` after each change.
- Compare `ns/op`, WAL size, and CPU profile.
