# Agent Plan: Unified WAL/Slab Architecture

This document tracks the architectural evolution to unify the Write-Ahead Log (WAL) and Value Log/Slab storage.

## Context / Problem

- Currently, `TreeDB` has two distinct value storage paths:
  1. **WAL (`vlog`):** Managed by the `caching` layer. Ephemeral, circular buffer (truncated by `Options.ValueLog.MaxRetainedBytes`), optimized for append speed.
  2. **Slabs (`data/*.slab`):** Managed by the `backend`. Permanent, refcounted/GC'd, optimized for long-term storage and delta-compaction.
- We attempted to use `Options.ValueLog.PointerThreshold` to write small values (32-64 bytes) to the WAL and store pointers in the Index.
- **Critical Bug:** The `caching` layer truncates old `vlog` files (treating them as ephemeral WAL), but the Index permanently references them. This causes `vlog file not found` corruption.
- **Goal:** Allow large/medium values to land in the WAL initially (fast write), but ensure they are safely transitioned to permanent storage (Slabs) or the WAL segment is promoted to permanent status, without data loss or corruption.

## Proposed Design

We aim to implement a lifecycle where values land in the WAL and are "compacted as appropriate into the slab".

### 1. Safety First (Immediate)
- **Status:** Done (in operation).
- **Action:** Disable `Options.ValueLog.PointerThreshold` (set to 0) in production/benchmarks until the architecture is fixed. This forces all values to be copied into the Index (or Slabs via `backend` logic) during flush, preventing dependencies on ephemeral logs.

### 2. Copy-on-Flush (The "Compaction" Approach)
- **Concept:** The `caching` layer writes values to `vlog`. The Memtable stores pointers.
- **Change:** During `Flush` (Memtable -> Backend):
  - Detect pointers that reference `vlog` files.
  - **Dereference and Copy:** Read the value from `vlog` and write it to the Backend (which puts it into `data/*.slab` or inline in `index.db`).
  - **Rewire:** Update the entry to point to the new location (or be inline).
- **Benefit:** Decouples WAL lifecycle from Backend lifecycle. WAL can be safely truncated once flushed.
- **Cost:** Write amplification (write to WAL, read from WAL, write to Slab). But this is standard LSM behavior (Compaction).

### 3. Vlog Promotion (Zero-Copy Optimization)
- **Concept:** If a `vlog` segment is "full" and completely flushed, move the file from `wal/` to `data/` and register it as a read-only Slab.
- **Challenges:**
  - `vlog` might contain mixed data (some flushed, some not).
  - `vlog` contains "Batch" framing/checksums which might differ from Slab format.
  - Ownership transfer complexity.

### 4. Shared Lifecycle (Refcounting)
- **Concept:** Backend `GC` becomes aware of `wal/` files.
- **Change:** `caching` layer does not delete `vlog` files based on size. Instead, it marks them "candidate for deletion". Backend `GC` scans Index. If `vlog` is unreferenced, it is deleted.
- **Challenge:** Cross-layer coupling.

## Testing Strategy (Test Driven Development)

We need rigorous testing to validate the fix (likely **Copy-on-Flush** is the robust starting point).

### Test Case: `TestFlushMovesValuesToSlab`
- **Setup:** `Options.ValueLog.PointerThreshold=1`. Write data. Verify it lands in `vlog`.
- **Action:** Flush Memtable.
- **Assert:**
  - `vlog` file can be deleted/truncated without data loss.
  - Backend `Get` succeeds.
  - Backend storage shows values are now in `slab` (or inline), NOT pointing to the deleted `vlog`.

### Test Case: `TestVlogTruncationSafety`
- **Setup:** Small `Options.ValueLog.MaxRetainedBytes`.
- **Action:** Write continuous stream of data. Ensure `Flush` keeps up.
- **Assert:** Old `vlog` files are deleted, but data remains readable (because it was moved).

## Implementation Plan

1.  **Reproduce:** Create a test case that demonstrates the `vlog file not found` corruption when `Options.ValueLog.PointerThreshold > 0` and `Flush` occurs but `vlog` is truncated.
2.  **Implement Copy-on-Flush:** Modify `caching/flush.go` (or `memtable` iterator) to resolve pointers during flush iteration.
3.  **Verify:** Run the reproduction test and confirm it passes.
4.  **Optimize:** Consider "Vlog Promotion" only if Copy-on-Flush proves too slow (IO heavy).

## Current Status

- **Blocked:** `Options.ValueLog.PointerThreshold` usage is disabled.
- **Next:** Implement reproduction test for `Copy-on-Flush` validation.
