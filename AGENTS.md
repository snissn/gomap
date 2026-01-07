# Agent Plan: Unified WAL/Slab Architecture

This document tracks the architectural evolution to unify the Write-Ahead Log (WAL) and Value Log/Slab storage.

## Context / Problem

- Currently, `TreeDB` has two distinct value storage paths:
  1. **WAL (`vlog`):** Managed by the `caching` layer. Ephemeral, circular buffer.
  2. **Slabs (`data/*.slab`):** Managed by the `backend`. Permanent, refcounted/GC'd.
- **Critical Bug Resolved:** The `caching` layer truncates old `vlog` files, but the Index permanently referenced them if `ValueLogPointerThreshold` was used. This caused `vlog file not found` corruption.
- **Solution Implemented:** **Copy-on-Flush**. When flushing Memtable to Backend, we now force resolution of `vlog` pointers and write the full value to the Backend. This ensures Backend stores data in permanent Slabs (or Inline), decoupling it from ephemeral WAL files.

## Status

- **Architecture:** Copy-on-Flush implemented in `caching/db.go`.
- **Verification:** Comprehensive tests in `caching/unified_wal_comprehensive_test.go`:
  - `TestUnifiedWAL_SplitLog_Flow`: Verified values move from Vlog -> Slab.
  - `TestUnifiedWAL_InterleavedWrites`: Verified mixed workloads.
  - `TestUnifiedWAL_LargeBatch`: Verified multi-segment handling.
  - `TestUnifiedWAL_CrashRecovery`: Verified replay works (Double Write mode).
- **Production:** `ValueLogPointerThreshold` is currently **DISABLED** (set to 0) in `run_celestia.sh` to ensure maximum stability while the new architecture "soaks" in tests. Re-enabling it (e.g. to 32) is now safe from a corruption standpoint, but led to "missing key" issues during Snapshot Restore in integration tests (Run 10), likely due to a separate issue in `ApplySnapshot` batching or backend indexing which needs investigation.

## Next Steps

1.  **Monitor:** Watch `celestia_run_12` (Threshold=0) for long-term stability and vacuum behavior.
2.  **Investigate:** Why did `ValueLogPointerThreshold=32` cause "missing validator set" during Snapshot Restore despite Copy-on-Flush? (Suspect: Batch handling during massive restore).
3.  **Optimize:** Once stable, re-enable `ValueLogPointerThreshold` to reduce write amplification (only one heavy write to WAL, then copy to Slab).

## Testing Strategy

The test suite `TreeDB/caching/unified_wal_comprehensive_test.go` serves as the regression guard.
Run with: `go test -v ./TreeDB/caching -run TestUnifiedWAL`
