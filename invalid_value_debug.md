# Debugging Context: "invalid value id length in Get"

## 1. Issue Description
**Error:** `multistore restore: failed to load store: invalid value id length in Get: 0`
**Context:** This error occurred during a Celestia mainnet sync using `TreeDB` as the backend. The error originates from `TreeDB/db/db.go` (or `api.go`) when `Get()` encounters a leaf entry with the `FlagValueID` flag set, but the associated value payload is not exactly 8 bytes long (in this case, it was 0 bytes).

## 2. Configuration (from `celestia_testing_info.md`)
The environment where the error occurred used the following settings:
- `DB_BACKEND=treedb`
- `TREEDB_LEAF_PREFIX_COMPRESSION=1` (Enabled)
- `TREEDB_FORCE_VALUE_POINTERS=1` (Forces values to be pointer-backed, though behavior depends on inline threshold)
- `TREEDB_SLAB_COMPRESSION=zstd`

## 3. Root Cause Analysis
The error indicates database corruption. A `FlagValueID` entry serves as a pointer to the "Value Index" (System Tree), and it *must* contain exactly 8 bytes (the `ValueID` uint64).

The corruption likely occurred during the write path (`AddLeafEntry`). If an upstream component (e.g., `Batch` processing in `db/batch.go` or `Zipper` merge logic) incorrectly passed an empty slice for the value while setting `FlagValueID`, the node builder would previously write this invalid entry to disk without complaint. Upon subsequent read (`Get`), the validation check would fail.

**Suspect Areas:**
- `TreeDB/db/batch.go`: Logic that transforms large values into `ValueID`s (`write` / `writeSerialized`).
- `TreeDB/zipper/zipper.go`: Logic that merges batch entries into the tree.
- `TreeDB/node/builder.go` & `leaf.go`: The low-level routines writing the node format.

## 4. Fix Implementation
To prevent this corruption from being written to disk, strict validation was added to the node insertion paths.

**Files Modified:**
- `TreeDB/node/node.go`: Added `ErrInvalidValueIDLength` to common errors.
- `TreeDB/node/builder.go`: Updated `AddLeafEntry` to check:
  ```go
  if flags&FlagValueID != 0 && len(value) != 8 {
      return ErrInvalidValueIDLength
  }
  ```
- `TreeDB/node/leaf.go`: Updated `AddLeafEntry` (used for updates/inserts in existing nodes) with the same check.

Now, any attempt to write a malformed `ValueID` entry will return an error at write time, preventing corruption and likely identifying the buggy caller immediately.

## 5. Secondary Issue: `index.db` Ballooning
**Observation:** After fixing the crash, it was observed that `index.db` grew to 16GB for a workload with ~1.6GB of active data (~10x bloat).
**Root Cause:** The `ProfileFast` and `ProfileBench` high-level presets in `TreeDB/profiles.go` explicitly enabled `PreferAppendAlloc = true`. This flag tells the page allocator to ignore the freelist and always append new pages to the end of the file. This is intended for short, high-speed benchmarks but causes infinite file growth in long-running nodes (like Celestia sync) unless background vacuum is very aggressive.
**Fix:** Removed `PreferAppendAlloc = true` from the default profile definitions in `TreeDB/profiles.go`. This ensures that pages are reused from the freelist by default, maintaining a stable file size.

## 6. Tertiary Issue: Corrupted Slab Stats (26PB)
**Observation:** `treemap info` reported `treedb.slabs.total_bytes=26691663622943823` (approx 26 Petabytes) despite the slab file being only 4GB.
**Root Cause:** A data race was found in `TreeDB/db/batch.go`. The `writeOptimistic` and `writeSerialized` methods were taking a direct reference to the `SlabWriteBytesByFile` map from the batch. Since batches are pooled and reused, another goroutine could clear or modify this map while `finalizeCommit` (and `applySystemUpdates`) was still processing it, leading to corrupted summation.
**Fix:** Modified `TreeDB/db/batch.go` to always copy the map before passing it to `finalizeCommit`. Additionally, added safety caps in `applySystemUpdates` to ignore unreasonably large deltas (>100GB per commit).

## 7. Pruner Configurability
Added environment variable support for tuning background pruning without changing application code:
- `TREEDB_BACKGROUND_PRUNE_INTERVAL`: Duration (e.g. `100ms`) or milliseconds.
- `TREEDB_BACKGROUND_PRUNE_MAX_PAGES`: Integer (e.g. `8192`).

## 9. Secondary Bug: Vacuum Corruption with `ForceValuePointers`
**Issue:** When `TREEDB_FORCE_VALUE_POINTERS=1` is enabled, the Vacuum/Compaction process (`bulk.BuildWithOptions`) incorrectly identified existing `ValueID` entries (which are 8-byte inline values pointing to the system tree) as "inline user values" that needed to be migrated to the Slab file.
**Consequence:** The migration logic cleared the 8-byte ValueID from the leaf entry (`val = nil`) and set `FlagPointer`, but then called `AddLeafEntry` with `FlagValueID` still set. This triggered the new safety check (`invalid value id length`), aborting the vacuum.
**Fix:** Updated `TreeDB/internal/bulk/builder.go` to explicitly exclude entries with `FlagValueID` from the "Force Pointer" migration path.
**Verification:** Added `TestVacuum_ForceValuePointers_PreservesValueIDs` to `TreeDB/db/value_id_integrity_test.go`.

## 10. Tertiary Bug: In-Place Compaction Skipped `ValueID` Entries
**Issue:** The fast-path for slab compaction (`UpdateValuePtrInPlace`) was designed to only update entries with `node.FlagPointer` set. It did not know how to handle `ValueID` entries, where the actual pointer is stored in the System Tree.
**Consequence:** When the compactor moved data for a key using `ValueID`, the in-place update would return `updated=false` and the pointer in the system tree was never updated. This caused the database to point to old, potentially deleted slab segments.
**Fix:**
1.  Modified `node.UpdateLeafValuePtr` to support updating 16-byte inline values (which is how `ValueID` mappings are stored in the system tree).
2.  Modified `tree.UpdateValuePtrInPlace` to correctly decode the pointer from inline values.
3.  Updated `db.ApplyCompactionMicroBatches` to detect `ValueID` entries and perform the in-place update on the System Tree.
**Verification:** Added `TestApplyCompaction_ValueID_InPlace` to `TreeDB/db/value_id_compaction_test.go`.

## 11. Summary of Fixes
1.  **Safety:** Added `ErrInvalidValueIDLength` check in `AddLeafEntry` to prevent writing corrupted (0-byte) ValueIDs.
2.  **Config:** Removed `PreferAppendAlloc=true` from `fast`/`bench` profiles to prevent `index.db` infinite growth (ballooning).
3.  **Tuning:** Increased default Pruner settings (`PruneMaxPages=40960`, `PruneInterval=100ms`) to handle high-throughput syncs.
4.  **Concurrency:** Fixed a data race in `batch.go` that caused corrupted slab stats (26PB).
5.  **Vacuum Logic:** Fixed Vacuum logic to respect `ValueID` entries when `ForceValuePointers` is active.
6.  **Compaction Logic:** Fixed In-Place compaction to support `ValueID` pointer updates in the System Tree.

---

# 12. Investigation: State Sync Stalls When `TREEDB_BENCH_DISABLE_BG=1`

## 12.1 Symptom (Server Run)
- `TREEDB_BENCH_DISABLE_BG=1` causes the Celestia mainnet harness to stall during state sync.
- `TREEDB_BENCH_DISABLE_BG=0` completes successfully.
- Current run directory: `/home/mikers/.celestia-app-mainnet-treedb-20260108092955`.
- `curl http://127.0.0.1:36657/status` reports `latest_block_height=0` and `catching_up=true`.

## 12.2 Log Evidence (State Sync)
From `/home/mikers/.celestia-app-mainnet-treedb-20260108092955/sync/node.log`:
- State sync starts and applies chunks 0-66.
- The last chunk (67) is fetched, but **never logged as applied**.
- No TreeDB errors/panics appear in the log; only P2P churn and seed lookup failures.

Key excerpts:
- `Starting state sync` / `Snapshot accepted, restoring`
- `Applied snapshot chunk to ABCI app ... chunk=66`
- `Fetching snapshot chunk ... chunk=67`
- **No** `Applied snapshot chunk ... chunk=67` / `State sync complete`

## 12.3 Relevant Code Paths
The behavior change for `TREEDB_BENCH_DISABLE_BG=1` is in `cosmos-db/treedb.go`:
- `BackgroundIndexVacuumInterval = -1`
- `BackgroundCompactionInterval = -1`
- `BackgroundCheckpointInterval = -1`
- `BackgroundCheckpointIdleDuration = -1`
- `MaxWALBytes = -1`

This disables all auto-checkpointing and background vacuum/compaction. TreeDB's cached flush loop still runs, but no periodic checkpoint is forced.

## 12.4 Hypotheses
1. **Snapshot Apply Stalls on Final Chunk**: `ApplySnapshotChunk` for chunk 67 is blocked (possible DB write/backpressure stall).
2. **No Auto-Checkpoint = Unbounded WAL**: lack of checkpoints may interact with large snapshot batches (e.g., huge WAL or flush backlog thresholds).
3. **Flush Backpressure Deadlock**: writers are blocked by backlog and cannot progress if a flush worker stalls (no explicit errors logged).

## 12.5 Proposed Next Steps
- Collect goroutine dump / pprof during stall:
  - `pprof` is enabled at `localhost:6062` by the harness script.
- Capture TreeDB stats during stall (if possible) to see backlog/flush queues.
- Re-run with `TREEDB_BENCH_DISABLE_BG=1` but **leave auto-checkpoint enabled** (only disable vacuum/compaction) to isolate checkpoint-related stalls.

## 12.6 Test-Driven Resolution (Planned)
Goal: Create a regression test that simulates a large snapshot-style write workload with background checkpoint disabled and ensures the final write completes and data is readable.

Planned test sketch (not yet implemented):
1. Open TreeDB in cached mode with:
   - `BackgroundIndexVacuumInterval = -1`
   - `BackgroundCheckpointInterval = -1`
   - `BackgroundCheckpointIdleDuration = -1`
   - `MaxWALBytes = -1`
2. Apply multiple large batches (simulate snapshot chunks).
3. Verify reads succeed for all keys without needing a manual checkpoint.
4. Add a timeout guard to detect hangs.

This test should fail if the current stall is reproducible in a smaller environment.

## 12.7 Goroutine Dump / Pprof Capture (Stalled Run)
Captured from the stalled node on `192.168.0.132` using the harness pprof port `6062`:
- Goroutine dump: `/home/mikers/pprof_goroutine_20260108102130.txt`
- CPU profile (30s): `/home/mikers/pprof_profile_20260108102137.pb.gz`

## 12.8 Goroutine Analysis & Fix Plan
### Observation (Stall)
The goroutine dump shows the state sync restore thread blocked in TreeDB backpressure:
- `goroutine 4137 [sync.Cond.Wait, 46 minutes]` -> `caching.(*DB).waitForStop` -> `caching.(*Batch).write` -> `kvstore/adapters/treedb.(*batch).Commit` -> `iavl.(*Importer).Commit`.
- `goroutine 701` is waiting in `snapshots.Manager.RestoreChunk` for the restore to finish, which never happens because the write is blocked.

### Hypothesis
Backpressure is triggered (`queueBacklogBytes >= stopBytes`), but no background flush is scheduled, so `waitForStop` only flushes one memtable (via `flushSome`) and then waits forever for a `bpCond` signal that never arrives. This is consistent with `TREEDB_BENCH_DISABLE_BG=1` and backlog created without a flush trigger (e.g. iterator-driven rotations, or DisableWAL delete-range paths).

### Fix
Ensure `waitForStop` schedules a background flush before waiting:
- Add `db.TriggerFlush()` in `TreeDB/caching/db.go` inside `waitForStop` when backlog exceeds stop.

### Regression Test
Added `TreeDB/caching/backpressure_wait_test.go`:
- `TestWaitForStopSchedulesFlush` creates queued memtables via `rotateMemtableLocked(false)` (no flush signal), forces backlog above stop, and asserts a `Set()` completes within a timeout.

## 12.9 Trace -> Bench Program (Local)
- Added `TreeDB/bench_trace_replay_test.go` to benchmark TreeDB using a trace summary (env-driven).
- Uses `TREEDB_TRACE_SUMMARY` plus optional tuning envs to replay phases with iterator activity in the benchmark harness.

## 12.10 Memtable Reuse Optimization
- Added a guarded memtable reuse pool for skiplist memtables to reduce allocation churn across rotations.
- Introduced a reader-tracking reclaimer that defers recycling until no active readers, with a background recycle loop.
- Wrapped iterator returns to release reader count on Close; Get/Has/GetAppend now bracket memtable reads.
- Benchmark (production config, scale=1.0) improved from ~95ms/op to ~86ms/op on local Apple M3.

## 12.11 Iterator Rotation Reduction (Optional)
- Added `IteratorMutableMaxBytes` option to allow iterators to read from mutable memtables without forcing rotations when the mutable size is small.
- Bench with `TREEDB_TRACE_ITERATOR_MUTABLE_MAX_BYTES=4194304` showed ~55ms/op vs ~72ms/op (scale=1.0, local Apple M3).

## 12.12 Server Trace Run (Iterator Mutable Max Bytes)
- Updated server run config to set `TREEDB_ITERATOR_MUTABLE_MAX_BYTES=4194304`.
- Started new server run via `nohup ./run_celestia.sh > ~/celestia_trace_run.log 2>&1 &` (PID `1239177`).
- Run completed successfully; log shows sync caught up and stopped cleanly.
- Trace output: `/home/mikers/treedb_trace_20260109064203.jsonl` and summary `/home/mikers/treedb_trace_20260109064203.summary.json`.
- Pulled summary to local: `tmp_trace_summary_20260109064203.json` and ran replay bench (scale=1.0, Apple M3):
  - Default: ~70.7ms/op.
  - Memtable modes: adaptive ~69.4ms/op, skiplist ~66.4ms/op, hash_sorted ~45.9ms/op, btree ~54.2ms/op.

## 12.13 Timeline Trace Replay
- Added timeline replay benchmark (`BenchmarkTraceReplayTimeline`) using JSONL trace timing + overlap.
- Pulled server trace to `tmp_traces/treedb_trace_20260109064203.jsonl` and ran timeline benchmark:
  - `TREEDB_TRACE_TIMELINE_DURATION_MS=1000` -> ~3.15s/op (Apple M3).
