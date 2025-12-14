# TODO / Roadmap (High Priority)

This repo currently exposes two TreeDB entrypoints:

- **Backend DB**: `treedb.OpenBackend(opts)` → the uncached engine (`TreeDB/db`)
- **Cached DB**: `treedb.Open(opts)` / `treedb.OpenCached(opts)` → write-back caching wrapper (`TreeDB/caching`) on top of the backend

The cached layer is **not** a read-cache; it is an LSM-style **write-back** layer:
`Memtable + WAL + background flush → backend`.

Today, the caching WAL is **not replayed** on open, and there is **no cross-process lock**.
This document outlines a coherent future plan to:

1. Provide a single public DB type/API with a runtime mode switch (cached vs backend).
2. Enforce **exclusive open** (single-writer, cross-process).
3. Implement **coherent crash recovery** so the same on-disk state is recovered whether the next opener chooses cached or backend mode.

## 1) Unify the Public API (One `treedb.DB`)

### Goal
Make backend-vs-cached a user option, not a separate concrete type, while keeping the API ergonomic and obvious.

### Proposed Public Shape
- `treedb.Open(opts)` returns a single public type: `*treedb.DB`.
- `opts` includes a mode flag:
  - `opts.EnableCaching bool` (existing field in `TreeDB/db.Options`) OR
  - `opts.Mode treedb.Mode` where `ModeCached` / `ModeBackend`.

Keep explicit helpers for power users:
- `treedb.OpenCached(opts)`
- `treedb.OpenBackend(opts)`

### Implementation Options
Pick one (both are valid):

1) **Concrete wrapper type (recommended for ergonomics)**
```go
type DB struct {
    cached  *caching.DB
    backend *db.DB
    mode    Mode
}
```
Forward methods to the chosen implementation. This preserves a concrete type for users.

2) **Interface return**
Expose a `treedb.DB` interface and return either backend or cached implementation.
This is flexible, but users lose a concrete type and discoverability suffers.

### Key API Compatibility Considerations
- Ensure iterator and batch APIs feel uniform:
  - `Iterator(start,end)` and `ReverseIterator(start,end)` should behave the same.
  - `NewBatch()` should return a single public batch type (wrapper around cached/backend batch).
- Preserve current semantics where possible, but it’s OK to make modest breaking changes if tests/features are maintained.

## 2) Exclusive Cross-Process Locking

### Goal
Prevent accidental concurrent opens across processes (cached and backend) and eliminate “open races”.

### Requirements
- Lock is acquired at the beginning of `treedb.Open*`.
- Lock is exclusive (writer lock); future read-only/shared modes can be considered later.
- Lock is released on `Close()` and on abnormal process termination (OS releases).

### Suggested Mechanism
Create a dedicated lock file, e.g.:
- `Dir/LOCK` or `Dir/index.db.lock`

Implement cross-platform locking via `golang.org/x/sys`:
- Unix: `flock` on the lock file fd
- Windows: `LockFileEx` / `UnlockFileEx`

### Behavior
- If already locked, `Open` returns a clear error (e.g. `ErrLocked`) including PID info if available.
- Lock should be held for the full lifetime of the DB handle (including cached wrapper).

## 3) Coherent Crash Recovery (Backend + Cached WAL Replay)

### Problem Today
Cached TreeDB writes go to:
- caching WAL (buffered) + memtable (RAM)
- flushed later to backend

If the process crashes:
- backend contains only the last flushed state,
- WAL files may exist, but **are not replayed**,
- so reopening as cached vs backend yields the **same** (backend-only) recovered state, losing unflushed updates.

### Goal
Make crash recovery deterministic and consistent:
After a crash, *any* opener (cached or backend) should recover the same state.

### High-Level Model
On-disk durability becomes:
- **Backend data** (meta + pages + slabs)
- **Cache WAL segments** (authoritative for writes not yet applied to backend)

Recovery pipeline (for both modes):
1. Acquire exclusive lock.
2. Recover backend meta/pages/slabs (existing `db.recover()`).
3. Discover cache WAL segments.
4. Replay WAL records into the backend in a bounded, idempotent way.
5. Persist a “WAL checkpoint” to backend meta.
6. Retire/delete WAL segments that are fully checkpointed.
7. Start normal operation (cached or backend mode).

### WAL Checkpointing (Avoid Replaying Forever)
Replaying WAL on every open is logically correct, but can cause disk growth and repeated work.
We need a durable checkpoint in backend meta, such as:
- `AppliedWALSegment uint64`
- `AppliedWALOffset  uint64` (optional; per-segment offset)

Rules:
- Only delete/rotate WAL segments once the backend commit that includes them is durable.
- Recovery replays only segments after the checkpoint.
- Replay must be **idempotent** (safe if run twice).

### Replay Granularity
Two reasonable approaches:

1) **Segment = memtable generation**
- Cached layer rotates WAL when rotating memtable.
- Recovery replays complete segments into backend, then checkpoints at segment boundaries.
- Simpler bookkeeping.

2) **Segment + offset**
- Supports mid-segment checkpointing (more complex).
- Needed if segments can become huge or if you want frequent checkpoints without rotation.

### Record Durability Semantics
Align with the existing contract:
- `Set` / `Batch.Write` (non-sync): may not survive crash; recovery replays only what made it to disk.
- `SetSync` / `Batch.WriteSync`: must survive crash; implementation must `fsync` the WAL (and/or backend commit) accordingly.

That means:
- Cached `SetSync` must ensure WAL buffers are flushed + fsynced.
- Cached `Batch.WriteSync` must ensure WAL durability for operations that are only in cache.

### Corruption / Partial Records
WAL records have CRCs. Recovery should:
- Stop at first truncated/invalid record at end-of-file (treat as clean truncation).
- For mid-file corruption, fail open with a clear error unless an explicit “ignore WAL” option is set.

### Safety Rule (Avoid Mixed Opens)
If cache WAL segments exist past the checkpoint:
- backend-only open should still run the same replay, or
- backend-only open should refuse unless `opts.IgnoreWAL` is set.

Recommendation: **always replay** in the shared recovery path so backend/cached open are consistent.

## 4) Spec Tests (North Star)

Add tests that lock in the contract and prevent regressions:

### A) “Crash then reopen” consistency test
Simulate:
1. Open cached.
2. Perform a set of `SetSync` operations (ensure WAL is durable).
3. Do **not** flush to backend (force them to remain only in WAL/memtable).
4. Simulate crash by closing without flushing memtables (test hook) or by directly writing WAL files and skipping cached state.
5. Reopen backend (or unified open in backend mode) and assert keys exist.
6. Reopen cached and assert the same state.

Notes:
- Tests can’t actually SIGKILL the process, but can simulate by:
  - writing WAL files directly using the WAL writer,
  - or adding an internal test-only hook that closes without flushing.

### B) Idempotent replay test
- Run recovery twice on the same WAL segments; results must be identical and not duplicate data or corrupt meta.

### C) Truncated record test
- Write a WAL file with a valid prefix then truncate the last record; recovery must succeed and replay the valid prefix only.

### D) Locking test
- Attempt to open the same dir twice; second must fail with `ErrLocked`.

## 5) Implementation Milestones (Suggested Order)

1. **Locking**
   - Add lock file type + acquire/release in `treedb.Open*`.
2. **Refactor Open path**
   - Route all opens through a shared internal `openAndRecover(opts)` pipeline.
3. **WAL segment discovery**
   - Enumerate `Dir/wal/` files, parse sequence numbers, sort.
4. **Checkpoint persistence**
   - Extend meta format (versioned) to store applied WAL checkpoint.
5. **Replay engine**
   - Apply WAL ops to backend via backend batches; commit; update checkpoint; cleanup.
6. **Mode selection**
   - Return `*treedb.DB` wrapper that chooses cached vs backend at runtime.
7. **Spec tests**
   - Implement tests A–D and ensure they pass on all platforms.

## 6) Open Questions (Worth Deciding Early)

- Where to store WAL checkpoint in meta (meta versioning / backward compatibility)?
- Should backend-only mode create/use a WAL at all, or only replay and then operate without WAL?
- Should “ignore WAL” be an explicit unsafe option?
- Do we want a “read-only open” mode (shared lock) later?

