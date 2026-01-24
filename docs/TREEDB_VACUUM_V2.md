# TreeDB Vacuum v2: Online Rewrite + Generation Swap

> **Legacy note:** The dedicated online/offline vacuum implementations have
> been removed in favor of `CompactIndex` (in-place rebuild). The details below
> describe the retired design.

## Problem (historical)

Before Vacuum v2, TreeDB exposed two index “vacuum” operations:

- **Online vacuum** (`(*db.DB).VacuumIndexOnline`): rebuilds the user tree by bulk-building into the *same* `index.db` (append-only allocation) and swaps the root with a short writer pause. This is useful for restoring scan locality but **cannot shrink** `index.db` and can cause large temporary (or sustained) file growth under churn.
- **Offline vacuum** (`db.VacuumIndexOffline`): rewrites `index.db` into a new file and swaps it in using a crash-safe protocol. This **shrinks** `index.db` but requires the DB to be closed (exclusive lock).

For write-heavy workloads/benchmarks, the “online vacuum grows / offline vacuum requires downtime” split is not ideal.

## Status

Legacy design only. Current TreeDB uses `CompactIndex` for in-place index
rebuilds and does not implement the v2 swap workflow described below.

## Goal

Implement a *single* high-quality vacuum that:

1. Builds a compact new `index.db` (shrinks disk footprint).
2. Runs **online** for most of the work, with only a brief exclusive window for cutover.
3. Preserves correctness for **long-lived snapshots/iterators** by keeping the old index mmap alive until readers drain.
4. Reuses the existing crash-safe swap artifacts (`index.db.new`, `index.db.new.ready`, `index.db.bak`) and recovery helper (`recoverIndexSwap`).

## Non-goals

- Cross-platform support for file swapping on Windows (TreeDB already uses `x/sys/unix` mmap APIs).
- Changing on-disk page or tree formats.

## High-level design

### A. Build phase (online, no writer pause)

1. Create/open `index.db.new` with the same pager chunk size as the active DB.
2. Take a stable snapshot of the current **user** tree and bulk-build it into `index.db.new`.
3. Start a write recorder (already exists: `vacuumRecorder`) to track keys changed during the build.

### B. Catch-up phase (online)

Repeatedly:

1. Drain recorded keys.
2. For each key, read its current entry from a fresh snapshot of the active DB.
3. Apply those point updates/deletes into the **new file’s** tree using a zipper built over `index.db.new`.

This mirrors the existing `VacuumIndexOnline` delta application logic, but the destination is the new file.

### C. Cutover phase (short exclusive writer window)

1. Acquire `writeMu` to stop backend commits.
2. Stop the write recorder and apply the final tail delta. If the tail is too large, temporarily resume recording and retry (existing “defer cutover” logic).
3. Rebuild the **system** tree into the new file from the latest snapshot (system tree is small; simplest correctness).
4. Write both Meta pages into the new file and `Sync()` it.
5. Write `index.db.new.ready` and `fsync` the directory.

### D. Swap on disk (short exclusive window)

Atomically swap files (same as offline vacuum):

1. `index.db` → `index.db.bak`
2. `index.db.new` → `index.db`
3. remove `index.db.new.ready`
4. remove `index.db.bak`

On POSIX, removing `index.db.bak` is safe even if there are active mmaps/Fds: the old file persists until the last reference closes, then the space is reclaimed.

### E. Generation-aware in-memory swap (key upgrade)

To avoid breaking old snapshots:

- Introduce an internal **index generation** object that holds:
  - pager (`*pager.Pager`)
  - allocator (`*freelist.Allocator`)
  - zipper (`*zipper.Zipper`)
  - MVCC reader registry (`*lifecycle.ReaderRegistry`)
  - graveyard (`*lifecycle.Graveyard`)
  - refcount (pinned by snapshots/iterators)

Snapshots pin the current generation on creation. When vacuum swaps to the new file:

1. Publish the new generation as “current” for new snapshots and writes.
2. Mark the old generation obsolete and drop the DB’s ref on it.
3. Keep the old generation pager alive until its refcount reaches zero, then close it (and only then will the kernel reclaim the unlinked file’s disk blocks).

This prevents old readers from blocking pruning in the new file (each generation has its own registry/graveyard).

## API shape

The existing `(*db.DB).VacuumIndexOnline(ctx)` can be upgraded to implement this behavior while keeping its signature.

`db.VacuumIndexOffline(opts)` remains as an explicit “DB closed” tool, but in practice the upgraded online vacuum provides the same shrinking behavior without requiring downtime, aside from the short cutover pause.

## Edge cases

- **Long-lived readers:** old snapshots/iterators must remain valid across swap.
- **Repeated vacuums:** safe even if a prior generation is still pinned (the old file is unlinked; subsequent swaps reuse the same artifact names).
- **Crash mid-swap:** reuse `recoverIndexSwap` semantics by only renaming `index.db` after `index.db.new.ready` exists and the new file is synced.
- **High write rate during vacuum:** delta replay loops and cutover defers bound writer pause.

## Tests

Add/extend tests in `TreeDB/db`:

1. **Shrink + data preservation:** online vacuum reduces `index.db` size (or page count) for churny workloads, and all keys remain readable.
2. **Snapshot across swap:** hold a snapshot open during vacuum; verify it continues to read the old view while the DB switches to the new index.
3. **Repeat vacuum with pinned readers:** ensure multiple vacuums work even when prior generation is still pinned.
4. **Swap artifact recovery:** failpoint tests leaving `index.db.new`/`ready`/`bak` combinations, then reopen and verify recovery picks the correct file.
