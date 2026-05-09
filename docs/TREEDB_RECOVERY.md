# TreeDB Crash Recovery (Cached + Backend)

This is a supporting crash-recovery explainer.

For the canonical TreeDB recovery spec, see:
- `TreeDB/docs/spec/README.md`
- `TreeDB/docs/spec/recovery.md`

## TL;DR

- TreeDB takes an **exclusive directory lock** on `Open` (one process at a time).
- Cached mode persists redo records to `Dir/maindb/wal/`.
- Cached mode persists value-log records to `Dir/maindb/value_vlog/` and
  optional split leaf-log records to
  `Dir/maindb/leaf_vlog/`, then flushes them into the backend in the background.
- On open, TreeDB always performs a single coherent recovery path:
  1) backend recovery (meta validation + torn-tail handling)
  2) cached journal discovery + replay into the backend (synced commits)
  3) journal cleanup (segments removed after successful replay)

This makes “last writer was cached vs backend” unambiguous: the next opener (either mode) recovers fully.

## What Can Exist On Disk After a Crash

Depending on timing, after an unclean shutdown the DB directory may contain:

- A consistent backend state (index pages), possibly missing the most recent cached writes that hadn’t flushed yet.
- One or more journal segments in `Dir/maindb/wal/`.
- Referenced value-log segments in `Dir/maindb/value_vlog/` representing cached
  writes that are not yet reflected in the backend.
- A torn/partial final journal record (e.g. crash mid-write).

## Recovery Pipeline

### 1) Backend recovery (always)

On open, the backend engine:

- validates redundant meta pages (so it can pick the newest consistent state),
- and truncates torn tail data where necessary (avoid partial-record corruption).

### 2) Cached journal discovery + replay (always)

Regardless of the mode you open in, TreeDB scans `Dir/maindb/wal/` for journal segments (sorted by sequence).
Each segment is replayed into the backend using `Batch.WriteSync`, so replay is durable. Large values
already stored in the value log are referenced by the replayed records.

Truncated/corrupt tails are treated as “stop replay for this segment” (safe best-effort for the final partial record).

### 3) Journal cleanup

After a journal segment is successfully replayed, it is removed.

Additionally, cached mode’s background flusher removes journal segments after it flushes the corresponding memtable.

## Safety Notes

- Two processes must not open the same directory concurrently. The lock enforces this.
- WAL off (journal disabled) still uses the value log; there is no backend-only mode.

## Where To Look in Code

- Journal segment discovery/replay: `TreeDB/db/wal_recovery.go`
- Cached journal writer/rotation/flush: `TreeDB/caching/db.go`
- Directory lock: `TreeDB/internal/lockfile/*`
