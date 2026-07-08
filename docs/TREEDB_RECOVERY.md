# TreeDB Crash Recovery (Command-WAL + Backend)

This is a supporting crash-recovery explainer.

For the canonical TreeDB recovery spec, see:
- `TreeDB/docs/spec/README.md`
- `TreeDB/docs/spec/recovery.md`

## TL;DR

- TreeDB takes an **exclusive directory lock** on `Open` (one process at a time).
- Public command-WAL profiles persist command frames to `Dir/maindb/wal/`.
- Cached mode persists value-log records to `Dir/maindb/value_vlog/` and
  optional split leaf-log records to
  `Dir/maindb/leaf_vlog/`, then flushes them into the backend in the background.
- On open, TreeDB always performs a single coherent recovery path:
  1) backend recovery (meta validation + torn-tail handling)
  2) command-WAL discovery + replay into the backend for command-WAL directories
  3) command-WAL cleanup only after replay/coverage proves the segment is safe
     to remove
- Legacy cached redo-journal replay is a compatibility path. Current public
  command-WAL directories reject old raw redo records by default instead of
  silently replaying them.

This makes “last writer was cached vs backend” unambiguous: the next opener (either mode) recovers fully.

## What Can Exist On Disk After a Crash

Depending on timing, after an unclean shutdown the DB directory may contain:

- A consistent backend state (index pages), possibly missing the most recent cached writes that hadn’t flushed yet.
- One or more command-WAL segments in `Dir/maindb/wal/`.
- Referenced value-log segments in `Dir/maindb/value_vlog/` representing cached
  writes that are not yet reflected in the backend.
- A torn/partial final command-WAL frame (e.g. crash mid-write).
- Legacy cached redo segments from old compatibility fixtures. Current public
  command-WAL opens fail closed on replayable legacy redo unless the caller
  explicitly opts into legacy replay for forensic recovery.

## Recovery Pipeline

### 1) Backend recovery (always)

On open, the backend engine:

- validates redundant meta pages (so it can pick the newest consistent state),
- and truncates torn tail data where necessary (avoid partial-record corruption).

### 2) Command-WAL discovery + replay

For current command-WAL directories, TreeDB scans `Dir/maindb/wal/` for typed
command-WAL frames. Complete frames above the durable applied command LSN are
replayed through the normal command executor and then covered by the backend
state. Truncated/corrupt terminal tails are treated as terminal tails only when
they are the active final segment. Non-terminal corruption fails closed.

Point/batch/range raw key/value commands replay as `RawKVBatch` frames. That is
the same public surface used by `Set`, `SetSync`, `Delete`, `DeleteSync`,
`DeleteRange`, `Batch.Write`, and `Batch.WriteSync`.

### 3) Legacy cached redo-journal compatibility

The old cached redo journal used raw `commitlog.Record` batches in the same
segment directory. That path is no longer the normal public recovery contract.
Replayable legacy redo is quarantined by default; compatibility tests and
forensic recovery code must opt in explicitly with
`AllowLegacyCachedRedoJournalReplay`.

When explicit legacy replay is enabled, TreeDB scans `Dir/maindb/wal/` for
legacy journal segments sorted by sequence. Each segment is replayed into the
backend using `Batch.WriteSync`, so replay is durable. Large values already
stored in the value log are referenced by the replayed records. Truncated or
corrupt tails are treated as “stop replay for this segment” only for the final
partial record.

### 4) Command-WAL / journal cleanup

After a command-WAL segment is replayed and covered by backend/applied-LSN
state, it may be removed. Legacy compatibility journal segments are removed
only after explicit replay succeeds.

Additionally, cached mode’s background flusher/checkpoint path removes covered
segments after it flushes the corresponding memtable and publishes the matching
coverage boundary.

## Safety Notes

- Two processes must not open the same directory concurrently. The lock enforces this.
- Benchmark-only no-WAL mode still uses the persistent value log, but recent
  writes are not recoverable through command-WAL until a checkpoint/close
  publishes them.
- Read-only opens do not perform mutating recovery. If committed command-WAL
  frames require replay, read-only open fails with recovery-required behavior
  instead of serving silent stale state.

## Where To Look in Code

- Command-WAL / legacy segment discovery and replay: `TreeDB/db/wal_recovery.go`
- Cached command-WAL append/rotation/flush: `TreeDB/caching/db.go`
- Directory lock: `TreeDB/internal/lockfile/*`
