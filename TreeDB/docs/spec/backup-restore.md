# Backup and Restore Specification

This document defines the operator-visible backup and restore contract for
TreeDB directories once command WAL durable-at-ack is enabled.

## 1. Restorable Directory File Set

A complete restorable root-layout backup includes:

- `maindb/index.db`;
- `maindb/format.json`, when present;
- `maindb/wal/commit-l<lane>-<seq>.log` needed for cached replay and future
  typed command-WAL replay;
- command-WAL cleanup metadata or manifest proof for omitted commit-log ranges,
  once command WAL cleanup metadata lands;
- `maindb/value_vlog/value-l<lane>-<seq>.log` referenced by roots, cached WAL,
  command WAL, snapshots/read views represented in the backup manifest, or
  external-ref protection metadata;
- `maindb/leaf_vlog/value-l<lane>-<seq>.log` and leaf-generation
  manifests/indexes when outer leaves are enabled;
- future typed external-ref payload files referenced by non-cleaned command WAL
  frames, once a separate authority contract activates them, plus future column
  files referenced by roots;
- `dictdb/**` and `templatedb/**`, including each side store's `index.db`,
  `format.json`, `wal/`, `value_vlog/`, `leaf_vlog/`, and cleanup metadata,
  when those stores are enabled;
- future column-store directories: parts, manifests, filters, delete bitmaps,
  dictionaries, compression metadata, locator roots, and their cleanup
  manifests;
- backup manifest, if the backup was taken with a live WAL-snapshot barrier.

`LOCK` files and transient maintenance files such as `index.db.new`,
`index.db.bak`, and `index.db.new.ready` are not required in a clean backup. If
present, restore must run existing index-swap recovery before command WAL
recovery.

Copying only `maindb/index.db` is not a backup. Copying only `maindb` is not a
complete root-layout backup when `dictdb`, `templatedb`, command-WAL external
refs, or future column external-ref files are enabled.

The typed `CommandEnvelope.ExternalRefs` section is currently dormant: V1 and
V2 reject every non-empty section before journal mutation. RawKV `SetRID`
commands instead use the active V2 `ExternalRefFenceV1` dependency closure.
The typed external-ref prepare, protection, restore, and quarantine steps below
describe a separate future activation, not authority accepted by current codecs.

## 2. Live Backup Support

A filesystem-level copy or snapshot of a live TreeDB root without a TreeDB
backup barrier is unsupported. A filesystem-level copy/snapshot of a live root
is supported only while a TreeDB backup barrier token is held, or after clean
close.

The backup barrier is backend-owned and coordinates the main DB, side stores,
command WAL, external-ref prepare guards, protected external refs, cleanup
metadata, and backup-retention pins.

### 2.1 Clean-Checkpoint Backup

Procedure:

1. `BeginBackupBarrier(mode=CleanCheckpoint)`.
2. Fence new command-WAL writers.
3. Wait for admitted command-WAL writes and, once activated, typed external-ref
   prepares.
4. Drain async collection publish and `FlushAll`.
5. Publish root descriptors and `AppliedLSN`.
6. Force backend checkpoint/meta durability boundary.
7. Write and fsync command-WAL cleanup metadata for cleanable ranges, if used.
8. Return a manifest with `command_wal_debt=0`.
9. Copy the manifest-listed root directory files or take a filesystem snapshot.
10. `EndBackupBarrier`.

If the checkpoint cannot publish `AppliedLSN` covering pre-cut command WAL,
`BeginBackupBarrier(mode=CleanCheckpoint)` must fail or return a manifest that
is not marked clean. It must not allow operators to infer that command WAL is
safe to omit.

### 2.2 WAL-Snapshot Backup

Procedure:

1. `BeginBackupBarrier(mode=WALSnapshot)`.
2. Fence command-WAL admission at a cut.
3. Once typed external refs are activated, wait for prepares before the cut to
   commit/protect or abort/classify.
4. Rotate/seal WAL, value-log, leaf-log, and any activated typed external-ref
   payload files needed by the cut, or record exact byte ranges plus checksums
   for active tails.
5. Fsync every manifest-listed WAL segment/range and activated typed
   external-ref payload to the selected durability boundary.
6. Write and fsync the backup manifest.
7. Release writers after the filesystem snapshot is taken, or keep manifest
   retention pins until `EndBackupBarrier` after file copy completes.
8. Restore validates and replays committed unapplied command WAL frames from the
   manifest.

The WAL-snapshot manifest includes root layout, file classes, exact
paths/ranges/sizes/checksums, `AppliedLSN`, cleaned commit-log ranges, command
WAL debt, side-store checkpoints, and backup token generation. The manifest
itself is a retention root until the barrier token is released.

## 3. Restore Validation

Restore/open validation before serving reads:

1. recover index swap artifacts;
2. load backup manifest if present;
3. discover commit-log command WAL segments and cleanup metadata;
4. treat missing commit-log segments as valid only when covered by durable cleanup
   metadata or by the backup manifest's cleaned-range proof;
5. once typed external refs are activated, rebuild their protected-ref index
   from all committed non-cleaned command WAL frames; current codecs instead
   reject every frame with a non-empty typed section;
6. for every complete typed command frame not covered by `AppliedLSN` plus
   cleanup:
   - validate frame checksum and command assertions;
   - validate command kind, scope, catalog/schema epochs, preconditions, and
     result assertions;
   - decode the canonical command payload;
   - after typed external-ref activation, compare declared refs to
     command-derived refs where applicable;
   - after activation, verify every required external ref exists, has expected
     size/checksum/class, and has dictionary/template/column dependency closure;
7. stop open before serving reads on any dormant non-empty typed external-ref
   section or, after activation, any missing or corrupt required external ref;
8. replay unapplied commands and publish recovered roots plus `AppliedLSN`
   atomically;
9. after activation, classify uncommitted prepared/final typed external-ref
   payload files;
10. quarantine, but do not immediately purge, files proven orphaned.

Backup/restore validation currently fails closed on every non-empty typed
command-WAL external-ref section. Active V2 recovery also fails closed on a
missing RawKV RID at or below the durable horizon; a missing RID strictly above
that horizon makes the relaxed suffix discardable. A restore may accept a
missing commit-log segment only when durable cleanup metadata or the backup
manifest proves the exact segment/range was safely cleaned.

## 4. Quarantine and Purge

After typed external-ref authority is activated, quarantine records must be
durable before those payload files are moved, hidden, or made unavailable for
normal recovery. A quarantine manifest records
source class, source registry path, `FileID`/part id, size, checksum,
classification reason, recovery generation, and the proof that no committed WAL,
published root, snapshot, read view, or backup manifest references the file.

Quarantined IDs remain reserved until purge is durable. Purge may run only
after a successful checkpoint/cleanup boundary or an explicit operator command.
