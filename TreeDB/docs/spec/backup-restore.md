# Backup and Restore Specification

This document defines the operator-visible backup and restore contract for
TreeDB directories once collection WAL durable-at-ack is enabled.

## 1. Restorable Directory File Set

A complete restorable root-layout backup includes:

- `maindb/index.db`;
- `maindb/format.json`, when present;
- `maindb/wal/commit-l<lane>-<seq>.log` needed for cached replay;
- `maindb/wal/collection-l<lane>-<seq>.log` ranges not covered by durable
  collection WAL cleanup metadata;
- collection WAL segment metadata and cleanup manifests;
- `maindb/value_vlog/value-l<lane>-<seq>.log` referenced by roots, cached WAL,
  collection WAL, snapshots/read views represented in the backup manifest, or
  side-ref protection metadata;
- `maindb/leaf_vlog/value-l<lane>-<seq>.log` and leaf-generation
  manifests/indexes when outer leaves are enabled;
- all collection WAL side-payload files, root-delta payloads, prepared/final
  side refs, and future column files referenced by roots or non-cleaned
  collection WAL;
- `dictdb/**` and `templatedb/**`, including each side store's `index.db`,
  `format.json`, `wal/`, `value_vlog/`, `leaf_vlog/`, and cleanup metadata,
  when those stores are enabled;
- future column-store directories: parts, manifests, filters, delete bitmaps,
  dictionaries, compression metadata, locator roots, and their cleanup
  manifests;
- backup manifest, if the backup was taken with a live WAL-snapshot barrier.

`LOCK` files and transient maintenance files such as `index.db.new`,
`index.db.bak`, and `index.db.new.ready` are not required in a clean backup. If
present, restore must run existing index-swap recovery before collection WAL
recovery.

Copying only `maindb/index.db` is not a backup. Copying only `maindb` is not a
complete root-layout backup when `dictdb`, `templatedb`, collection WAL side
payloads, or future column side files are enabled.

## 2. Live Backup Support

A filesystem-level copy or snapshot of a live TreeDB root without a TreeDB
backup barrier is unsupported. A filesystem-level copy/snapshot of a live root
is supported only while a TreeDB backup barrier token is held, or after clean
close.

The backup barrier is backend-owned and coordinates the main DB, side stores,
collection WAL, side-ref prepare guards, protected side refs, cleanup metadata,
and backup-retention pins.

### 2.1 Clean-Checkpoint Backup

Procedure:

1. `BeginBackupBarrier(mode=CleanCheckpoint)`.
2. Fence new collection writers.
3. Wait for admitted collection writes and side-ref prepares.
4. Drain async collection publish and `FlushAll`.
5. Publish root descriptors and applied watermarks.
6. Force backend checkpoint/meta durability boundary.
7. Write and fsync collection WAL cleanup metadata for cleanable ranges.
8. Return a manifest with `collection_wal_debt=0`.
9. Copy the manifest-listed root directory files or take a filesystem snapshot.
10. `EndBackupBarrier`.

If the checkpoint cannot publish or watermark pre-cut collection WAL,
`BeginBackupBarrier(mode=CleanCheckpoint)` must fail or return a manifest that
is not marked clean. It must not allow operators to infer that collection WAL is
safe to omit.

### 2.2 WAL-Snapshot Backup

Procedure:

1. `BeginBackupBarrier(mode=WALSnapshot)`.
2. Fence collection WAL admission at a cut.
3. Wait for side-ref prepares before the cut to commit/protect or
   abort/classify.
4. Rotate/seal WAL, value-log, leaf-log, and side-payload files needed by the
   cut, or record exact byte ranges plus checksums for active tails.
5. Fsync every manifest-listed WAL segment/range and side ref to the selected
   durability boundary.
6. Write and fsync the backup manifest.
7. Release writers after the filesystem snapshot is taken, or keep manifest
   retention pins until `EndBackupBarrier` after file copy completes.
8. Restore validates and replays committed unapplied collection WAL from the
   manifest.

The WAL-snapshot manifest includes root layout, file classes, exact
paths/ranges/sizes/checksums, applied watermarks, collection WAL cleaned ranges,
collection WAL debt, side-store checkpoints, and backup token generation. The
manifest itself is a retention root until the barrier token is released.

## 3. Restore Validation

Restore/open validation before serving reads:

1. recover index swap artifacts;
2. load backup manifest if present;
3. discover collection WAL segments and cleanup metadata;
4. treat missing collection WAL segments as valid only when covered by durable
   cleanup metadata or by the backup manifest's cleaned-range proof;
5. rebuild protected side-ref index from all committed non-cleaned collection
   WAL;
6. for every committed transaction not covered by applied watermark plus
   cleanup:
   - validate frame checksum and transaction checksum;
   - validate `CollectionUID`, generation, schema epoch, catalog digest, and
     root epochs;
   - decode embedded root deltas/descriptors and derive the canonical side-ref
     set;
   - compare canonical side refs to declared `SideRefs`;
   - verify every side ref exists, has expected size/checksum/class, and has
     dictionary/template/column dependency closure;
7. stop open before serving reads on any missing or corrupt required side ref;
8. publish descriptors and applied watermarks atomically for unapplied
   transactions;
9. classify uncommitted prepared/final side files;
10. quarantine, but do not immediately purge, files proven orphaned.

Backup/restore validation fails closed on missing collection WAL side refs. A
restore may accept a missing collection WAL segment only when durable cleanup
metadata or the backup manifest proves the exact segment/range was safely
cleaned.

## 4. Quarantine and Purge

Quarantine records must be durable before side files are moved, hidden, or made
unavailable for normal recovery. A quarantine manifest records source class,
source registry path, `FileID`/part id, size, checksum, classification reason,
recovery generation, and the proof that no committed WAL, published root,
snapshot, read view, or backup manifest references the file.

Quarantined IDs remain reserved until purge is durable. Purge may run only
after a successful checkpoint/cleanup boundary or an explicit operator command.
