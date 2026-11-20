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

### Document files on a separate Linux mount

A root-layout store can have a mount at `maindb/value_vlog`. Its document
segments belong to the same backup as the native index and WAL. Stop writes,
complete checked Close, and keep both filesystems mounted while copying and
verifying the closed store. A traversal with `--one-file-system` omits those
segments. Taking independent live snapshots of the two filesystems does not
establish a common cut.

Before opening this layout, verify the native store and document mount in the
mount namespace the gateway or maintenance process will actually use. Compare
the filesystem UUIDs and the two directory device/inode pairs against the
values recorded when provisioning this store. The example assumes two distinct Linux filesystems with only the document
mount nested inside the store. Run it in the service launch shell. Supply
the four expected values from the installation record outside the store.

```sh
set -eu
: "${store:?}" "${store_uuid:?}" "${document_uuid:?}"
: "${store_dir_id:?}" "${document_dir_id:?}"  # decimal device:inode from stat
doc="$store/maindb/value_vlog"
check_value() {
    expected=$1
    shift
    actual=$("$@") && test "$actual" = "$expected"
}
test "$store_uuid" != "$document_uuid" || exit 1
check_value "$store_uuid" findmnt --target "$store" -n -o UUID || exit 1
check_value "$store_dir_id" stat --format='%d:%i' -- "$store" || exit 1
check_value "$document_uuid" findmnt --mountpoint "$doc" -n -o UUID || exit 1
check_value "$document_dir_id" stat --format='%d:%i' -- "$doc" || exit 1
```

An absent mount, empty UUID or identity mismatch must stop that invocation
before writable Open. Keep the mount stable until checked Close. If a document
dependency is missing, recovery can select an older complete durable root.
A successful Open alone cannot verify that the intended mount or cut was used.

Linux device numbers can change at reboot, particularly for loop and
device-mapper volumes. TreeDB persists the device number as part of a file's
identity. If the same UUID returns on a different device number, keep the
gateway stopped. Reestablish the original device mapping, or restore from a
verified closed backup into the new layout. Updating the launch record alone
leaves the persisted dependencies bound to the original device.

For a restore onto new devices or inodes:

1. Verify the full closed backup against its source manifest. Extract both
   parts into their intended private destination mounts. Independently verify
   the intended destination filesystems and content, and record their UUIDs
   and directory identities outside the copy.
2. On the closed private copy, run `quicksilver_snapshot_restore "$store"`
   (from `cmd/quicksilver_snapshot_restore`). It calls
   `RebindDurableRootSnapshotLayoutWithContextV1` on each present side-store
   index first, then on the staged `maindb` with the store root as its
   side-store root. A failure can leave the staging copy partially rebound.
   Keep the original backup and start again from a fresh verified extraction.
3. Use the proposed destination identities for the pre-Open check. Compare
   typed readback against the expected backup cut and check Close. Install the
   verified destination identity record for subsequent launches only after
   those checks succeed.

### Paged vector projection closure

For schema7 local vector projections, the two directory roots are only the
entry points of the retained asset closure. Snapshot validation and GC traverse
every local directory child, source metadata reference and graph section with
the same error-aware visitor. Missing, mixed or corrupt children fail validation;
copying only the root assets is insufficient. Source-owner and ANN-owner scope
lists describe local coverage and never filter physical references during this
full traversal. Reader generation pins protect the transitive closure even when
a prepared domain outlives its source session.

The opt-in DPM2 dependency directory is also a COW root in `index.db`; preserve
both recoverable root slots and the required-format marker. Snapshot namespace
rebinding changes physical identity values in the staged private copy while
preserving logical keys and validating each selected root's page extent.
For both dependency layouts and both recoverable slots, dictionary and template
namespace parent identities and epochs are captured from the exact destination
parent handles using the same epoch derivation as fresh dictionary and template
authority.
Other producer namespace epochs, child resource generations, frontiers, root
coordinates and commit sequences remain unchanged.
Keeping the source namespace epoch would let recovery validate the copied
physical parent but cause a later dictionary or template resource merge to reject
fresh destination authority. Ordinary open still validates persisted identities without
rebinding; a byte-identical replacement remains an identity mismatch.
These local snapshot rules do not activate the dormant command-WAL external-ref
backup contract above. Schema7 destructive generation retirement remains
unsupported; pre-publication crash orphans use existing explicit maintenance GC.
See [storage format](storage-format.md) and [recovery](recovery.md).

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
