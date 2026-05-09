# Recovery Specification

This document defines startup recovery behavior.

## 1. Recovery Entry Points

Recovery is executed during `Open` for read-write handles.

High-level order:

1. recover index metadata/root state,
2. scan value-log, leaf-log, and future side-store availability,
3. replay cached commit logs (if WAL mode permits),
4. scan collection WAL transactions and cleanup metadata,
5. load applied collection watermarks,
6. validate required side refs and replay unapplied collection WAL transactions,
7. expose recovered state,
8. clean replayed commit-log and safely watermarked collection WAL segments.

Read-only opens do not run mutating recovery. If collection WAL segments
contain committed unapplied transactions, read-only open must fail with a
recovery-required error unless the caller explicitly requests a stale read-only
mode. Silent stale read-only open is incompatible with collection
durable-at-ack semantics.

## 2. Backend Index Recovery

## 2.1 New DB bootstrap

If page count is less than 2:

- allocate two meta pages,
- allocate empty user root leaf,
- allocate empty system root leaf,
- write both meta pages,
- initialize commit sequence to zero.

## 2.2 Existing DB meta selection

- Read both meta pages (`MetaPage0`, `MetaPage1`).
- Validate page checksum and page type.
- Build candidate set from valid pages.
- Sort by descending `CommitSeq`.
- Pick first candidate passing structural checks:
  - user root page valid,
  - system root page valid,
  - freelist head valid (or zero).

If neither candidate is valid, open fails.

## 2.3 State install

From chosen meta:

- set active meta page id,
- set pager page count from `TotalPages` when non-zero,
- set allocator head from `FreelistHeadID`.

## 3. WAL Segment Discovery

Segments are discovered from `<dir>/wal` by filename parsing.

Accepted patterns include:

- canonical: `commit-l<lane>-<seq>.log`, `value-l<lane>-<seq>.log`
- legacy accepted: `commit-<seq>.log`, `value-<seq>.log`, `wal-<seq>.log`, `vlog-<seq>.log`

Discovered segments are sorted by `(lane, seq)`.

Collection WAL segments use their own cleanup metadata. A missing collection WAL
segment is acceptable only when covered by durable cleanup metadata that proves
the segment's transaction range was safely cleaned. A missing non-cleaned
collection WAL segment is recovery corruption.

## 4. Replay Algorithm

Replay is skipped only when durability mode is `DurabilityWALOffRelaxed`.

Otherwise:

### 4.1 Build RID map from value-log segments

- Scan each value-log segment with `valuelog.Reader`.
- Decode record stream and map `RID -> ValuePtr`.
- If duplicate RID is encountered, fail recovery.
- Truncated tail (`EOF` / `UnexpectedEOF`) is treated as tail stop for that segment.
- Dictionary references are validated during scan; missing dict causes recovery failure.

### 4.2 Read commit-log segments

- Read batches from commit-log segments.
- Batch ordering rules:
  - batches with non-zero `Seq` sorted by `Seq` then read order,
  - legacy `Seq=0` batches preserve original read order.
- Truncated tail in commit log stops replay at partial tail safely.

### 4.3 Apply batches to backend

Each commit record maps to backend batch op:

- `OpDelete` -> `Delete(key)`
- `OpSetInline` -> `Set(key, value)`
- `OpSetRID` -> lookup `RID` in map, then `SetPointer(key, ptr)`

WAL fence mode implications:

- `rid_join`: pointer-eligible writes replay through `OpSetRID` and therefore
  depend on the RID map/fence rules above.
- `simple_inline`: pointer-eligible writes replay through `OpSetInline`; RID
  join is not required for those records.

Each replayed batch is committed with `WriteSync`.

### 4.4 Cleanup

After successful replay:

- replayed commit-log files are removed,
- value-log segments remain (they are persistent storage).

If replay fails, commit logs are not cleaned.

## 5. Error Handling Rules

Recovery must fail on:

1. both meta pages invalid,
2. no structurally valid root candidate,
3. missing RID for a commit-log RID record that is not covered by an allowed
   sequence-fence skip rule,
4. missing required dictionary bytes for compressed frame decode/validation,
5. hard corruption in non-tail portions.

Recovery may continue past:

- truncated tail records in final value/commit segments.
- current cached key/value commit-log batches whose sequence-numbered RID fence
  is unsatisfied and whose replay rules explicitly skip the whole fenced batch.

Collection WAL recovery is intentionally stricter than the current cached
key/value RID fence. A complete WAL-on collection transaction with a missing
required side ref is a recovery error unless it is an incomplete tail without a
valid commit marker. Recovery must not skip that complete collection transaction
and continue applying later transactions for the same collection.

Collection WAL decoder outcomes are distinct from commit-log outcomes:

| Outcome | Recovery behavior |
|---|---|
| `CompleteValid` | validate side refs and replay, or skip only by collection watermark |
| `CompleteCorrupt` | fail closed; a complete bad checksum/digest/closure is not a tail |
| `TerminalIncompleteTail` | ignore only if terminal active segment and no later non-cleaned segment exists |
| `NonTerminalShortRead` | fail closed |
| `UnsupportedVersion` | fail closed; durable acknowledged writes may be present |
| `UnsupportedSkippableRecord` | skip only when record feature bits and cleanup metadata permit |
| `MissingSegment` | fail closed unless durable cleanup metadata covers the segment/range |
| `DuplicateWALLSN` | fail closed |
| `DuplicateCollectionSeq` | fail closed |
| `MaliciousLength` | fail closed before allocation |
| `MixedVersionSegment` | fail closed unless an explicit migration reader supports both versions |

Read-only open must scan collection WAL gates and committed frames. If complete
unapplied collection WAL exists, read-only open fails with recovery-required
unless a future read-only replay overlay is explicitly implemented.

Every collection WAL recovery skip, block, or hard failure must be assigned one
stable category:

| Category | Severity |
|---|---|
| `incomplete_tail` | safe skip only for terminal tail without durable commit marker |
| `already_applied_watermark` | safe skip when covered by durable applied watermark |
| `record_checksum_mismatch` | hard failure |
| `unsupported_wal_version` | hard failure unless explicitly skippable |
| `segment_gap_without_cleanup` | hard failure |
| `missing_required_side_ref` | hard failure |
| `corrupt_required_side_ref` | hard failure |
| `side_ref_closure_mismatch` | hard failure |
| `collection_identity_mismatch` | hard failure |
| `collection_generation_mismatch` | hard failure |
| `schema_epoch_mismatch` | hard failure |
| `base_root_mismatch` | hard failure unless handled by formal accumulator state |
| `root_descriptor_epoch_mismatch` | hard failure |
| `base_system_root_mismatch` | hard failure |
| `watermark_inconsistency` | hard failure |
| `duplicate_wallsn` | hard failure |
| `duplicate_collection_seq` | hard failure |
| `dependency_gap` | block/fail for later same-collection transactions |
| `system_delta_precondition_failed` | hard failure |
| `replay_publish_failure` | retryable recovery failure only before descriptor/watermark commit |
| `cleanup_manifest_missing` | cleanup blocked; missing segment hard-fails open |
| `cleanup_manifest_corrupt` | cleanup blocked or hard-fails if needed for missing segment proof |
| `cleanup_failure` | safe leak/debt after watermark/checkpoint |
| `orphan_prepared_side_ref` | quarantine candidate when no committed WAL/root references it |
| `read_only_recovery_required` | read-only open failure |

`missing_required_side_ref`, `corrupt_required_side_ref`,
`side_ref_closure_mismatch`, schema/root/identity mismatches, and watermark
inconsistency are hard recovery failures for complete committed collection WAL
records.

Implementations must expose typed errors equivalent to:

```go
type CollectionWALErrorCategory string

type CollectionWALError struct {
    Category          CollectionWALErrorCategory
    TxnID             string
    WALLSN            uint64
    CollectionUID     string
    CollectionUIDHash string
    CollectionSeq     uint64
    SegmentID         string
    SegmentOffset     uint64
    SideRef           *SideRefSummary
    Cause             error
}
```

The implementation must provide helpers equivalent to `IsIncompleteTail(err)`,
`IsRecoveryRequired(err)`, and `IsCollectionWALCorruption(err)`. Error category
strings are metric suffixes and artifact fields; they must not be built from
arbitrary error messages.

Before recovery deletes, quarantines, rewrites, or marks clean any collection WAL
segment or side-ref file, it must write and fsync a forensic artifact.

Artifact directory:

```text
collection_wal/recovery-artifacts/<unix_nano>-<pid>-<open_uuid>/
```

Required files:

- `recovery-report.json`;
- `segments.json`;
- `transactions.json`;
- `side-refs.json`;
- `watermarks.json`;
- `cleanup-decisions.json`;
- `quarantine-manifest.json` when quarantine occurs.

The artifact directory, every required file, and the parent directory entry must
be fsynced before recovery performs the side effect that the artifact explains.
If artifact creation fails, recovery must leave WAL and side files untouched
unless the failure is part of an explicit operator-approved destructive repair.

Artifacts must not contain document payloads, raw user keys, raw index keys, raw
collection names, raw root names, or absolute host paths by default. They must
include metadata, checksums, digests, redacted names, segment ids, offsets,
lengths, watermarks, and error categories sufficient to reconstruct:

- which transaction failed or was skipped;
- which side refs were required;
- which side refs were missing, corrupt, protected, released, or quarantined;
- current applied watermark per collection;
- segment offset and checksum state;
- cleanup eligibility;
- `safe_to_restart`, `safe_to_backup`, `safe_to_compact`,
  `safe_to_delete_files`, and `requires_operator_action`.

Skipped transactions must be auditable through a redacted
`SkippedTransactionRecord` in the recovery artifact and CLI reports. Required
skip categories are `incomplete_tail`, `already_applied_watermark`,
`duplicate_collection_seq`, and `orphan_prepared_side_ref`. Required fields are
`skip_id`, `skip_category`, `txn_id`, `wallsn`, `collection_uid`,
`collection_uid_hash`, `collection_seq`, `segment_id`, `segment_offset`,
`record_length`, `commit_marker_present`, `record_checksum_valid`,
`side_ref_summaries`, `watermark_before`, and `watermark_after`.

Collection WAL recovery must also validate the canonical embedded side-ref set
decoded from root deltas and descriptors against the declared side-ref set.
Declared refs alone are not trusted.

Collection WAL recovery uses `CollectionSeq` as the dependency and skip key.
`WALLSN` is only a global append position for deterministic scanning,
diagnostics, and cleanup accounting. A higher cleaned or published `WALLSN` from
one collection must never cause recovery to skip a lower unapplied
`CollectionSeq` from another collection.

Collection WAL recovery must enforce the abstract state machine in
`collection-wal-durability-plan.md`. In particular:

- descriptor-only and watermark-only backend commits are invalid in WAL-on
  modes and must stop open unless a future format provides an explicit repair
  protocol;
- replay skip requires the same collection's applied sequence plus matching
  guard history, never global `WALLSN` ranges;
- side refs referenced by complete uncleaned WAL remain protected until
  `CanCleanSideRef` is true;
- read-only recovery detection uses `CanSkipReplay` and `CanReplay`, not raw file
  presence alone.

Read-write recovery must run collection WAL replay before collection managers,
native-wire servers, or user collection handles can observe collection state.
Read-only open must fail with recovery-required if unapplied committed
collection WAL exists, unless an explicit stale read-only mode is added.

## 6. Restore and Backup-Manifest Validation

Restore/open validation before serving reads:

1. recover index swap artifacts;
2. load backup manifest if present;
3. discover collection WAL segments and cleanup metadata;
4. validate missing collection WAL segments only when covered by durable cleanup
   metadata or by the backup manifest's cleaned-range proof;
5. rebuild the protected side-ref index from all committed non-cleaned
   collection WAL;
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

Backup/restore validation must fail closed on missing side refs. A restore may
accept a missing collection WAL segment only when durable cleanup metadata or
the backup manifest proves the exact segment/range was safely cleaned.

## 7. Read-Only Open

Read-only open must not perform mutating collection WAL replay.

Before exposing state, read-only open must scan collection WAL segment metadata,
cleanup metadata, and applied watermarks enough to determine whether any
complete committed collection WAL transaction is not covered by the durable
applied watermark/cleanup boundary.

Default behavior:

- if dirty committed collection WAL exists, return public
  `ErrRecoveryRequired` with a collection-WAL recovery reason.

Read-only stale mode:

- a future explicit option may expose only checkpointed roots;
- the option name must include `Stale`;
- it must report collection WAL debt;
- it must be rejected by backup, restore validation, and offline maintenance
  tools.

## 8. Offline Maintenance Preconditions

Offline value-log rewrite, offline index vacuum, and any offline compaction or
root rewrite must prove collection WAL cleanliness before opening the DB
read-only as a maintenance source. Clean means no committed unapplied
collection WAL, no uncleaned committed collection WAL segment needed for
recovery, no active/incomplete prepare group whose committed/uncommitted status
is unclassified, and durable cleanup metadata for any missing segment.

Offline tools may alternatively run full read-write recovery first, then reopen
the clean directory for maintenance. They must not proceed using stale
read-only roots.

## 9. Post-Recovery Expectations

After successful open:

- recovered keys are queryable through normal API,
- replayed commit-log files are absent,
- value-log files used by pointers remain present,
- future writes proceed on freshly rotated active segments.

## 10. Recovery Invariants

1. Replay order for equal keys must honor commit sequence ordering semantics.
2. RID join (when present) must be exact; no synthetic value reconstruction is permitted.
3. Commit-log cleanup occurs only after successful replay.
4. Value-log segments are not deleted as part of normal replay cleanup.
