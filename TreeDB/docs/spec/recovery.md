# Recovery Specification

This document defines startup recovery behavior.

## 1. Recovery Entry Points

Recovery is executed during `Open` for read-write handles.

Current high-level order for non-collection cached WAL:

1. recover index metadata/root state,
2. discover value-log and commit-log segments from their canonical directories,
3. validate reachable value-log and split leaf-log records needed by replay,
4. replay cached commit logs if WAL mode permits,
5. expose recovered backend roots and memtables,
6. clean obsolete commit-log segments only after replay permits it.

Target user-command WAL extension:

User-command WAL replay is target behavior owned by `user-command-wal.md` until
the implementation gate lands. In that target, recovery discovers command WAL
segments, loads the checkpointed `AppliedLSN`, validates frames and required
external refs, replays complete commands with `LSN > AppliedLSN` through the
deterministic command executor, then checkpoints the recovered `AppliedLSN`
before cleaning replayed WAL.

Command effects and `AppliedLSN` are one recovered state. Open must select a
durable tuple equivalent to:

```text
(UserRootPageID, SystemRootPageID, AppliedLSN, CommitSeq, required value-log/leaf-log reachability)
```

If a crash leaves command effects without matching `AppliedLSN`, or
`AppliedLSN` without the command effects it claims, recovery must select the
previous tuple or fail closed. It must not repair the split by guessing.

Read-only opens do not run mutating recovery. If command WAL segments contain
committed unapplied commands, read-only open must fail with a
recovery-required error unless the caller explicitly requests a stale read-only
mode. Silent stale read-only open is incompatible with durable-at-ack command
semantics.

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

Commit-log segments are discovered from `<maindb>/wal` by filename parsing.
Value-log segments are discovered from `<maindb>/value_vlog`; split leaf-log
segments are discovered from `<maindb>/leaf_vlog`.

Before `command_wal_v1`, accepted patterns include:

- canonical commit log: `commit-l<lane>-<seq>.log`
- legacy accepted commit-log aliases: `commit-<seq>.log`, `wal-<seq>.log`
- legacy value-log aliases accepted only by explicit legacy parsers:
  `value-<seq>.log`, `vlog-<seq>.log`, and legacy mixed `wal/value-l*.log`
  names

After `command_wal_v1` activation, command WAL segments use only the shared
`commit-l<lane>-<seq>.log` family for required command replay. Old raw batch
payloads are unsupported in command WAL directories; activation must start from
a clean WAL state or an explicit rebuild.

Discovered command segments are sorted by `(lane, seq)` and replayed by command
LSN.

Typed command WAL frames use the same commit-log segment family as raw WAL. A
missing commit-log segment is acceptable only when covered by durable cleanup
metadata or backup-manifest proof that the segment's LSN range was safely
cleaned. A missing non-cleaned commit-log segment that may contain required
command frames is recovery corruption.

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

- Before `command_wal_v1`, read raw batches from commit-log segments.
- After `command_wal_v1`, read typed command frames from commit-log segments.
- Command frame ordering uses `LSN`; duplicate LSN is corruption.
- Old raw `commitlog.Record` batches in a command WAL directory fail closed.
- Truncated tail in commit log stops replay at partial tail safely.

### 4.3 Apply batches or command frames to backend

Before `command_wal_v1`, each commit record maps to backend batch op:

- `OpDelete` -> `Delete(key)`
- `OpSetInline` -> `Set(key, value)`
- `OpSetRID` -> lookup `RID` in map, then `SetPointer(key, ptr)`

WAL fence mode implications:

- `rid_join`: pointer-eligible writes replay through `OpSetRID` and therefore
  depend on the RID map/fence rules above.
- `simple_inline`: pointer-eligible writes replay through `OpSetInline`; RID
  join is not required for those records.

Each replayed batch is committed with `WriteSync`.

After `command_wal_v1`, raw writes replay as `RawKVBatch` command frames through
the deterministic command executor. Recovery must publish command effects and
advance `AppliedLSN` in the same backend durability boundary. A restart during
replay must observe either the old root plus old `AppliedLSN`, or the new root
plus advanced `AppliedLSN`; it must not observe command effects without the
matching `AppliedLSN` or `AppliedLSN` without command effects.

Replay publishes must advance `AppliedLSN` only over a contiguous LSN prefix.
If command LSN `N` is ready but a lower LSN is neither already applied nor part
of the same publish boundary, recovery must stop or fail closed rather than
publishing `N` out of order.

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
- pre-command-WAL cached key/value commit-log batches whose sequence-numbered
  RID fence is unsatisfied and whose replay rules explicitly skip the whole
  fenced batch.

Typed command WAL recovery is intentionally stricter than the current cached
key/value RID fence. A complete typed command frame with a missing required
external ref is a recovery error unless it is an incomplete terminal tail without
a valid commit marker. Recovery must not skip that complete command and continue
serving a state that claimed durable-at-ack semantics.

Typed command WAL decoder outcomes are part of the existing commit-log recovery
path, not a collection-specific recovery path:

| Outcome | Recovery behavior |
|---|---|
| `CompleteValid` | validate external refs and replay, or skip only by durable `AppliedLSN` |
| `CompleteCorrupt` | fail closed; a complete bad checksum/digest/closure is not a tail |
| `TerminalIncompleteTail` | ignore only if terminal active segment and no later non-cleaned segment exists |
| `NonTerminalShortRead` | fail closed |
| `UnsupportedVersion` | fail closed; durable acknowledged writes may be present |
| `UnsupportedSkippableRecord` | skip only when record feature bits and cleanup metadata permit |
| `MissingSegment` | fail closed unless durable cleanup metadata covers the segment/range |
| `DuplicateLSN` | fail closed |
| `MaliciousLength` | fail closed before allocation |
| `MixedVersionSegment` | fail closed unless an explicit migration reader supports both versions |

Read-only open must scan command WAL gates and committed typed frames. If
complete unapplied command WAL exists, read-only open fails with
recovery-required unless a future read-only replay overlay or explicitly stale
mode is implemented.

Every command WAL recovery skip, block, or hard failure must be assigned one
stable category:

| Category | Severity |
|---|---|
| `incomplete_tail` | safe skip only for terminal tail without durable commit marker |
| `already_applied_lsn` | safe skip when covered by durable `AppliedLSN` |
| `record_checksum_mismatch` | hard failure |
| `unsupported_wal_version` | hard failure unless explicitly skippable |
| `unsupported_command_kind` | hard failure unless explicitly skippable |
| `segment_gap_without_cleanup` | hard failure |
| `missing_required_external_ref` | hard failure |
| `corrupt_required_external_ref` | hard failure |
| `external_ref_closure_mismatch` | hard failure |
| `catalog_epoch_mismatch` | hard failure |
| `schema_epoch_mismatch` | hard failure |
| `precondition_failed` | hard failure unless the command kind defines an idempotent skip |
| `result_assertion_mismatch` | hard failure |
| `duplicate_lsn` | hard failure |
| `replay_publish_failure` | retryable recovery failure only before `AppliedLSN` is advanced |
| `cleanup_manifest_missing` | cleanup blocked; missing segment hard-fails open |
| `cleanup_manifest_corrupt` | cleanup blocked or hard-fails if needed for missing segment proof |
| `cleanup_failure` | safe leak/debt after checkpoint |
| `orphan_prepared_external_ref` | quarantine candidate when no committed WAL/root references it |
| `read_only_recovery_required` | read-only open failure |

`missing_required_external_ref`, `corrupt_required_external_ref`,
`external_ref_closure_mismatch`, catalog/schema mismatches, precondition
failures, and result assertion mismatches are hard recovery failures for complete
committed typed command WAL frames.

For strict command kinds, finding an existing effect while durable `AppliedLSN`
does not cover the command is also a hard failure. Idempotent skip is allowed
only for command kinds that specify proof, such as a stable command ID, payload
digest, document digest, matched/modified assertion, and catalog/schema guard.

Implementations must expose typed errors equivalent to:

```go
type CommandWALErrorCategory string

type CommandWALError struct {
    Category      CommandWALErrorCategory
    CommandID     string
    LSN           uint64
    Kind          string
    Scope         string
    SegmentID     string
    SegmentOffset uint64
    ExternalRef   *ExternalRefSummary
    Cause         error
}
```

The implementation must provide helpers equivalent to `IsIncompleteTail(err)`,
`IsRecoveryRequired(err)`, and `IsCommandWALCorruption(err)`. Error category
strings are metric suffixes and artifact fields; they must not be built from
arbitrary error messages.

Before recovery deletes, quarantines, rewrites, or marks clean any command WAL
segment range or external-ref file, it must write and fsync a forensic artifact.

Artifact directory:

```text
command_wal/recovery-artifacts/<unix_nano>-<pid>-<open_uuid>/
```

Required files:

- `recovery-report.json`;
- `segments.json`;
- `commands.json`;
- `external-refs.json`;
- `applied-lsn.json`;
- `cleanup-decisions.json`;
- `quarantine-manifest.json` when quarantine occurs.

The artifact directory, every required file, and the parent directory entry must
be fsynced before recovery performs the side effect that the artifact explains.
If artifact creation fails, recovery must leave WAL and external-ref files
untouched unless the failure is part of an explicit operator-approved destructive
repair.

Artifacts must not contain document payloads, raw user keys, raw index keys, raw
collection names, raw root names, or absolute host paths by default. They must
include metadata, checksums, digests, redacted names, segment ids, offsets,
lengths, `AppliedLSN`, and error categories sufficient to reconstruct:

- which command failed or was skipped;
- which external refs were required;
- which external refs were missing, corrupt, protected, released, or quarantined;
- current `AppliedLSN`;
- segment offset and checksum state;
- cleanup eligibility;
- `safe_to_restart`, `safe_to_backup`, `safe_to_compact`,
  `safe_to_delete_files`, and `requires_operator_action`.

Skipped commands must be auditable through a redacted `SkippedCommandRecord` in
the recovery artifact and CLI reports. Required skip categories are
`incomplete_tail`, `already_applied_lsn`, and `orphan_prepared_external_ref`.
Required fields are `skip_id`, `skip_category`, `command_id`, `lsn`, `kind`,
`scope`, `segment_id`, `segment_offset`, `record_length`,
`commit_marker_present`, `record_checksum_valid`, `external_ref_summaries`,
`applied_lsn_before`, and `applied_lsn_after`.

Command WAL recovery uses `LSN` as the dependency and skip key. There is no
separate collection sequence or `WALLSN` stream in the active target.

Read-write recovery must run command WAL replay before collection managers,
native-wire servers, or user handles can observe state derived from typed
commands. Read-only open must fail with recovery-required if unapplied committed
command WAL exists, unless an explicit stale read-only mode is added.

## 6. Restore and Backup-Manifest Validation

Restore/open validation before serving reads:

1. recover index swap artifacts;
2. load backup manifest if present;
3. discover commit-log command WAL segments and cleanup metadata;
4. validate missing commit-log segments only when covered by durable cleanup
   metadata or by the backup manifest's cleaned-range proof;
5. rebuild the protected external-ref index from all committed non-cleaned
   command WAL frames;
6. for every complete typed command frame not covered by `AppliedLSN` plus
   cleanup:
   - validate frame checksum and command assertions;
   - validate command kind, scope, catalog/schema epochs, preconditions, and
     result assertions;
   - decode the canonical command payload;
   - compare declared external refs to command-derived refs where applicable;
   - verify every required external ref exists, has expected size/checksum/class,
     and has dictionary/template/column dependency closure;
7. stop open before serving reads on any missing or corrupt required external ref;
8. replay unapplied commands and publish recovered roots plus `AppliedLSN`
   atomically;
9. classify uncommitted prepared/final external-ref files;
10. quarantine, but do not immediately purge, files proven orphaned.

Backup/restore validation must fail closed on missing required external refs. A
restore may accept a missing commit-log segment only when durable cleanup
metadata or the backup manifest proves the exact segment/range was safely
cleaned.

## 7. Read-Only Open

Read-only open must not perform mutating command WAL replay.

Before exposing state, read-only open must scan command WAL segment metadata,
cleanup metadata, and `AppliedLSN` enough to determine whether any complete
committed typed command frame is not covered by the durable
`AppliedLSN`/cleanup boundary.

Default behavior:

- if dirty committed command WAL exists, return public `ErrRecoveryRequired`
  with a command-WAL recovery reason.

Read-only stale mode:

- a future explicit option may expose only checkpointed roots;
- the option name must include `Stale`;
- it must report command WAL debt;
- it must be rejected by backup, restore validation, and offline maintenance
  tools.

## 8. Offline Maintenance Preconditions

Offline value-log rewrite, offline index vacuum, and any offline compaction or
root rewrite must prove command WAL cleanliness before opening the DB read-only
as a maintenance source. Clean means no committed unapplied command WAL, no
uncleaned committed commit-log segment needed for recovery, no
active/incomplete prepare group whose committed/uncommitted status is
unclassified, and durable cleanup metadata for any missing segment.

Offline tools may alternatively run full read-write recovery first, then reopen
the clean directory for maintenance. They must not proceed using stale read-only
roots.

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
