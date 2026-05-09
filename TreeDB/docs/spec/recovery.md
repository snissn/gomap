# Recovery Specification

This document defines startup recovery behavior.

## 1. Recovery Entry Points

Recovery is executed during `Open` for read-write handles.

High-level order:

1. recover index metadata/root state,
2. replay cached commit logs (if WAL mode permits),
3. expose recovered state,
4. clean replayed commit-log segments.

Read-only opens do not run mutating recovery. If future collection WAL segments
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

Collection WAL recovery must also validate the canonical embedded side-ref set
decoded from root deltas and descriptors against the declared side-ref set.
Declared refs alone are not trusted.

## 6. Post-Recovery Expectations

After successful open:

- recovered keys are queryable through normal API,
- replayed commit-log files are absent,
- value-log files used by pointers remain present,
- future writes proceed on freshly rotated active segments.

## 7. Recovery Invariants

1. Replay order for equal keys must honor commit sequence ordering semantics.
2. RID join (when present) must be exact; no synthetic value reconstruction is permitted.
3. Commit-log cleanup occurs only after successful replay.
4. Value-log segments are not deleted as part of normal replay cleanup.
