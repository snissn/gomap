# Value-Log Lifecycle Specification

This document owns value-log and split leaf-log lifecycle. The active target for
future WAL-protected external refs is the user-command WAL in
`user-command-wal.md`. The deprecated collection root-delta WAL plan in
`collection-wal-durability-plan.md` remains useful historical context for
external-ref preparation, protection, cleanup, and column-file closure risks.

## 1. Persistent Pointer Model

TreeDB value-log pointers are durable storage references.

A pointer remains valid while its segment is reachable from any live index state.
Segments must not be deleted based only on age.

## 2. Segment States

Conceptually, a value-log segment can be:

1. active (currently written lane head),
2. retained (still required by live pointers, queued flush state, or snapshots),
3. eligible (unreferenced and not active),
4. deleted/zombied.

## 3. Reachability Source of Truth

Reachability is defined by pointer references found in index trees.

`ValueLogGC` computes this by scanning:

- user tree,
- system tree,
- collection root trees referenced by system-tree descriptors under
  `collections/root/...`,
- committed but unapplied command-WAL external refs, once command WAL is
  implemented.

Entries with `node.FlagPointer` and `IsValueLogFileID(ptr.FileID)` mark a segment as referenced.

Command-WAL external refs are retention roots before they are reachable from
published roots. GC and rewrite must consult the protected command-WAL
external-ref index and the external-ref prepare guard before deleting,
truncating, rewriting, or moving value-log bytes. The first implementation must
skip value-log records protected only by command WAL rather than patching WAL
records in place.

WAL-only protection may be released only after the transaction is covered by a
durable `AppliedLSN`, the root descriptors containing the refs
are durable, and the value-log reachability tracker has incorporated those
published roots or a full reachability scan has completed.

Command-WAL protected value-log refs are also capacity charges. Admission,
GC, rewrite, checkpoint, and cleanup must charge both the logical referenced
bytes and the incremental retained segment bytes that cannot be deleted because
of the protected ref. A tiny protected byte range that pins an otherwise
collectible large value-log segment is charged by the retained segment debt, not
only by the byte range. When protected value-log debt reaches the command WAL
soft threshold, maintenance is triggered; at the stop threshold, new collection
writes block; at the hard threshold, new collection writes fail before ack.

Collection read views are also retention roots. If a live `CollectionReadView`
can reach a pending mutable, queued, or publishing unit that references a
value-log record, GC and rewrite must retain that record even if the command
WAL frame has already been applied and WAL-only protection is otherwise
eligible for release.

### 3.1 Command WAL Maintenance Barrier

Every physical maintenance operation that can delete, rewrite, move, truncate,
rename, or stop protecting value-log, leaf-log, side-payload, column,
dictionary, or template bytes must acquire the backend command WAL
maintenance barrier before computing candidates.

The barrier must:

1. wait for active external-ref prepare guards to either commit/protect or
   abort/classify;
2. rebuild or refresh the protected external-ref index if recovery has not already
   done so;
3. return an immutable protection snapshot containing WAL-only refs, read-view
   refs, pending publish refs, unclassified prepare groups, and active backup
   manifest refs;
4. hold a retention token until the operation has finished all destructive
   steps.

If the barrier cannot be acquired or recovery/protection state is incomplete,
maintenance must fail closed. Dry-run may report debt but must not delete.

Operation-specific rules:

| Operation | Command WAL precondition |
|---|---|
| `ValueLogGC` | Merge protected command-WAL value-log file IDs into the referenced set. A protected segment is not eligible even when no published root references it. |
| `ValueLogRewriteOnline` | Source records protected solely by command WAL must be skipped in PR1. Rewriting and patching command WAL refs is forbidden until a separate crash-tested redirect protocol exists. |
| `LeafGenerationGC` | Leaf-log generations referenced by command WAL or collection read views are live generations. |
| online index vacuum | Require command WAL debt zero for the roots being rewritten, or publish/checkpoint dirty command WAL first. A future root-remap maintenance WAL transaction may relax this only with crash tests. |
| offline index vacuum | Reject dirty command WAL and unclassified prepared external refs before read-only open. |
| `CompactStorage` | The initial checkpoint must be command-WAL-aware. If it reports command WAL debt, compaction must abort before value-log rewrite, GC, leaf GC, index vacuum, or zero-byte cleanup. |
| zero-byte cleanup | Check protected external-ref index and backup manifest pins before unlinking. |

Maintenance lock order is: acquire backend `maintenanceMu`, acquire the
command WAL maintenance barrier, take the immutable protection snapshot and
retention token, compute candidates, perform the destructive phase, then release
the token. No command WAL append lock may be held while acquiring backend
publish locks.

### 3.2 Command WAL Operator Runbook

Operators must use `treemap command-wal health --json` before manual cleanup,
backup triage, compaction triage, or restart triage for a directory that has
`command_wal_v1` enabled.

Health states:

| State | Meaning | Operator rule |
|---|---|---|
| `clean` | no pending command WAL, cleanup debt below threshold | restart, backup, compaction, and ordinary maintenance are safe under normal rules |
| `pending` | committed WAL or protected external refs are required for recovery | restart and backup are safe only when the whole directory, command WAL, and protected external refs are included; manual deletion is unsafe |
| `recovery_required` | complete unapplied command WAL exists | read-only open fails; take a whole-directory copy, then run read-write recovery |
| `corrupt` | hard command WAL or required external-ref failure | no manual deletion; preserve WAL, external refs, cleanup manifests, and artifacts; restore missing files or escalate |
| `cleanup_debt` | data is durable but obsolete files remain | run checkpoint/cleanup; delete files only when `safe-delete` reports `safe_to_delete=true` |

`ValueLogGC` and value-log rewrite reports must include whether bytes are
blocked by command-WAL external refs. Required command-WAL blocker fields are
`gc_blocked`, `gc_blocked_bytes`, `gc_blocked_segments`,
`gc_blocked_external_refs`, `oldest_blocking_age_ms`, `blocking_txn_ids`,
`blocking_external_refs`, `blocking_reason`,
`protected_external_ref_logical_bytes`, and
`protected_external_ref_retained_segment_bytes`.

### 3.3 Incremental Accounting Fast Path

TreeDB maintains commit-time reference counters per value-log segment.

- Counters are updated from commit deltas (old pointer removal / new pointer add).
- `ValueLogGC` may use these counters as a fast path instead of full-tree scans.
- A compact metadata snapshot (`vlog_ref_counts.meta`) is stored in the DB dir.

If counters are unavailable, stale, or corrupt, TreeDB rebuilds by scanning trees
and rewrites metadata.

## 4. GC Algorithm (`DB.ValueLogGC`)

For each segment in current value-log set:

1. determine referenced set (incremental counters when valid; otherwise full scan),
2. keep if referenced,
3. keep if current active segment for that lane,
4. otherwise mark eligible.

For eligible segments:

- `DryRun=true`: report only.
- `DryRun=false`:
  - mark segment zombie in manager,
  - refresh value-log set,
  - verify file removal and report bytes/segment counts.

GC must never delete a currently active lane head.

## 5. Cached-Layer Retention Interaction

Cached mode tracks retained value-log paths while memtables/flush queues may still point at them.

Checkpoint and retention pruning:

- preserve currently active value-log paths,
- preserve retained paths,
- drop only paths no longer live/retained,
- refresh manager segment set after changes.

Backend maintenance that creates new value-log or split leaf-log segments must
reconcile cached-mode split writers after the backend operation. Reconciliation
refreshes the value-log reader and advances each cached writer lane past the
maximum observed on-disk segment sequence, preventing later cached writes from
reusing segment filenames created by compaction, rewrite, leaf packing, or index
vacuum.

## 6. Rewrite/Compaction

### 6.1 Online rewrite (`DB.ValueLogRewriteOnline`)

Online rewrite copies live pointer-backed values into fresh value-log segments
and updates keys in bounded commit batches. Cached-mode callers must checkpoint
first, protect cached value-log paths, and allocate rewrite RIDs from the shared
cached allocator.

### 6.2 Offline rewrite (`ValueLogRewriteOffline`)

Offline rewrite rewrites live pointer records into fresh segments and swaps index.

#### Preconditions

- exclusive DB lock,
- clean commit log (no pending `commit-*.log`),
- readable existing value-log segments.

#### Procedure

1. Open DB read-only under exclusive lock.
2. Build new value-log segments by iterating current trees and copying referenced records.
3. Rebuild index file (`index.db.new`) with rewritten pointers.
4. Write ready marker and fsync.
5. Atomically swap `index.db.new` into `index.db`.
6. Remove obsolete value-log segments.
7. Report before/after size and segment counts.

#### Safety properties

- Only referenced records are copied.
- Pointer map deduplicates source record copies when needed.
- Old segments are removed only after index swap succeeds.

### 6.3 Full storage compaction (`DB.CompactStorage`)

`DB.CompactStorage` is the preferred online operator path for reclaiming TreeDB
storage. It composes:

1. checkpoint,
2. value-log rewrite,
3. value-log GC,
4. split leaf-generation pack,
5. split leaf-generation GC,
6. index vacuum,
7. settle GC passes,
8. zero-byte value-log cleanup,
9. final debt audit.

Applied full storage compaction holds backend maintenance serialization for the
whole sequence. Plan mode computes the same debt model without mutating storage.

Value-log deletion in this lifecycle remains reachability-based. Zero-byte
cleanup is limited to untracked `value_vlog` segment files and must honor
cached-mode protected paths.

## 7. Read Integrity Options

Value-log read integrity mode:

- `IntegrityVerify` (default): verify value-log checksums.
- `IntegritySkipChecksums`: disable checksum verification (unsafe).

Template and dictionary lookup failures for encoded/compressed records are treated as read/recovery errors.

## 8. Operational Guidance

- Use `ValueLogGC` regularly to reclaim fully unreachable segments.
- Prefer `DB.CompactStorage` for explicit online "make this DB compact"
  maintenance across value logs, split leaf logs, and `index.db`.
- Use `ValueLogRewriteOffline` only when an exclusive offline rewrite is
  intentionally required.
- Monitor retained bytes and optional guardrails:
  - `ValueLog.MaxRetainedBytes` (warning threshold),
  - `ValueLog.MaxRetainedBytesHard` (pointer admission cap).

## 9. Lifecycle Invariants

1. Pointers remain readable after reopen/checkpoint.
2. Unreferenced segments may be deleted; referenced segments must remain.
3. Segment deletion is reachability-based, not age-based.
4. Rewrite must preserve key/value visibility across reopen.
