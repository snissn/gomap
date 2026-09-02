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

### 2.1 Physical-identity deletion and quarantine

All managers and writers that can publish or delete segments in one live DB
namespace share a physical-identity registry.
The registry must be installed at construction, before the initial segment
scan; attaching it afterward is forbidden because an already-open unregistered
handle could otherwise resurrect an identity another manager committed as
deleted. Stable publication pins and delete leases use the captured file-handle
identity, not a later pathname lookup.

Destructive segment removal first moves the canonical name into a private,
same-parent, identity-encoded quarantine directory. Validation and unlink then
operate on that exact renamed object. A replacement created at the old
canonical name is never selected for cleanup. An interrupted quarantine is
durable recovery state: read-write open reconciles it before scanning segments,
while read-only open returns `ErrRecoveryRequired` without mutation.

### 2.2 External-version logical pruning is not segment GC

`TreeDB/mvcc.PruneVersions` deletes obsolete physical index keys only after its
persisted external timestamp floor makes those historical reads invalid. It
does not delete, truncate, or select value-log segments by timestamp or age.
The ordinary raw delete path updates index reachability; existing snapshot
roots remain retention roots. Value-log GC/rewrite may reclaim payload segments
later only through the reachability rules in this document.

The MVCC prune scan is snapshot-bound and bounded by its iterator plus delete
batch size. A live pre-prune snapshot continues to protect and resolve its old
value pointers. The prune's visited/retained/pruned byte accounting describes
logical physical-record bytes, not immediately reclaimed segment bytes.

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

A `RawKVBatchV2` `SetMaterializedRID` is not an external-ref retention root:
the complete command frame contains the exact RID and value bytes needed to
recreate a missing record. Before apply it is protected by retaining the command
frame itself; after apply, the recreated or reused pointer participates in the
ordinary published-root and cached-memtable reachability rules. `SetRID` entries,
including those sharing the same V2 frame, retain the external-ref protections
above.

Command-WAL-only protection may be released only after the command frame is covered by a
durable `AppliedLSN`, the root descriptors containing the refs are durable, and
the value-log reachability tracker has incorporated those published roots or a
full reachability scan has completed.

Command-WAL protected value-log refs are also capacity charges. Admission,
GC, rewrite, checkpoint, and cleanup must charge both the logical referenced
bytes and the incremental retained segment bytes that cannot be deleted because
of the protected ref. A tiny protected byte range that pins an otherwise
collectible large value-log segment is charged by the retained segment debt, not
only by the byte range. When protected value-log debt reaches the command WAL
soft threshold, maintenance is triggered; at the stop threshold, new mutating
commands that would create value-log external refs block; at the hard threshold,
those commands fail before ack.

Collection read views are also retention roots. If a live `CollectionReadView`
can reach a pending mutable, queued, or publishing unit that references a
value-log record, GC and rewrite must retain that record even if the command-WAL
frame has already been applied and command-WAL-only protection is otherwise
eligible for release.

### 3.1 Command WAL Maintenance Barrier

Every physical maintenance operation that can delete, rewrite, move, truncate,
rename, or stop protecting value-log, leaf-log, external-ref payload, column,
dictionary, or template bytes must acquire the backend command WAL
maintenance barrier before computing candidates.

The barrier must:

1. wait for active external-ref prepare guards to either commit/protect or
   abort/classify;
2. rebuild or refresh the protected external-ref index if recovery has not already
   done so;
3. return an immutable protection snapshot containing command-WAL-only refs, read-view
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
| online index vacuum | Require command WAL debt zero for the roots being rewritten, or publish/checkpoint dirty command WAL first. A future root-remap maintenance command may relax this only with crash tests. |
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
`command_wal_v2` enabled.

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
`gc_blocked_external_refs`, `oldest_blocking_age_ms`, `blocking_command_ids`,
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

### 6.2 Online split leaf-generation pack (`DB.LeafGenerationPack`)

Leaf-generation pack uses a two-phase copy/publish state machine:

1. **Copy, without `writeMu`:** acquire a coherent snapshot and its leaf-generation
   pins; discover source refs from that snapshot; read source leaf/value-log
   records; write recompressed frames below a hidden, non-scanned staging
   directory; allocate and write private COW index pages; and apply collection-root
   zipper deltas. Private pages use a disjoint logical ID namespace in a staging
   pager that reads unchanged children through the pinned source pager. They do
   not enlarge the live pager, enter committed `PageCount`, consume the foreground
   freelist, or require crash recovery accounting.
2. **Copy durability, without `writeMu`:** flush and fsync the staging leaf-log
   frames. The private index pages are in-memory relocation input and are never
   an authoritative publication candidate; the final live-pager pages are
   synchronized before meta publication.
   `LeafGenerationPackOptions.Sync` is retained for API compatibility but no
   longer weakens this durability contract. The copied records, in-memory
   staging pages,
   and copy-time retired-page list remain unreachable.
3. **Publish, under `writeMu`:** revalidate the snapshot's `CommitSeq`,
   `RootPageID`, `SystemRootPageID`, `LeafGenerationStateVersion`, index
   generation, and exact source-generation state/file lists. Any mismatch
   discards the whole attempt and performs one full retry; no copied root,
   retired-page list, or private allocation is reused.
4. **Install:** while holding `writeMu` and the value-log visibility gate, link
   each retained staged-file handle into `leaf_vlog` with no replacement,
   validate that every destination link is
   still the creation-handle identity, and fsync each distinct parent exactly
   once as one namespace batch. The batch freezes one
   `ResourceOuterLeafPack` token per reachable segment, bound to its immutable
   byte frontier, digest, physical identity, generation, and packed-pointer
   reachability. The original link remains in the private staging namespace
   until successful publication cleanup; promotion never unlinks a source
   pathname that may have been rebound. Capture and resource-set pins deny
   deletion through
   `RegisterSegment`; manager registration must report the same physical
   identities before publication can continue.
   Registration is tentative and publish-owned: existing snapshots retain their
   immutable old set, while `AcquireSnapshot` and `RefreshValueLogSet` cannot
   construct a set containing candidates before root publication. Rebase private
   page IDs by appending final COW pages to the live pager, synchronize those
   pages, publish the new roots and generation manifest through the alternate
   meta page, and only then make committed retired pages reclaimable. On
   platforms that require
   an explicit mapped-view flush, its address and length are aligned to the
   platform mapping granularity. Linux finishes with `fdatasync`; Darwin uses
   `F_FULLFSYNC`, Windows uses `FlushFileBuffers`, and unsupported stable-file
   adapters fail closed. `SyncPages` therefore promises durability of the
   requested pages, not that only those bytes reach stable storage.

If exact revalidation fails, the in-memory staging pager and staged records are
discarded and a full copy retries once; no private page or retired-page list is reused. A failure
before writing the alternate meta page rolls the live append cursor back and
removes candidates while visibility remains exclusive. A sync failure after the
alternate meta write is outcome-ambiguous: all candidate pages and segments are
retained, the handle fails closed with `ErrRecoveryRequired`, and reopen selects
the highest valid durable meta state. Startup removes orphan
`.leaf-pack-copy-*` directories only after acquiring the database lock, so it
cannot delete an active attempt.

Before promotion, leaf pack closes both staging append writers and the private
staging value-log manager, while retaining separately reopened exact segment
handles and identity pins. Any failure after a destination link is installed,
or after the alternate meta write has an ambiguous durability outcome, poisons
the live handle and retains those authorities until `Close`. Cleanup also
checks the creation identity before unlinking a pathname, so a rebound path is
never deleted.

Packed promotion currently fails closed before creating staging state on
platforms without the exact relative-parent, cross-parent no-replace, and
namespace-persistence primitives. In particular, Windows returns the typed
`ErrNamespacePersistenceUnsupported`; portable packed promotion there is
deferred rather than weakened to path-based rename evidence.

`maintenanceMu`, the snapshot generation pins, and `teardownMu` remain held
across copy and publish. Therefore leaf-generation GC cannot reclaim a source
before the replacement root and generation state are durable, and `Close`
cannot tear down the snapshot, pager, or private reader during copy. Ordinary
value-log pointers embedded in copied leaf pages remain persistent references;
leaf packing does not alter their reachability or lifetime.

`BenchmarkLeafGenerationPackCopyPublish` provides the pinned before/after
performance fixture. Run five externally alternating base/head invocations with
`-benchtime=1x -count=1 -benchmem`; it reports copy bytes/second, frames, wall
time, foreground write p95/p99 and completed writes, exclusive publish hold
time, copy attempts, `B/op`, and `allocs/op`.

### 6.3 Offline rewrite (`ValueLogRewriteOffline`)

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

### 6.4 Full storage compaction (`DB.CompactStorage`)

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
The plan records index page/span/freelist debt and a typed `index-vacuum`
disposition. Full mode uses the bounded production thresholds; Exhaustive runs
for any measured reclaimable index debt. Apply re-probes immediately before the
phase, invokes the RecoverableRootSet-fenced online replacement on supported
writable platforms, and checkpoints only after a successful replacement.
One bounded settle replacement runs when later GC/checkpoint phases create new
policy debt; completion is based on the audit after that settle pass.
Transient stale/mutation races are `deferred`, Windows is `unsupported`, and
permanent errors fail compaction. Deferred or unsupported required work keeps
all completion flags false. `PolicyFullyCompacted` means selected planner debt
converged; `ByteMinimized` additionally requires every Exhaustive byte phase to
complete.

Each cold debt audit performs at most one page-granular reachability walk over a
coherent snapshot of the user, system, collection, and protected roots. The
snapshot basis includes the commit and root IDs, leaf-generation state version,
retained value-log set, and protected root/path sets. Dynamic protected-root
snapshots are captured on both sides of backend state and must match; cached
providers also contribute their monotonic root-domain version so same-ID ABA is
observable. For unversioned callbacks, equal canonical root sets with unchanged
backend commit/root state are the same audit-visible basis because retiring or
reusing a protected page changes that backend state. Value-log reference counts
retain every logical pointer projection, while live-byte accounting counts a
grouped physical record once. Audit results are published to the incremental
reference tracker only after the protected basis brackets two exact backend-state
checks; one invalidation is retried and a second returns
`ErrCompactStorageAuditStale`. Later settle/final audits perform zero new walks
when they reuse structural reachability on an exact complete-basis match. File
sizes, pins, and zero-byte debt are recomputed on every audit.

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
