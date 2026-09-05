# Write Path and Durability

This document defines write semantics for TreeDB cached mode and backend mode.

## 0. Frozen Publication Contract

This section is normative for every production durability profile. It freezes
the target contract used by the deterministic power-loss oracle and by the
implementation tickets under [#1595](https://github.com/snissn/gomap/issues/1595).
The counterexamples in `TreeDB/power_loss_oracle_test.go` intentionally describe
gaps in the implementation at the time this contract was adopted; later tickets
convert those stable test names into positive conformance cases.

For every generation `N`:

```text
Recoverable(Meta[N]) =>
  Stable(IndexClosure[N]) &&
  Stable(FreelistGeneration[N]) &&
  Stable(ValueLogClosure[N]) &&
  Stable(OuterLeafClosure[N]) &&
  Stable(AuxiliaryAssetClosure[N]) &&
  Stable(RequiredDirectoryEntries[N]) &&
  Stable(PublicationSeal[N]) &&
  Stable(AppliedWALCoverage[N])
```

`PublicationSeal[N]` is the immutable `DurableRootRecordV1` page bound by the
selected meta, together with its `DependencyManifestV1` and COW closure. The
oracle's `before-publication-seal-write` and
`after-publication-seal-write` cuts bracket the exact root-record page write.
The record becomes stable only with the subsequent exact-index sync and becomes
recovery-selectable only after the alternate meta is written and synced. These
events describe ordered writes; they are not claims that an fsync happened.

`Stable` means present in the modeled image after process-volatile file bytes
and unsynced namespace mutations have been discarded. `Write`, mapped dirtying,
and userspace `Flush` affect only volatile state. File sync promotes the covered
file bytes. Directory sync promotes creation, rename, and unlink of names in
that directory. A file sync cannot substitute for the required directory sync.

The fixed consequences are:

- A relaxed acknowledgement may lose a recent complete suffix after power
  loss. It may not create an incomplete recoverable root or a hole in replay.
- Recovery considers exactly the two alternating sealed meta generations,
  validates each bounded closure, selects the newest complete generation, and
  may fall back to the older complete generation.
- Until a newer sealed root supersedes it, every index page, freelist page,
  value-log record, outer-leaf record, auxiliary asset, command-WAL range, and
  directory entry needed by either recoverable generation remains intact.
- `visibleCommitSeq`, `durableCommitSeq`, and
  `oldestRecoverableCommitSeq` are distinct. Candidate visibility does not make
  a meta recoverable and cannot move the reclamation horizon.
- Composite and nested roots carry the deterministic transitive union of their
  child dependencies. Publication does not rediscover dependency closure by
  scanning filenames.
- `Checkpoint`, clean `Close`, and every public `*Sync` operation are durable
  boundaries in every production profile. Command-WAL `*Sync` may stop at a
  stable complete frame closure without forcing a backend root; journal-free `*Sync`
  waits for a sealed root covering the call.
- A write or sync error after target-meta bytes may have been dirtied poisons
  the writable handle. Writes, publication, cleanup, GC, rewrite, and
  reclamation fail with the public recovery-required error until reopen.

### 0.1 Normative publication state machine

| State | Stable authority | Permitted next action | Failure rule |
|---|---|---|---|
| `visible` | last sealed complete meta; candidate is process-local | prepare a complete candidate and its transitive dependency set | discard private work; retain the old recovery horizon |
| `dependencies-stable` | old sealed metas plus synced dependency bytes and names | write/sync COW index, freelist, manifest, and seal pages, excluding meta | retain dependencies and retry; no new meta is recoverable |
| `sealed-candidate` | old sealed metas plus complete stable candidate closure | write the alternate meta exactly once | any error after target-meta dirtying poisons the handle |
| `meta-dirtied` | old sealed metas remain the only proven recovery authority | sync the target meta | any error poisons the handle; recovery validates both slots |
| `meta-stable` | newly synced, validated meta and its complete closure | advance `durableCommitSeq`, then the recovery/reclamation horizon when safe | cleanup still requires exact sealed coverage proof |
| `cleanup-eligible` | both recoverable metas and all pins remain protected | unlink only proof-covered WAL/assets, then sync deletion directories | a stale plan deletes nothing; unsynced unlink is not durable deletion |

The alternate meta slot is selected from the last successfully synced durable
meta slot, never from visible or candidate state. No dependency, directory,
index, meta, or durability wait occurs while DB/write/commit/root-build
serialization locks are held.

### 0.2 Normative mode and API matrix

These canonical profiles define the current public surface. `bench_unsafe` is
explicitly outside the production guarantee.

| Profile | Ordinary acknowledgement | Public `*Sync` | `Flush` / `FlushAll` | `Checkpoint` and clean `Close` | Modeled power-loss result |
|---|---|---|---|---|---|
| `command_wal_durable` | waits for stable complete command-frame closure | same durable frame closure; root publication is not required | visibility/draining only | waits for a sealed complete root covering the captured frontier | every durable acknowledgement recovers; selected root is complete |
| `command_wal_relaxed` | may lead WAL and root sync | waits for stable complete command-frame closure | visibility/draining only | waits for a sealed complete root covering the captured frontier | may lose only a complete recent suffix; replay remains contiguous |
| `no_wal_fast` | may lead sealed-root publication | waits for a sealed complete root covering the call | visibility/draining only | waits for a sealed complete root covering the captured frontier | may lose only a complete recent suffix back to the last sealed root |
| `bench_unsafe` | benchmark-defined | benchmark-defined | benchmark-defined | benchmark-defined | no production guarantee |

All production profiles enable integrity checks. `Flush` never means file or
directory sync. There is no implicit relaxed downgrade for an explicit sync
request and no public fast-close/abort alias in this contract.

## 1. Durability Modes

The resolved profile owns `Options.Durability`; the durability enum alone is
not a public acknowledgement contract.

### 1.1 `DurabilityDurable` (`command_wal_durable`, default)

- Command WAL is enabled.
- Ordinary supported public acknowledgements wait for a stable,
  dependency-complete command-WAL prefix.
- Sync operations (`SetSync`, `WriteSync`, `DeleteSync`) use the same durable
  prefix boundary; they need not checkpoint the backend root per call.
- Checkpoint and clean close publish a sealed complete root.

### 1.2 `DurabilityWALOnRelaxed` command-WAL relaxed / legacy compatibility

- With `CommandWAL=true`, ordinary command-WAL frames use relaxed
  acknowledgement, while explicit sync operations close and persist a durable
  V2 prefix before acknowledgement.
- Without command WAL, this is the legacy compatibility cached redo-journal
  mode with relaxed sync operations.
- Ordinary writes remain crash-consistent for process failure patterns and may
  be lost across power loss; a successful command-WAL explicit sync is durable.

### 1.3 Legacy compatibility `DurabilityWALOffRelaxed`

- Command WAL and the legacy compatibility cached redo journal are disabled.
- Value log remains enabled.
- Ordinary writes may acknowledge ahead of sealed-root publication.
- Explicit sync operations wait for a sealed complete root covering the call.
- `Flush` is a visibility/draining boundary. `Checkpoint` and clean `Close`
  wait for a sealed complete root covering their captured frontier.

This document owns the canonical durability-mode matrix. Other docs may
summarize these modes but should not maintain independent durability matrices.
Supported collection commands use this same profile matrix even when their
process-visible writes are buffered and root publication follows later. See
`collections-write-domain.md` for staging, visibility and replay coverage;
unsupported commands must fail before admission rather than weaken a durable
profile. `user-command-wal.md` owns command coverage and encoding. The older
collection root-delta WAL plan is deprecated historical context.

## 2. Value Placement (Inline vs Pointer)

### 2.1 Threshold selection

- Baseline inline threshold: 512 bytes (`page.DefaultInlineThreshold`).
- Cached-mode pointer threshold default: 512.
- `ValueLog.PointerThreshold > 0` overrides default.
- `ValueLog.ForcePointers=true` stores all values out-of-line.

### 2.2 Pointer hard cap behavior

If `ValueLog.MaxRetainedBytesHard` is set and retained bytes exceed the cap,
new large values stop using pointers until pressure drops.

## 3. Cached-Mode Write Pipeline

For each write batch, implementation conceptually performs:

1. Choose lane.
2. For eligible values, append to value log and build `ValuePtr` references.
3. If WAL enabled, append to the commit-log segment family. Pre-command-WAL
   directories use the legacy raw batch payload (inline or RID form);
   command-WAL directories use a `RawKVBatch` command frame payload.
4. Apply entries to mutable memtable.
5. Acknowledge caller based on sync mode and durability mode.

Journal and value-log writes are decoupled resources with separate rotation/sync paths.

### 3.1 Target raw-KV revision flow

Entry revisions are target native write metadata, not a separate write domain.

- Raw KV writes assign the next live-entry revision as part of the same logical
  mutation that installs the value/tombstone in the memtable or backend root.
- All raw-KV write paths draw revisions from one persisted revision domain. The
  hot path should be a scalar allocator/floor selected with the root metadata,
  not a second ordered-root write. Open, replay, profile changes, and Raft
  failover must seed the active allocator above the durable `MaxEntryRevision`;
  otherwise versioned mutation support fails closed before visibility.
- Cached mode must carry revisions through mutable memtables, queued memtables,
  flush iterators, merge iterators, and backend publication before versioned
  reads are advertised as supported.
- Backend-only mode must carry revisions through `batch.Entry`, zipper apply,
  leaf builders, and root publication without publishing an additional
  metadata root per user commit.
- Command-WAL mode records deterministic logical raw KV commands before
  visibility. Live apply and replay use the accepted command LSN as the
  mutation revision for every raw key touched by that command frame only when the
  command LSN allocator is seeded above the persisted revision floor. Otherwise
  the accepted command frame must carry one effective mutation revision for the
  raw keys it touches.
- Backend-only WAL-off raw writes use the backend commit sequence that publishes
  their root as the mutation revision only when that sequence is seeded above the
  persisted revision floor; otherwise the backend path must allocate from the
  shared revision domain before building/publishing the root.
- Cached WAL-off raw writes use a cached mutation sequence allocated before the
  mutable memtable entry becomes visible. That same revision is carried through
  queued memtables, flush/merge iterators, and backend publication; flush must
  not rewrite a snapshot-visible revision.
- Future Raft-applied raw writes use the Raft apply identity as the mutation
  revision, and any local command-WAL frame for that apply must carry or derive
  that same identity.
- Sync boundaries cover the value, revision, and local recoverability boundary
  together. When a backend root is published, the root tuple and any applied
  command boundary are selected with that value/revision effect. In cached
  WAL-on mode, `WriteSync` must not require backend root publication per point
  write; it covers the accepted WAL frame, value/revision payload, and memtable
  replay input until a later flush/checkpoint publishes roots. A crash must not
  recover a value without the matching revision or a revision without the
  matching value.
- Implementations must treat a sidecar-per-write revision tree as a rejected
  design for this target because it adds a second publish to the hot raw write
  path.

### 3.2 Commit Fence Metadata (WAL + Value Log)

For commit-log batches that carry sequence numbers (`Record.Seq > 0`), the
sequence acts as a durable commit fence:

- all records in a batch share one commit sequence (enforced by commit-log
  batch writer and validated during recovery replay),
- RID-backed records in that batch are only published during recovery when all
  referenced RIDs are present in the scanned value-log set,
- if any RID is missing (for example, partial/torn value-log flush), the whole
  fenced batch is skipped.

This prevents replaying partial pointer commits and avoids phantom pointer
visibility after crash recovery.

This skip behavior is specific to the existing pre-command-WAL cached key/value
commit log and its RID fence. The target user-command WAL replaces raw
`commitlog.Record` batches with `RawKVBatch` command frames, uses command LSN
ordering and applied-LSN checkpointing, and does not require compatibility
replay for old raw record batches after `command_wal_v2` activation. Complete
command frames whose dependencies or external refs are missing fail recovery
closed unless the command kind defines a formal idempotent skip rule.

`Options.JournalCompression` is not command-frame compression. Strict V2
command frames remain raw length/CRC-bounded segment records even when generic
commitlog compression is requested, so torn-tail inspection can classify the
frame identity, LSN, and durability class before replay. A compressed command
WAL therefore requires a dedicated payload-aware format and recovery proof;
turning on generic segment compression is intentionally rejected by strict V2
readers and is not a write-throughput optimization.

Command-WAL collection writes add a stronger visibility boundary for command kinds
that are `WAL-supported`: no collection read, scan, uniqueness check,
update/delete planner, or pending-state merge may observe a mutation before its
command WAL frame is committed/recoverable. Unsupported mutation kinds must be
classified as `WAL-rejected` or `WAL-off-only` in `user-command-wal.md`.

Raw public key/value writes are part of the command-WAL surface when a DB is
opened with `CommandWAL=true`: `Set`, `SetSync`, `Delete`, `DeleteSync`,
`DeleteRange`, `Batch.Write`, and `Batch.WriteSync` append typed `RawKVBatch`
command frames before publishing visibility. Public cached raw operations that
cannot be replayed as typed commands yet fail closed with
`ErrCommandWALRejected`; currently that includes callback-based `Update` and
`UpdateSync`.

Raw backend apply routes are also part of the span-native default-admission
support surface. Backend stats report `treedb.raw.span_native.route.<route>.*`
for point puts, point deletes, mixed point batches, range-delete batches, empty
batches, mixed range-delete batches, and close/checkpoint drains. Default-auto
admitted point routes may use span-native apply; unsupported backend rows fail
closed with named reasons such as `range_delete_barrier`, `below_threshold`,
`disabled`, `admission_policy_decline`, or `close_or_checkpoint`. Public
command-WAL `Update` and `UpdateSync` rejections are reported under
`treedb.raw.span_native.public.route.<route>.fallback.reason.command_wal_barrier.*`
because they return before a backend batch exists; backend
`treedb.raw.span_native.route.<route>.*` counters preserve
`command_wal_barrier` only for backend batches that exist but are explicitly
fenced by command-WAL publish or checkpoint applied-LSN boundaries.

The user-command WAL is a local crash-recovery log, not a Raft log. Future Raft
entries may share command-envelope payloads, but consensus ordering and local
recoverability remain separate responsibilities.

## 4. Backend Commit Model

Backend applies flushed operations through copy-on-write zipper merge.

Commit visibility sequence:

1. rewrite affected pages,
2. optionally sync page data,
3. write next meta page (`MetaPage0` or `MetaPage1` alternating),
4. optionally sync meta write,
5. publish new state (commit sequence, roots, value-log set).

Target conditional transactions use the same backend commit model for final
publication. The transaction body may perform reads and stage writes without
holding the final commit lock. Commit validation runs against the transaction's
snapshot/read-set token and the recent-write oracle immediately before publish.
The final validation, recent-write oracle update, and root/meta publication
must be one serialized commit/CAS boundary so two conflicting transactions
cannot both validate against the same pre-publish state. The transaction body
must remain outside that final serialized boundary.

## 5. API Durability Semantics

### 5.1 Non-sync APIs

- `Set`, `Delete`, `Batch.Write`: higher throughput, no fsync durability guarantee.

### 5.2 Sync APIs

- `SetSync`, `DeleteSync`, `Batch.WriteSync`: in command-WAL profiles, a durable
  command-frame prefix; in `no_wal_fast`, a sealed complete root covering the
  call. Legacy profiles without command WAL retain their historical journal
  boundary semantics.

### 5.3 External-version MVCC commits

The opt-in `TreeDB/mvcc.Store.CommitAt` stages every encoded put/tombstone for a
caller timestamp into one raw TreeDB batch. Validation and duplicate detection
finish before batch creation. A staging error closes the unwritten batch; a
commit error may be ambiguous as to whether the whole batch published, but
partial visibility is forbidden by the underlying batch boundary.

- `CommitRelaxed` calls `Batch.Write`. It is an atomic visibility boundary, not
  an fsync promise. Survival follows the configured relaxed mode and later
  checkpoint/close boundaries.
- `CommitDurable` fails with `ErrDurabilityUnavailable` unless the handle's
  effective mode is `DurabilityDurable`, then calls `Batch.WriteSync`. A nil
  return covers the configured WAL/value payload fsync recovery boundary; it
  does not force an immediate backend-root checkpoint.
- After timestamp/mode and non-nil Store validation, empty mutation lists are
  no-ops: they do not access storage, probe the handle's open/durability state,
  or manufacture a sync boundary.

External timestamps are key bytes in the reserved MVCC subspace. They neither
select TreeDB's internal commit sequence nor invoke conditional transactions;
the caller owns conflict detection and timestamp assignment.

`AdvanceDiscardFloor` and `PruneVersions` accept the same relaxed/durable mode
split. Durable floor advancement requires `DurabilityDurable` and publishes its
metadata record with `Batch.WriteSync`. Durable pruning first re-writes and
syncs the already-published floor, then syncs each bounded delete batch. Thus a
recovered physical deletion cannot exist without a recovered floor that rejects
affected historical reads. Relaxed maintenance is atomic and ordered but has
only the configured relaxed crash boundary; a later checkpoint/close is needed
for a reopen guarantee. Repeating the current floor in durable mode is an
explicit durability upgrade and re-syncs the metadata; only the equal relaxed
advance is a no-op.

### 5.4 Collection API Durability

Collection mutators do not have separate `*Sync` Go methods. Their baseline
acknowledgement is profile-dependent, as defined in
`collections-write-domain.md` and the normative matrix in section 0.2. The
following guarantees apply to collection command kinds marked supported in
`command-wal-support-matrix.json`; unsupported operations must fail closed rather
than silently use a weaker flush-boundary guarantee:

- `DurabilityDurable`: ordinary supported collection/catalog mutator success
  waits for the typed command and all required external refs to form a stable,
  dependency-complete command-WAL prefix. It is power-loss durable without an
  immediate backend-root checkpoint.
- `DurabilityWALOnRelaxed`: in the command-WAL relaxed profile, ordinary
  collection/catalog success may lead the stable WAL frontier. A successful
  backend `DB.Checkpoint` or clean `DB.Close` waits for a sealed complete root
  covering its captured frontier, including required external dependencies.
  `Collection.Flush` and `CollectionManager.FlushAll` only drain visibility;
  neither establishes power-loss durability. Until a durable boundary covers
  them, recent ordinary acknowledgements may lose only a complete suffix.
- `DurabilityWALOffRelaxed`: under production `no_wal_fast`, ordinary success
  may lead sealed-root publication and an explicit sync waits for a sealed root
  covering the call. `Flush` and `FlushAll` remain visibility/drain operations;
  `Checkpoint` and clean `Close` are sealed-root boundaries. The separate
  `bench_unsafe` profile carries no production guarantee.

`Flush` and `FlushAll` publish roots and advance `AppliedLSN` when they cover
typed command frames. `Checkpoint` is the database-wide durability/cleanup
boundary. `Close` is a final admission-cut and safe-reopen boundary, not a
promise that every safe-to-delete WAL file was physically removed.

## 6. Checkpoint Semantics

`DB.Checkpoint()` in cached mode is a forced durability/cleanup boundary.

Checkpoint ownership and writer admission are separate states:

- `flushMu` serializes checkpoint/flush ownership. `checkpointMu` and its
  condition variable serialize checkpoints and wake writers when an admission
  phase changes. The `checkpointing` state remains active for the entire
  operation.
- `writeMu` is held only for the frontier cut. While it is held, checkpoint
  rotates the non-empty mutable memtable into a stable queue, captures the
  queue frontier, and invokes the command-WAL cutover hook. The hook snapshots
  the maximum covered command LSN and rotates the active command-WAL segment.
- Once that hook has run, command-WAL point writes may enter the fresh mutable,
  command-WAL, and value-log generation while the captured frontier drains.
  Those post-cut writes remain visible in cached state but are not part of the
  active checkpoint's backend publication or `AppliedCommandLSN` boundary.
  Range-span writes remain gated through checkpoint drain because their atomic
  command append and span-layer publication still require `flushMu`; their wait
  is reported under `checkpoint_drain`, not as post-frontier admission.
- Cached redo-WAL mode, unsafe WAL-off mode, and external command-WAL mode
  without an installed cutover hook retain the full-checkpoint writer stop.
  They do not have the command-LSN/segment ownership proof needed for safe
  post-frontier admission. Maintenance also retains its full writer stop.

The active checkpoint then:

1. serialize with flush/checkpoint locks,
2. rotate non-empty mutable memtable into queue,
3. capture a stable queue/command-LSN frontier and rotate WAL ownership,
4. reopen command-WAL admission into the fresh post-cut generation when the
   cutover hook established that ownership,
5. flush only the captured frontier with sync intent,
6. publish roots and the covered `AppliedCommandLSN`,
7. force a backend boundary even if the queue was empty,
8. remove only WAL segments covered by that published boundary, and
9. run value-log retention checks and pruning while preserving active,
   retained, and command-WAL-protected references.

In backend-only mode, checkpoint is implemented as an empty sync batch write.

With user-command WAL, `DB.Checkpoint()` is also a command-WAL boundary. The
short cutover gate closes admission for the checkpoint cut and waits for
in-flight commands admitted before the cut. The checkpoint then drains its
captured publish/write domains, publishes roots, advances durable `AppliedLSN`,
creates the backend durability boundary containing that `AppliedLSN`, and only
then reports a clean command WAL state or clean commit-log ranges. Reopening
admission after the cut does not relax `Checkpoint()` completion: success still
means the entire captured frontier, durability metadata, and covered-segment
cleanup completed. A publish or cleanup failure is returned to the caller, and
post-cut segments remain recovery-owned. Root publication and `AppliedLSN`
advancement must be atomic with respect to meta/root recovery: after any crash,
open must select either the old roots plus old `AppliedLSN` or the new roots
plus new `AppliedLSN`, never a split state. A future
checkpoint-without-publication mode must report command WAL debt and retain all
required WAL segments and external refs.

For a cached command-WAL checkpoint with no pending public frames, maintenance
may republish the selected roots with the same `AppliedCommandLSN` only when
the recovery-selectable fallback slot still has an older applied LSN. It writes
no command-WAL frame and does not advance the next LSN; it waits for the
fallback slot to converge before covered-segment cleanup can use that proof.
If the publication fails, cleanup remains fail-closed and the older fallback
continues to retain the required command-WAL coverage.

The authoritative `AppliedLSN` must be selected by the same backend meta choice
as the roots whose effects it covers. The V1 storage target is the
in-page-marked meta-page field `AppliedCommandLSN`; the meta write must select
the roots, the command-WAL V1 marker, and `AppliedCommandLSN` together. A
post-commit sidecar, format-config marker, manifest, system-root-only update, or
stats update is not a valid source of recovery truth.

Stable split-leaf manifest replacement is a separately certified dependency
operation: exactly one sync persists the encoded manifest file and exactly one
retained-parent sync persists its rename. A token captured after those steps is
already content-synced, so `SyncThrough` must not repeat the file sync. Until
durable-root publication transfers that exact token into the selected
candidate's `DependencyManifestV1`, the replacement does not by itself make the
manifest a root or `AppliedLSN` dependency and does not change the broader
publication ordering.

Under the user-command WAL target, `DB.Checkpoint()` success must cover command
WAL. A checkpoint that cannot publish `AppliedLSN` covering pre-cut command
frames must return an error or expose explicit command WAL debt through a new
API; it must not return `nil` and call the command WAL state clean.

Checkpoint/write coordination is observable without conflating unrelated
causes. `treedb.cache.write.wait.<reason>.*` reports totals, maximum/last
latency, count, fixed cumulative latency buckets, and p50/p95/p99 bucket upper
bounds for `frontier_cutover`, `checkpoint_drain`, and `maintenance`.
`treedb.cache.write.post_frontier_admission.count_total` counts point writes
admitted while an older command-WAL frontier remains active. The compatibility
aggregate `treedb.cache.write.wait_for_checkpoint.*` remains available.
Flush-lock and I/O ownership stay separate in
`treedb.cache.checkpoint.flushmu_wait_*` and
`treedb.cache.checkpoint.stage.<cutover|wal_rotate|value_log_flush|command_wal_publish|flush_all|backend_boundary|wal_cleanup>.*`.

## 7. Auto-Checkpoint Defaults (Cached Mode)

When WAL is enabled, public `treedb.Open` defaults to:

- `BackgroundCheckpointInterval = 30s`
- `BackgroundCheckpointIdleDuration = 2s`
- `MaxWALBytes = 2 GiB`

Automatic interval, idle, and size passes are maintenance triggers. In external
command-WAL mode they rotate and clean only the recovery-covered command-WAL
prefix; they do not publish the current visible root frontier. Legacy or
otherwise unwired backends fall back to a full checkpoint. Explicit
`Checkpoint`, sync, and close remain full durable barriers. The triggers bound
reclaimable WAL pressure, but uncovered segments may remain until a later
durable frontier covers them.

## 8. Profiles and Intent Bundles

The current public profile surface maps high-level intent to command-WAL-backed
durability/integrity bundles:

- `ProfileCommandWALDurable`: ordinary acknowledgements and explicit syncs wait
  for a durable dependency-complete command-WAL prefix. This is the recommended
  default server profile.
- `ProfileCommandWALRelaxed`: ordinary acknowledgements are relaxed; explicit
  syncs wait for a durable dependency-complete command-WAL prefix.
- `ProfileNoWALFast`: production no-WAL profile; ordinary acknowledgements are
  relaxed and explicit syncs wait for a sealed complete root.
- `ProfileBenchUnsafe`: benchmark/test-only ceiling with no durability promise
  and an explicit benchmark constructor/parser boundary.

All production profiles verify value-log integrity. `Open` makes their owned
durability and integrity fields immutable while preserving caller-owned tuning.

Deprecated raw cached-journal bundles remain available only for low-level
compatibility tests and forensic reproduction. Public servers, wrappers, and new
documentation should present the three canonical production profiles, and
benchmark documentation may additionally present the explicit unsafe ceiling.

## 9. Required Invariants

Implementations and refactors must preserve:

1. Legacy/benchmark WAL-off compatibility mode does not disable value-log
   pointer storage.
2. Pointer records remain readable across reopen and checkpoint.
3. Sync API semantics depend on durability mode exactly as above.
4. Checkpoint establishes a backend boundary and clears obsolete commit logs.
5. Target revision metadata is covered by the same visibility and durability
   boundary as its value.
6. Target conditional transaction conflict detection must not rely on
   persistent tombstones after all active transactions that can observe them
   have closed.

## 10. Compatibility Note (Pre-Alpha)

TreeDB is pre-alpha. WAL/value-log/index on-disk behavior may change between
versions without backward-compatibility guarantees.
