# Write Path and Durability

This document defines write semantics for TreeDB cached mode and backend mode.

## 1. Durability Modes

`Options.Durability` selects one of three modes.

### 1.1 `DurabilityDurable` (default)

- Commit log (WAL/journal): enabled.
- Sync operations (`SetSync`, `WriteSync`, `DeleteSync`) use fsync durability boundaries.
- Non-sync operations may be lost on power loss.

### 1.2 `DurabilityWALOnRelaxed`

- Commit log enabled.
- Sync operations are relaxed (no fsync boundary).
- Crash-consistent for process failure patterns, but not guaranteed durable across power loss.

### 1.3 `DurabilityWALOffRelaxed`

- Commit log disabled.
- Value log remains enabled.
- Sync operations are relaxed.
- Durable boundary for recent writes is checkpoint/flush based, not per-write journal replay.

This document owns the canonical durability-mode matrix. Other docs may
summarize these modes but should not maintain independent durability matrices.
Before user-command WAL coverage lands, collection APIs have an additional
write-domain distinction: acknowledged collection writes can remain
flush-boundary durable rather than durable-at-ack. That current behavior is
owned by `collections-write-domain.md`. The active target for extending
durable-at-ack coverage is the user-command WAL in `user-command-wal.md`; the
older collection root-delta WAL plan is deprecated historical context.

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
- `WriteSync` and other sync boundaries cover the value, revision, root tuple,
  and any applied command boundary together. A crash must not recover a value
  without the matching revision or a revision without the matching value.
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
replay for old raw record batches after `command_wal_v1` activation. Complete
command frames whose dependencies or external refs are missing fail recovery
closed unless the command kind defines a formal idempotent skip rule.

WAL-on collection writes add a stronger visibility boundary for command kinds
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
snapshot/read-set token and the recent-write oracle immediately before publish;
only the final root/meta publication is serialized by existing commit
machinery.

## 5. API Durability Semantics

### 5.1 Non-sync APIs

- `Set`, `Delete`, `Batch.Write`: higher throughput, no fsync durability guarantee.

### 5.2 Sync APIs

- `SetSync`, `DeleteSync`, `Batch.WriteSync`: in durable mode, fsync durability boundary; in relaxed modes, relaxed boundary only.

### 5.3 Collection API Durability

Collection mutators do not have separate `*Sync` Go methods. Their baseline
acknowledgement is mode-dependent. Current shipped collection write-domain
behavior is flush-boundary durable, as defined in
`collections-write-domain.md`. The bullets below describe the target collection
user-command WAL overlay for command kinds that have passed the support matrix:

- `DurabilityDurable`: non-sync collection mutator success is process-crash
  recoverable through command WAL. It is not by itself a power-loss fsync
  guarantee. `Flush`, `FlushAll`, `Checkpoint`, `Close`, or native-wire
  `ack_policy=synced` can add the configured fsync boundary when the server can
  actually satisfy that boundary.
- `DurabilityWALOnRelaxed`: collection mutator success is process-crash
  recoverable through command WAL once the typed frame and required external
  refs are fresh-process-readable. It is not a power-loss guarantee. Native-wire
  `synced` must be rejected unless the server advertises a separately named
  mode-relative relaxed sync policy.
- `DurabilityWALOffRelaxed`: collection mutator success is not durable-at-ack.
  `Flush`, `FlushAll`, `Checkpoint`, and `Close` are the public persistence
  boundaries for pending collection state.

`Flush` and `FlushAll` publish roots and advance `AppliedLSN` when they cover
typed command frames. `Checkpoint` is the database-wide durability/cleanup
boundary. `Close` is a final admission-cut and safe-reopen boundary, not a
promise that every safe-to-delete WAL file was physically removed.

## 6. Checkpoint Semantics

`DB.Checkpoint()` in cached mode is a forced durability/cleanup boundary.

Current behavior:

1. serialize with flush/checkpoint locks,
2. rotate non-empty mutable memtable into queue,
3. rotate WAL writers so future writes use fresh segments,
4. flush queued memtables with sync intent,
5. force backend boundary even if queue was empty,
6. remove old WAL segments not currently active and not retained,
7. run value-log retention checks and pruning.

In backend-only mode, checkpoint is implemented as an empty sync batch write.

Under the user-command WAL target, `DB.Checkpoint()` is also a command-WAL
boundary. That later gate must close admission for the checkpoint cut, wait for
in-flight commands admitted before the cut, drain async publish and write
domains, publish roots, advance durable `AppliedLSN`, create the backend
durability boundary containing that `AppliedLSN`, and only then report a clean
command WAL state or clean commit-log ranges. Root publication and `AppliedLSN`
advancement must be atomic with respect to meta/root recovery: after any crash,
open must select either the old roots plus old `AppliedLSN` or the new roots
plus new `AppliedLSN`, never a split state. A future
checkpoint-without-publication mode must report command WAL debt and retain all
required WAL segments and external refs.

The authoritative `AppliedLSN` must be selected by the same backend meta choice
as the roots whose effects it covers. The V1 storage target is the
in-page-marked meta-page field `AppliedCommandLSN`; the meta write must select
the roots, the command-WAL V1 marker, and `AppliedCommandLSN` together. A
post-commit sidecar, format-config marker, manifest, system-root-only update, or
stats update is not a valid source of recovery truth.

Under the user-command WAL target, `DB.Checkpoint()` success must cover command
WAL. A checkpoint that cannot publish `AppliedLSN` covering pre-cut command
frames must return an error or expose explicit command WAL debt through a new
API; it must not return `nil` and call the command WAL state clean.

## 7. Auto-Checkpoint Defaults (Cached Mode)

When WAL is enabled, public `treedb.Open` defaults to:

- `BackgroundCheckpointInterval = 30s`
- `BackgroundCheckpointIdleDuration = 2s`
- `MaxWALBytes = 2 GiB`

These bound uncheckpointed log growth in long-running workloads.

## 8. Profiles and Intent Bundles

The current public profile surface maps high-level intent to command-WAL-backed
durability/integrity bundles:

- `ProfileCommandWALDurable`: command-WAL raw and collection writes with durable sync/checksum settings. This is the recommended default server profile.
- `ProfileCommandWALRelaxed`: command-WAL raw and collection writes with relaxed sync/read-integrity settings for high-throughput ingest and benchmark comparisons.
- `ProfileBench`: no-WAL benchmark-only ceiling with deterministic background maintenance behavior. It is not a production durability profile.

Legacy/raw bundles such as `ProfileDurable`, `ProfileFast`, and
`ProfileWALOnFast` remain compatibility profiles during the command-WAL
transition. Public servers, wrappers, and new documentation should not present
them as the primary supported profile surface.

## 9. Required Invariants

Implementations and refactors must preserve:

1. WAL off does not disable value-log pointer storage.
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
