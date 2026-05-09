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
3. If WAL enabled, append commit-log batch (inline or RID form).
4. Apply entries to mutable memtable.
5. Acknowledge caller based on sync mode and durability mode.

Journal and value-log writes are decoupled resources with separate rotation/sync paths.

### 3.1 Commit Fence Metadata (WAL + Value Log)

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

This skip behavior is specific to the existing cached key/value commit log and
its RID fence. Collection WAL transactions use stricter side-ref semantics:
a complete WAL-on collection transaction with a missing required side ref is a
recovery error, not a skipped batch, because later same-collection transactions
depend on collection-local sequencing and root-group atomicity.

## 4. Backend Commit Model

Backend applies flushed operations through copy-on-write zipper merge.

Commit visibility sequence:

1. rewrite affected pages,
2. optionally sync page data,
3. write next meta page (`MetaPage0` or `MetaPage1` alternating),
4. optionally sync meta write,
5. publish new state (commit sequence, roots, value-log set).

## 5. API Durability Semantics

### 5.1 Non-sync APIs

- `Set`, `Delete`, `Batch.Write`: higher throughput, no fsync durability guarantee.

### 5.2 Sync APIs

- `SetSync`, `DeleteSync`, `Batch.WriteSync`: in durable mode, fsync durability boundary; in relaxed modes, relaxed boundary only.

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

## 7. Auto-Checkpoint Defaults (Cached Mode)

When WAL is enabled, public `treedb.Open` defaults to:

- `BackgroundCheckpointInterval = 30s`
- `BackgroundCheckpointIdleDuration = 2s`
- `MaxWALBytes = 2 GiB`

These bound uncheckpointed log growth in long-running workloads.

## 8. Profiles and Intent Bundles

Profiles map high-level intent to durability/integrity bundles:

- `ProfileDurable`: durable mode + checksum verification.
- `ProfileFast`: WAL off relaxed + checksum skip + index optimization bundle + 4 MiB pager chunks with moderate pager sync parallelism + the current run_celestia value-log compression defaults (`auto` / balanced / snappy / medium autotune).
- `ProfileWALOnFast`: WAL on relaxed + checksum skip + the same index + pager chunk/sync + value-log compression bundle.
- `ProfileBench`: fast profile plus disabled background checkpoint/prune triggers for determinism.

## 9. Required Invariants

Implementations and refactors must preserve:

1. WAL off does not disable value-log pointer storage.
2. Pointer records remain readable across reopen and checkpoint.
3. Sync API semantics depend on durability mode exactly as above.
4. Checkpoint establishes a backend boundary and clears obsolete commit logs.

## 10. Compatibility Note (Pre-Alpha)

TreeDB is pre-alpha. WAL/value-log/index on-disk behavior may change between
versions without backward-compatibility guarantees.
