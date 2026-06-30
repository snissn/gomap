# Single-Group Raft Cluster Boundary

Status: normative for the initial `TreeDB/internal/raftcluster` package.

This document records the #3044-0 storage and configuration boundary that later
single-group Raft slices may depend on. It does not choose a Raft library, start
a consensus loop, admit writes, enable `ack_policy=raft_committed`, perform
production snapshot transfer, truncate logs, rejoin nodes, or route multiple
groups.

## Scope

The initial boundary owns:

- one local `NodeID`;
- one `GroupID`;
- a peer membership set that must include the local node;
- a fail-closed config format and required-feature floor;
- deterministic local storage paths for Raft log, stable metadata, apply
  metadata, snapshots, and peer metadata.

## Feature And Version Floor

The initial config format is version `1.0`.

The initial required feature is
`treedb.raftcluster.single_group_provider` at version `1.0`.

Unknown required features, newer major versions, and newer minor versions must
fail closed before opening any provider state. The package's zero-value feature
set is normalized to the current single-group provider floor so callers do not
need to repeat the default feature name.

## Storage Layout

Validation first resolves TreeDB's storage root and main DB directory from
`Options.Dir`. Local Raft cluster state defaults under the resolved root:

```text
<root>/raftcluster/
  nodes/<node-id>/
    groups/<group-id>/
      log/
      stable/
      apply/
      snapshots/
      peers/<peer-id>/
```

An explicit `ClusterDir` may replace `<root>/raftcluster`, but validation still
requires `Options.Dir` so the package can reject storage overlap with TreeDB's
main DB paths. Supported layouts resolve as follows:

- root layout: `Options.Dir = <root>` and main DB at `<root>/maindb`;
- main-DB layout: `Options.Dir = <root>/maindb` and cluster state at
  `<root>/raftcluster`;
- flat or `DisableSideStores=true` layout: root and main DB both resolve to
  `Options.Dir`, with cluster state at `<dir>/raftcluster`.

The Raft paths must be distinct from:

- `<main-db>/wal/`
- `<main-db>/value_vlog/`
- `<main-db>/leaf_vlog/`
- `<main-db>/column_assets/`
- `<main-db>/index.db` for flat layouts
- `<root>/dictdb/` and `<root>/templatedb/` for root/main-DB layouts

The Raft log directory stores consensus-log bytes for the selected future Raft
provider. It is not the TreeDB command WAL.

The `apply/` directory stores durable R3a/FSM apply metadata for this local
node/group, including `apply-progress-v1.log` and `apply-results-v1.log`. These
files are part of the Raft group storage layout and must be backed up/restored
with the group; they are not consensus log bytes and are not stored under
TreeDB's local command WAL.

## Local Command WAL Boundary

The TreeDB command WAL is local physical crash-recovery state and is not a Raft
log. A future Raft provider may commit deterministic command entries by quorum,
but applying those entries to a local TreeDB still has to satisfy the local
command-WAL recoverability rule before that node can report local durability,
advance durable applied-index/idempotency metadata, or return
`ack_policy=raft_committed` success.

## Read-Index Boundary

The package exposes a small read-index contract for future Raft adapters:

- `ReadIndexProvider` obtains a quorum-backed `ReadIndexProof`;
- `ReadIndexBarrier` optionally pins the proof to an expected node/group;
- `ReadIndexProof` must include node ID, group ID, a non-zero read index, and
  `HasQuorum=true`;
- linearizable nativewire reads must convert the proof into an
  `AppliedIndexReadBarrier` and wait until the local node has applied through
  that index before reading local state.

This is only a contract. It does not implement a real Raft read-index provider,
leader transfer handling, lease reads, follower reads, or production routing.

`raftharness.ReadIndexProvider` is the in-process test adapter for this
contract. It derives read-index proofs from an injected committed-entry log so
tests can exercise nativewire read-index/read-barrier composition, but that
evidence is not production quorum evidence and the provider does not apply
entries by itself.

## Recovery Status Boundary

`RecoveryStatusV1` is the report-only readiness contract for snapshot/tail
replay recovery work. It is derived from local durable evidence:

- durable FSM apply progress in `apply-progress-v1.log`;
- local TreeDB `AppliedCommandLSN` coverage;
- snapshot manifest validation and durable boundary-result digest checks;
- optional `AppliedIndexReadBarrier` state for the local read-safety proof.

The stable readiness labels are:

```text
unsafe_no_snapshot
unsafe_manifest_unverified
tail_pending
tail_complete
read_safety_pending
ready_applied_index
unsupported
```

`ready_applied_index` is the only label that may set `safe_to_serve_reads=true`,
and only after a manifest is verified, the requested tail target is locally
applied, and the applied-index read barrier is satisfied. `tail_complete`
without an applied-index proof is not a read-serving claim.

The stable snapshot, tail, and read-safety labels are:

```text
snapshot: no_snapshot, manifest_verified, manifest_rejected
tail: no_snapshot, pending, complete, unknown
read_safety: not_requested, applied_index_satisfied, applied_index_lagging, target_mismatch
```

`RecoveryMetricsV1` freezes the metric keys and low-cardinality labels exported
from the status object. The required metric keys are:

```text
treedb.raftcluster.recovery.safe_to_serve_reads
treedb.raftcluster.recovery.applied_index
treedb.raftcluster.recovery.required_applied_index
treedb.raftcluster.recovery.snapshot_last_included_index
treedb.raftcluster.recovery.tail_target_index
treedb.raftcluster.recovery.tail_lag_entries
treedb.raftcluster.recovery.applied_command_lsn
```

Unsupported production operations must fail closed with explicit status instead
of silently implying support. Current unsupported operation labels are:

```text
log_truncation
production_rejoin
production_snapshot_transfer
```

`raftharness` may report recovery status for injected committed-entry tests. Its
snapshot install helper reconstructs a snapshot cut by replaying the committed
prefix and then uses `ReplaySnapshotTailToNodeV1` for the tail. That is harness
evidence only: it is not production Raft snapshot transfer, does not delete Raft
log entries, and does not implement node replacement/rejoin.

## Value Log Boundary

`<main-db>/value_vlog/` is persistent value storage for large values
referenced by `ValuePtr` records in the index. Raft storage must not treat
`value_vlog` as temporary WAL space, must not truncate it by age, and must not
delete segments unless TreeDB value-log GC or rewrite/compaction proves they are
unreachable.

## Non-Goals

- no real consensus loop;
- no server write admission or redirects;
- no `ack_policy=raft_committed` behavior;
- no real read-index provider, lease-read provider, or follower read routing;
- no production snapshot transfer/install beyond metadata contracts and injected
  harness evidence;
- no Raft log truncation;
- no production node replacement or rejoin protocol;
- no multi-group routing.
