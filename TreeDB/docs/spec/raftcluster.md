# Single-Group Raft Cluster Boundary

Status: normative for the initial `TreeDB/internal/raftcluster` package.

This document records the single-group Raft storage/configuration boundary,
recovery-status reporting boundary, first submit/apply bridge, and first
HashiCorp Raft backed quorum provider that later issue `#3044` slices may
depend on. It does not implement production snapshot transfer, truncate logs,
rejoin nodes, follower read routing, lease reads, or route multiple groups.

## Scope

The initial boundary owns:

- one local `NodeID`;
- one `GroupID`;
- a peer membership set that must include the local node;
- a fail-closed config format and required-feature floor;
- deterministic local storage paths for Raft log, stable metadata, apply
  metadata, snapshots, and peer metadata;
- a single-group write-admission boundary;
- a pre-commit deterministic/catalog preflight boundary;
- a commit-source boundary for deterministic `CommandEntryV1` bytes;
- a committed-entry applier boundary for applying locally through R3a/raftfsm;
- `HashicorpRaftProvider`, the first real single-group quorum commit provider
  behind `SingleGroupSubmitter`.

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

For `HashicorpRaftProvider`, the Raft log directory stores
`raft-log.bolt` and the stable metadata directory stores
`raft-stable.bolt`. These are HashiCorp Raft consensus/stable-state stores,
not the TreeDB command WAL.

The `apply/` directory stores durable R3a/FSM apply metadata for this local
node/group, including `apply-progress-v1.log` and `apply-results-v1.log`. These
files are part of the Raft group storage layout and must be backed up/restored
with the group; they are not consensus log bytes and are not stored under
TreeDB's local command WAL.

## Local Command WAL Boundary

The TreeDB command WAL is local physical crash-recovery state and is not a Raft
log. A Raft provider may commit deterministic command entries by quorum, but
applying those entries to a local TreeDB still has to satisfy the local
command-WAL recoverability rule before that node can report local durability,
advance durable applied-index/idempotency metadata, or return
`ack_policy=raft_committed` success.

Target raw KV entry revisions are part of that local recoverability boundary.
If a committed command mutates raw KV and the Raft apply identity is the
`EntryRevision` authority, local apply must install the value or tombstone and
its revision through the normal command-WAL/TreeDB executor before durable
applied-index, idempotency-result metadata, or recovery status can advance past
that command. Deterministic conditional raw KV preconditions are command input:
unsupported or malformed conditional frames must fail closed before commit when
preflight can detect them, and after commit they must not be marked applied or
locally recoverable until the value, revision, root tuple, and local
`AppliedLSN` boundary are durable together.

## Submit/Apply Bridge

`SingleGroupSubmitter` is the first production-facing submit/apply bridge. It
is intentionally one group only:

- admission is checked through `AdmissionProvider` and fails closed for
  follower or unavailable states;
- request route metadata, when present, is binding: `ClusterRouteGroupID` must
  match the local single-group submitter before command preflight, commit, or
  local apply can run;
- submitted deterministic native-wire `CommandEntryV1` bytes are decoded before
  commit so unsupported or malformed R3a commands fail before local mutation;
- decoded entries are preflighted against the local deterministic apply/catalog
  boundary before the commit source assigns a Raft log identity, so conflicts
  such as missing collections reject without consuming an index;
- the commit source must return a committed entry with a non-zero term/index
  and explicit `CommitEvidenceProductionConsensusV1` evidence;
- deterministic harness evidence is a separate evidence kind and never proves
  production quorum commitment;
- the committed entry is applied through a `CommittedCommandApplierV1`, currently
  provided by the raftfsm adapter, and local apply acknowledgements are
  serialized by the single-group bridge so a later committed index cannot race
  ahead of an earlier committed index;
- `AckRaftCommitted` is returned only after production-consensus evidence plus
  local recoverability are both true;
- every accepted submit currently requires production-consensus evidence; lower
  ack policies only control the response durability level after that commit and
  must not be upgraded to `raft_committed` in the adapter response.

The bridge is a provider boundary. `HashicorpRaftProvider` is the first
production consensus adapter for that boundary. It implements
`AdmissionProvider` from live HashiCorp Raft node state and implements
`CommitSource` by submitting the deterministic entry bytes through
`Raft.Apply`; it returns `CommitEvidenceProductionConsensusV1` only after the
Apply future completes with a committed log term/index. The provider persists
its Raft log/stable stores under the raftcluster storage layout and applies
committed command entries through the local `CommittedCommandApplierV1`.

HashiCorp Raft stores internal log entries, including initial configuration
entries and leader-term no-op entries, in the same log index space as user
commands. The raftfsm durable apply store may therefore be opened with
`AllowInitialIndexGap` for this provider so the first applied TreeDB command can
use the committed Raft log index even when index `1` is an internal Raft entry.
After that, command indexes must remain strictly increasing with non-decreasing
terms, but they may have gaps where committed Raft log entries did not deliver a
TreeDB command to the FSM.

The `SequencedCommitSource` helper supplies deterministic in-process term/index
assignment and is useful for adapter tests and smoke wiring. It only proves
`AckRaftCommitted` when the caller explicitly sets production-consensus
evidence; real production evidence should come from `HashicorpRaftProvider` or
a future quorum adapter after quorum commit.

Nativewire and Mongo gateway cluster submitter adapters use this bridge for the
single-group create/insert slice. Response metadata carries the post-apply
catalog version so follow-on guarded commands do not build entries from a stale
catalog guard.

## Read-Index Boundary

The package exposes a small read-index contract for future Raft adapters:

- `ReadIndexProvider` obtains a quorum-backed `ReadIndexProof`;
- `ReadIndexBarrier` optionally pins the proof to an expected node/group;
- `ReadIndexProof` must include node ID, group ID, a non-zero read index,
  `HasQuorum=true`, and explicit evidence provenance;
- production linearizable reads accept only `ReadIndexEvidenceProduction`,
  which a real Raft adapter may set only after a read-index or equivalent
  production quorum proof;
- linearizable nativewire reads must convert the proof into an
  `AppliedIndexReadBarrier` and wait until local state covers every TreeDB
  command at or below that proof index before reading local state.

HashiCorp Raft read-index proofs may land on internal no-op or configuration
log entries. A production read-index adapter must therefore track applied Raft
log coverage or translate the proof to the latest TreeDB command index at or
below the proof; it must not assume the FSM's last applied TreeDB command index
is identical to the provider's latest applied Raft log index.

`HashicorpRaftProvider.ReadIndex` implements the first production leader-local
provider for this contract. It uses HashiCorp Raft leader/quorum evidence,
checks the committed prefix has the current term, scans command-free gaps, and
waits for TreeDB applied progress before returning
`ReadIndexEvidenceProduction`. It fails closed on follower state, leadership
loss, target mismatch, missing applied progress, or missing log evidence. This
does not add leader transfer handling, lease reads, follower reads, or
production routing.

The catalog-meta provider has the corresponding internal no-log authority
proof for immutable vector serving. `LinearizableCatalogMetaReadProofV1`
performs `VerifyLeader`, anchors the lease at that successful verification,
binds it to one leader term, requires a
committed entry from that term, waits for the local Raft and catalog applied
floors, and returns a bounded process-local monotonic lease. Repeated proof
reads do not append `LogBarrier` entries. Local lease validation rechecks
leader, term, Raft applied progress, and the exact catalog applied identity;
expiry or any mismatch fails closed. This lease is not a serializable or
caller-trusted cross-process capability; authenticated propagation remains the
serving layer's responsibility.

`raftharness.ReadIndexProvider` is the in-process test adapter for this
contract. It derives read-index proofs from an injected committed-entry log so
tests can exercise nativewire read-index/read-barrier composition, but that
evidence is not production quorum evidence and the provider does not apply
entries by itself. Harness proofs are marked `ReadIndexEvidenceTestHarness` and
must fail closed at nativewire's production `linearizable` read boundary.

Vector partition shard search V1 consumes this same generic boundary. It
resolves one owner group from the vector partition placement record, calls
`RoutedReadIndexCoordinator` for that group, independently validates the
returned proof/apply target, and only then pins M3 partition assets. It does not
add a vector-specific read proof, remote forwarding path, or consistency
mechanism. See
[vector-partition-shard-search-v1.md](vector-partition-shard-search-v1.md).

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

- no bespoke consensus implementation beyond the narrow HashiCorp Raft adapter;
- no leader transfer, redirect handling, or lease reads;
- no lease-read provider, follower read routing, or read-index routing beyond
  the leader-local `HashicorpRaftProvider.ReadIndex` proof;
- no production snapshot transfer/install beyond metadata contracts and injected
  harness evidence; `HashicorpRaftProvider` snapshots currently fail closed;
- no Raft log truncation;
- no production node replacement or rejoin protocol;
- no multi-group routing.
