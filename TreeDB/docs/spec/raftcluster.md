# Single-Group Raft Cluster Boundary

Status: normative for the initial `TreeDB/internal/raftcluster` package.

This document records the single-group Raft storage/configuration boundary and
the first submit/apply bridge that later #3044 slices may depend on. It does
not choose a Raft library, start a consensus loop, install snapshots, truncate
logs, implement recovery-status reporting, or route multiple groups.

## Scope

The initial boundary owns:

- one local `NodeID`;
- one `GroupID`;
- a peer membership set that must include the local node;
- a fail-closed config format and required-feature floor;
- deterministic local storage paths for Raft log, stable metadata, apply
  metadata, snapshots, and peer metadata;
- a single-group write-admission boundary;
- a commit-source boundary for deterministic `CommandEntryV1` bytes;
- a committed-entry applier boundary for applying locally through R3a/raftfsm.

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
log. A Raft provider may commit deterministic command entries by quorum, but
applying those entries to a local TreeDB still has to satisfy the local
command-WAL recoverability rule before that node can report local durability,
advance durable applied-index/idempotency metadata, or return
`ack_policy=raft_committed` success.

## Submit/Apply Bridge

`SingleGroupSubmitter` is the first production-facing submit/apply bridge. It
is intentionally one group only:

- admission is checked through `AdmissionProvider` and fails closed for
  follower or unavailable states;
- submitted deterministic native-wire `CommandEntryV1` bytes are decoded before
  commit so unsupported or malformed R3a commands fail before local mutation;
- the commit source must return a committed entry with a non-zero term/index
  and explicit `CommitEvidenceProductionConsensusV1` evidence;
- deterministic harness evidence is a separate evidence kind and never proves
  production quorum commitment;
- the committed entry is applied through a `CommittedCommandApplierV1`, currently
  provided by the raftfsm adapter;
- `AckRaftCommitted` is returned only after production-consensus evidence plus
  local recoverability are both true;
- lower ack requests may pass through the same bridge but must not be upgraded
  to `raft_committed` in the adapter response.

The bridge is a provider boundary, not a complete consensus integration. The
`SequencedCommitSource` helper supplies deterministic in-process term/index
assignment and is useful for adapter tests and smoke wiring. It only proves
`AckRaftCommitted` when the caller explicitly sets production-consensus
evidence; a production Raft adapter is responsible for setting that evidence
only after quorum commit.

Nativewire and Mongo gateway cluster submitter adapters use this bridge for the
single-group create/insert slice. Response metadata carries the post-apply
catalog version so follow-on guarded commands do not build entries from a stale
catalog guard.

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

## Value Log Boundary

`<main-db>/value_vlog/` is persistent value storage for large values
referenced by `ValuePtr` records in the index. Raft storage must not treat
`value_vlog` as temporary WAL space, must not truncate it by age, and must not
delete segments unless TreeDB value-log GC or rewrite/compaction proves they are
unreachable.

## Non-Goals

- no full consensus loop or selected Raft library in this package;
- no leader transfer, redirect handling, or lease reads;
- no real read-index provider, lease-read provider, or follower read routing;
- no snapshot install/export behavior beyond reserving a local path;
- no recovery-status endpoint or rejoin protocol;
- no Raft log truncation;
- no multi-group routing.
