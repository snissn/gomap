# Single-Group Raft Cluster Boundary

Status: normative for the initial `TreeDB/internal/raftcluster` package.

This document records the #3044-0 storage and configuration boundary that later
single-group Raft slices may depend on. It does not choose a Raft library, start
a consensus loop, admit writes, enable `ack_policy=raft_committed`, install
snapshots, truncate logs, or route multiple groups.

## Scope

The initial boundary owns:

- one local `NodeID`;
- one `GroupID`;
- a peer membership set that must include the local node;
- a fail-closed config format and required-feature floor;
- deterministic local storage paths for Raft log, stable metadata, snapshots,
  and peer metadata.

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

The Raft log directory stores consensus-log bytes for the selected future Raft
provider. It is not the TreeDB command WAL.

## Local Command WAL Boundary

The TreeDB command WAL is local physical crash-recovery state and is not a Raft
log. A future Raft provider may commit deterministic command entries by quorum,
but applying those entries to a local TreeDB still has to satisfy the local
command-WAL recoverability rule before that node can report local durability,
advance durable applied-index/idempotency metadata, or return
`ack_policy=raft_committed` success.

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
- no read-index or follower read semantics;
- no snapshot install/export behavior beyond reserving a local path;
- no Raft log truncation;
- no multi-group routing.
