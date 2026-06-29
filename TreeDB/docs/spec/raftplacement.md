# Raft Placement Catalog

Status: normative for the initial `TreeDB/internal/raftplacement` package.

This document records the first executable slice of #3046. The package defines
a v1 placement catalog and a pure route resolver for collection-level Raft
groups only. It does not change native-wire read policies, Mongo gateway
behavior, `raftentry` single-group scope handling, server routing, submitter
APIs, meta-group replication, rebalancing, or token/ring partitioning.

## V1 Catalog Shape

The catalog contains:

- a fail-closed catalog format version and required-feature floor;
- group definitions keyed by `raftcluster.GroupID`;
- group members and optional leader hints keyed by `raftcluster.NodeID`;
- collection placements keyed by `{database, catalog, collection}`;
- one owning `raftcluster.GroupID` for each placed collection.

The initial catalog format is version `1.0`.

The initial required feature is
`treedb.raftplacement.collection_groups` at version `1.0`.

Unknown required features, newer major versions, and newer minor versions MUST
fail closed before route resolution is available.

## Validation Rules

Validation MUST reject:

- an empty group set;
- duplicate group IDs;
- empty or duplicate group members;
- invalid group/member/leader-hint IDs;
- leader hints that are not members of their group;
- collection placements that reference unknown groups;
- invalid `{database, catalog, collection}` identities;
- duplicate collection placements;
- placement modes other than the default collection-level mode.

The v1 resolver maps each placed `{database, catalog, collection}` identity to
exactly one owning `GroupID`. Resolving an unplaced collection MUST fail closed.

## Deferred Scope

The v1 catalog intentionally rejects token and ring modes. Future token/ring
work needs a separate catalog version, shard-key rules, query routing contract,
unique-index semantics, rebalancing model, and benchmark evidence before those
fields become accepted.
