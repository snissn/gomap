# Vector partition M8 production multi-group topology

Status: internal, pre-alpha, experimental/off. The topology is an integration
and benchmark surface for #3917, not a deployment or membership-management API.

## Claim boundary

`VectorPartitionM8ProductionMultiGroupV1` composes persistent partition-local
HNSW assets with distinct real three-node HashiCorp Raft data groups, one real
three-node catalog-meta lifecycle group, serialized M5 TCP services, and the M6
coordinator. It is the first `production_multi_group` evidence path in the
vector-partition graph. Earlier M0 deterministic simulation and M6 in-process
local-service artifacts remain labeled `simulation` and `local_service`.

Production evidence here means real consensus commit/read/apply proofs and a
real serialized network boundary. It does not mean multi-host deployment,
durable cluster membership management, or a feature enabled for users. The
loopback topology owns ephemeral Raft directories and listeners; the HNSW
assets and collection remain persistent storage.

## Construction and activation

`NewVectorPartitionM8ProductionMultiGroupV1` requires a canonical `ready`
manifest with at least four placements, at least two distinct owner groups,
one physical asset per partition, and a non-empty owner-specific asset-set
digest. It rejects duplicate or missing placements and digest mismatches.

Construction performs the M7 lifecycle in this order:

1. open one three-node data Raft group for each declared owner;
2. commit a deterministic entry and retain production quorum evidence;
3. open the three-node catalog-meta group and `BeginBuildV1`;
4. obtain a production read-index/apply proof from every data group and record
   the group ready with its asset-set digest;
5. `PrepareV1` and atomically `ActivateV1` the complete ready set;
6. expose one bounded M5 TCP endpoint per group and create the M6 coordinator
   against the replicated lifecycle authority.

Four-or-more-group evidence distributes preferred leaders across `node-a`,
`node-b`, and `node-c`; it must not present one process-local leader as proof of
distributed ownership. Evidence records every group, node set, leader,
commit/read/applied index, proof kind, endpoint, and endpoint hit count.

## Search and failure semantics

`Coordinator()` returns the bounded M6 coordinator. Searches carry the exact
database/catalog/collection/index and source/partition/router generation
identity, snapshot consistency, candidate/response byte caps, `top_k`, local
`ef_search`, probe count, and wall-clock deadline across the TCP boundary.

The TCP protocol is length framed and bounded. It propagates caller deadlines
and peer disconnect cancellation, rejects malformed or ambiguous frames, and
preserves M5 typed error codes, owner group, and leader hints. M6 permits at
most its documented redirect retry and returns all results or an error. A
transport failure, unavailable group, stale or mixed generation, corrupt shard
proof, timeout, or cancellation MUST return no partial neighbors and no partial
group list.

`StopGroup(group)` is a test/fault-injection operation: it closes that group's
listener and active connections. `Close()` is idempotent, waits for serving
goroutines, drains the coordinator's generation-pinned router sessions, then
closes generation readers before persistent assets; an already-closed network
connection is successful cleanup.

The topology is immutable for its constructed catalog and generation epoch.
Replacement builds a new topology/coordinator and closes the old one before
retiring old sources. M8 records deterministic router-session snapshots before
warmup, after warmup, and after measured cells so the cold/open versus
steady-state manifest boundary is auditable. A valid measured snapshot keeps
the exact warmed identity set and cold/open/miss/reader-pin counters unchanged,
adds cache hits with balanced leases, and reports no lifecycle invalidation or
close. Those snapshots are not a substitute for matched base/head performance
evidence.

## Persistent retained-asset adapter

The benchmark `-m8-existing-db DIR` path opens a prior M3 database read-only.
It discovers exactly one compatible collection/index, verifies the configured
partition count, and clones only the manifest's group placement labels into
the ephemeral M8 topology. The local asset identities, lifecycle ready-set
digest, source database, and physical packs remain unchanged. The adapter MUST
NOT rebuild, mutate, truncate, or delete `DIR`.

## Lifecycle operator model

The underlying M7 authority remains the operational contract:

- build/status: `BeginBuildV1`, group-ready records, and the linearizable
  lifecycle status identify one complete generation;
- cutover: `PrepareV1` followed by `ActivateV1`; incomplete ready sets fail
  closed and a newly activated generation retires the prior generation;
- invalidation: relevant collection/index mutation must pass through
  `InvalidateBeforeRelevantMutationV1` before the mutation commits;
- rebuild/abort: a failed build is retired as aborted; a successful replacement
  follows a fresh build/ready/prepare/activate sequence;
- cleanup: retire or mark the generation cleanable, record cleanup for every
  owner group, then `CompleteCleanupV1`; reader pins and active generations
  prevent premature physical removal;
- recovery: catalog-meta snapshot/rejoin and backup/restore tests must preserve
  lifecycle authority and continue to reject stale generations.

There is no standalone M8 administration command. These methods and the
benchmark are internal proof surfaces until a separate deployment API is
designed.

## Benchmark contract

`cmd/treedb_vector_partition_bench -mode production_multi_group` is the
canonical runner. It validates 2..64 groups, exactly three nodes per group,
groups no greater than partitions, positive probe/`ef_search`/concurrency
sweeps, and declared resource caps. An untimed all-partition warmup precedes
profile capture and measured cells.

Every JSON result is self-validating and contains the exact command and Git
identity/dirty state, host/CPU/memory/NUMA/mount identity, dataset manifest and
checksum, complete topology and lifecycle evidence, budgets, samples, timing
boundary, network/candidate byte counters, resource measurements, fault result,
profile paths, limitations, and an explicit gate ledger. CPU, allocation
baseline/final, heap, block, mutex, and execution trace profiles cover the
measured query cells plus the unavailable-endpoint fault.

The checked-in 10k path materializes persistent HNSW packs for CI. The retained
1M path reuses graph-built M3/M5 packs. `-m8-variant-dbs` requires exactly three
distinct immutable descriptors and executes them sequentially, one fresh OS
process per variant so process peak RSS is attributable to that variant. The
blocked matrix parent validates only the manifest and descriptors and does not
materialize a second fixture corpus:

1. graph assignment with disjoint memberships;
2. graph assignment with bounded overlap `0.20`;
3. stable-ID hash assignment with disjoint memberships as an attribution
   baseline.

The matrix rejects missing, duplicated, mutable, or identity-mismatched
variants rather than silently substituting a disjoint row. A declared overlap
variant is incomplete unless its realized extra-membership count equals
`floor(overlap_ratio * source_rows)`; incomplete materialization fails both the
required-variant and overlap-storage gates even when its raw byte ratio is
below the threshold. Exact correctness is owned by canonical source truth
versus the exhaustive exact partition union;
the router, partition-local HNSW, transport, and coordinator merge retain
separate recall/parity attribution. Approximate HNSW recall is judged by the
declared recall gate and is never described as exact exhaustive parity.

## Capacity and enablement

The report records persistent asset bytes, measured process peak RSS,
membership load versus the default 5% hard-cap epsilon, request/response and
logical candidate bytes, and retained M3 mapped-pack evidence. The current M8
runner does not independently sample mmap residency and says so in the artifact.
Observed persistent bytes and RSS are not a resource-bounds pass unless a
configured resource limit is compared. The strict matrix compares configured
persistent-asset and process-RSS ceilings plus every coordinator/shard count,
byte, fanout, concurrency, result, search-budget, and wall-clock limit against
the same request scope. Both configured resource ceilings participate in the
matrix artifact identity, so runs with different acceptance bounds cannot
overwrite one another. Request, RPC, retry, redirect, merge, and byte limits
use observed per-request/per-shard maxima rather than averages derived from
aggregate throughput counters. Client concurrency scales the configured
aggregate shard-request ceiling; a process-wide observed peak is not compared
to the smaller per-request worker limit. Retry and redirect observations are
per-query aggregates, so their ceilings are the independently enforced
per-shard-task limit multiplied by the maximum observed shard-task fanout.
Partition-load evidence includes both primary and overlap memberships.

The feature remains experimental/off unless every #3917 north-star gate passes
or the user explicitly accepts the narrower result with one linked measured
bottleneck. No paper QPS, multi-host, billion-vector, live repartitioning,
serving-replica, partial-result, online-freshness, or distributed-document-fetch
claim follows from this topology.
