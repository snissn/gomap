# Distributed TreeDB partitions on EC2: target contract

Status: **target design, not an implemented or qualified deployment**.
Execution tracker: [#4805](https://github.com/snissn/gomap/issues/4805);
contract packet: [#4806](https://github.com/snissn/gomap/issues/4806).
Product nodes P1–P7 are issues #4807–#4813 respectively.

Planning base: `d9d2466267f9c728bbd62d627b981ff3f74a3e24`.
The execution tracker owns ordering and acceptance; this document defines the
interfaces and invariants its implementation must satisfy. Existing public
APIs and formats retain their current contracts until their owning changes land.

## Goal and present boundary

Scale one logical collection beyond one machine's durable capacity, with
bounded work per query and per node, quorum-durable writes, recoverable
placement, and measured performance across independent EC2 failure domains.
A successful process-count test or an increase to a size constant does not
establish this goal.

| Verified source at the planning base | Consequence |
| --- | --- |
| `nativewire/fixed_peer_tcp_runtime_v1.go: validateFixedPeerConfigV1` allows at most 32 nodes/data groups, makes every node a catalog voter, and requires exact configuration on reopen | The fixed-peer runtime is the starting point, not the final many-node control plane |
| `OpenFixedPeerTCPRuntimeV1` assembles real persistent HashiCorp providers and opens only hosted data groups | Reuse consensus, durable apply, routing and transport seams |
| PR #4734 at `dea3fb9d972eb9d45ba992de3fee44d9bd73d6a3` requires one vector owner group and one local data group | Its routed insert proof does not establish a sharded writable collection; the candidate is not merged |
| PR #4804 landed one graph per logical domain over bounded chunks | A storage chunk must not become an independently searched Raft shard |
| `raftfsm.ExportRaftSnapshotV1` holds the storage barrier and FSM lock through archive construction | Snapshot/export already exists; large-data work must bound interference, not replace it without cause |
| `NewVectorPartitionProductionNodeV1` uses the three-node in-process harness | Shard TCP from that constructor is not proof of inter-host Raft |
| #4753 remains the local representative-scaling evidence owner | Local ANN quality/performance acceptance remains separate from distributed acceptance |

Read the canonical [fixed-peer contract](fixed-peer-tcp-runtime-v1.md),
[Raft placement](raftplacement.md),
[partition lifecycle/router contract](vector-partition-raft-v1.md),
[coordinator contract](vector-partition-coordinator-v1.md),
[storage format](storage-format.md), [recovery](recovery.md), and
[value-log lifecycle](value-log-lifecycle.md) with this target.
The value log is persistent storage, not a disposable Raft log.

## Units of ownership

Keep these identities distinct:

| Unit | Target responsibility |
| --- | --- |
| Canonical source shard | Exactly one authority for each stable document ID and its revision |
| Logical ANN domain (`DomainID` in the manifest) | One graph/search frontier; one serving owner group per placement epoch |
| Physical chunk | Bounded immutable piece of one domain; no independent route, frontier or top-K |
| Raft group | Small quorum that replicates one or more canonical/derived partitions |
| Node | Hosts an admitted subset of groups and assets |
| Catalog group | Small replicated authority for topology, schemas, placement epochs and lifecycle |
| Coordinator | Bounded query routing and canonical merge; no full-source storage requirement |

Identity names are context-sensitive in existing APIs: manifest `PartitionID`
identifies a physical pack, while some router/search APIs use partition language
for logical domains or a domain anchor. Integration must preserve explicit
`DomainID` → pack IDs → owner mappings; do not globally rename or reinterpret IDs.

Canonical source and derived ANN partitions may share a Raft group or occupy
different groups. When colocated, they share the group's ordered log/apply stream,
but retain distinct source/domain identities and placement bindings. Canonical
commit still records projection intent durably; it does not itself prove that
the derived index has reached the promised visibility frontier. Recovery replays
canonical state and pending projections in log order with idempotent,
revision-monotonic derived apply. A colocated optimization may combine effects
only if it proves the same commit/visibility contract. Moving either role must
validate its catalog placement epoch independently; sharing a group never
authorizes the other role's ownership change.

Raft replication and ANN overlap are different multipliers. Replication protects
an owner's state; overlap adds derived membership in another ANN domain.
Account for both independently in storage, transfer, build and query metrics.
Do not put every machine in one Raft group, or require every machine to hold the
entire corpus.

For the initial distributed product, keep all chunks of a domain colocated
within its owner group. Replicas of that group receive the same complete domain
generation. Cross-machine graph-frontier traversal is outside this contract.
A domain that exceeds the supported per-owner envelope must be rebuilt/split
through a controlled generation transition, or explicitly rejected.

## Control plane and bounded runtime

Reuse `CatalogMetaAuthorityV1`, `CatalogProofV1`,
`NewCatalogMetaGroupRoutedSubmitter`, and existing read/apply barriers.
Separate a small catalog voter set (initial target: three voters) from data nodes.
A data node that is not a catalog voter needs an authenticated, ordered way to
obtain catalog state and proof; a startup JSON file or cached epoch alone is not
leadership or freshness evidence.

Define bootstrap trust, cluster identity, monotonic epochs, watch/snapshot
resynchronization, unavailable-authority behavior, and publication fencing in
the same change. Strict operations must either establish the required current
authority or fail. Do not replace quorum-backed reads with time-based freshness
assumptions without a separately reviewed consistency contract.

Open only assigned groups. Bound group count per node, connections, goroutines,
file handles, mapped bytes, buffers, pending proposals, concurrent snapshots,
RPC bytes and queue residence. Preserve independent ingress/forward/read
capacity so nested RPCs cannot starve each other. Use admitted configuration
limits and measured capacity rather than silently making existing caps unlimited.

Balance leaders separately from replica bytes. Placement must respect distinct
node identities, physical-host/AZ policy and free capacity, including temporary
snapshot, movement and rebuild copies. A three-voter group only supports the
failure cases in which two voters remain mutually reachable.

## Sharded source and build contract

Extend existing collection/typed-column and partition builders. Introduce stable
source-shard identity plus a local ordinal/revision; global document IDs remain
the external identity. An existing single-source ordinal must not silently
become an ordinal in another shard.

Build/import must be restartable from immutable, checksummed input partitions.
Record input identity, completed ranges, owner, source revision, build parameters
and durable publication progress. Enforce byte/work budgets before allocations
and writes. No coordinator or worker may materialize the whole corpus merely
to distribute it.

Version and page manifests/directories when current global row, membership or
metadata caps prevent scale. The committed root binds the exact set of pages,
source shards, domain roots, chunks and feature versions. Missing, mixed,
duplicated, gapped or corrupt state refuses activation. Paging must preserve
bounded validation and exact identity; raising a constant is insufficient.

Offline construction can stage immutable assets through an object store, but
objects do not become authority by being uploaded. Publish a generation only
after its owner groups durably admit and verify all required objects.
Request-path full rebuilds and whole-index exact fallback remain forbidden.

## Writes, derived visibility and reads

Canonical ID ownership must be deterministic and independent of approximate
query routing. A vector update may change ANN-domain membership; it must not
change the canonical authority for that ID accidentally.

The implementation sequence preserves #4734's single-ID insert semantics first.
P4's complete exit requires exact-ID insert, replace and delete, including durable
tombstones; upsert and cross-group atomic batches remain unsupported. Advertise
an operation only after its complete path and capability contract land. Extend existing command entries, command WAL, durable apply
and idempotency results, not a benchmark-only mutation endpoint.

The target protocol has two distinct milestones:

1. **Canonical commit:** the authoritative group commits/applies the mutation
   under its current ownership epoch and records idempotency outcome.
2. **Search visibility:** each affected derived owner applies the necessary
   revision/tombstone, and the returned visibility token identifies the
   searchable frontier.

Use a durable owner log/outbox or equivalent replayable existing mechanism for
derived propagation. Never rely on an in-memory best-effort fanout after ack.
The selected P4 session-token contract binds canonical source mutation revision,
index generation, placement epoch and required derived-owner apply frontiers.
Its encoding and the durable projection mechanism must be tested before adoption;
this document does not reserve protocol command IDs.

For an API that promises immediately searchable success, success waits until
its declared visibility condition is met. If canonical commit is known but
search visibility is not, return a typed committed-but-not-yet-visible result
only through an explicitly versioned contract; otherwise preserve commit
ambiguity. Never return a definite non-commit after a possible commit.
Retries with the same identity must not duplicate effects.

A query pins one placement/generation view, establishes the required read/apply
frontier for selected groups, and reacquires live state after barriers. It then
uses the existing router, one graph per selected domain, and deterministic
stable-ID merge. Missing/failed required owners return an error without partial
top-K. Older revisions and tombstones cannot be resurrected from overlapping
memberships, delayed propagation or a retired generation.

Per-group linearizability is not a global multi-group snapshot. P3 provides
immutable-generation search with current placement authorization. P4 adds the
versioned session visibility token defined above: the required owner frontiers
are minimum visibility constraints, not an atomic snapshot of all mutable groups.
These are the selected initial semantics. Unsupported global-linearizable or
transactional shapes fail closed; version or narrow public consistency labels
rather than labeling unrelated local reads a global snapshot.

Exact current-ID visibility and approximate ANN recall remain separate tests.
A bounded ANN miss alone does not prove a committed write is absent.

## Snapshot, recovery, replacement and movement

Reuse the existing archive/export/install and recovery validators. A future
streaming exporter must first obtain a durable consistent cut and retain every
referenced value-log/column/chunk asset before releasing locks. It cannot read
mutable index files concurrently and call that a snapshot.

Bound export lock hold time, archive/staging bytes, network bandwidth, disk
space and log catch-up debt. Throttle background work before it displaces
serving. Preserve checksums, format/feature floors, applied indices,
idempotency results and stale-generation refusal through interrupted restore.

Replica replacement and ownership movement are different operations:

- Replica replacement retains the same group identity. Catch up a new member,
  verify its recoverable state, change voting membership using the pinned
  HashiCorp Raft API, and remove the old member only under a safe quorum policy.
- Domain/source-shard movement changes placement. Prepare destination assets
  and frontier, fence the source writer with an epoch, commit publication through
  catalog authority, and retire old state only after readers and retry obligations
  drain. Every phase must resume or fail safely after coordinator/node restart.

Persist operation IDs and expected epochs. Reject concurrent conflicting moves,
duplicate bootstrap, stale owners and unsafe voter removal. Do not implement
membership changes by editing `fixed-peer-v1.json`.

P6 owns canonical source-shard movement as well as derived-domain movement.
A source move transfers canonical rows, revisions/tombstones, idempotency state
and pending projection obligations, fences the sole canonical writer, and
publishes the catalog/token owner. Outstanding visibility tokens require a
verified placement-lineage translation or an explicit refresh response; they
must not silently lose their minimum revision. Test source movement separately
from derived-domain movement.

A later split must preserve canonical IDs, revision/tombstone history and
deduplication state while publishing a complete replacement generation.
Split quality and fanout must be measured; it is not just file slicing.
Automatic merge and an autonomous balancing optimizer are optional follow-ups,
not prerequisites for a correct bounded operator-driven move/split.

## EC2 deployment and operations

Create a reproducible provider-neutral inventory and an EC2 adapter using the
same production binary, APIs and resource admission. Deployment should specify
private endpoints, failure-domain placement, authenticated/encrypted internal
RPC and Raft transport, client authorization, credential rotation, bounded
readiness/drain, telemetry and upgrade/rollback behavior.

Start with persistent storage for authoritative DB and Raft state. Treat EC2
instance-store data as disposable unless the complete loss/rebuild contract has
been explicitly qualified. AWS documents loss on stop, hibernate, termination
and relevant device failures in its
[instance-store lifetime guide](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-store-lifetime.html).
Raft quorum replication is not a backup: separately verify restore of catalog,
source data, derived assets and idempotency state after an admitted disaster.

Record cost estimates, instance/volume/network limits, dataset source, exact
topology, maximum run duration, resource tags and cleanup plan before launching
a campaign. This target design does not provision resources or spend money.
Default deployment must not expose the current unauthenticated private runtime
to the internet.

## Qualification and completion

#4753 owns local ANN qualification; #4250 owns the distributed benchmark
contract and final retained evidence; #3983 owns fault/recovery acceptance.
No second evidence owner should silently change their denominators or claims.

Freeze actual sizes and numerical performance objectives before measurements.
Use a staged matrix: small real-process conformance, multi-host baseline,
then increasing node/group counts and data sizes. Required coverage includes
a data footprint beyond one admitted node's capacity and a topology beyond the
current 32-node boundary. These are target cases, not achieved results.

Measure strong scaling on a fixed corpus and weak scaling at fixed data per
owner, at matched recall, consistency and durability. Report search goodput,
all attempted queries, p50/p95/p99, write and search-visibility latency, build and
rebuild time, CPU, B/op and allocs/op, retained/peak RSS, network/AZ bytes,
replication and overlap bytes, disk/snapshot debt, recovery and cost per useful
operation. Count clients, all daemons and background work.

Predeclare admissible failures, repeats, warmup, cache state, noise treatment,
hard resource caps and stop conditions. Query fanout must track selected domains,
not the number of machines or chunks. No benchmark may silently downgrade
strict reads, omit failed attempts, use in-process consensus, or rebuild the
index in its adapter.

Each feature must minimize avoidable allocation/copy work through existing
prepared/batch/owned-buffer paths, prove ordinary public callers reach that
path, update affected specs and examples, and pass current-head tests/review/CI.
A failed performance target remains fix-needed; green correctness alone is
not qualification.
