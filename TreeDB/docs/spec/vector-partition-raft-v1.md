# Vector-partitioned ANN over Raft: M1 manifest and M4 router contracts

Status: **M1 durable identity/lifecycle and M4 representative-router
contracts** for #3908/#3910/#3913. M1 persists and validates generation
manifests. M4 builds, persists, reopens, and searches the generation-bound
representative router. M5 now owns the bounded shard-search service contract,
and M6 owns transport-neutral coordinator fanout and merged top-k in
`vector-partition-coordinator-v1.md`. A production remote transport and
multi-group acceptance remain M8 work.

## M1 durable lifecycle

Each ready manifest stores typed `ColumnAssetRef`s, not paths. Every partition
asset has an explicit logical `partition_id`; every declared logical partition
must have at least one asset, while the router is a separate asset. Asset refs,
checksums, lengths, and aggregate referenced bytes are bounded and canonical.
The reduced checkpoint state pins one ready active generation. `Deactivate`
appends an immutable transition that changes it to retained/prepared-only;
deletion then permits column-asset GC. Corrupt filenames, manifests, lifecycle
records, physical aliases, or catalog-group mappings fail closed.

An exact deletion appends DELETE_PREPARE with a durable, checksummed,
identity-bound VPR1 reclaim payload. DELETE_COMPLETE removes the generation
from the reduced live set while retaining its generation in the monotonic high
water, which proves intentional no-active state and prevents resurrection.
There are no mutable active, retired, inactive, or deleting pointer files.
Recovery derives every state from the highest VCP1 checkpoint plus its exact
VLC1 tail; it never falls back to an older checkpoint.

`VectorPartitionStatusV1.Ready` means the stored generation is complete.
`Active` additionally requires the reduced checkpoint state to select that
generation and its source identity to remain valid. M1 provides explicit
in-process reader-pin handles; status and deletion observe those pins.
`SnapshotReferences` and `CatalogReferences` remain trusted caller-supplied
cleanup authority inputs: M1 does not infer external backup or catalog
reachability. M7 owns durable catalog/cutover derivation. Raft archives are
self-contained copies and do not pin live side-store files after export.

## M4 deterministic representative router

`BuildAndPublishVectorPartitionRouterV1` consumes a byte-identical durable M1
`building` manifest plus the complete primary vector membership of every
logical partition. It revalidates the live source identity, index-definition
digest, dimensions, membership closure, and bounded router configuration before
building. `ReadVectorPartitionRouterSourceRowsV1` exposes an owned
ordinal/document-ID/FP32 snapshot for membership construction; publication
reopens the current typed-column source and bit-verifies every supplied FP32
component at its authoritative graph ordinal. Missing, duplicate, stale, or
caller-modified input fails before model construction or append. The input
order is not significant: partitions and source ordinals are canonicalized
before deterministic hierarchical cosine k-means.

`RouterConfigV1` persists the seed, branch factor, leaf-size stop,
representative budget per partition, maximum depth, maximum Lloyd iterations,
and vector/dimension/representative/scalar-work/persisted-byte caps. A
conservative full 64-layer native-pack bound is checked before row or adjacency
allocation, and the actual encoded length is checked again before append.
Farthest-first initialization and all distance/ordinal ties are stable. Empty clusters are
repaired deterministically by moving the farthest eligible member, with source
ordinal as the tie break. Each partition stops when its representative budget
is reached or no eligible leaf remains. The persisted build metrics distinguish
leaf-size, depth, and no-split stops and record Lloyd iterations and repairs.
The validator reconstructs root/member totals, parent/depth paths, leaves,
per-partition representative caps, canonical node/representative order, and
metric totals; forged hierarchy or build metadata fails closed.

The router asset is a native TreeDB HNSW search pack, not a sidecar centroid
file. Every normalized representative vector is accompanied by a strict
versioned `VKR1` document-ID record containing:

- router generation and canonical model SHA-256;
- partition, source ordinal, leaf, depth, member count, and full root-to-leaf
  node/member-count path;
- the complete router configuration and build metrics; and
- the native HNSW `M`, construction-ef, and search-ef values.

The pack base identity binds the M1 source generation, checksum, and schema.
The ready manifest binds its exact byte length, CRC-backed `ColumnAssetRef`,
SHA-256, logical asset identity, router generation, and ready-set digest.
Publication is one M1 `building` to `ready` transition. Cancellation or any
input, append, resource-authority, or publication failure leaves the generation
non-active and never exposes a partial router. Existing building-manifest
fields cannot be rewritten during promotion. If M1 already declares
representative memberships they must exactly match the computed source
medoids; otherwise the digest-bound READY promotion fills the complete mapping.
Every ready generation carries the mapping in both its manifest authority and
the strict router records; open requires exact agreement.

`OpenVectorPartitionRouterV1` runs under the M1 storage barrier. It requires
the current active ready generation and source/index identity, verifies the
asset range and SHA-256, opens a mapped or bounded heap-backed native pack,
checks pack base identity and HNSW metadata, strictly decodes every `VKR1`
record, reconstructs the canonical model, and recomputes its digest. It maps
the HNSW locality row order back to canonical representative order before
serving and acquires an M1 reader pin in the same barrier. `Close` excludes
concurrent searches, closes the prepared view, and releases that pin.

Exact routing is the correctness oracle and requires
`candidate_budget >= representative_count`; it scans all persisted
representatives. Approximate routing passes its explicit candidate budget as a
hard distinct layer-0 scoring limit to the native prepared HNSW path. Candidate
budget and partition-probe count are separate controls. Both paths reduce
multiple representative hits to the minimum cosine distance per partition and
return unique partitions ordered by `(distance, partition_id)`. A zero/invalid
budget, non-finite or dimension-mismatched query, malformed asset, stale
generation, closed handle, or candidate set that cannot supply the requested
number of unique partitions is an error; no partial partition list is
returned.

Build, open, search, and cumulative runtime status report generation/digest,
representative and hierarchy counts, build/append/publication/open/search
time, router/mapped/heap bytes, candidates, edges, selected partitions,
failures, and active handles. These counters describe the local persisted M4
path only. The benchmark additionally reports process CPU and Linux `VmHWM`
availability explicitly, per-path p50/p95/p99 from router-internal search time,
and sequential-process `runtime.MemStats` allocation deltas per operation.

## Identity and placement

Canonical TreeDB documents and embeddings are authoritative. The `_id` token
route determines their canonical Raft-group owner. A **logical vector
partition** is separate, derived, rebuildable neighborhood membership used only
to select ANN search packs. A vector may appear in multiple partition-local
packs only as an overlap membership; that is neither a Raft replica nor an
additional authoritative document write.

Terminology is intentionally non-interchangeable:

| Term | Meaning |
| --- | --- |
| Raft replica | a consensus copy of one canonical group |
| overlap membership | derived vector ID and score assets in another ANN partition |
| serving replica | an optional read-serving copy; distinct from both above |

## Generation and consistency

Every artifact carries separate immutable identities: `vector_index_id` names
the index definition; `source_snapshot_generation` names the source vector
snapshot; `partition_generation` names derived memberships/packs; and
`router_generation` names representatives/router. Router, membership,
representative, and partition-local HNSW artifacts MUST bind all four. States
are `building`, `ready`, `cutover`, and `invalidated`.
`ready` requires every referenced artifact to be present, checksum-valid, and
length-capped before allocation on its declared owner. A `cutover` is atomic
over the complete generation.

The M7 state machine below refines this earlier coarse vocabulary. Its
persisted states are `building`, `staged`, `prepared`, `active`, `invalidated`,
`retired`, and `cleanable`; `cutover` is the guarded atomic operation that
replaces one active generation with another, not a persisted state.

V1 is snapshot-bound: insert, delete, embedding replacement, index-definition
change, source snapshot replacement, or any committed mutation affecting a
referenced vector invalidates the generation before the mutation becomes
searchable. Until full rebuild and cutover, distributed search MUST fail closed;
it MUST NOT mix generations or return partial top-k. A Raft read-index only
proves the group applied point; it does not make an older snapshot-built
generation fresh.

### M7 replicated mutation fence

The catalog authority records a bounded, canonical mutation fence per
`(database, catalog, collection, index_name)`. Invalidating an active
generation advances that fence and marks it **pending** before the relevant
data command is admitted. While pending, lifecycle build and activation both
fail closed, including a candidate whose claimed source epoch equals the
fence. The shared nativewire and Mongo submitters confirm the fence only after
their data-Raft bridge proves commit and completes deterministic local apply.
That proof is independent of the client-selected response acknowledgment:
successful `visible`, `flushed`, and `synced` requests release the same fence
as `raft_committed`; failed or ambiguous submits do not.

A failed, ambiguous, or crashed data submit intentionally leaves the fence
pending: serving remains unavailable until recovery proves the data outcome and
commits the exact confirmation, or a separately authorized repair disposition
is made. A second relevant mutation cannot reuse a pending proof. Relevant
mutations are also refused while any generation for that collection/index is
building, staged, or prepared, so source capture is never concurrent with a
data mutation.

The per-index fences are coordinated by a second, collection-keyed replicated
barrier. A build reads the collection mutation epoch before source capture and
must commit that exact watermark at begin-build. A relevant data command opens
the barrier only after local encoding and route preflight succeed, then owns it
by deterministic command/idempotency digest until the data result is committed,
deterministically applied, and every affected per-index fence is confirmed.
This confirmation is independent of the client-selected response acknowledgment
policy and runs under a bounded internal context after commit/apply, so client
cancellation or deadline expiry cannot strand the fence between the data apply
and catalog confirmation. Exact retries reuse the same barrier; a distinct
concurrent mutation is refused. This closes
the first-generation race where no active index record yet exists to invalidate.

#### Durable ingress inventory

| supported ingress | single/batch/admin mutation forms | mandatory shared boundary |
| --- | --- | --- |
| nativewire server | create collection; insert batch, replace batch, delete batch, and single-document BSON-set update | `SubmitCommandEntryWithVectorPartitionAdmissionV1` before data consensus, then `ConfirmCommittedVectorPartitionMutationV1` after commit/apply |
| Mongo gateway | create collection and Mongo insert/update/delete after normalization to create collection, insert batch, BSON-set update, or delete batch | `SubmitCommandEntryWithVectorPartitionAdmissionV1` before data consensus, then `ConfirmCommittedVectorPartitionMutationV1` after commit/apply |

There is no ingress-specific lifecycle hook: both rows use the same routed
submitter composition and the same catalog mutation barrier. Benchmark-support
route scaffolds are intentionally fail-closed and are not a supported mutation
root. Replicated create/drop index and drop collection are not supported R3a
commands; cluster mode rejects them before submit, so they cannot bypass this
inventory through a local metadata mutation.

### V1 operator boundary (#4018)

Vector-partition operations are **off by default**. A node that has assembled
the #4014 topology and #4016 service may register `OperationsV1` with an
explicit `OperationsConfigV1{Enabled:true}`. All eight request limits are
required and are rejected before query cloning or coordinator dispatch:
query/request/candidate/response bytes, top-k, probes, ef-search, and merge
entries. Build concurrency, build memory, and retained-generation policy are
not exposed here because the current group builder has no enforceable shared
cap seam. Fanout, request concurrency, retries, and redirects remain bounded by
the node-owned `VectorPartitionCoordinatorLimitsV1`; the operator boundary does
not duplicate those construction-time limits.

`OperationsV1.Status` evaluates production topology and the required serving
groups first, then performs the same fresh meta-Raft linearizable read fence as
serving and reads the live catalog proof, lifecycle record/source identity, and
ready groups. `ready` is returned only for an active, complete generation;
`authority_unavailable`, `topology_unavailable`, `catalog_unavailable`,
`catalog_mismatch`, `source_mismatch`, `generation_absent`,
`lifecycle_not_active`, and `group_assets_unavailable` fail closed. Stable
counters record disabled calls, health checks, searches, the exact rejected
request-cap class, selected partitions/groups, requests, RPCs, retries,
redirects, failures, candidates, edges, and query/request/candidate/response
bytes. Those values reuse
coordinator response accounting. Cache hit/miss and
structured-log sink volume have no public owning snapshot yet and are not
fabricated by this boundary.

Register, prepare, activate, invalidate, inventory, rebuild request, retire,
and cleanup eligibility delegate to the existing `ServiceV1` lifecycle APIs;
operations do not create a second control plane. `documentservice` publishes
only this enabled boundary, never the wrapped service that could bypass its
caps. For rollback, stop registering the enabled operations boundary and close
the topology; requests become unavailable while catalog lifecycle state and
recovery debt remain durable. After restart, reassemble the same backend, then
check `OperationsV1.Status` before serving. For stale generations, group outage,
or failed activation, retain the fail-closed status, request rebuild, and use
the existing retire/cleanup eligibility workflow only after pins and the
lifecycle authority permit it.

Fence state is included in the canonical catalog snapshot and is exposed to
operators with its collection, index name, epoch, and pending bit. Retire and
cleanup of an invalidated generation are blocked while its fence is pending;
this preserves the exact record required for confirmation. Thus crash,
failover, replay, snapshot/rejoin, backup restore, and cleanup all preserve
the same fail-closed recovery debt. The retained source watermark remains after
confirmed cleanup, preventing an older delayed candidate from resurrecting.

Serving keeps two SHA-256 identities distinct. The local manifest
`ReadySetDigest` binds that generation's placements and assets. The replicated
lifecycle ready-set digest binds the lifecycle identity plus every required
group readiness proof. Catalog authority returns the latter after validating
the exact active generation; the coordinator carries it through each shard
request and response proof, while local asset opening continues to validate
the manifest digest. They are never compared as though they were the same
hash.

Replicated activation also does not mutate or consult the standalone M1 active
pointer. M6 opens the exact prepared router generation named by replicated
placement, then M7 validates that router against catalog authority before any
router search or shard dispatch. A missing, stale, corrupt, or locally
unprepared exact generation fails closed.

Catalog proof refresh uses a quorum-verified, current-term meta-Raft read that
waits for the committed Raft prefix and exact local catalog applied identity.
It appends no `LogBarrier` entry. The proof names its leader, term, commit and
applied floors, catalog command index, and a short process-local monotonic
lease. A follower, expired lease, changed term, insufficient applied progress,
or changed catalog identity fails closed.

Activation-side code may publish one immutable serving snapshot only after it
has captured the exact catalog/lifecycle authority, pinned the matching router
and every local generation, opened every local partition, and recaptured the
same authority. The snapshot binds the lifecycle, catalog, topology,
ready-set, manifest, router, authorization-overlay, and source-watermark
identities. Publication swaps one complete snapshot; replacement or
invalidation prevents new pins while existing pins drain. Snapshot acquisition
validates the retained local proof and exact applied identity with no quorum call,
catalog log append, manifest reconstruction, generation open, or partition open.
Ordinary `OperationsV1.Search` remains strict: after bounded admission it takes
exactly one fresh current-term quorum-backed no-log catalog proof, pins the
matching snapshot, and sends a server-authenticated short-lived capability to
the coordinator and shards. Those stages require an exact snapshot identity
and applied-index floor but perform no additional catalog or data-group
consensus and no request-side asset opens. Forged, expired, wrong-generation,
or invalidated capabilities fail closed; a request pinned before invalidation
may drain. The background serving-proof refresh remains quorum-backed and
no-log, but is retained as separate work rather than charged to a request.
#4096 owns this strict proof propagation. `OperationsV1.SearchFast` is a
separate explicit local-snapshot shape: the caller must bound index age and may
require an indexed-through watermark, which is satisfied by a local wait and
never falls back to strict search. `OperationsV1.PinSearchSnapshot` applies the
same bounds once, caps session lifetime and retained snapshots, and reuses that
immutable snapshot until close, expiry, or invalidation. Both shapes perform no
request-side consensus and still apply the current atomic authorization overlay
before returning results. #4098 owns these shapes; they do not weaken the
strict default or accept caller-authored authority.

The concrete replica-local catalog authority intentionally does not implement
the serving interface; only the linearizable adapter can capture or refresh a
proof. A follower without routed meta-leader proof, or a local catalog view
that differs from the proof's exact catalog command index, fails closed rather
than serving its cached active generation.

`VectorPartitionLifecycleCoordinatorV1` provides the bounded meta-Raft
workflow: begin (with exact source/catalog/mutation identity), caller-owned
group build/stage readiness callback, group-ready recording, prepare,
activation/cutover, abort, retire, cleanability, per-group cleanup, completion,
and recovery status. The callback returns only a validated bounded readiness
proof; M7 does not fabricate remote asset upload or network transport.

V1 distributed responses contain response-owned stable IDs and scores only.
Missing owner, stale generation, malformed/capped asset, unsupported
consistency, or shard failure is an explicit error with no incomplete result.

## M0 oracle and evidence boundary

### Canonical V1 partition-result contract

`collections.VectorPartitionCanonicalScoreContractV1` is the executable
production result contract. It normalizes both query and source vector in
FP32, accumulates their normalized dot product left-to-right with explicit
separately rounded binary64 products and additions, and
rounds the final cosine score once to FP32. Across partition responses,
duplicate stable IDs retain the highest canonical score. Final top-k order is
descending score with bytewise ascending stable ID as the exact tie break.

Generation-pinned exact scans operate only on the already validated persistent
search pack and hold the same reader pin as HNSW search. Native HNSW traversal
may use an optimized score kernel to discover candidates, but every published
candidate is rescored with the canonical contract before shard and coordinator
ordering. Empty IDs, non-finite scores, duplicate response IDs, noncanonical
wire order, incomplete partition/group coverage, source-generation mismatch,
or partial top-k fail closed.

Production evidence schema 3 reports recall independently for the full-source/
global exact oracle, primary-home, final-membership, exhaustive exact partition
union, exact representative routing, approximate representative routing,
exact-route local HNSW, approximate-route local HNSW, and end-to-end output.
It also records exact coordinator ID/score parity and names every remaining
loss owner. These attribution fields diagnose a red enablement gate; they do
not turn approximate all-partition HNSW into an exact oracle.

By default, `cmd/treedb_vector_partition_bench` is a sequential **simulation**. It emits
`result_kind=simulation_only` and `production_evidence=false`; its Markdown
artifacts repeat that it is not production Raft evidence. It records exact
global top-k, partition oracle, representative routing, exact partition-local
search, real TreeDB partition-local HNSW, and end-to-end simulation separately.

`-stage router` is the distinct M4 local-path mode. It creates a temporary
TreeDB source index and M1 building manifest, publishes the real persisted M4
router, reopens it, and executes both the exact persisted representative oracle
and native prepared-HNSW router. It emits
`result_kind=router_local_path_evidence`, still with
`production_evidence=false`. Exact coarsening recall and approximate-HNSW
recall are separate fields; `hnsw_recall_loss` is their delta. Candidate and
partition-probe budgets remain separate and every stage records searches,
candidates, native edges, and search time. This timed boundary excludes RPC,
coordinator, Raft, shard search, and the M8 matched-recall acceptance gate.

The HNSW stage deterministically derives one collection per logical modulo
partition in a temporary TreeDB. The harness uses the public lifecycle:
`SaveFormatConfig` with `RequiredFeatureCommandWALV1`, `db.Open`,
`NewCollectionManager`, JSON `float32_vector` typed-column ownership,
`VectorIndexStrategyColumnGraph`, `InsertBatch`, `Flush`,
`RebuildVectorIndex`, and response-owned `SearchVectorIndex` results. It builds
these derived collections and indexes once per harness run only when that stage
is selected. For each query it uses exact representative routing to choose
logical partitions, searches their prepared local indexes, then merges by
cosine distance followed by stable document ID.

Every local response MUST report
`SearchRouteHNSWSearchPack=1`, route
`exact_hnsw_search_pack_v1`, an active pack, and zero pack fallbacks. The
harness fails instead of accepting another route. Repeated overlap rows reuse
a cache whose key contains the query, top-k, and sorted partition-ID set.
Artifacts distinguish logical, executed, and cached searches, and serialize
all HNSW evidence counters even when their value is explicitly zero.
This is real TreeDB local-HNSW loss attribution, but its temporary modulo
partitioning, sequential coordinator, and absent network/Raft path remain
non-production simulation. With every partition probed, the partition oracle
MUST equal global exact top-k under ordered distance and stable-ID comparison.

The checked-in fixture manifest at `testdata/vector_partition_10k/` uses
generator `treedb_vector_partition_fixture_v2` and arithmetic contract
`ieee754_binary64_explicit_fma_v1`. Normalization, exact-oracle dot products,
and representative-routing dot products use explicit `math.FMA` accumulation
so compiler- or architecture-dependent multiply-add contraction cannot alter
the generated fixture or truth. Its stable checksum binds the exact IEEE-754
binary64 bits of every generated vector and query, then the ordered exact-truth
IDs and distance bits. Exact-truth checksum binding covers top-10 for
fixtures with at least ten vectors and every available neighbor for smaller
fixtures. The generated corpus includes an explicit duplicate/tie case,
clusters, and boundary shape. Tests resolve the
single root fixture from `cmd/treedb_vector_partition_bench`. The manifest read
itself has a 64 KiB bound. `-seed` MUST equal the manifest generation seed;
the result repeats that bound seed and its fixed-width bit pattern is part of
the artifact basename. The basename also binds the full fixture checksum,
partition count, probes, exact overlap bits, top-k, and a full SHA-256 over the
canonical selected-stage set, so evidence from different datasets, partition
configurations, or independently selected stage sets cannot overwrite another
row.
Combined vector/query counts, dimensions, partition count, and result metrics
are validated before allocating large work buffers or building TreeDB evidence.
The JSON schema version is strict; unknown versions, non-finite metrics, and
provenance other than exact 40-hex SHAs are rejected by the executable contract.

`-max-fixture-bytes` caps a conservative model of simultaneous live,
benchmark-owned material rather than only raw vector elements. The preflight
model includes contiguous generated `float64` vector/query matrices and row
headers, bounded exact and selected top-k candidates, representative routing,
whole-partition HNSW JSON serialization plus decoded JSON/vector batch material,
HNSW query merge storage, and the persistent cross-overlap HNSW result cache.
It applies 25 percent allocation slack. It explicitly excludes TreeDB engine
and index internals, Go runtime/GC metadata, and CLI/artifact encoding. The
artifact records the requested cap, modeled peak, and this scope. Checksum
validation consumes the already generated matrices and does not regenerate a
second full fixture.

An independent fixed work gate rejects more than 200,000,000 modeled
benchmark-owned vector/query corpus visits before fixture generation or
evidence. One visit is one vector considered for one query. The model counts
checksum exact truth once, the mandatory global truth pass for every
probe/overlap artifact row, and each enabled exhaustive or representative
routing corpus pass. It excludes TreeDB HNSW engine-internal search work. The
canonical `10k x 128`, three-probe, two-overlap, all-stage shape has 67 corpus
passes, or 85,760,000 visits, and remains within the gate.

The schema reserves every descendant evidence family even when M0 emits
`measurement_status=simulation_not_measured` and explicit finite zero values:
build wall/CPU/RSS/temp/final bytes; balance/cut/overlap; representatives and
router latency; fanout/RPC/bytes/shard/merge/failure counters; and QPS,
percentiles, recall@1/10/100, allocations, resident/mapped bytes. Artifact
provenance is the real `HEAD` and `merge-base HEAD origin/main`, a bounded
`pull_request.base.sha` and `pull_request.head.sha` pair from
`GITHUB_EVENT_PATH` in shallow PR CI, or explicit `GITHUB_SHA`/`BASE_SHA`
overrides when no pull-request event is present. A bounded pull-request event
pair overrides both environment values, ensuring GitHub's synthetic merge
`GITHUB_SHA` is never recorded as the candidate head; empty or malformed
provenance is rejected.

## Parent invariant matrix

| Parent invariant | M0 executable evidence | Owner after M0 |
| --- | --- | --- |
| `_id` ownership differs from ANN membership | `TestDocsVectorPartitionRaftM0Contract` | M1/M5 |
| overlap is not Raft replication | docs contract test | M3/M7 |
| four generation identities, ready/cutover/invalidation | docs contract test | M1/M7 |
| snapshot mutation fails closed | docs contract test | M7 |
| IDs/scores only; no partial top-k | docs contract test | M5/M6 |
| exact all-partition parity and stable ties | `TestTruthOracleTieOrderingAndAllPartitionParity` | M2--M6 |
| real local HNSW and fail-closed route evidence | `TestTreeDBHNSWStageUsesExactSearchPackAndMatchesHighEFLocalTruth` | M3/M5 |
| deterministic persisted representatives and exact/approximate routing | `TestPartitionRouterBuildPublishSearchReopenAndPinsV1` | M4 |
| independent candidate/probe bounds and no partial routing | `TestPartitionRouterCandidateLimitIsHardV1` | M4/M5 |
| cap-before-allocation/malformed input | `TestMalformedCapAndFiniteInputsRejectBeforeSimulation` | M1--M8 |
| simulation is not production Raft evidence | `TestCanonicalRunWritesJSONAndMarkdown` | M8 |

## Provenance ledger

| Source | Allowed use | Code copied/adapted |
| --- | --- | --- |
| arXiv:2403.01797 | algorithm statements and evaluation vocabulary | none |
| `larsgottesbueren/gp-ann` | reference/oracle only; GitHub declares no license | none |
| TreeDB existing dependencies | separately licensed, pinned modules only | none newly introduced by M0 |

M0 makes no paper speedup claim. It implements no production partition format,
partitioner, overlap membership, kRt, RPC/coordinator, Raft catalog, or routed
Raft read. M1 owns persisted generation/placement; M2 partitions; M3 overlap
and local packs; M4 router; M5 routed reads; M6 merge/fanout; M7 lifecycle; M8
real multi-group matched-recall evidence.

## M1 durable generation record

`collections.VectorPartitionManifestV1` is the M1 binary record, with format
`vector_partition_manifest_v1`. It binds a collection, the SHA-256 digest of
the existing `VectorIndexDefinition`, source base generation/checksum/schema
and row count, derived partition generation, and a complete logical
partition-to-Raft-group mapping. Logical partition IDs are dense `[0,n)` and
are not `_id` token partitions; multiple logical partitions can name one Raft
group.

The record separates `building` from `ready`. A building record has no router
or ready-set reference and is never active. A ready record requires a matching
router generation and canonical SHA-256 ready-set digest over length-prefixed
placement and asset descriptors, including the router's required canonical
`partition_id=0`. It contains exactly one disjoint membership
for every source ordinal and separately records bounded overlap memberships.
Raw `VectorPartitionStoreV1` lifecycle mutation is fail-closed: publication,
deactivation, and deletion require collection authority. Collection-authorized
publication verifies the current source identity and referenced assets, and all
collection-owned lifecycle mutation takes the root storage barrier before the
collection mutation authority. Reader pins and snapshots use that same barrier.
A durable DELETE_PREPARE initially transitions any non-active building or ready
generation, including a retired ready generation, to deleting. An identical
DELETE_PREPARE retry is idempotent. Conflicting delete-preparation, BUILD, or
READY retries against a deleting generation, and attempts to recreate a
completed generation, fail before a new lifecycle entry can be installed.
All decoded count-derived allocations, strings, assets, memberships and record
bytes are capped before allocation; unknown/trailing records fail closed.

Local publication installs an immutable BUILD checkpoint, then READY and
LOCAL_ACTIVATE deltas. A crash or retry therefore observes building, ready but
inactive, or ready and active state—never a partially encoded manifest. Every
LOCAL_ACTIVATE also advances the checkpointed activation high water. A READY
retry at or below that watermark fails closed even after the newer active or
retired generation has been deleted and its live pointer cleared; a prepared
generation above the watermark may still complete an interrupted first
activation.
Raft snapshot archives include `db/vector_partitions`. Deletion requires an
explicit proof that the generation is inactive and has no reader pin, snapshot
reference, or catalog reference; M1 does not infer those external references.
DELETE_PREPARE stores the bounded, versioned, canonical, checksummed reclaim
payload containing every original asset ref. If an original ref shares a
physical segment with a live column-manifest ref, the mixed-segment rewrite
appends RECLAIM_PROGRESS with those superseded live refs before the remap may
be published. A persistence error, or cancellation observed before remap
publication, leaves the old manifest mapping authoritative. Recovery retries
with both original and superseded refs, and retains the reduced reclaim state
until all segment debt is physically absent; the durable-root fallback
generation can therefore delay, but never bypass, DELETE_COMPLETE.

### V1 API/schema contract, VPM1 wire version 3, and bounds

`V1` in public API, type, and schema names identifies that pre-alpha contract;
it is distinct from the explicit VPM1 wire version.
The canonical generation payload is binary `VPM1` (big-endian magic
`0x56504d31`, version `3`), followed by fixed-order length-prefixed fields;
there are no tagged optional fields. The JSON form is an inspection/exchange
encoding of that same record: unknown fields, a second JSON value, trailing
bytes, and non-canonical ordering fail closed. VPM1 is embedded in VCP1
checkpoint state instead of published as a mutable `.vpm` file. Version 2 adds
a whole-record SHA-256 integrity digest covering identity, policy, generations,
placement, every membership family, and asset descriptors; this is separate
from the ready-set asset contract. Version 3 adds each asset descriptor's
optional membership digest. Native partition HNSW assets require it; the digest
also appears in their pack wire-version-2 header and binds generation,
partition, ordered authoritative stable IDs, and home/overlap kinds.

| Record area | Required content | Validation boundary |
| --- | --- | --- |
| identity | collection, index name, SHA-256 index-definition digest; source generation/checksum/schema/row count; partition and router generations | exact live TVIS/base identity; ready router generation equals partition generation |
| placement | dense logical `partition_id` to one Raft group, with many logical partitions allowed per group | IDs are exactly `[0, partition_count)` and canonical |
| memberships | one disjoint membership per source ordinal; bounded overlap and representatives | ordinal/partition coverage, sorted order, per-vector and per-partition caps; the same ordinal/partition pair cannot be both home and overlap |
| assets | typed `ColumnAssetRef`, length, CRC, SHA-256 and logical asset ID for each partition plus router; native partition packs also carry the canonical membership digest | references are namespace-bound, unique, streamed and checksum-verified before every collection-authorized publication; native membership identity is recomputed from the authoritative source and must match both descriptor and pack header; router partition ID is exactly zero |
| ready set | SHA-256 over canonical placements, partition assets and router descriptor | mismatches, mixed router/generation, or missing partition asset reject |

Default decode limits are 16 MiB encoded bytes, 65,536 partitions, 1,048,576
source rows, and 2,097,152 aggregate memberships across disjoint, overlap,
and representative lists (so the declared 1M-row fixture plus 20% overlap and
representatives is representable), 262,144 assets, 4 KiB strings, 16
memberships per vector, and
65,536 representatives per partition. A single asset is capped at 8 GiB and
total referenced bytes at 16 GiB. Count-derived allocations are checked against
these limits before allocation.

VCP1 checkpoints use version 1, a SHA-256 checksum, a 30 MiB cap, at most two
live generations, a first-generation floor, and separate monotonic generation
and activation high-water fields. BUILD may choose any positive initial
generation, but every successor is exactly the prior high water plus one.
Therefore, an absent generation inside the floor/high-water interval is exact
completed-deletion proof; lower never-created IDs are not accepted as
idempotent cleanup retries. The activation watermark survives deletion of all
live generation pointers. VLC1 version-1 records form a sequence- and
previous-digest-bound immutable tail capped at 4 MiB per checkpoint epoch. The
physical identity namespace is capped at 64 MiB and 4,096 entries.

### Lifecycle, publication, and cleanup authority

| Transition | Immutable operation | Observable result |
| --- | --- | --- |
| absent -> building | BUILD in a new VCP1 checkpoint epoch | complete non-active building generation |
| building -> ready | READY digest-bound promotion delta | complete prepared generation, still not active |
| ready -> active | LOCAL_ACTIVATE delta | one complete ready generation is locally active and the activation high water advances |
| active -> retired | DEACTIVATE delta | generation remains prepared but is not active |
| non-active building/ready (including retired ready) -> deleting | caller fences plus DELETE_PREPARE carrying VPR1 | initial cleanup is accepted; an identical retry is idempotent; conflicting or resurrection transitions fail closed |
| deleting -> progress | RECLAIM_PROGRESS before mixed-segment remap publication | original and superseded debt remain protected and retryable |
| deleting -> absent | physical GC followed by DELETE_COMPLETE | generation leaves the live set; generation high water prevents resurrection |

The storage barrier is canonical-root scoped (including symlink aliases) and
serializes publication, deletion, reader acquisition, and snapshot export.
`AcquireVectorPartitionReaderPinV1` returns an idempotently released handle;
`VectorPartitionStatusV1` reports its count and deletion rechecks it under the
same barrier. `SnapshotReferences` and `CatalogReferences` are deliberately
external, caller-supplied proofs in M1. M7, not M1, owns their durable catalog
derivation and cluster cutover authority.

### Snapshot and portability semantics

Raft export copies `db/vector_partitions` together with column assets while
holding the same root barrier. For each identity it validates the complete live
namespace, selects only the highest checkpoint plus its contiguous current
tail, and omits lower audit epochs. It binds each selected regular file to its
stable physical identity before and after streaming. Export writes an explicit
empty `db/vector_partitions` directory when no lifecycle authority exists.
Restore rejects a missing directory, legacy mutable files, symlinks, hard-link
aliases, malformed chains, corrupt highest checkpoints, and extra audit epochs
before replacing the target namespace. It also streams and verifies the exact
ranges, CRC32 values, and SHA-256 digests of every asset referenced by a
non-deleting manifest. A restored manifest’s typed refs resolve against the
archived assets instead of the prior target directory. Snapshot archives are
copies: exporting an archive does not create a live reader pin or a durable
catalog reference. File names
are opaque SHA-256-derived identities; checkpoint payloads, not host paths, are
portable across restored DB roots.

On Windows, M1 can open an already restored `vector_partitions` namespace but
fails closed for creation, publication, activation, deletion, and reclaim
journaling. The exact no-replace installation and parent-name persistence proof
is not available there. This is an explicit platform capability boundary, not
a claim of Unix directory durability.

### M1 evidence boundary

M1 correctness tests publish a genuinely authorized ready generation and prove
that its checkpoint-reduced active state, router asset, and partition assets
survive close/reopen and Raft snapshot/install before `OpenActive` succeeds.
Scale measurements use canonical manifests with 10k, 100k, and 1M disjoint
memberships; fixture and authority construction remain outside the timed
operation. The measurement harness exposes no production authorization bypass
and is not production Raft or ANN evidence.

The evidence ledger reports encoded VPM1 manifest bytes separately from full
snapshot archive bytes. Only the former is used for the metadata-bytes/vector
gate; the latter also includes the archived side-store namespace and referenced
assets. Full-process maximum RSS includes compile and setup, while `B/op` and
allocations/op are scoped to the timed benchmark operation.

`cmd/treedb_vector_dataset_export` also supports a declared 1M-vector local
corpus (`-docs 1000000`) within its pre-allocation byte caps. Its manifest pins
vector/query checksums, dimensions, metric, query set, and an exhaustive
distance-then-ID top-k truth stream for a declared leading query prefix.
`-truth-queries` defaults to all exported queries when omitted for standalone
compatibility, accepts `0..queries`, and is recorded as
`exact_truth_queries`. Explicit zero emits an empty truth stream while
retaining all exported query vectors. The fixed 200,000,000 comparison gate
applies to `docs * truth_queries`, not every exported benchmark query. The
exporter materializes document vectors once in a
contiguous `float32` corpus, precomputes their squared norms, and reuses those
bytes for vector/JSON output, query selection, and exact truth. Truth selection
retains only bounded top-k candidates. Its modeled truth peak cap includes the
document corpus, norm array, generator/query scratch, top-k candidates/results,
conservative encoded truth-row growth, and a 1 MiB per-row encoding allowance;
combined output vector bytes remain separately capped. A feasible declared 1M
export uses one bounded truth query:
`GOWORK=off go run ./cmd/treedb_vector_dataset_export -out "$OUT" -docs 1000000 -queries 1 -dims 64 -top-k 10`.
It is a corpus export contract, not a claim that M0 has run a 1M-vector
exhaustive top-k benchmark.

## Verification

```sh
GOWORK=off go test ./cmd/treedb_vector_partition_bench ./cmd/treedb_vector_dataset_export ./TreeDB/docs -count=1
GOWORK=off go test ./cmd/treedb_vector_partition_bench -run 'Test.*(Fixture|Truth|Oracle|Manifest|Deterministic|Malformed|Cap).*' -count=1
GOWORK=off go test ./TreeDB/internal/vectorpartition ./TreeDB/collections -run 'Test.*(KMeans|Representative|PartitionRouter|HNSW).*' -count=1
```
