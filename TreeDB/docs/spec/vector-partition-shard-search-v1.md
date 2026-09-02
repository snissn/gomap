# Vector partition shard search V1

Status: internal, pre-alpha  
Owner: vector partitioning M5 (#3914)  
Consumes: M1 placement, M3 partition-local search assets, and the generic
group-routed read-index/apply contract from #3474

## Purpose and boundary

`VectorPartitionShardSearchServiceV1` serves one bounded request over one or
more logical vector partitions owned by one Raft group. The response contains
stable IDs, authoritative FP32 cosine scores, bounded search counters, and the
actual group read proof. It never returns full documents.

This service is a local owner endpoint, not a coordinator:

- it does not run the kRt router or scatter to several groups;
- it does not forward to a remote leader or search a convenient local copy;
- it does not submit, apply, rebuild, publish, activate, or otherwise mutate
  TreeDB state;
- it does not create a vector-specific consistency mechanism.

The owner group is resolved from the validated M1
`VectorPartitionPlacementRecordV1`. The service then calls #3474's
`RoutedReadIndexCoordinator` for that exact group and leader target. Only after
the coordinator returns a valid production quorum proof and confirms local
apply through the proof index may the service pin and open M3 assets.

## Snapshot and freshness semantics

V1 supports only `linearizable_generation_snapshot`.

That policy means:

1. the serving owner group has completed a quorum-backed read-index proof;
2. the serving node has applied through the proof index;
3. the named immutable vector partition generation was complete and active
   locally when pinned;
4. all returned partitions were searched from that exact pin.

It does **not** mean "latest committed vector mutation." Vector generation
publication is asynchronous from arbitrary document mutations. Requests that
claim latest-vector, follower-stale, lease, or another consistency policy fail
with `unsupported_consistency`.

The response repeats source, partition, and router generation identity together
with serving node, group, read term/index, and applied term/index. A coordinator
must reject partials whose proof does not exactly match its fanout plan.

## Request

`VectorPartitionShardSearchRequestV1` is an in-process native contract. A
network decoder must enforce the same limits before allocating its slices.

Required identity:

- version, request ID, and cancellation ID;
- database, catalog, collection, vector index, and index-definition digest;
- exact source generation/checksum/schema hash/row count;
- exact partition and router generation;
- target group and optional exact leader node;
- a strictly increasing list of logical partition IDs.

Required search shape:

- FP32 query vector;
- `cosine` metric and `no_document_partition` mode;
- `top_k`, `ef_search`, and `none` or `basic` stats;
- request, candidate, and response byte budgets;
- optional Unix-nanosecond deadline.

The mode promises an IDs/scores-only partition response, not an exact-scan
candidate-discovery algorithm. Each partial reports the immutable asset's
actual route: `hnsw_search_pack_v1` for a native persistent pack or
`exact_fp32_scan_v1` for the bounded in-memory fallback. A request that claims
the old `exact_no_document` mode fails during preflight; it cannot silently run
HNSW under an exact-search label.

Default service ceilings are:

| Resource | Ceiling |
| --- | ---: |
| dimensions / query bytes | 4,096 / 16 KiB |
| partitions per request | 32 |
| `top_k` / `ef_search` | 256 / 4,096 |
| request bytes | 64 KiB |
| candidate bytes | 64 MiB |
| response bytes | 64 MiB |
| identity / stable-ID bytes | 4,096 / 4,096 |

Before routing or opening local state, validation checks dimensions, all
floating-point values, zero norm, partition ordering/counts, `top_k`,
`ef_search`, and overflow-safe conservative request/candidate/result byte
bounds. The conservative result bound uses the maximum stable-ID size so a
request cannot force an unsafe result allocation and hope that actual IDs are
short.

## Execution order and pin lifetime

The service executes the following fail-closed sequence:

1. validate shape, scalar values, canonical partition IDs, and byte budgets;
2. resolve every partition through the immutable M1 placement record and
   require one exact target group;
3. reject a remote owner without observing local collection state;
4. obtain and independently validate #3474's read-index proof and applied
   progress for the exact local node/group;
5. acquire an M1 reader pin, then validate that the exact generation is ready,
   active, source-current, and free of missing/corrupt/stale assets;
6. open every requested M3 partition searcher before searching any partition;
7. run M3's IDs/scores-only search with the explicit `ef_search`;
8. validate IDs, finite scores, counters, and response bytes before returning
   the single success envelope.

Any open or search failure discards all accumulated partials. Per-request
generation and partition leases are released on success, error, cancellation,
deadline, or caller context loss. The production collection adapter caches the
validated immutable generation, its long-lived M1 reader pin, and opened
mapped/heap search packs. A cold generation load reads and validates lifecycle
and vector-source authority once, then retains a generation-scoped in-memory
activation lease, the DB's coherent scalar state token, and a collection-bound
partition asset/membership open plan. Distinct cold partition opens reuse that
immutable plan instead of decoding the lifecycle manifest or scanning all
generation memberships again; each open still acquires its own reader pin and
revalidates the membership digest, asset bytes, and mapped-pack identity. Warm
request leases perform only bounded in-memory activation/state comparisons,
clone the bounded placement identity, and reuse the cached partition
searchers; they do not scan the lifecycle directory or call operational
vector-index status APIs. A normal replacement `BUILD` leaves the active
generation lease valid. Successful `READY` activation or deactivation advances
the mutation-driven activation revision. A DB state publication evicts the
cache entry without permanently tombstoning it and forces one new full
authority load, which either confirms the generation/source or fails closed.

The catalog/lifecycle owner must call
`InvalidateVectorPartitionGenerationV1` when it replaces the service's static
placement generation. Invalidation permanently tombstones that generation in
the source, blocks stale reloads, and retires its resources after existing
request leases drain. Lifecycle activation changes are also detected by the
collection-owned authority lease, so a missed service-owner notification
cannot keep serving a retired generation. Source `Close` similarly rejects new
pins and lets active requests drain. Reader-pin acquisition and cold authority
loading propagate caller cancellation through lifecycle checkpoint reads. If
cancellation abandons an in-flight exact fallback search, the request pin keeps
its resource alive until that search exits; no close path unmaps a pack
underneath an active request.

A concurrent activation can therefore make the requested generation old, but
cannot mix old and new assets inside one accepted request.

## Stable failure classes

`VectorPartitionShardSearchErrorV1` carries one of:

- `invalid_request`, `unsupported_consistency`;
- `missing_owner`, `unknown_owner`, `remote_owner`, `route_mismatch`;
- `not_leader` (with catalog leader hint), `group_unavailable`;
- `generation_mismatch`, `assets_unavailable`;
- `response_too_large`;
- `canceled`, `deadline_exceeded`.

The wrapper preserves the generic routed-read or M3 error as its cause.

## Observability and performance attribution

Each response separates:

- route/owner resolution;
- read-index plus apply wait;
- generation and partition open;
- M3 search;
- response validation/copy;
- total request.

Basic partial stats include candidates, edges, search route, pack bytes,
mapped/heap bytes, and open time. Service totals count requests, successes,
every fail-closed class, owner routes, read proofs, partitions, candidates,
response bytes, mapped/heap opens, partition cache hits/misses, cancellation,
and timeout. The collection generation source separately counts generation
cache hits/misses, partition hits/misses, and invalidations. The service has no
mutation API or mutation dependency.

Read-index/apply time is always separate and must never be attributed to ANN
search. See `../performance/vector-partition-m5.md` for the checked-in scoped
measurement and its explicit evidence boundary.
