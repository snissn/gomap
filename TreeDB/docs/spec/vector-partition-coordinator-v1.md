# Vector partition coordinator V1

Status: internal, pre-alpha, experimental

Owner: vector partitioning M6 (#3915)

Consumes: M1 placement, M3 partition-local search assets, the persisted M4
router, and the M5 shard-search contract

## Purpose and boundary

`VectorPartitionCoordinatorV1` is the bounded, transport-neutral scatter/gather
coordinator for one vector query. It:

1. opens and validates one persisted M4 router generation;
2. selects logical vector partitions;
3. resolves those partitions through one immutable M1 placement snapshot;
4. groups and chunks them into M5 shard-search requests;
5. dispatches those requests through a fixed worker pool;
6. validates every M5 response and read proof;
7. deduplicates stable IDs and returns one deterministic top-k.

The coordinator returns stable IDs and authoritative FP32 cosine scores. It
does not fetch documents, mutate TreeDB, activate or rebuild a generation,
choose a serving replica, implement a network codec, or create a
vector-specific consistency mechanism.

`VectorPartitionShardSearchDispatcherV1` deliberately owns transport and
connection cancellation. An in-process registry can implement it for tests and
local evidence. M8 now supplies a bounded TCP envelope adapter in
`vector-partition-m8-production-topology.md`; production network measurements
and multi-group Raft acceptance still require the topology harness and evidence
defined there.

## Public construction and dependency APIs

The internal constructor is:

```go
NewVectorPartitionCoordinatorV1(VectorPartitionCoordinatorOptionsV1)
```

Its options contain a validated `raftplacement.ResolvedCatalogV1`, an exact
`VectorPartitionPlacementRecordV1`, a
`VectorPartitionCoordinatorRouterSourceV1`, a
`VectorPartitionShardSearchDispatcherV1`, and optional hard limits. The
constructor validates placement against the catalog and clones the group and
partition slices. Its routing authority is therefore an immutable M1 snapshot,
not caller-owned mutable state.

Callers outside TreeDB's internal Raft packages can use:

```go
NewVectorPartitionCoordinatorForTopologyV1(
    VectorPartitionCoordinatorTopologyV1,
    VectorPartitionCoordinatorRouterSourceV1,
    VectorPartitionShardSearchDispatcherV1,
    VectorPartitionCoordinatorLimitsV1,
)
```

This adapter validates the public topology into the canonical M1
catalog/placement representation before constructing the same coordinator.

`CollectionVectorPartitionCoordinatorRouterSourceV1` adapts a real
`collections.Collection`. The coordinator opens one context-aware M4 router
session per index/generation and leases it to concurrent searches. Every lease
still validates runtime status against the M1 snapshot and (when configured)
the replicated lifecycle authority before routing or M5 dispatch. Cold opens
are singleflight and a waiting request may cancel without canceling a shared
session. A runtime-status or lifecycle rejection retires that exact session:
new leases cannot reuse it and its persistent reader pin is released after its
last lease. Every router-close error is joined to its owning search result;
the first is retained for `Close` without growing an unbounded error history,
so retirement cannot silently hide release failure.
`Close` rejects new searches, drains leases, then closes any remaining pinned
router handles.

The coordinator is immutable for one accepted catalog/collection/index/source/
partition epoch. Catalog or generation replacement constructs a replacement
coordinator and drains the old coordinator before its sources retire; there is
no cross-epoch or process-global router cache. `Stats` includes a deterministic
sorted snapshot of bounded per-placement router accounting with cold-open, hit/miss,
open-failure, lease, reader-pin/release, invalidation, and close accounting.
The coordinator admits exactly one immutable placement key, so repeated
same-epoch reopens aggregate into one retained record instead of growing a
session-history cache. Its first successful session binds the accepted
ready-set and router-model digests; a reopen with changed digests fails closed
as a generation mismatch. `manifest_open_attempts` is the cumulative count of
source calls that perform the manifest/asset validation-open boundary;
repeated session hits perform no such open.

The execution API is:

```go
Search(context.Context, VectorPartitionCoordinatorRequestV1)
    (VectorPartitionCoordinatorResponseV1, error)
```

`Stats` returns a concurrency-safe cumulative
`VectorPartitionCoordinatorStatsV1` snapshot.

## Request and generation identity

`VectorPartitionCoordinatorRequestV1` requires:

- version, request ID, and cancellation ID;
- database, catalog, collection, vector index, and exact index-definition
  digest matching the constructed M1 placement;
- a nonempty finite FP32 query with nonzero norm;
- cosine metric;
- exact or approximate M4 router mode, an explicit representative-candidate
  budget, and a partition-probe count;
- `linearizable_generation_snapshot` consistency;
- `basic` stats mode; `none` is rejected because coordinator response
  validation and budget enforcement require actual candidate/edge and stage
  counters from every M5 partial;
- `top_k`, `ef_search`, request/candidate/response byte limits, and a merge
  entry limit;
- an optional Unix-nanosecond deadline.

The request does not carry caller-selected source or derived-generation
numbers. After open, the coordinator requires the M4 runtime status to match
the constructed M1 placement exactly:

- ready state, collection, index, and index-definition digest;
- source generation, checksum, schema hash, and row count;
- partition generation and router generation;
- partition count and every canonical partition-to-group placement;
- valid ready-set and router-model SHA-256 digests;
- nonzero representatives and consistent runtime partition count.

Exact router mode additionally requires a candidate budget at least as large
as the persisted representative count. A mismatch fails before M5 dispatch;
the coordinator never mixes router, partition, source, or placement
generations.

## Request state machine

The request state sequence is fail closed:

| State | Required transition |
| --- | --- |
| preflight | validate request identity, search shape, finite query, and caller budgets |
| router pin | acquire a generation-pinned M4 session and exact per-request runtime-status validation |
| route | search that pinned router and require exactly the requested number of unique, finite partition scores |
| plan | resolve every selected partition through M1, group and chunk deterministically, and reserve all budgets |
| fanout | run every M5 task through the fixed worker pool, with one bounded not-leader retry path |
| verify | validate every M5 envelope, partition order, proof, score, counter, and byte total |
| dedupe | keep the best FP32 score for each stable ID and count duplicates/disagreements |
| merge | select top-k by descending score, then bytewise ascending stable ID |
| success | publish one owned response with exact generation identity, probes, counters, and timings |

There is no partially successful state. Any error zeroes the response, cancels
the shared child context, waits for every started worker to exit, releases its
router lease, and returns one classified error. Identity/lifecycle rejection
also retires the session so its reader pin cannot indefinitely block generation
cleanup. The coordinator retains no query result after return; its only
retained request-independent state is a bounded session for its immutable
accepted epoch until retirement or `Close`.

The effective deadline is the earliest of the caller context deadline, the
request deadline, and `now + MaxWallClock`. The default wall-clock ceiling is
30 seconds. The effective deadline is propagated to every M5 request.
Both the exact representative scan and approximate native HNSW traversal poll
that context during search. Cancellation is also checked before work, before
and after dispatch, while validating partials, during dedupe, and periodically
during heap merge.

## Deterministic planning and fanout

The M4 router's selected partition order is retained in
`ProbedPartitions`. Planning rejects an out-of-range, repeated, or non-finite
router result. For dispatch:

- partitions are grouped by their exact M1 owner group;
- group IDs are sorted bytewise;
- partition IDs within a group are sorted numerically;
- each group is chunked into at most `MaxPartitionsPerRequest` IDs;
- task and derived request IDs follow that canonical group/chunk order;
- `ProbedGroups` contains the same sorted group order.

This makes grouping, chunking, request budgets, and merge input independent of
Go map iteration order.

The fanout worker count is
`min(task_count, MaxConcurrentRequests)`. The default is eight. The coordinator
does not spawn one unbounded goroutine per group. On the first terminal task
failure it cancels siblings and joins the whole pool before returning.

Only M5 `not_leader` is retryable. By default, one retry and one redirect are
allowed. A redirect hint must name a member of the already resolved group.
Every attempt uses the same generation, partitions, query, and budgets.
Unknown hints, non-members, exhausted retry/redirect budgets, and all other M5
errors are terminal.

## Hard budgets

Zero-valued limit fields select the following defaults:

| Coordinator resource | Default hard ceiling |
| --- | ---: |
| selected partitions | 256 |
| owner groups | 64 |
| M5 requests | 256 |
| concurrent M5 requests | 8 |
| retries / redirects | 1 / 1 |
| router candidates | 1,000,000 |
| query bytes | 16 KiB |
| `top_k` / `ef_search` | 256 / 4,096 |
| partitions per M5 request | 32 |
| identity / stable-ID bytes | 4,096 / 4,096 |
| merge entries | 65,536 |
| total logical M5 request bytes | 4 MiB |
| total candidate bytes | 64 MiB |
| total response bytes | 64 MiB |
| wall clock | 30 s |

Limits shared with M5 cannot exceed the M5 service ceilings. The concurrency
limit cannot exceed the request limit, `ef_search` cannot be below `top_k`,
and the wall-clock limit cannot be below 1 ms.

Planning reserves resources before dispatch:

- logical request bytes use an overflow-checked fixed envelope plus query,
  partition, and identity bytes. Each task reserves its longest valid group
  member as the target identity, so a bounded not-leader redirect cannot grow
  past the task or aggregate request budget. Every M5 request must also fit
  M5's 64 KiB request ceiling;
- candidate reservation is computed per selected partition. For partition `p`,
  the floor is
  `max(membership_rows[p] * 64, conservative_search_scratch_bytes(p), ef_search * 64)`.
  The request must cover the overflow-checked sum of those partition floors;
  only the remaining surplus is divided deterministically by membership weight
  and then combined across partitions in each M5 chunk. Consequently,
  `selected_partitions * ef_search * 64` is only the traversal component, not a
  sufficient general budget formula for exact scans or large partitions;
- response reservation uses M5's downstream stable-ID ceiling for every
  `partition * top_k` result before sizing response slices. This remains true
  when the coordinator's own accepted stable-ID cap is lower because M5 V1
  preflight has no per-request stable-ID cap;
- merge capacity requires `partition_probes * top_k` to fit both the request
  and coordinator merge limits.

After dispatch, actual measured response bytes and `candidates * 64` are
checked again against the caller limits. Overflow, under-reservation, or any
reported value above a bound is an all-request `budget_exceeded` failure.

## M5 response and proof validation

Every accepted M5 response must match its planned task exactly:

- version and derived request ID;
- partition count and partial count;
- group, serving node, and requested target;
- source, partition, and router generation identity;
- nonzero read term/index and applied term/index;
- `applied_index >= read_index`. The applied term may precede the read term
  after an election when M5 has proved that the intervening committed prefix is
  command-free;
- leader equals serving node, and both belong to the resolved group;
- one partial per requested partition in the planned order;
- at most `top_k` neighbors per partial;
- only `hnsw_search_pack_v1` or `exact_fp32_scan_v1` route labels;
- nonempty bounded stable IDs, finite FP32 scores, no within-partial duplicate
  IDs, and descending-score/bytewise-ID ordering;
- overflow-safe candidate/edge sums, exact logical response-byte accounting,
  and matching reported totals.

The coordinator does not synthesize or weaken an M5 proof. A malformed,
generation-mixed, follower-served, out-of-order, duplicate, non-finite,
misaccounted, or over-budget response fails the whole request.

## Dedupe and top-k

Across partitions, stable ID is the only dedupe key. The coordinator keeps the
highest FP32 score. Repeated IDs increment `Duplicates`; bitwise-different
scores additionally increment `ScoreDisagreements`. The best score wins even
when responses arrive in a different order.

The final bounded heap and output sort use:

1. higher cosine score first;
2. bytewise lexicographically smaller stable ID first for equal scores.

This ordering is deterministic and matches the all-partition correctness
oracle. If fewer than `top_k` live unique IDs exist, the coordinator returns
the smaller complete set.

## Errors, counters, and timings

`VectorPartitionCoordinatorErrorV1` exposes these stable classes:

- `invalid_request`, `generation_mismatch`, `route_mismatch`;
- `budget_exceeded`, `malformed_response`;
- `not_leader`, `unavailable`;
- `canceled`, `deadline_exceeded`.

The optional `GroupID` identifies the failed task. Wrapped M5 errors are mapped
without turning a service failure into partial success.

Each success reports selected partitions/groups; requests, RPCs,
retries/redirects; query/request/response/candidate bytes; candidates, edges,
merge entries, duplicates, and score disagreements. Timing separates router
open/search, placement, queue, dispatcher RPC, transport residual,
read-index/apply, generation open, shard search, response materialization,
dedupe, merge, and total request time.

`Stats` accumulates request/success/error/cancel/timeout counts, successful
fanout and dedupe counters, and total time under a mutex. It is operational
status, not a durability record.

The scoped 1M local-service result is recorded in
`../performance/vector-partition-m6.md`. It validates the real M4/M6
composition and exact all-partition merge, but it uses an in-process M5
contract simulation with synthetic read proofs and no production network or
Raft execution.
