# Graph-partitioned vector search V1 contract

Status: internal, pre-alpha, experimental/off
Owner: #4013 (V1 contract)
Consumes: the M1/M4/M5/M6 contracts in `vector-partition-raft-v1.md`,
`vector-partition-shard-search-v1.md`, and `vector-partition-coordinator-v1.md`

## Purpose and admission boundary

### Router representation revision R (#4773)

The public search API remains V1, but newly built routers use
`treedb_vector_partition_router_v3`: a **global** representative budget B,
not B per domain. Canonically ordered nonempty logical domains reserve one
token each before quotas are apportioned by integer largest remainders,
weighted by membership count, with domain/node order breaking ties and a
non-unary-forest capacity of `2*n-1`. B below the domain count fails.

A logical domain is an unrepresented container. It learns and emits up to its
quota of genuine spherical bucket centroids; it does not emit or charge an
additional aggregate domain center. Every emitted centroid consumes one token.
Splitting a retained centroid preserves that centroid and consumes one token
per emitted child centroid. Residual quota is apportioned only among buckets
that are large and shallow enough to recurse. The reference defaults are
fanout 64 and minimum recursive cluster size 250. Initialization is a
deterministic sampled traversal; empty-cluster repair reuses assignment
distances. If a bucket's FP64 member sum has zero norm, its center is the
normalized vector of the first member in canonical ordinal order. Identical
vectors, leaf size, depth, insufficient quota, and failed
non-unary splits stop subdivision. Unused tokens are reported, not filled with
fabricated duplicate nodes. Physical packing does not multiply the
logical-domain quota.

Representative identity is `(logical domain, represented node ID)`. Source
ordinal is provenance, may repeat at several levels, and is not a uniqueness
key. Ordering, node ancestry, true leaf flags, quotas, config, centers, and
source identity are digest-bound and checked on open. Routing remains
nearest-center distance, with deterministic domain/representative ties;
frequency voting is diagnostic only.

Search explicitly separates returned width **w**, retained traversal beam **E**,
and actual score-call ceiling **C** (`VectorPartitionRouterSearchOptionsV2`).
C is independently bounded to [1, 1,000,000] before execution.
Require `1 <= w <= E <= actual representatives`; C is positive and may exceed
the representative count because upper-level navigation can score a vector
again. Every such score is charged. Exhaustion returns a budget error and no
partial routes; no clamping or exact fallback occurs. Fewer distinct domains
than requested is a separate coverage error. The explicit exact reference
scores all representatives; use `w=E=N, C>=N` for the full-domain oracle. An
exact scan truncated to w is only the matched-width representative reference.

This is an intentional pre-alpha format break: router model/record/asset
semantics are version 3. Manifest binary version 5 and ready-promotion payload
version 3 remain current because their `(source ordinal, domain, represented
node)` mapping did not change. Router records are 16-byte-identity records
(source ordinal, domain, node); path metadata retains leaf flags and quota.
Rebuild old DB/benchmark directories; do not migrate or silently reinterpret
version-2 router assets. Existing publication, generation pin, checkpoint,
reopen, GC/rewrite reachability, and persistent-value-log obligations remain.
The mapped immutable owner retains vector storage; live owners still clone
their retained vectors before the pin is released.

The benchmark uses `-router-global-budget`, `-router-width`, `-router-beam`,
and `-router-score-budget`. M8 evidence is schema 5, system-node config is
schema 2, and policy diagnostic method/digests bind the new beam semantics.
Old #4744/#4745 receipts remain historical; neither rebuilding nor changing
their labels makes them evidence for this revision. R does not correct local
HNSW, change memberships, or establish the final scaling/recall verdict.

This document freezes the supported snapshot-bound graph-partitioned vector
search V1 contract. It is an admission contract, not an enablement claim: the
feature remains internal and experimental/off until its owning rollout gate
accepts it. The original V1 admission contract did not add a runtime route or
promote the benchmark simulation to production evidence. The R revision above
explicitly changes derived-asset formats without changing that enablement gate.

V1 has two deliberately disjoint result classifications:

| Classification | Contract name | What is proved |
| --- | --- | --- |
| exact | `exact_partition_union_v1` | The exhaustive union of all canonical partition memberships, searched by the exact FP32 path and merged under the canonical score contract, equals global exact top-k. |
| approximate | `approximate_hnsw_recall_qualified_v1` | A selected set of partition-local native HNSW searches is measured against exact truth by recall and is not an exactness claim. |

No accepted ANN recall loss may be relabeled as an exact-union failure, and no
exactness failure may be accepted as ordinary ANN recall loss.

## Exact correctness obligations

An `exact_partition_union_v1` result is conformant only when all of these hold:

1. The M1 manifest/source identity is byte-for-byte bound at open: collection,
   index definition digest, source generation/checksum/schema/row count,
   partition generation, router generation, ready-set digest, complete
   logical-domain-to-physical-pack mapping, and canonical pack placement. A
   missing, stale, mixed, or source_mismatch
   manifest/source/generation identity is rejected before routing or fanout as
   `generation_mismatch` (with the failed identity in detail), not a partial
   result. Exact placement partition/group drift is separately
   `route_mismatch`; it is not folded into `generation_mismatch`.
2. Where routing is used, the exact representative route scores every persisted
   representative and selects unique logical domains deterministically by
   `(distance, domain_id)`. Every selected domain expands to all of its required
   physical search packs. Representative selection is not a substitute for
   an exhaustive exact union.
3. The union covers every canonical physical search pack exactly as named by
   the accepted generation. Each pack is searched with `exact_fp32_scan_v1`;
   overlap may yield repeated stable IDs but may not create a second logical
   document.
4. Scores use the canonical FP32 cosine contract. Global dedupe keeps the best score per stable ID and final top-k ordering is `(score descending, stable ID bytewise ascending)`. Equal-score ties and duplicate arrival order therefore
   cannot change the answer.
5. M1 lifecycle authority is the only immutable-generation authority. Without
   the standalone live-delta extension below, a relevant committed insert,
   delete, embedding replacement, index-definition change, or source-snapshot
   replacement means mutation invalidates the derived generation for future V1
   requests. With the extension, the exact immutable manifest remains fixed and
   is serveable only while the registered `VectorIndex` proves matching live
   revision and source coverage for that manifest. A pin accepted before either
   a live revision or immutable-generation transition may finish against its
   captured composite view; no request may mix identities.
6. The coordinator and shard service are all-or-error. Missing owner, stale or
   mixed proof/identity, unavailable partition, corrupt asset, cancellation, or
   failed search returns an error and an empty response; no partial top-k is
   returned. Stable public classes include `generation_mismatch` and
   `assets_unavailable` for missing/corrupt local assets and the M5/M6
   availability classes for unavailable owners/groups. Placement partition/group
   drift is `route_mismatch`, while manifest/source/generation identity drift is
   `generation_mismatch`.

The production-path exact-union gate is
`TestM8ProductionMultiGroupAssetsCheckedIn10kCISmokeV1`: it opens every
generation-pinned persistent partition pack, calls `SearchExactWithOptionsV1`,
and compares all IDs and FP32 score bits against the canonical source oracle.
The companion `TestM8CanonicalFP32ScoreContractTiePrecisionAndDedupeV1` locks
the best-duplicate and stable-ID tie merge rule. The older
`TestTruthOracleTieOrderingAndAllPartitionParity` remains historical
float64/modulo-fixture coverage, not the canonical persisted V1 gate. M5 and
M6 additionally validate response identity, partition order, no
within-partial duplicates, and the same global merge ordering before accepting
a response.

## ANN obligations and the all-partition HNSW disposition

Representative routing and partition-local HNSW are separate approximations:

- `RouteExactV1` is the representative-routing oracle. Approximate
  representative routing has an explicit candidate budget and reports recall
  separately from partition-local loss.
- `hnsw_search_pack_v1` is a partition-local ANN traversal. The historical
  `exact_hnsw_search_pack_v1` route label names the prepared native search-pack
  asset and IDs/scores response mode; it does **not** assert exhaustive HNSW
  traversal or exact top-k.
- Probing every partition with partition-local HNSW remains
  `approximate_hnsw_recall_qualified_v1`: it is **not an exact or rerank-rescued path** in V1. Its required evidence is recall against the
  exact union/global oracle, with candidate/edge/search-route counters. An
  exact claim instead uses `exact_fp32_scan_v1`; a future rerank rescue needs a
  separate versioned contract and before/after performance evidence.

`TestTreeDBHNSWStageUsesExactSearchPackAndMatchesHighEFLocalTruth` proves that
the harness opens the declared native pack and records its route/counters. Its
high-ef fixture parity is a regression check, not a universal HNSW exactness
guarantee. The stage remains recall-qualified under this contract.

## Response and document boundary

V1 shard and coordinator responses are IDs/scores-only. Stable IDs and owned FP32 scores are sufficient for dedupe, ordering, and top-k. They must not fetch or materialize documents, embeddings, or distributed document payloads.
Document materialization is an optional caller-side operation after V1 returns;
it has no snapshot, placement, or all-or-error guarantee from this contract.

## Standalone live-delta extension

The production standalone collection adapter may keep one active immutable M1
base searchable across acknowledged document mutations by binding it to the
collection's already registered `VectorIndex`. This is not a second mutable
index. The same index owns the immutable-manifest binding, monotonically
increasing live revision, exact collection document-generation coverage, per-logical-domain
deltas, stable-ID ownership/tombstones, and captured composite search views.
The extension is deliberately absent from replicated/Raft lifecycle serving;
requests with no standalone live binding retain the immutable V1 behavior.

The binding preserves the immutable base identity
`(collection, index-definition digest, source identity, partition generation,
domain-pack mapping)`. Live revision and coverage are distinct proof fields and
must never rewrite that identity. A standalone request pins one combined
identity before fanout and sends the same revision and coverage to every shard.
A collection mutation that commits while this coordinator pin is active waits
at live-overlay publication; every local shard therefore opens the pinned
revision, and mutation acknowledgement completes after the request releases it.
A shard with a live-bound lease rejects a request that omits or mismatches that
identity. Cold authority reacquisition may accept a newer current collection
document state only when the durable binding and recovered/replayed overlay
cover it exactly; unrelated database state-token changes do not invalidate that proof.
Missing, stale, partial, or corrupt coverage fails closed.

An acknowledged insert, embedding replacement, delete, or logical-domain move
publishes ownership, tombstones, all affected domain views, live revision, and
source coverage atomically. The mutation is immediately searchable in memory;
each foreground or replayed command-WAL document command serializes compact
carrier metadata, the changed owner record, and the dirty HNSW records of only
the touched logical domains, then includes that root delta in the same ordered
document/column/locator/system-root publication before acknowledgement or
advancement of the applied command frontier. This does not rebuild or rewrite
the immutable partition base, serialize untouched logical domains, or scan
collection rows. An exact owner-record count makes missing tombstones fail
closed. Owner epochs and a monotonic global domain-epoch high-water make
cutover replacement atomic; older epoch records may remain physically present
but are unreachable from the current compact metadata, and a retired domain
never reuses one of their epochs after rebind or reopen. V1 inline state
receives one complete V3 materialization before incremental publication resumes.
The first live binding
is durably published before overlay mutations are admitted, and ordinary
checkpoint plus command-WAL replay reconstructs later acknowledged changes on
reopen. Recovery loads the checkpointed carrier before applying later
document-only commands; it never rebuilds the carrier from collection rows or
requires a post-checkpoint binding command. Snapshot restore validates both
directions of ownership: every live owner names one current delta row, and every
current delta row has exactly one matching nondeleted owner. Mapping,
representative, revision, coverage, or ownership mismatch rejects the whole
live state.

Search applies live tombstones and ownership to base candidate eligibility,
before native HNSW top-k admission; stale base rows may navigate but cannot be
selected. Each selected logical domain searches its live delta exactly once,
even when the domain expands to physical packs in several owner groups. The
base and delta results use the existing deterministic stable-ID merge and score
ordering. Admission has both a 128K stable-owner-ID ceiling and a conservative
256 MiB retained-plus-snapshot byte budget. The byte budget is dimension- and
M-dependent and charges live vectors, all bounded HNSW layers, retained
obsolete nodes, stable IDs, owners/tombstones, router representatives, and
transient snapshot/JSON copy headroom before accepting the document mutation.
Capacity exhaustion is explicit and fail-closed. Repeated-update tombstones are
compacted by atomically replacing domain indexes built from live nondeleted
owners. Node- or byte-cap admission that can reclaim retained history performs
one such cutover and fully re-estimates the proposed mutation before rejecting
it. A normal mutation copies only the touched HNSW neighborhood into its
rollback journal. The collection publication barrier spans that speculative
edit, durable grouped-root publication, and search-view handoff: existing
requests keep their immutable view pinned, while new pins cannot observe
unpublished owners, revision metadata, or graph adjacency. Accepted handoff
failure invalidates the carrier fail-closed; prepublication failure restores
the exact prior graph and can retry deterministically. Publishing a newer exact
immutable manifest takes the same collection publication barrier, installs an
empty overlay, and retires the old generation without invalidating its already
captured pins. Direct public mutation of a registered live carrier takes that
same barrier, so it cannot modify a shared domain while a grouped-root attempt
may still roll back. Persistence acknowledgement uses already-maintained byte
and mutation-sequence state; it does not rescan the domain graph.

Preflight includes the bounded live-delta candidate and scratch requirements
before coordinator budgets are distributed. Search time includes base and
delta traversal. Counters separately report base/delta candidate work,
base/delta returned-result contribution, logical domains searched, cutovers,
request-path rebuilds, and exact fallbacks. A healthy live request performs no
request-path reconciliation scan or exact fallback and reuses warmed immutable
packs across mutations. The aligned coordinator, shard, and public operations
candidate ceiling is 80 MiB: enough for the declared one-million-row base floor
plus the maximum live-node and stable-ID preflight floors without weakening any
per-request accounting.

## Historical evidence ownership

Issue #3999 is historical scoped HNSW evidence, not the current owner of the broad all-partition exactness gate. Its closed result may describe measured partition-local HNSW recall and remaining limitations, but it does not convert HNSW into an exact oracle or waive an exact-union failure. #4013 owns this V1 contract and its classification/admission wording; performance qualification remains outside this issue (including #4015 and the production lanes).

## Excluded behavior

V1 excludes live repartitioning, live-delta serving under replicated/Raft
lifecycle authority, distributed document fetch, multi-host qualification, and
eventually consistent coverage. The standalone extension provides only the
acknowledged mutation freshness and exact fail-closed identity described above;
all other serving remains immutable snapshot-bound.

## Verification admission

`TestDocsVectorPartitionV1CorrectnessAndApproximationContract` guards this
frozen boundary. `TestVectorIndexPartitionLive*` covers the owner/persistence/
cutover lifecycle, and
`TestVectorPartitionLiveProductionCoordinatorMutationAndColdReloadV1` covers
the public production generation-source-to-shard-to-coordinator path. They
complement the exact-union, HNSW-route, lifecycle, source-identity,
response-proof, and all-or-error executable tests in the M1 through M6
packages; documentation is not a substitute for them.

### Optional offline quality diagnostics

The M8 benchmark's explicit `-m8-quality-diagnostics` selection is an offline,
static-generation attribution contract, not a public serving policy. It MUST
bind canonical query/truth, source/model and complete logical-domain to physical
pack ownership. It MUST NOT count overlap twice, invent unsampled score traces,
or interpret independent domain-cost and pack-cost optima as the same feasible
route. Actual coordinator truth masks are attached from measured output and
remain distinct from offline local-search masks.

The existing canonical score/tie, exact-union, all-or-error, generation-pin and
visibility contracts are unchanged. Historical receipts without the selection
retain their previous method. A selected diagnostic's producer, command binding,
work preflight and retained replay must agree on the new method and fields;
missing or conflicting data reject rather than falling back to a partial pass.
See `TreeDB/docs/performance/vector-partition-m8.md` for costs, commands and the
representative-baseline boundary.

Selected quality evidence on `candidate_coverage_shortfall`,
`router_score_budget_exhausted`, or `mixed_router_refusal` rows MUST also be
recomputed from reopened static assets. Structural validation alone cannot bind
query/truth/model digests, coverage costs or nearest-member routing. Only the
unavailable local/coordinator observations are suppressed, using the same rule
as the producer. Failed serving is not permission to trust self-reported static
observations.

Offline trace-ID preparation uses the context-aware ordinal-map copy while the
prepared owner is pinned. It checks cancellation before allocation, during the
copy, and before cache publication; cancellation returns no partial mapping and
releases the operation's pin. This does not add trace preparation to serving.

Offline qualification fixtures may specify `query_ordinal_offset` (default zero,
omitted from legacy JSON). It selects only query generator ordinals, never corpus
ordinals. The nonnegative half-open range end MUST fit in signed 64 bits before
allocation; the legacy generator MUST reject nonzero offsets. Query bytes and
canonical truth remain checksum-bound, so fresh ranges require fresh fixture and
truth-cache identities. Relative query indices in reports and calibration splits
remain unchanged. Existing retained descriptors MUST still match the complete
fixture checksum; corpus equality alone does not authorize descriptor reuse.

### Optional same-candidate ranking comparison

The M8-only `-m8-router-policy-diagnostics` selection additionally compares
minimum-distance, frequency and frequency-first/distance-rest rankings under
one immutable representative owner. It MUST use one candidate collection per
comparison, count distinct returned representative identities, preserve the
remaining distance order after prepending the frequency winner, and bind set
and sequence identities separately. Exact-reference voting MUST first truncate
to the declared nearest returned width while charging the full scan.

A typed candidate-coverage shortfall MUST retain its work and candidate identity
but return no partial policy route. Producer and retained consumer MUST preserve
all query outcomes and actual logical-domain/physical-pack cost. This is an
offline coverage experiment, not admission of any new public routing policy or
model. Ordinary query results, score/tie order, generation lifetime, write
placement, durability and default allocation behavior remain unchanged.

Selected router-policy preflight MUST bound query/truth identity checks on cache
hits across both producer and strict-replay EF/concurrency populations, not just
new candidate collections. Cached identity validation MUST NOT be removed to
avoid that cost. Nearest-width and policy ordering MUST observe cancellation
while sorting under the captured owner; canceled diagnostics return no routes.
