# Graph-partitioned vector search V1 contract

Status: internal, pre-alpha, experimental/off
Owner: #4013 (V1 contract)
Consumes: the M1/M4/M5/M6 contracts in `vector-partition-raft-v1.md`,
`vector-partition-shard-search-v1.md`, and `vector-partition-coordinator-v1.md`

## Purpose and admission boundary

This document freezes the supported snapshot-bound graph-partitioned vector
search V1 contract. It is an admission contract, not an enablement claim: the
feature remains internal and experimental/off until its owning rollout gate
accepts it. It does not add a runtime route, change a persistent format, or
promote the benchmark simulation to production evidence.

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
   partition generation, router generation, ready-set digest, and complete
   canonical partition placement. A missing, stale, mixed, or source_mismatch
   manifest/source/generation identity is rejected before routing or fanout as
   `generation_mismatch` (with the failed identity in detail), not a partial
   result. Exact placement partition/group drift is separately
   `route_mismatch`; it is not folded into `generation_mismatch`.
2. Where routing is used, the exact representative route scores every persisted
   representative and selects unique partitions deterministically by
   `(distance, partition_id)`. Representative selection is not a substitute for
   an exhaustive exact union.
3. The union covers every canonical logical partition exactly as named by the
   accepted generation. Each partition is searched with `exact_fp32_scan_v1`;
   overlap may yield repeated stable IDs but may not create a second logical
   document.
4. Scores use the canonical FP32 cosine contract. Global dedupe keeps the best score per stable ID and final top-k ordering is `(score descending, stable ID bytewise ascending)`. Equal-score ties and duplicate arrival order therefore
   cannot change the answer.
5. M1 lifecycle authority is the only generation authority. A relevant
   committed insert, delete, embedding replacement, index-definition change, or
   source-snapshot replacement means mutation invalidates the derived generation
   for future V1 requests. Rebuild and READY activation are required before a
   new generation can serve. A pin accepted before the transition may finish
   against that one immutable generation; no request may mix generations.
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

## Historical evidence ownership

Issue #3999 is historical scoped HNSW evidence, not the current owner of the broad all-partition exactness gate. Its closed result may describe measured partition-local HNSW recall and remaining limitations, but it does not convert HNSW into an exact oracle or waive an exact-union failure. #4013 owns this V1 contract and its classification/admission wording; performance qualification remains outside this issue (including #4015 and the production lanes).

## Excluded behavior

V1 excludes online deltas, live repartitioning, serving replicas, distributed
document fetch, multi-host qualification, and any mutation-freshness promise.
It is snapshot-bound and fail-closed rather than eventually refreshed.

## Verification admission

`TestDocsVectorPartitionV1CorrectnessAndApproximationContract` guards this
frozen boundary. It complements the exact-union, HNSW-route, lifecycle,
source-identity, response-proof, and all-or-error executable tests in the M1
through M6 packages; it is not a substitute for them.
