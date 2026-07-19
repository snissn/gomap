# Vector-partitioned ANN over Raft: M0 contract

Status: **M0 executable contract** for #3908/#3909. This document deliberately
defines boundaries and evidence before M1--M8 implement production assets.

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

V1 is snapshot-bound: insert, delete, embedding replacement, index-definition
change, source snapshot replacement, or any committed mutation affecting a
referenced vector invalidates the generation before the mutation becomes
searchable. Until full rebuild and cutover, distributed search MUST fail closed;
it MUST NOT mix generations or return partial top-k. A Raft read-index only
proves the group applied point; it does not make an older snapshot-built
generation fresh.

V1 distributed responses contain response-owned stable IDs and scores only.
Missing owner, stale generation, malformed/capped asset, unsupported
consistency, or shard failure is an explicit error with no incomplete result.

## M0 oracle and evidence boundary

`cmd/treedb_vector_partition_bench` is a sequential, in-memory **simulation**.
It emits `result_kind=simulation_only` and `production_evidence=false`; its
Markdown artifacts repeat that it is not production Raft evidence. It records
the exact global top-k, partition oracle, representative/local-HNSW attribution
placeholders, and end-to-end simulation separately. With every partition
probed, the partition oracle MUST equal global exact top-k under distance then
stable-ID ordering.

The checked-in fixture manifest at `testdata/vector_partition_10k/` is generated
deterministically and has a stable checksum over canonical generated vector,
query, and exact-truth bytes,
explicit duplicate/tie case, clusters, and boundary shape. Tests resolve the
single root fixture from `cmd/treedb_vector_partition_bench`. Counts,
dimensions, partition count, and result metrics are validated before allocating
large work buffers. The JSON schema version is strict; unknown versions and
non-finite metrics are rejected by the executable contract.

The schema reserves every descendant evidence family even when M0 emits
`measurement_status=simulation_not_measured` and explicit finite zero values:
build wall/CPU/RSS/temp/final bytes; balance/cut/overlap; representatives and
router latency; fanout/RPC/bytes/shard/merge/failure counters; and QPS,
percentiles, recall@1/10/100, allocations, resident/mapped bytes.

## Parent invariant matrix

| Parent invariant | M0 executable evidence | Owner after M0 |
| --- | --- | --- |
| `_id` ownership differs from ANN membership | `TestDocsVectorPartitionRaftM0Contract` | M1/M5 |
| overlap is not Raft replication | docs contract test | M3/M7 |
| four generation identities, ready/cutover/invalidation | docs contract test | M1/M7 |
| snapshot mutation fails closed | docs contract test | M7 |
| IDs/scores only; no partial top-k | docs contract test | M5/M6 |
| exact all-partition parity and stable ties | `TestTruthOracleTieOrderingAndAllPartitionParity` | M2--M6 |
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

`cmd/treedb_vector_dataset_export` also supports a declared 1M-vector local
corpus (`-docs 1000000`) within its pre-allocation byte caps. Its manifest pins
vector/query checksums, dimensions, metric, query set, and an exhaustive
distance-then-ID top-k truth stream. Exact truth is deliberately comparison
capped; a feasible declared 1M export uses one bounded query shard:
`GOWORK=off go run ./cmd/treedb_vector_dataset_export -out "$OUT" -docs 1000000 -queries 1 -dims 64 -top-k 10`.
It is a corpus export contract, not a claim that M0 has run a 1M-vector
exhaustive top-k benchmark.

## Verification

```sh
GOWORK=off go test ./cmd/treedb_vector_partition_bench ./cmd/treedb_vector_dataset_export ./TreeDB/docs -count=1
GOWORK=off go test ./cmd/treedb_vector_partition_bench -run 'Test.*(Fixture|Truth|Oracle|Manifest|Deterministic|Malformed|Cap).*' -count=1
```
