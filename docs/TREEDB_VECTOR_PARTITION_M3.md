# TreeDB vector partitioning M3

M3 derives optional, bounded ANN memberships and native partition-local HNSW
packs from the immutable M2 disjoint artifact. These are rebuildable search
assets. They contain stable document IDs, validated FP32 vectors, row
references, and HNSW topology; they never contain canonical documents or
change `_id` token/Raft ownership.

## Deterministic overlap

`vectorpartition.BuildOverlap` first validates the M2 artifact. In each
bulk-synchronous round it proposes the non-home partition with the highest
cut-edge reduction, orders proposals by reduction, stable ID, and partition
ID, and rechecks every proposal against the immutable round snapshot before
applying it. The result enforces:

- global budget `floor(ratio * source_count)`;
- the original M2 per-partition capacity;
- at most 16 overlap memberships for one vector; and
- edge cut after overlap no greater than the disjoint edge cut.

Capacity saturation or the per-vector cap leaves budget explicitly unspent.
Ratio zero emits only home memberships and therefore preserves exact M2
partition loads and edge cut. Capacity, budget, and unspent budget are stored
in the integrity-bound M1 `BalancePolicy` as canonical
`m3_bounded_overlap_v1` accounting and are reported again after reopen.

## Native pack construction and reopen

The M2 artifact is keyed by stable ID, while a rebuilt native vector index may
use a different row order. `VectorPartitionSourceOrdinalsV1` obtains the
stable-ID/native-ordinal map from the authoritative immutable source reader.
M3 rejects missing, duplicate, or foreign IDs instead of assuming ingestion
order equals HNSW row order.

`MaterializeVectorPartitionLocalSearchAssetsV1` then:

1. validates count and conservative byte caps before row/pack allocation;
2. loads authoritative source rows for each home and overlap membership;
3. puts the selected highest-level source node at local ordinal zero;
4. remaps every layered HNSW adjacency list from source to partition-local
   ordinals, preserving the layered framing and dropping cross-partition
   neighbors; and
5. writes the existing native `hnsw_search_pack_v1` format through the column
   asset manager.

The returned descriptors are installed in M1's manifest, which binds each
logical partition to its exact asset ref, length, CRC, and SHA-256.
`OpenVectorPartitionLocalSearcherForGenerationV1` rechecks that binding after
database close/reopen, maps or bounded-copies the pack, verifies the source
identity and native HNSW header, and holds an M1 generation reader pin until
`Close`. Missing, corrupt, stale-generation, or malformed assets fail closed.

`SearchWithMetrics` uses the no-document native HNSW route and returns
candidate/edge accounting. Search status exposes route, pack/mapped/heap bytes,
open time, searches, failures, candidates, edges, memberships, and pins.
Lifecycle status independently re-verifies all referenced assets and reports
missing, corrupt, and stale counts. Results are response-owned stable IDs and
FP32 cosine scores only.

## Reproducible evidence

The real M3 harness is selected with `-stage overlap,partition_index`. For every
requested ratio it creates a fresh TreeDB, ingests the frozen fixture into a
real collection, rebuilds the native source HNSW, maps stable IDs to native
ordinals, materializes all partition packs, publishes a building M1
generation, checkpoints, closes, reopens, opens all packs, and searches the
fixture queries. It compares every returned score against an exact FP32 local
oracle and reports local recall separately from native search work.

The canonical 10k matrix is:

```sh
OUT=$(mktemp -d /tmp/treedb_vector_partition_m3_XXXXXX)
GOWORK=off go run ./cmd/treedb_vector_partition_bench \
  -dataset testdata/vector_partition_10k \
  -stage overlap,partition_index \
  -partitions 16 -overlap 0,0.20 -top-k 10 -seed 1 \
  -partition-repetitions 4 -partition-pivots 8 \
  -partition-max-leaf-bucket 128 -partition-degree 16 \
  -format json -out "$OUT"
```

The report records the M2 artifact digest and source/manifest identities,
budget/used/unspent, loads, before/after edge cut, build wall time, peak and
resident RSS availability, temporary/final/pack/mapped/heap bytes, bytes per
source vector, pack/open time, warm latency/QPS/allocations, candidate/edge
counts, exact local recall, and asset-health counts. The overlap `0.20` final
pack bytes must remain at or below `1.35x` the disjoint row. Evidence is not a
product enablement default: enablement remains disabled pending a clustered
1M quality or fixed-probe win. No repository M3 1M corpus is currently
available, so the report says unavailable rather than extrapolating.

The checked-in 10k report is
[`TreeDB/docs/spec/artifacts/vector-partition-m3-evidence-v1.json`](../TreeDB/docs/spec/artifacts/vector-partition-m3-evidence-v1.json).
It was captured from implementation commit
`9d62ae1aad292eb0880a52e603b57ba68fd31a75` over base
`a2d7bd55808136beaea1b6f823668f7b5d28cad8`. The disjoint and requested
`0.20` rows used 4,527,160 and 4,527,938 pack bytes respectively
(`1.000172x`, passing the `1.35x` gate). Both exercised 2,048 native local
searches with 135.75 candidates and 4,263.535 edges per search. The hard
capacity admitted one overlap membership and left 1,999 budget units unspent;
edge cut fell from 5,184 to 5,088, while exact-local recall remained
`0.321631`. This fixture therefore supplies cost, lifecycle, and native-path
evidence, not the clustered/1M enablement win required by the issue.

Routing, RPC, Raft placement, distributed merge, and document fetch remain
later milestones.
