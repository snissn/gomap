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

- global requested extra-membership target `floor(ratio * source_count)`;
- a declared total-membership capacity for the overlap variant (the immutable
  M2 home assignment and its original epsilon cap are not changed);
- at most 16 overlap memberships for one vector; and
- edge cut after overlap no greater than the disjoint edge cut.

The production M3 runner derives the narrow target-aware capacity
`max(m2_cap, ceil((source_count + requested)/partitions))` and requires exact
realization. If affinity or the per-vector cap still prevents the target, the
build fails closed with requested, realized, rejected, and capacity evidence;
it never publishes a silently under-filled overlap variant. Exact M3 builds
first prioritize cut-reducing memberships, then deterministically fill any
remaining legal non-home slots in canonical ID/partition order. Those fills
cannot increase edge cut and make the declared global target materially mean
the requested membership count rather than only the graph's affinity gain.
Ratio zero emits
only home memberships and therefore preserves exact M2 partition loads and
edge cut. The integrity-bound M1 `BalancePolicy`, retained M3 descriptor, and
machine report carry requested, realized, rejected, and declared-capacity
accounting; the target and capacity are also part of the variant build
identity.

## Native pack construction and reopen

The M2 artifact is keyed by stable ID, while a rebuilt native vector index may
use a different row order. `VectorPartitionSourceOrdinalsV1` obtains the
stable-ID/native-ordinal map from the authoritative immutable source reader.
M3 rejects missing, duplicate, or foreign IDs instead of assuming ingestion
order equals HNSW row order.

`MaterializeVectorPartitionLocalSearchAssetsV1` then:

1. validates shape caps, scans authoritative stable IDs and topology, and
   computes the exact encoded size including the header, directory, alignment,
   ID bytes, every adjacency section, and row-reference sections before
   allocating vectors or full topology;
2. derives a canonical SHA-256 over the partition generation, partition ID,
   and ordered authoritative stable-ID/home-or-overlap membership sequence;
3. loads authoritative source rows for each home and overlap membership;
4. puts the selected highest-level source node at local ordinal zero;
5. remaps every layered HNSW adjacency list from source to partition-local
   ordinals, preserving the layered framing and dropping cross-partition
   neighbors; and
6. writes the native `hnsw_search_pack_v1` format through the column asset
   manager only after the actual encoded length exactly matches the preflight
   and remains within the 256 MiB cap.

The returned descriptors are installed in M1's manifest, which binds each
logical partition to its exact membership digest, asset ref, length, CRC, and
SHA-256. Partition packs use wire version 2 of the existing pack format to
persist the same membership digest in the header; ordinary non-partition packs
remain wire version 1.

The column manifest keeps vector-index control state inline. Small records retain
the existing version-2 encoding; records that would exceed the reserved inline
leaf budget use the bounded version-3 Snappy envelope and decode back to the same
version-2 logical state. Decode size and final inline size are both capped, so a
large HNSW layer/asset set cannot overflow a 4 KiB manifest leaf or trigger an
unbounded allocation during reopen or command-WAL replay.

`OpenVectorPartitionLocalSearcherForGenerationV1` rechecks that binding after
database close/reopen, maps or bounded-copies the pack, verifies the source
identity, recomputed membership set, descriptor, and native HNSW header, and
holds an M1 generation reader pin until `Close`. Missing, corrupt,
stale-generation, cross-membership, or malformed assets fail closed.

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
resident RSS availability, the closed/checkpointed source physical baseline,
sampled peak M3-derived physical delta, final M3-derived on-disk bytes, pack
payload/mapped/heap bytes, physical bytes per source vector, pack/open time,
warm latency/QPS/allocations, candidate/edge counts, exact local recall, and
asset-health counts. The derived physical measurements include asset framing
and alignment plus the M1 lifecycle/manifest state. The overlap `0.20` final
derived physical bytes must remain at or below `1.35x` the disjoint row; pack
payload is retained as a separate diagnostic. Evidence is not a product
enablement default: enablement remains disabled pending a clustered 1M quality
or fixed-probe win.

## Final runtime-code 1M retained evidence

The deterministic 1,000,000-vector fixture at
`/mnt/fast4tb/tmp/treedb_m6_1m_safe_TEzTe1/fixture` (32 queries, dimension 16,
checksum `71239d1335ddd724835d415f57acae7f8bb36a6af52642d1e710392a883b2d6f`)
was built at measured runtime-code head
`acdaaadff7e4c1341ad5fb792360620deac9faa0` with
`/mnt/fast4tb/tmp/issue4001_finalhead_bench` using:

```sh
/mnt/fast4tb/tmp/issue4001_finalhead_bench \
  -dataset /mnt/fast4tb/tmp/treedb_m6_1m_safe_TEzTe1/fixture \
  -out "$OUT" -m3-persist-db "$DB" -format json \
  -stage overlap,partition_index -partitions 16 -top-k 10 -overlap "$OVERLAP" \
  -partition-assignment "$ASSIGNMENT" -partition-repetitions 1 \
  -partition-pivots 8 -partition-max-leaf-bucket 32 -partition-degree 4 \
  -partition-hnsw-m 16
```

The graph-disjoint, graph-overlap-`0.20`, and stable-ID-hash-disjoint reports
are respectively
`/mnt/fast4tb/tmp/issue4001_fh_graph_disjoint_out_guHDMc/vector_partition_m3_3c7a5665803b_7a2ec9d690c1_acdaaadff7e4.json`
(SHA-256 `56ec4a78c444a4edeb06f3a795d1441fa2b1ed15ac257abbdde678162f1a75d3`),
`/mnt/fast4tb/tmp/issue4001_fh_overlap_out_TssY5Z/vector_partition_m3_3c7a5665803b_7a2ec9d690c1_acdaaadff7e4.json`
(`4838c692a01ef5ef238c6dcd8149660049b99c47775403a91af1c0f2a37492ea`),
and
`/mnt/fast4tb/tmp/issue4001_fh_stable_out_zcCOa6/vector_partition_m3_7a8ec9915de7_7a2ec9d690c1_acdaaadff7e4.json`
(`fd1c43716c60290744817102f549a6681eed5ca27484c3558e4d97d9d50f151c`).
The overlap descriptor records `requested=200000`, `realized=200000`,
`rejected=0`, capacity `75000`, and sixteen loads of `75000`; its final
derived bytes are `353839232` versus `294891612` for graph-disjoint
(`1.1998958858x`, below the M3 `1.35x` bound).

The M8 matrix used exactly those immutable directories:

```sh
/usr/bin/time -v -o "$OUT/time-v.log" /mnt/fast4tb/tmp/issue4001_finalhead_bench \
  -dataset /mnt/fast4tb/tmp/treedb_m6_1m_safe_TEzTe1/fixture -out "$OUT" \
  -profiles "$PROFILES" -format json -mode production_multi_group \
  -m8-variant-dbs /mnt/fast4tb/tmp/issue4001_fh_graph_disjoint_db_1wK4gE,/mnt/fast4tb/tmp/issue4001_fh_overlap_db_JSViB8,/mnt/fast4tb/tmp/issue4001_fh_stable_db_dBMCDV \
  -partitions 16 -top-k 10 -overlap 0.20 -raft-groups 4 -raft-nodes-per-group 3 \
  -probes 16 -ef-search 4096 -concurrency 1 -warmup 0 -router-candidates 1024 \
  -m8-max-rss-bytes 4294967296 -m8-max-persistent-asset-bytes 536870912
```

The successful retained matrix is
`/mnt/fast4tb/tmp/issue4001_fh_matrix_retry_out_49yzvN/vector_partition_m8_matrix_acdaaadff7e4_3c30f06adbe8.json`
(SHA-256 `704cf98926f2778eb0aeee4be6978d56ba815e665f28781473113b682cf4e01c`).
Its `stderr.log` is empty and `time-v.log` records exit `0` after `7:49.42`,
with maximum RSS `2195688 KiB`. Required variants, failure honesty, pack
reachability, recall, balance, overlap storage, and resource bounds pass. The
matrix-level overlap materialization is `0.2` and storage ratio is
`1.1998982982`. It deliberately remains
`experimental_gate_failures` / `enablement_off_follow_up_required`: exhaustive
correctness, fixed-probe reduction, matched-recall QPS/tail, and coupled graph
acceptance do not pass, and existing behavior is pending latest-head required
suites. These are evidence dispositions, not waived gates.

An earlier final-head attempt remains preserved at
`/mnt/fast4tb/tmp/issue4001_fh_matrix_out_k2ebQ0`: it exited `1` after its
graph-disjoint child emitted a report and before graph-overlap began. Its outer
stderr was not redirected, so no exact text was retained. It is a diagnostic
artifact only; the retry above retains explicit stdout, stderr, time, child
reports, and profiles.

Commits after `acdaaadff7e4c1341ad5fb792360620deac9faa0` only align test-fixture
accounting with the new descriptor schema and retain this evidence in the
documentation; they do not change the measured production code. Required
tests, latest-head CI, and exact-head review apply to the final PR head, while
the performance artifacts above remain explicitly bound to the runtime-code
head.

The checked-in 10k report is
[`TreeDB/docs/spec/artifacts/vector-partition-m3-evidence-v1.json`](../TreeDB/docs/spec/artifacts/vector-partition-m3-evidence-v1.json).
It was captured from implementation commit
`c33d077f6b2eff677b8899425401835d0292758e` over base
`a2d7bd55808136beaea1b6f823668f7b5d28cad8`. The disjoint and requested
`0.20` rows consumed 4,653,462 and 4,654,264 final derived physical bytes
respectively (`1.000172x`, passing the `1.35x` gate); their serialized pack
payloads were 4,527,672 and 4,528,450 bytes. Both exercised 2,048 native local
searches with 135.75 candidates and 4,263.535 edges per search. The hard
capacity admitted one overlap membership and left 1,999 budget units unspent;
edge cut fell from 5,184 to 5,088, while exact-local recall remained
`0.321631`. This fixture supplies cost, lifecycle, and native-path evidence,
not the clustered/1M enablement win required by the issue.

Routing, RPC, Raft placement, distributed merge, and document fetch remain
later milestones.
