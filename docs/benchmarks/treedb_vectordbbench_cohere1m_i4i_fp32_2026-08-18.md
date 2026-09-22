# TreeDB VDBBench Cohere 1M FP32 on AWS i4i.4xlarge

Run date: 2026-08-18/19 UTC. This is a standalone TreeDB characterization, not
an apples-to-apples leaderboard ranking. The
[machine-readable result](treedb_vectordbbench_cohere1m_i4i_fp32_2026-08-18.json)
contains the full concurrency curve, provenance, memory, cost, route proof, and
artifact hashes.

## Setup

- TreeDB commit: `b4948fffa4772100864c79eccc2d4bbd4ff06285`
- VDBBench commit: `8fd8abccc6ecf769600292f0274673a18e6834b9`
- AWS `i4i.4xlarge` in `us-west-1a`: Intel Xeon Platinum 8375C, 16 vCPU
  (8 physical cores), 123 GiB RAM
- Cohere Medium 1M: 768-dimensional FP32 cosine vectors, top-100, IDs-only
  responses
- HNSW: `M=16`, `efConstruction=300`, `efSearch=192`
- Dataset, TreeDB data, compiled service binary, caches, logs, and results were
  on local instance-store NVMe. A 20 GiB gp3 root volume held only the OS.
- VDBBench and TreeDB shared the instance.

## Search result

- Recall: **0.9580**; NDCG: **0.9643**
- Serial p99 / p95: **4.8 / 4.0 ms**
- Peak QPS: **1,990.20** at concurrency 40
- Exact-route preflight: `exact_hnsw_search_pack_v1`, fallback `none`, no
  documents fetched, all 213 score batches optimized, and mmap/direct vector
  and adjacency views active

| concurrency | QPS | p99 ms | p95 ms | avg ms |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 266.80 | 6.65 | 4.31 | 3.74 |
| 5 | 1,113.49 | 7.92 | 5.98 | 4.48 |
| 10 | 1,647.91 | 18.51 | 8.32 | 6.05 |
| 20 | 1,943.28 | 27.44 | 16.27 | 10.23 |
| 30 | 1,974.07 | 44.85 | 28.85 | 15.08 |
| 40 | 1,990.20 | 77.44 | 49.06 | 19.90 |
| 60 | 1,948.11 | 171.19 | 110.46 | 30.36 |
| 80 | 1,968.48 | 267.94 | 173.00 | 39.86 |

Concurrency 20 is the useful knee: it reaches 97.6% of peak QPS with much less
queueing than concurrency 40. VDBBench's leaderboard latency metric is the
separate serial p99, so the leaderboard-comparable TreeDB latency is 4.8 ms,
not the 77.4 ms concurrent p99 at the peak-QPS point.

## Load, build, memory, and cost

- Load: 1,000,000 vectors in **1,366.41 s (22.77 min)**, about 732 vectors/s,
  with batch size 1,000 and one loader
- Index build: approximately **85.7 min**; load plus build was approximately
  **108.5 min**
- Load peak RSS / process high-water mark: **55.84 / 58.73 GiB**
- Build finalization RSS: at least **25.41 GiB**
- Search peak RSS: **6.35 GiB**; post-search RSS was **5.02 GiB**, about
  4.49 GiB file-backed and 0.53 GiB anonymous
- No swap was used
- Instance runtime: **8,323 s (2.312 h)**; compute cost: **$3.5003** at
  $1.514/hour
- At 730 hours/month the instance is $1,105.22/month. Linear price normalization
  gives **1,800.7 peak QPS per $1,000/month**; this arithmetic is not a claim
  that performance scales linearly with instance cost.

The serving representation fits comfortably in memory after reopen. Local
NVMe mainly protects load/build and cold-start behavior; this run does not
measure a cold-cache search curve. The all-phases 58.73 GiB high-water mark
leaves too little headroom to call a 64 GiB instance safe yet.

The combined VDBBench invocation completed load, but its 30-second client HTTP
timeout expired while the synchronous optimize request continued in TreeDB.
After optimization completed, the service was restarted and the persisted
index was reopened for exact-route proof and a search-only VDBBench invocation.
Consequently, the raw search result has zero load fields; the lifecycle figures
above come from the original load and optimization logs. No index rebuild was
performed between those phases.

## Reading this against the VDBBench leaderboard

The comparison is partly aligned. Both use the Cohere 1M/768D/top-100 case,
VDBBench max QPS, and serial p99. The official VDBBench methodology says its
standard open-source server hosts use the same Xeon 8375C CPU model as this
i4i instance. The Milvus label also has 16 server vCPUs.

It is not a controlled reproduction:

- TreeDB scored FP32 vectors at 0.9580 recall. The marquee Milvus rows are
  scalar-quantized and selected near 0.919 recall.
- The SQ4U Milvus row uses FP16 refinement (`refine_k=1.5`); the SQ8 row does
  **not** refine. It is therefore inaccurate to describe both rows as
  quantized-plus-rerank.
- The official standard benchmark uses a separate 8-core/32 GiB client host in
  the same region. TreeDB's client shared its 16 vCPUs with the server and used
  loopback. That can help latency while hurting saturated QPS, so there is no
  honest scalar correction.
- Storage layout, runtime/container topology, warmup state, and repetition
  policy were not reproduced. This TreeDB result is one query sweep.

External snapshot from the
[Zilliz VDBBench leaderboard](https://zilliz.com/vdbbench-leaderboard?dataset=vectorSearch)
and its [published result artifact](https://github.com/zilliztech/VectorDBBench/blob/main/vectordb_bench/results/Milvus/result_20260403_standard_milvus.json),
retrieved 2026-08-18:

| row | encoding / refinement | recall | QPS | serial p99 ms |
| --- | --- | ---: | ---: | ---: |
| TreeDB i4i.4xlarge | FP32 exact scoring | .9580 | 1,990.20 | 4.8 |
| Milvus 16c64g SQ8 marquee | SQ8, no refinement | .9192 | 5,973.00 | 2.4 |
| Milvus 16c64g SQ4U marquee | SQ4U + FP16 refine 1.5x | .9189 | 9,575.69 | 2.3 |
| Milvus 16c64g SQ4U | SQ4U + FP16 refine 2.5x | .9541 | 7,704.36 | 2.7 |
| Milvus 16c64g SQ4U | SQ4U + FP16 refine 3x | .9620 | 7,023.67 | 3.0 |

The closest published Milvus recall bracket is therefore a better target than
the marquee row. It suggests a **3.5-3.9x raw QPS gap** around TreeDB's recall,
but it still compares different query algorithms and topologies. The marquee
SQ8 and SQ4U gaps are 3.0x and 4.8x respectively at materially lower recall.
Against TreeDB's price-normalized 1,800.7 QPS, the corresponding $1,000/month
target gaps are 3.3x and 5.3x; the closest-recall bracket is 3.9-4.3x. These
price ratios are arithmetic targets, not measured scaling results.

The published Milvus rows report 282-355 seconds for load plus optimize versus
approximately 6,508 seconds for TreeDB, an 18-23x target gap. Load concurrency,
batching, process layout, and implementation differ, so this is not a controlled
ratio; it is still large enough to confirm that TreeDB load/build is the clearest
non-query optimization opportunity.

## Minimum next run

On the next i4i run, keep the graph fixed and sweep `efSearch` to produce a
TreeDB recall/QPS curve spanning approximately 0.92, 0.94, and 0.958 recall.
Warm the reopened index, repeat each search cell three times, and run the
VDBBench client on a separate same-AZ 8-vCPU/32-GiB Intel host. That will isolate
the matched-recall gap without paying for another Milvus deployment. A search
CPU profile at the concurrency-20 knee should be captured in the same run.

The full off-repository export is intentionally not committed because it
contains a compiled binary and raw telemetry. Its SHA-256 is
`8f9eb0554717e4b6ddde55eb4f37e929a2fb29f65924a5d97e51e962584e0857`.
