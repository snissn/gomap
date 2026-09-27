# TreeDB versus Qdrant: 500K × 768 (2026-09-17 HST)

Same-machine, end-to-end Python-client comparison of TreeDB normalized SQ8
traversal plus packed FP32 reranking against Qdrant 1.19.0 scalar-int8 traversal
plus FP32 rescoring. Both exceed a predeclared 95% mean recall@10 target;
achieved recalls are published, not claimed identical.

TreeDB/Qdrant SQ8 median QPS ratio: **1.135× with one client (+13.5%)** and
**1.661× with six concurrent clients (+66.1%)**. These are cross-database
comparisons, not TreeDB-versus-TreeDB speedups.

## Results

Each entry is the median of six warm 10-second windows. The range is the six
observed QPS values, not a confidence interval. Latencies are medians of each
window's percentiles, not percentiles pooled across repetitions.

| Query arm | Concurrent Python clients | Recall@10 | efSearch | Median QPS | QPS range | p50 µs | p95 µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB SQ8 | 1 | 96.60% | 256 | 1398.6 | 1386.2–1409.0 | 700.7 | 788.9 |
| Qdrant SQ8 | 1 | 95.85% | 128 | 1231.8 | 1212.0–1245.7 | 793.1 | 891.3 |
| Qdrant FP32 search | 1 | 95.90% | 128 | 947.9 | 941.1–959.2 | 1041.9 | 1197.0 |
| TreeDB SQ8 | 6 | 96.60% | 256 | 4295.0 | 4214.8–4378.4 | 1227.8 | 2114.5 |
| Qdrant SQ8 | 6 | 95.85% | 128 | 2586.1 | 2503.1–2593.0 | 2178.7 | 3309.9 |
| Qdrant FP32 search | 6 | 95.90% | 128 | 2192.0 | 2104.2–2211.1 | 2522.8 | 3819.2 |

Qdrant FP32 search is a reference on the same SQ8-equipped collection using
`quantization.ignore=True`; it is not a separate FP32-only storage or memory arm.

## Workload and measurement boundary

- Identical 500,000 source FP32 vectors, 768-dimensional cosine, 200 observed
  queries, top-10, original-cosine float64 exhaustive truth.
- Identical returned `id/content/meta` payloads; **no vectors** returned and
  **diagnostics disabled** during timing. Content is short synthetic text, not
  original Wikipedia document bodies.
- TreeDB Python native command-v4 versus Qdrant Python gRPC
  (`prefer_grpc=True`, qdrant-client 1.19.0). Query vectors are preconverted to
  Python lists. Timings include client encoding, transport, server work, and
  client decoding. Worker clients are independent; first-connection cost is
  included for both backends. Validation is outside the timed call boundary.
- AMD Ryzen 7 9700X, about 30 GiB usable RAM, local loopback; both services and
  the client share affinity CPUs 0–5. TreeDB `GOMAXPROCS=6`; BLAS/OpenMP threads=1.
  Shared quiet workstation, **not a dedicated runner or separate client/server
  machine**. Python 3.14.4, NumPy 2.5.2; TreeDB built with Go 1.26.0 and CGO.
- One fresh graph build per backend, no rebuild between timing repetitions.
  TreeDB uses the `command_wal_durable` profile. These are warm read-only query
  measurements, not durability, ingestion, or mutation benchmarks.

## Configurations and recall selection

| Setting | TreeDB SQ8 | Qdrant SQ8 |
| --- | --- | --- |
| HNSW M | 16 | 16 |
| Construction ef | 32 | 100 |
| Selected search ef | 256 | 128 |
| Reranking | R=E=256, packed FP32 batch | FP32 rescore, oversampling=2.0 |
| Vector representation | `cosine_normalized_f32_v1`, `minima_sq8` scalar-u8/v1 | Cosine FP32 in RAM, scalar-int8 in RAM |
| Scalar quantization | Existing legacy calibration | Quantile unset (whole range), `always_ram=True` |
| Payload | Typed collection `id/content/meta` | On disk, `with_payload=True` |

Qdrant uses three fully indexed segments, `full_scan_threshold=10000`,
`indexing_threshold=10000`, `max_optimization_threads=1`, and keyword payload
indexes on `meta.user_id` and `meta.fpath`; TreeDB declares those same scalar
fields. Qdrant's FP32 query reference uses ef=128. This is a production-
configuration comparison, **not equal construction work**.

Each arm independently selects the first passing coordinate from
ef=[32,64,128,256,512,1024,2048] on all 200 observed queries. TreeDB R=E;
Qdrant SQ8 explicitly uses `rescore=True`, oversampling=2.0. TreeDB ef=128
achieved 94.85%, so the grid selected 256; Qdrant selected 128. This is a
fixed-grid operating point, **not an exhaustive best-configuration search**.

Both services were fully query-ready before timing; Qdrant had 500,000 points
and 500,000 indexed vectors, green status, and a healthy optimizer. A TreeDB
route probe verified typed HNSW SQ8 candidate generation, packed FP32 reranking,
and zero output-vector reads. All 200 queries were warmed per arm before
measurement and again before each window. Six permutations of the three arms
balance measurement position and pairwise order at each concurrency. Only the
measured backend receives benchmark queries.

## Validation and source identity

All **757,271 timed calls** completed without query errors. Returned IDs,
payloads, uniqueness, descending score order, and original-cosine score
agreement were checked after timing (absolute score tolerance 1e-5). Every
returned embedding/vector was absent. A separate post-run check reconciled all
36 raw timing samples with completed-call counts, QPS arithmetic, and p95.

No system swap-in/out occurred during any timed window. Both warmed servers
had zero VmSwap. Both owned services exited cleanly with status 0; no forced
kill or resource-guard failure occurred.

Main at collection: `71a14db9405bd8d3ab3e443209c3cb92bc61c97a`.
Measured TreeDB binary: clean actual product commit
`6b0fa446ff92b430c1d00ee3d2ee915f0bcb15bc`. Serving code and Python client are
unchanged between these commits; the three intervening changed files are Q4
documentation/offline analyzer/tests, not serving code.

| Input/artifact | SHA-256 |
| --- | --- |
| TreeDB service binary | `8ac7d0b5e920f75c17a59acc42d31edafc47593049da0477cfdf62f26b942b3e` |
| `documents.f32` | `d4f332061a17d7728ff2c44f0859130e1b43f43360143dc3d754733dcf4e04d8` |
| `queries.f32` | `d52ee3198dfc5641a6f2668b22b5994b489e86807616b3d7d77ac87de03cf1ed` |
| Exhaustive truth | `2dedf807b043fe83aa9c7a7a9f1b568f31c9afd77c5f3a15d0b9adb177d755df` |
| Frozen methodology | `b3970fb5ee8cd7b61ecdcd5ebffda01f4c3b11ca85ee24a8d6753677c3e492b3` |
| Complete result | `3c79408b56bc389c089ad16e19d4d3bd102fad363c99ff31a82b190786dc0a64` |

Dataset origin is the [public Cohere Wikipedia mirror at pinned revision de34a7af](https://huggingface.co/datasets/YoKONCy/Cohere-1M-wikipedia-768d/tree/de34a7af7f436d7aceb4fecdda01490e552efdde),
source train rows 0–499999 and query rows 0–199. Retained staging hashes match
these FP32 inputs. The mirror's origin claim is not independent equivalence to
the missing historical export. Raw source vectors are not unit-normalized
despite the mirror README's claim; both engines apply cosine normalization.
No projections, deduplication, or row reordering; zero exact train/query vector
overlap. This is **not an official/full 1M reproduction or unseen-query
evaluation**.

Measurement started 2026-09-18T04:00:59Z and finished 2026-09-18T04:09:28Z
(18:09 HST September 17). The fresh TreeDB build began at 03:55 UTC. Two
scratch-probe preparation attempts used the helper's wrong default index name
and produced no timed results. The same owned, already-built database was
reopened and admitted after correcting the probe; no extra graph build,
product patch, or discarded timing sample was needed.

## Evidence retention and limitations

The local archive is `gomap-q5-evidence/treedb-qdrant-current-dqJiyC/` and contains
the pinned scratch driver, methodology, calibration IDs/recalls, complete
result, per-window measurements, raw monotonic start/end NPZ samples, serving
admission/readiness, resource snapshots, clean-process exits, and both
preparation attempts. Dataset, scratch-script, and methodology hashes were
rechecked after completion. **Large raw artifacts and the one-off driver are
retained locally, not included in this documentation update**; this report
does not claim independent reproduction by another operator.

The driver reuses the landed
[TreeDB controller/client helpers](../../benchmarks/vector_db_compare/minima_treedb_runner.py),
[normalized-v4 query helper](../../benchmarks/vector_db_compare/minima_cohere_v4_production_gate.py),
[Qdrant collection/readiness helpers](../../benchmarks/vector_db_compare/minima_cohere_qdrant_rss_diagnostic.py),
and [timed-window helper](../../benchmarks/vector_db_compare/minima_cohere_matched_performance.py).
It does not execute the legacy matched-performance campaign or introduce a new
committed benchmark framework.

Six-client results are scoped application throughput, **not maximum engine
throughput**. Repeats measure timing variability of these two builds, not
variability across independently built graphs. There were no filters,
concurrent writes, cold-cache queries, or original document bodies. Resource
sizes include each backend's WAL and are descriptive only, not a storage-
efficiency winner claim.

The [August 21 1M/top-100/IDs-only cloud report](treedb_vectordbbench_cohere1m_c6i_dense_curve_2026-08-21.md)
is a different workload and dataset export. These 500K/top-10/payload local
points must not be inserted into its curves or pooled with its results.
