# TreeDB README Benchmark Refresh (2026-11-25)

This report records the clean post-Sprint 15 November 25, 2026 non-AWS/EC2
benchmark refresh run on `192.168.0.185` (`mikers-B560-DS3H-AC-Y1`) at `main`
commit `afcb9a21d2e6dea1e0258816e3af938fee5fe38f` (immediately following Sprint
15 regression fixes `#5830` / S15-2, `#5824` / S15-3, and `#5832` / S15-1).

## Environment

- **Measured Commit**: `afcb9a21d2e6dea1e0258816e3af938fee5fe38f` (plus the
  3-line `-tags usearch_bench` compile fix in
  `TreeDB/collections/vector_usearch_bench_test.go` for the USearch serving
  comparison; runtime serving code unchanged)
- **Host**: `mikers-B560-DS3H-AC-Y1`, Linux `6.8.0-138-generic` `x86_64`
- **CPU**: 11th Gen Intel Core i5-11400F @ 2.60 GHz (6 cores / 12 threads)
- **RAM**: 31 GiB (zero swap activity)
- **Go**: `go1.26.3 linux/amd64` (`GOWORK=off`)
- **External comparators**:
  - Docker `mongo:8` (`mongod` v8.2.7; `--network host` loopback parity in YCSB)
  - SQLite via Go CGO (`sqlite_bench`)
  - USearch `v2.25.2` (`linux/amd64`)
  - `go-ycsb` (`mongodb` binding)
  - Docker `qdrant/qdrant:v1.19.0` (`qdrant-client 1.20.0`, `grpcio 1.85.4`, `prefer_grpc=True`)
  - Docker `pgvector/pgvector:pg16@sha256:84a35586...` and `milvusdb/milvus:v2.6.20@sha256:e514fced...`
- **Execution Discipline**: All 11 suites were executed sequentially on a quiet
  host with disposable DB directories and containers reclaimed after
  durability/reopen checks.

## Scope

- **Included (all 11 non-AWS/EC2 README suites unified at `afcb9a21d`)**:
  1. YCSB Server Workload (Relaxed + Durable-B vs MongoDB 8.2.7, `--network host`)
  2. Indexed Collection Insert Workload (`100k` docs, 2 secondary indexes, batch `16000`)
  3. Collection Read And Lookup Workload (`100k` docs, 2 secondary indexes)
  4. Collection Concurrency Workload (`GOMAXPROCS=12`, template-v1)
  5. Typed Metadata Mutation Workload (`GOMAXPROCS=2`, `taskset -c 4,5`, `rows128/dims8`)
  6. Vector Search Serving Workload (Tier S `10k × 64d`, TreeDB vs USearch 2.25.2)
  7. TreeDB Mongo Gateway Client-Shape Workload (`200k` gateway BSON docs, standalone COW)
  8. Mongo API Full-Sweep Workload (`100k` BSON docs, standalone COW vs MongoDB 8.2.7) + Writer Scaling
  9. Indexed Text/Vector/Hybrid Insert And Search Workload (`256` docs, `dims=16`, `M=8`)
  10. Vector Search Matched-Recall Comparison (`100k` & `250k` 128d, `bench_vector_db_compare.sh`)
  11. TreeDB versus Qdrant: same-machine 500K vectors (Cohere 768d, 6-CPU shared budget)
- **Explicitly excluded (AWS/EC2)**: `VectorDBBench Cohere 1M` (`c6i.8xlarge`)
  remains unchanged in `README.md`.
- **Historical external snapshot preserved**: `application.db` Offline Density
  Workload remains on the June 2 Celestia mainnet snapshot because the external
  mainnet `application.db` source directory is no longer stored on `185`.

---

## 1. YCSB Server Workload

Fresh-fixture alternating AB/BA/AB comparison (`100,000` records, `10,000`
operations, `16` threads, `uniform` key distribution, `95%` READ / `5%` UPDATE,
`fieldcount=10`, `fieldlength=100`, `readallfields=true`, `writeallfields=false`,
`0` operation errors across all 12 cells). GoMap runs `treedb-mongo-gateway`
(`afcb9a21d`, standalone COW, BSON); MongoDB runs Docker `mongo:8` (`8.2.7`)
with `--network host --bind_ip 127.0.0.1` for native loopback parity with GoMap.

| ACK class | MongoDB 8.2.7 run median ops/s | GoMap Mongo gateway run median ops/s | Paired ratio median | MongoDB load median ops/s | GoMap load median ops/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Relaxed: both `w:1`, `j` absent (`command_wal_relaxed`) | 27,185.5 | 73,195.4 | 2.694336 (+169.43%) | 45,805.7 | 29,402.8 |
| Durable-B: MongoDB `w:1,j:true` vs GoMap `w:1` (`command_wal_durable`) | 20,674.8 | 32,387.5 | 1.556642 (+55.66%) | 49,139.0 | 2,343.0 |

Per-pair breakdown:

| Class / pair | MongoDB load ops/s | GoMap load ops/s | MongoDB run ops/s | GoMap run ops/s | GoMap/Mongo run | GoMap READ / UPDATE p99 (us) | MongoDB READ / UPDATE p99 (us) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| relaxed / 1 | 48,414.4 | 29,271.9 | 27,166.4 | 73,195.4 | 2.694336 | 676 / 773 | 1,208 / 905 |
| relaxed / 2 | 45,805.7 | 30,420.9 | 27,185.5 | 52,062.4 | 1.915080 | 1,570 / 2,079 | 1,188 / 770 |
| relaxed / 3 | 44,867.5 | 29,402.8 | 27,254.1 | 75,161.0 | 2.757787 | 611 / 758 | 1,325 / 1,090 |
| durable-B / 1 | 48,138.1 | 2,343.0 | 19,434.4 | 30,147.9 | 1.551265 | 339 / 7,831 | 740 / 12,735 |
| durable-B / 2 | 49,139.0 | 2,309.9 | 20,674.8 | 32,400.0 | 1.567125 | 295 / 7,371 | 950 / 13,479 |
| durable-B / 3 | 49,213.7 | 2,347.5 | 20,806.0 | 32,387.5 | 1.556642 | 312 / 6,943 | 674 / 14,015 |

Caveats: Durable-B compares MongoDB explicit `w:1,j:true` against GoMap's
ordinary `command_wal_durable` ACK with `w:1`, `j` absent. Load uses `w:1`,
`j` absent on both engines, so GoMap's durable load waits on stable command-WAL
dependencies while standalone MongoDB batches journal flushes on its background
interval; load rates are therefore asymmetric in Durable-B.

---

## 2. Indexed Collection Insert Workload

Run via `scripts/bench_collections_canonical.sh` (`100,000` documents, `2`
secondary indexes, batch size `16000`, `command_wal_relaxed`, data and index
outer leaves in value log) at post-S15 `afcb9a21d` (includes `#5832` relaxed
publish fsync/rotation fix and `#5830` `treemap compact` floor & WAL cleanup
fix).

| engine / format | layout | docs/sec | compacted B/doc | compaction method |
| --- | --- | ---: | ---: | --- |
| TreeDB template-v1 | data and index outer leaves in value log | 483,092 | 26.7 | `exhaustive_compact` (`68.9` offline compact; `96.7` `full_leafgen_pack_gc`) |
| TreeDB BSON | data and index outer leaves in value log | 520,562 | 98.6 | `full_leafgen_pack_gc` |
| TreeDB JSON | data and index outer leaves in value log | 417,188 | 118.1 | `full_leafgen_pack_gc` |
| SQLite native columns | WAL normal | 374,532 | 156.7 | `VACUUM` |
| SQLite JSON | WAL normal | 323,311 | 231.7 | `VACUUM` |

All three TreeDB formats exceed SQLite native columns on both indexed insert
throughput (`1.11×–1.39×`) and compacted storage density (`5.9×` smaller than
SQLite native and `8.7×` smaller than SQLite JSON for `template-v1`
`exhaustive_compact`).

---

## 3. Collection Read And Lookup Workload

Run via `scripts/bench_collections_harness.sh --include-sqlite --engine command_wal_relaxed --batch-size 16000 --count 1 --benchtime 1s`
with `TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=true` and
`TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=true` (`2` secondary indexes,
post-S15 `afcb9a21d` including `#5824` one-shot `GetInto` / lock-free read-path
fixes).

| operation | TreeDB template-v1 ops/sec | TreeDB JSON ops/sec | SQLite native columns ops/sec | SQLite JSON ops/sec |
| --- | ---: | ---: | ---: | ---: |
| Primary read | 1,126,634 (887.6 ns/op) | 1,065,417 (938.6 ns/op) | 231,803 (4,314 ns/op) | 304,878 (3,280 ns/op) |
| Unique secondary lookup | 296,560 (3,372 ns/op) | 297,530 (3,361 ns/op) | 312,305 (3,202 ns/op) | 286,779 (3,487 ns/op) |
| Nonunique secondary lookup | 118,906 (8,410 ns/op) | 115,687 (8,644 ns/op) | 41,681 (23,992 ns/op) | 27,086 (36,919 ns/op) |

---

## 4. Collection Concurrency Workload

Run via `COUNT=1 BENCHTIME=1s CPU_LIST=1,2,4,8,12 ./scripts/bench_collections_concurrency.sh`
(TreeDB `template-v1`, `command_wal_relaxed`, batch size `16000`, `2` secondary
indexes, post-S15 `afcb9a21d`).

| benchmark (`GOMAXPROCS=12`) | ns/op | derived reader ops/sec | writer docs/sec | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| `ReadPrimaryParallel` | 202.1 | 4,948,046 | — | 80 | 1 |
| `ReadPrimaryIntoParallel` | 214.9 | 4,653,327 | — | 0 | 0 |
| `SecondaryLookupUniqueParallel` | 672.6 | 1,486,768 | — | 2,200 | 24 |
| `SecondaryLookupNonUniqueParallel` | 1,812 | 551,876 | — | 5,714 | 85 |
| `MixedReadWritePrimary` (1 writer) | 227.5 | 4,395,604 | 13,859 | 101 | 1 |
| `MixedReadWriteSecondaryUnique` (1 writer) | 743.9 | 1,344,267 | 23,017 | 2,317 | 25 |

---

## 5. Typed Metadata Mutation Workload

Exact command:

```sh
GOMAXPROCS=2 taskset -c 4,5 go test \
  ./TreeDB/collections ./TreeDB/documentservice \
  -run '^$' \
  -bench '^Benchmark(TypedMinima|ServiceTyped)MetadataMutation$/^rows128$/^dims8$' \
  -benchmem -benchtime=10x -count=5
```

Medians of 5 samples on `i5-11400F` (`rows=128`, `dims=8`, `0` vector assets rebuilt):

| public boundary | publication profile | median ns/op | batches/sec | rows/sec | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Collection API | durable, fsync-on-publish | 17,008,470 | 58.79 | 7,526 | 1,537,211 | 10,965 |
| Document service | test backend, no fsync-on-publish | 14,627,988 | 68.36 | 8,750 | 1,362,241 | 9,640 |

---

## 6. Vector Search Serving Workload

Run via `COUNT=3 BENCHTIME=1000x CPU_LIST=1,8 ./scripts/bench_vector_search_compare.sh`
(Tier S exact-FP32 no-document, `10,000` documents, `64` dimensions, `M=16`,
`efConstruction=128`, `efSearch=128`, `topK=10`, query stream length `16`,
USearch `2.25.2` `linux/amd64`).

| row | cpu | median ns/op | derived ops/sec | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB `Collection.SearchVectorIndexWithBuffer` | 1 | 67,034 | 14,918 | 0 | 0 |
| TreeDB `OpenVectorIndexSearcher` + `SearchWithBuffer` | 1 | 64,254 | 15,563 | 0 | 0 |
| TreeDB `OpenVectorIndexSearcher` + `SearchWithBuffer` parallel | 8 | 10,278 | 97,295 | 0 | 0 |
| USearch `Search` | 1 | 76,684 | 13,041 | 136 | 3 |
| USearch `SearchParallel` | 8 | 11,888 | 84,118 | 136 | 3 |

---

## 7. TreeDB Mongo Gateway Client-Shape Workload

Run via `TREEDB_OPEN_MODE=standalone ./scripts/mongo_gateway_fast_client_matrix.sh`
(`200,000` gateway-shaped BSON documents, batch size `1000`, `16` insert
producers, two secondary indexes `email` and `city`, TreeDB `command_wal_relaxed`
standalone COW, settled read state, `16` concurrent readers).

| TreeDB access path | load docs/sec | concurrent `_id` reads/sec | concurrent indexed `email` reads/sec | `_id` p95 us | `email` p95 us |
| --- | ---: | ---: | ---: | ---: | ---: |
| Direct collection API | 239,715 | 904,133 | 521,593 | 35 | 95 |
| Mongo raw wire over TCP | 236,864 | 197,922 | 99,365 | 127 | 144 |
| TreeDB native wire over TCP | 246,963 | 70,504 | 96,129 | 222 | 252 |
| Mongo driver raw command | 239,742 | 72,147 | 108,656 | 226 | 224 |
| Mongo driver command | 243,531 | 69,981 | 106,969 | 223 | 227 |
| Mongo driver CRUD | 224,320 | 71,861 | 106,706 | 225 | 226 |

Note: `driver-unack` (`w:0`) is intentionally excluded for TreeDB because TreeDB
rejects unacknowledged `w:0` writes before mutation. On the same run, TreeDB
post-load physical `du` measured `33.27 MiB` (`174.4 B/doc`) versus Docker
MongoDB 8.2.7 `driver` physical `du` `219.06 MiB` (`1,148.5 B/doc`, `0.15×`),
with MongoDB `driver` measuring `251,641` load docs/sec, `34,916` `_id`
reads/sec (`793 us` p95), and `29,686` `email` reads/sec (`956 us` p95).

---

## 8. Mongo API Full-Sweep Workload & Writer Scaling

Run via `scripts/treedb_benchmark_run_report.sh --tier pr --mongo-docs 100000 --mongo-treedb-open-mode standalone --mongo-treedb-profile command_wal_relaxed --skip-raw --skip-collections --skip-load-modes --skip-load-scaling`
against Docker `mongo:8` (`8.2.7`). Physical MiB reflects the post-sweep
`checkpoint` snapshot `du` (after `480,000+` concurrent `updateOne` writes,
before offline `exhaustive` compaction) versus MongoDB physical `du`.

| secondary indexes | TreeDB load docs/sec | MongoDB load docs/sec | TreeDB `_id` r16 ops/sec | MongoDB `_id` r16 ops/sec | TreeDB `age` range r16 ops/sec | MongoDB `age` range r16 ops/sec | TreeDB physical MiB | MongoDB physical MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 160,729 | 451,420 | 118,615 | 37,065 | 98,798 | 26,089 | 195.66 | 213.38 |
| 1 | 127,376 | 251,970 | 118,486 | 36,392 | 95,073 | 25,668 | 199.33 | 217.01 |
| 2 | 109,390 | 204,153 | 118,021 | 35,836 | 93,502 | 25,897 | 198.21 | 217.58 |

Concurrent `_id` `updateOne` writer scaling (`concurrent_id_update_set_w*` from
the full sweep, ops/sec):

| writers | idx=0 TreeDB / MongoDB | idx=1 TreeDB / MongoDB | idx=2 TreeDB / MongoDB |
| ---: | ---: | ---: | ---: |
| 1 | 8,807 / 7,791 (1.13x) | 8,777 / 7,788 (1.13x) | 8,895 / 7,841 (1.13x) |
| 2 | 15,396 / 14,506 (1.06x) | 14,575 / 14,595 (1.00x) | 15,238 / 14,675 (1.04x) |
| 4 | 19,809 / 23,845 (0.83x) | 19,214 / 23,831 (0.81x) | 19,516 / 23,901 (0.82x) |
| 8 | 31,147 / 32,605 (0.96x) | 31,275 / 31,553 (0.99x) | 31,049 / 31,578 (0.98x) |
| 16 | 42,764 / 33,310 (1.28x) | 42,230 / 34,364 (1.23x) | 40,948 / 34,372 (1.19x) |
| 32 | 55,463 / 34,466 (1.61x) | 55,408 / 35,671 (1.55x) | 55,905 / 34,743 (1.61x) |

---

## 9. Indexed Text/Vector/Hybrid Insert And Search Workload

Exact command:

```sh
GOWORK=off \
TREEDB_INDEX_BENCH_DOCS=256 \
TREEDB_INDEX_BENCH_DIMS=16 \
TREEDB_INDEX_BENCH_M=8 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkIndexInsertSearch2564$' \
  -v -benchmem -benchtime=5x -count=3
```

Medians of 3 samples (`-benchtime=5x`):

| row | timed boundary | ns/op median | ops/sec | B/op | allocs/op | key counters |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Indexed insert/readiness | 256-doc batch insert + flush + vector rebuild | 185,495,996 | 5.39 ops/sec / 1,380.1 docs/sec | 11,880,849 | 39,659 | 45,469 insert ns/doc; 479,192 flush ns/doc; 200,277 vector rebuild ns/doc |
| Text candidates (`text-v2` default) | `SearchHybridTextCandidates`, no docs | 97,733 | 10,231.96 | 115,011 | 198 | 64 text candidates; 0 docs fetched; 0 fail/fallback |
| Text candidates (`text-v1` legacy) | `SearchHybridTextCandidates`, no docs | 200,820 | 4,979.58 | 92,825 | 632 | 64 text candidates; 0 docs fetched; 0 fail/fallback |
| Vector candidates | `SearchHybridVectorCandidates`, no docs | 35,890 | 27,862.91 | 40,256 | 82 | 64 vector candidates; 0 docs fetched; 0 fail/fallback |
| Hybrid `text-v2` + scalar filter | `SearchHybrid` lexical + scalar filter, no docs | 61,521 | 16,254.61 | 47,875 | 300 | 0 docs fetched; 0 fail/fallback |

Ordinary vector+scalar filtered Hybrid remains unsupported and fails closed
without success metrics.

---

## 10. Vector Search Matched-Recall Comparison (`bench_vector_db_compare.sh`)

Run via `scripts/bench_vector_db_compare.sh` with
`TREEDB_VALIDATION_EXACT_SOURCE=dataset` on `192.168.0.185` (`i5-11400F`,
`afcb9a21d`) using pinned `pgvector/pgvector:pg16@sha256:84a35586...` and
`milvusdb/milvus:v2.6.20@sha256:e514fced...` (`etcd:v3.5.25`, `minio:RELEASE.2024-12-18T13-15-44Z`).
All systems use `M=16`, `efConstruction=128`, 128d cosine distance, persistent
storage closed/reopened before search, and `1,000` measured queries (`256`
exact-truth validated queries at `250k` due to `treedb-vector-dataset-export`'s
100M comparison cap).

Measurement boundary: `scripts/bench_vector_db_compare.sh` runs **in-process**
TreeDB `column_graph` HNSW (`treedb_vector_search_demo`) against containerized
PostgreSQL+pgvector and Milvus Standalone servers. It is therefore reported
alongside (rather than replacing) the August 12 networked single-daemon /
four-group server matrix in `README.md`.

| system | corpus | EF | insert | index build | reopen/load | recall@10 | c=1 QPS / p95 | c=32 QPS / p95 | storage |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB `column_graph` HNSW (in-process) | 100k | 64 | 8.575 s | 12.018 s | 0.238 s | 0.9978 | 9,695.5 / 0.131 ms | 40,376.2 / 1.20 ms | 167.51 MB |
| PostgreSQL+pgvector (`pg16`) | 100k | 64 | 3.870 s | 31.958 s | 0.049 s | 0.9985 | 1,509.3 / 0.776 ms | 5,246.3 / 11.81 ms | 137.39 MB |
| Milvus Standalone (`v2.6.20`) | 100k | 64 | 21.894 s | 8.145 s | 2.216 s | 0.9988 | 802.7 / 1.32 ms | 2,563.9 / 22.70 ms | 206.61 MB |
| TreeDB `column_graph` HNSW (in-process) | 250k | 128 | 23.179 s | 34.937 s | 0.458 s | 1.0000 | 5,077.6 / 0.242 ms | 23,781.7 / 0.648 ms | 411.42 MB |
| PostgreSQL+pgvector (`pg16`) | 250k | 128 | 9.667 s | 122.343 s | 0.060 s | 1.0000 | 456.5 / 2.56 ms | 1,835.6 / 32.86 ms | 343.26 MB |
| Milvus Standalone (`v2.6.20`) | 250k | 128 | 52.234 s | 12.685 s | 2.099 s | 0.9992 | 632.6 / 1.65 ms | 1,868.2 / 31.63 ms | 516.41 MB |

TreeDB search guardrails across all `100k`/`250k` rows (`c=1` and `c=32`):
`0.0` docs fetched, `0.0` response-owned allocs, `1.0` `hnsw_pack` route share,
`0.0` graph/typed-column/route fallbacks, and `0.0` vector scratch decodes.

---

## 11. TreeDB versus Qdrant: Same-Machine 500K Vectors

Same-machine comparison on `192.168.0.185` (`i5-11400F`, shared 6-CPU budget
`CPUs 0–5`, TreeDB `GOMAXPROCS=6` at `afcb9a21d` with `command_wal_durable`
`-legacy-storage`, vs Docker `qdrant/qdrant:v1.19.0` with `--cpuset-cpus=0-5`).
500,000 768-dimensional cosine vectors (`YoKONCy/Cohere-1M-wikipedia-768d@de34a7af`
rows `0–499999`, SHA-256 `d4f332061a17d7728ff2c44f0859130e1b43f43360143dc3d754733dcf4e04d8`),
`200` observed queries, `topK=10`, float64 exhaustive cosine truth, and
returned document payloads **without vectors** (diagnostics disabled). End-to-end
Python clients use TreeDB native-v4 (`treedb_client`) vs Qdrant gRPC
(`qdrant-client 1.20.0`, `grpcio 1.85.4`, `prefer_grpc=True`). Numbers are
medians of six counterbalanced warm 10-second windows per concurrency (`0`
query errors across all 36 windows).

| Query arm | Clients | Recall@10 | efSearch | Median QPS | QPS range | p50 us | p95 us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB SQ8 + packed FP32 rerank | 1 | 97.50% | 128 | 1,039.1 | 1,030.9–1,048.8 | 958.9 | 1,084.6 |
| Qdrant SQ8 + FP32 rescore | 1 | 96.05% | 128 | 881.9 | 874.1–890.0 | 1,118.2 | 1,303.1 |
| Qdrant FP32 search reference | 1 | 96.20% | 128 | 695.7 | 683.1–717.2 | 1,414.5 | 1,726.5 |
| TreeDB SQ8 + packed FP32 rerank | 6 | 97.50% | 128 | 2,496.9 | 2,425.4–2,558.3 | 2,263.4 | 4,047.9 |
| Qdrant SQ8 + FP32 rescore | 6 | 96.05% | 128 | 2,180.1 | 2,137.1–2,213.9 | 2,657.7 | 3,979.8 |
| Qdrant FP32 search reference | 6 | 96.20% | 128 | 1,618.5 | 1,602.8–1,633.2 | 3,576.4 | 5,418.4 |

At independently selected operating points exceeding 95% recall@10, TreeDB SQ8
delivered **+17.8% higher one-client QPS (`1.178×`)** and **+14.5% higher
six-client QPS (`1.145×`)** than Qdrant 1.19.0 SQ8 while achieving **+1.45 pp
higher recall@10 (`97.50%` vs `96.05%`)**, and **+49.4% (1-client) / +54.3%
(6-client)** higher QPS than Qdrant FP32 reference. Note that the September 17
report (`docs/benchmarks/treedb_vs_qdrant_cohere500k_2026-09-17.md`) ran on a
faster Ryzen 7 9700X host; this November 25 run uses the same `i5-11400F` host
as the rest of the refresh suite.
