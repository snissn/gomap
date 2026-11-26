# TreeDB README Benchmark Refresh (2026-11-26, Post-Sprint 16)

This report records the clean post-Sprint 16 November 26, 2026 non-AWS/EC2
benchmark refresh run on `192.168.0.185` (`mikers-B560-DS3H-AC-Y1`) at `main`
commit `e78d8b40a41e8859aaae9e1758e2b0e343f54e54` (immediately following all
five Sprint 16 June-parity optimization issues: `#5839` / S16-1 via `#5850` &
`#5851`, `#5840` / S16-2 via `#5847`, `#5841` / S16-3 via `#5852`, `#5854` &
`#5855`, `#5842` / S16-4 via `#5848`, and `#5843` / S16-5 via `#5849` &
`#5853`).

## Sprint 16 Delta Summary (Post-S15 `afcb9a21d` -> Post-S16 `e78d8b40a`)

| Workload / Metric | Post-S15 (`afcb9a21d`) | Post-S16 (`e78d8b40a`) | Sprint 16 Delta |
| --- | ---: | ---: | ---: |
| YCSB Relaxed (`w:1`) GoMap run / paired ratio vs MongoDB 8 | 73,195.4 ops/s (`2.694x`) | **74,649.3 ops/s (`2.745x`)** | **+2.0%** (`+174.5%` vs MongoDB) |
| YCSB Durable-B GoMap run / paired ratio vs MongoDB 8 | 32,387.5 ops/s (`1.557x`) | **35,471.1 ops/s (`1.784x`)** | **+9.5%** (`+78.4%` vs MongoDB) |
| 2-index bulk insert (`template-v1` / `BSON` / `JSON`) | 483.1k / 520.6k / 417.2k docs/s | **657.0k / 612.4k / 506.3k docs/s** | **+36.0% / +17.6% / +21.4%** |
| Compacted `full_leafgen_pack_gc` (`template-v1` / `BSON` / `JSON`) | 96.7 / 98.6 / 118.1 B/doc | **25.2 / 28.0 / 36.3 B/doc** | **-74% / -72% / -69%** |
| Single-threaded unique secondary lookup (`template-v1` / `JSON`) | 296.6k / 297.5k ops/s | **604.2k / 586.2k ops/s** | **2.04x / 1.97x** |
| Single-threaded nonunique secondary lookup (`template-v1` / `JSON`) | 118.9k / 115.7k ops/s | **211.5k / 215.0k ops/s** | **1.78x / 1.86x** |
| Concurrency `ReadPrimaryIntoParallel` (`GOMAXPROCS=12`, 0 allocs) | 214.9 ns/op (4.65M/s) | **202.6 ns/op (4.94M/s)** | **-5.7% ns (+6.1% ops/s)** |
| Concurrency `SecondaryLookupUniqueParallel` (`GOMAXPROCS=12`) | 672.6 ns/op (24 allocs) | **296.1 ns/op (3 allocs)** | **2.27x faster, 8x fewer allocs** |
| Concurrency `MixedReadWritePrimary` (1 writer) | 4.40M reads/s, 13.9k writes/s | **4.95M reads/s, 18.2k writes/s** | **+12.6% reads, +31.3% writes** |
| Concurrency `MixedFixedReadLoadPrimary` (`500k` / `1.5M` reads/s) | n/a | **275.9k / 223.3k writer docs/s** | Fixed-rate harness (`128`-doc batches) |
| Suite 7 `direct` `_id r16` / `load` (`200k` BSON, standalone COW) | 904.1k / 239.7k | **1,447.0k / 371.9k** | **+60.0% / +55.2%** |
| Suite 7 `driver-command` `_id r16` / `load` (`200k` BSON) | 70.0k / 243.5k | **120.8k / 377.5k** | **+72.6% / +55.0%** |
| Suite 7 post-load physical `du` (`200k` BSON) | 33.27 MiB | **15.47–15.51 MiB** | **-53.5%** (`0.07x` MongoDB `218.9 MiB`) |
| Suite 8 `load_insert_many` (`100k` BSON, idx=0/1/2) | 160.7k / 127.4k / 109.4k | **292.7k / 276.4k / 239.6k** | **+82% / +117% / +119%** |
| Suite 8 `updateOne w32` (`100k` BSON, idx=0/1/2) | 55.5k / 55.4k / 55.9k | **72.2k / 70.5k / 71.8k** | **+30% / +27% / +28%** (`2.0–2.1x` MongoDB) |
| Suite 8 post-sweep `checkpoint` physical `du` (idx=0/1/2) | 195.7 / 199.3 / 198.2 MiB | **18.97 / 101.80 / 102.19 MiB** | **-90% / -49% / -48%** |

## Environment

- **Measured Commit**: `e78d8b40a41e8859aaae9e1758e2b0e343f54e54` (plus the
  3-line `-tags usearch_bench` compile fix in
  `TreeDB/collections/vector_usearch_bench_test.go` for the USearch serving
  comparison; runtime serving code unchanged)
- **Host**: `mikers-B560-DS3H-AC-Y1`, Linux `6.8.0-138-generic` `x86_64`
- **CPU**: 11th Gen Intel Core i5-11400F @ 2.60 GHz (6 cores / 12 threads)
- **RAM**: 31 GiB (zero swap activity)
- **Go**: `go1.26.0 linux/amd64` (`GOWORK=off`)
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

- **Included (all 11 non-AWS/EC2 README suites unified at `e78d8b40a`)**:
  1. YCSB Server Workload (Relaxed + Durable-B vs MongoDB 8.2.7, `--network host`)
  2. Indexed Collection Insert Workload (`100k` docs, 2 secondary indexes, batch `16000`)
  3. Collection Read And Lookup Workload (`100k` docs, 2 secondary indexes)
  4. Collection Concurrency Workload (`GOMAXPROCS=12`, template-v1, including fixed-read-load pacing)
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
(`e78d8b40a`, standalone COW, BSON); MongoDB runs Docker `mongo:8` (`8.2.7`)
with `--network host --bind_ip 127.0.0.1` for native loopback parity with GoMap.

| ACK class | MongoDB 8.2.7 run median ops/s | GoMap Mongo gateway run median ops/s | Paired ratio median | MongoDB load median ops/s | GoMap load median ops/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Relaxed: both `w:1`, `j` absent (`command_wal_relaxed`) | 27,333.4 | 74,649.3 | 2.744788 (+174.48%) | 48,013.7 | 33,825.2 |
| Durable-B: MongoDB `w:1,j:true` vs GoMap `w:1` (`command_wal_durable`) | 21,381.7 | 35,471.1 | 1.784305 (+78.43%) | 48,849.7 | 2,324.8 |

Per-pair breakdown:

| Class / pair | MongoDB load ops/s | GoMap load ops/s | MongoDB run ops/s | GoMap run ops/s | GoMap/Mongo run | GoMap READ / UPDATE p99 (us) | MongoDB READ / UPDATE p99 (us) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| relaxed / 1 | 45,796.3 | 34,124.1 | 26,973.3 | 74,230.2 | 2.751988 | 658 / 964 | 1,482 / 1,278 |
| relaxed / 2 | 48,013.7 | 33,825.2 | 27,333.4 | 75,024.4 | 2.744788 | 665 / 680 | 1,217 / 836 |
| relaxed / 3 | 48,795.1 | 31,812.9 | 27,880.6 | 74,649.3 | 2.677464 | 620 / 859 | 1,217 / 910 |
| durable-B / 1 | 48,948.9 | 2,291.4 | 21,381.7 | 33,718.4 | 1.576975 | 326 / 7,219 | 714 / 12,743 |
| durable-B / 2 | 48,849.7 | 2,339.9 | 21,969.1 | 40,720.4 | 1.853531 | 343 / 7,043 | 704 / 15,103 |
| durable-B / 3 | 46,742.0 | 2,324.8 | 19,879.5 | 35,471.1 | 1.784305 | 308 / 6,539 | 671 / 19,935 |

Caveats: Durable-B compares MongoDB explicit `w:1,j:true` against GoMap's
ordinary `command_wal_durable` ACK with `w:1`, `j` absent. Load uses `w:1`,
`j` absent on both engines, so GoMap's durable load waits on stable command-WAL
dependencies while standalone MongoDB batches journal flushes on its background
interval; load rates are therefore asymmetric in Durable-B.

---

## 2. Indexed Collection Insert Workload

Run via `./scripts/bench_collections_canonical.sh -count 3` (`100,000`
documents, `2` secondary indexes, batch size `16000`, `command_wal_relaxed`,
data and index outer leaves in value log) at post-Sprint 16 `e78d8b40a`
(includes S16-1 `#5850`/`#5851` bulk-insert fast paths and S16-2 `#5847`
`full_leafgen_pack_gc` storage-density fix).

| engine / format | layout | docs/sec | compacted B/doc | compaction method |
| --- | --- | ---: | ---: | --- |
| TreeDB template-v1 | data and index outer leaves in value log | 657,030 | 26.5 | `exhaustive_compact` (`25.2` `full_leafgen_pack_gc`; `68.8` offline compact) |
| TreeDB BSON | data and index outer leaves in value log | 612,370 | 28.0 | `full_leafgen_pack_gc` |
| TreeDB JSON | data and index outer leaves in value log | 506,329 | 36.3 | `full_leafgen_pack_gc` |
| SQLite native columns | WAL normal | 372,949 | 156.7 | `VACUUM` |
| SQLite JSON | WAL normal | 312,858 | 231.7 | `VACUUM` |

All three TreeDB formats exceed SQLite native columns on both indexed insert
throughput (`1.36×–1.76×`) and compacted storage density (`5.9×` smaller than
SQLite native and `8.7×` smaller than SQLite JSON for `template-v1`
`exhaustive_compact`; `5.6×` smaller for BSON and `4.3×` smaller for JSON via
`full_leafgen_pack_gc`).

---

## 3. Collection Read And Lookup Workload

Run via `scripts/bench_collections_harness.sh --include-sqlite --engine command_wal_relaxed --batch-size 16000 --count 1 --benchtime 1s`
with `TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=true` and
`TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=true` (`2` secondary indexes,
post-Sprint 16 `e78d8b40a` including S16-4 `#5848` secondary-lookup
optimizations).

| operation | TreeDB template-v1 ops/sec | TreeDB JSON ops/sec | SQLite native columns ops/sec | SQLite JSON ops/sec |
| --- | ---: | ---: | ---: | ---: |
| Primary read | 1,098,780 (910.1 ns/op) | 1,034,768 (966.4 ns/op) | 242,014 (4,132 ns/op) | 310,655 (3,219 ns/op) |
| Primary read (`ReadPrimaryInto`) | 1,240,849 (805.9 ns/op) | 1,223,691 (817.2 ns/op) | — | — |
| Unique secondary lookup | 604,230 (1,655 ns/op) | 586,166 (1,706 ns/op) | 313,873 (3,186 ns/op) | 287,109 (3,483 ns/op) |
| Nonunique secondary lookup | 211,506 (4,728 ns/op) | 214,961 (4,652 ns/op) | 45,455 (22,000 ns/op) | 26,922 (37,144 ns/op) |

---

## 4. Collection Concurrency Workload

Run via `COUNT=1 BENCHTIME=1s CPU_LIST=1,2,4,8,12 ./scripts/bench_collections_concurrency.sh`
(TreeDB `template-v1`, `command_wal_relaxed`, batch size `16000`, `2` secondary
indexes, post-Sprint 16 `e78d8b40a` including S16-4 `#5848` and S16-5
`#5849`/`#5853`).

| benchmark (`GOMAXPROCS=12`) | ns/op | derived reader ops/sec | writer docs/sec | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| `ReadPrimaryParallel` | 202.7 | 4,933,399 | — | 80 | 1 |
| `ReadPrimaryIntoParallel` | 202.6 | 4,935,834 | — | 0 | 0 |
| `SecondaryLookupUniqueParallel` | 296.1 | 3,377,237 | — | 79 | 3 |
| `SecondaryLookupNonUniqueParallel` | 1,115 | 896,861 | — | 4,178 | 6 |
| `MixedReadWritePrimary` (1 writer, saturated readers) | 202.1 | 4,948,046 | 18,198 | 97 | 1 |
| `MixedReadWriteSecondaryUnique` (1 writer, saturated readers) | 389.1 | 2,570,033 | 17,999 | 290 | 4 |

Fixed-read-load pacing (`BenchmarkCollectionMixedFixedReadLoadPrimary`,
`GOMAXPROCS=12`, `-benchtime=3s -count=5` medians, `128`-doc writer batches,
`4096` seed docs):

| fixed read target | achieved reads/sec | median writer docs/sec | read p50 (ns) | read p99 (ns) |
| --- | ---: | ---: | ---: | ---: |
| `rate_500k` (500,000 reads/sec) | 500,023 | 275,878 | 2,537 | 256,904 |
| `rate_1500k` (1,500,000 reads/sec) | 1,495,691 | 223,291 | 2,497 | 157,236 |

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
| Collection API | durable, fsync-on-publish | 18,208,476 | 54.92 | 7,030 | 1,505,976 | 10,959 |
| Document service | test backend, no fsync-on-publish | 14,122,493 | 70.81 | 9,064 | 1,339,100 | 9,601 |

---

## 6. Vector Search Serving Workload

Run via `COUNT=3 BENCHTIME=1000x CPU_LIST=1,8 ./scripts/bench_vector_search_compare.sh`
(Tier S exact-FP32 no-document, `10,000` documents, `64` dimensions, `M=16`,
`efConstruction=128`, `efSearch=128`, `topK=10`, query stream length `16`,
USearch `2.25.2` `linux/amd64`).

| row | cpu | median ns/op | derived ops/sec | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB `Collection.SearchVectorIndexWithBuffer` | 1 | 65,908 | 15,173 | 0 | 0 |
| TreeDB `OpenVectorIndexSearcher` + `SearchWithBuffer` | 1 | 65,378 | 15,296 | 0 | 0 |
| TreeDB `OpenVectorIndexSearcher` + `SearchWithBuffer` parallel | 8 | 10,167 | 98,357 | 0 | 0 |
| USearch `Search` | 1 | 79,980 | 12,503 | 136 | 3 |
| USearch `SearchParallel` | 8 | 11,819 | 84,610 | 136 | 3 |

---

## 7. TreeDB Mongo Gateway Client-Shape Workload

Run via `TREEDB_OPEN_MODE=standalone ./scripts/mongo_gateway_fast_client_matrix.sh`
(`200,000` gateway-shaped BSON documents, batch size `1000`, `16` insert
producers, two secondary indexes `email` and `city`, TreeDB `command_wal_relaxed`
standalone COW, settled read state, `16` concurrent readers).

| TreeDB access path | load docs/sec | concurrent `_id` reads/sec | concurrent indexed `email` reads/sec | `_id` p95 us | `email` p95 us |
| --- | ---: | ---: | ---: | ---: | ---: |
| Direct collection API | 371,930 | 1,447,011 | 550,218 | 18 | 110 |
| Mongo raw wire over TCP | 402,560 | 234,285 | 192,711 | 103 | 125 |
| TreeDB native wire over TCP | 350,096 | 93,901 | 105,634 | 280 | 231 |
| Mongo driver raw command | 364,605 | 115,848 | 107,077 | 217 | 229 |
| Mongo driver command | 377,490 | 120,807 | 113,793 | 209 | 213 |
| Mongo driver CRUD | 336,105 | 106,443 | 113,382 | 237 | 215 |

Note: `driver-unack` (`w:0`) is intentionally excluded for TreeDB because TreeDB
rejects unacknowledged `w:0` writes before mutation. On the same run, TreeDB
post-load physical `du` measured `15.47–15.51 MiB` (`81.1–81.3 B/doc`) versus
Docker MongoDB 8.2.7 `driver` physical `du` `218.90 MiB` (`1,147.7 B/doc`,
`0.07×`), with MongoDB `driver` measuring `253,156` load docs/sec, `35,833` `_id`
reads/sec (`768 us` p95), and `33,409` `email` reads/sec (`799 us` p95).

---

## 8. Mongo API Full-Sweep Workload & Writer Scaling

Run via `scripts/treedb_benchmark_run_report.sh --tier pr --mongo-docs 100000 --mongo-treedb-open-mode standalone --mongo-treedb-profile command_wal_relaxed --skip-raw --skip-collections --skip-load-modes --skip-load-scaling`
against Docker `mongo:8` (`8.2.7`). Physical MiB reflects the post-sweep
`checkpoint` physical `du` (after `480,000+` concurrent `updateOne` writes,
before offline `exhaustive` compaction) versus MongoDB physical `du`.

| secondary indexes | TreeDB load docs/sec | MongoDB load docs/sec | TreeDB `_id` r16 ops/sec | MongoDB `_id` r16 ops/sec | TreeDB `age` range r16 ops/sec | MongoDB `age` range r16 ops/sec | TreeDB physical MiB | MongoDB physical MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 292,742 | 411,559 | 118,446 | 35,711 | 98,356 | 25,312 | 18.97 | 213.38 |
| 1 | 276,414 | 243,760 | 124,489 | 33,685 | 88,947 | 25,947 | 101.80 | 216.99 |
| 2 | 239,582 | 183,053 | 125,480 | 37,014 | 105,341 | 25,153 | 102.19 | 217.56 |

Note on `idx=0` physical `du`: TreeDB's checkpoint snapshot reported `69.37 MiB`
and background leaf-generation GC completed before the isolated directory `du`
measurement (`18.97 MiB`); `idx=1` and `idx=2` checkpoint snapshots (`101.66` /
`102.05 MiB`) matched their post-sweep physical `du` (`101.80` / `102.19 MiB`,
down `49%` from post-S15 `199.33` / `198.21 MiB`).

Concurrent `_id` `updateOne` writer scaling (`concurrent_id_update_set_w*` from
the full sweep, ops/sec):

| writers | idx=0 TreeDB / MongoDB | idx=1 TreeDB / MongoDB | idx=2 TreeDB / MongoDB |
| ---: | ---: | ---: | ---: |
| 1 | 9,821 / 7,757 (1.27x) | 9,796 / 7,759 (1.26x) | 9,787 / 7,808 (1.25x) |
| 2 | 17,362 / 14,352 (1.21x) | 17,251 / 14,801 (1.17x) | 17,738 / 14,581 (1.22x) |
| 4 | 16,506 / 23,285 (0.71x) | 22,366 / 23,872 (0.94x) | 22,007 / 23,761 (0.93x) |
| 8 | 36,191 / 32,113 (1.13x) | 35,918 / 31,324 (1.15x) | 36,111 / 32,366 (1.12x) |
| 16 | 53,618 / 33,571 (1.60x) | 52,272 / 34,006 (1.54x) | 53,846 / 33,916 (1.59x) |
| 32 | 72,159 / 34,023 (2.12x) | 70,526 / 35,290 (2.00x) | 71,775 / 33,914 (2.12x) |

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
| Indexed insert/readiness | 256-doc batch insert + flush + vector rebuild | 189,824,102 | 5.27 ops/sec / 1,348.6 docs/sec | 12,472,600 | 39,866 | 45,964 insert ns/doc; 485,386 flush ns/doc; 210,148 vector rebuild ns/doc |
| Text candidates (`text-v2` default) | `SearchHybridTextCandidates`, no docs | 96,472 | 10,365.70 | 115,020 | 198 | 64 text candidates; 0 docs fetched; 0 fail/fallback |
| Text candidates (`text-v1` legacy) | `SearchHybridTextCandidates`, no docs | 197,570 | 5,061.50 | 92,825 | 632 | 64 text candidates; 0 docs fetched; 0 fail/fallback |
| Vector candidates | `SearchHybridVectorCandidates`, no docs | 36,960 | 27,056.28 | 40,256 | 82 | 64 vector candidates; 0 docs fetched; 0 fail/fallback |
| Hybrid `text-v2` + scalar filter | `SearchHybrid` lexical + scalar filter, no docs | 56,585 | 17,672.53 | 47,147 | 266 | 0 docs fetched; 0 fail/fallback |

Ordinary vector+scalar filtered Hybrid remains unsupported and fails closed
without success metrics.

---

## 10. Vector Search Matched-Recall Comparison (`bench_vector_db_compare.sh`)

Run via `scripts/bench_vector_db_compare.sh` with
`TREEDB_VALIDATION_EXACT_SOURCE=dataset` on `192.168.0.185` (`i5-11400F`,
`e78d8b40a`) using pinned `pgvector/pgvector:pg16@sha256:84a35586...` and
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
| TreeDB `column_graph` HNSW (in-process) | 100k | 64 | 8.831 s | 12.247 s | 0.236 s | 0.9978 | 9,313.7 / 0.131 ms | 42,183.3 / 0.371 ms | 163.48 MB |
| PostgreSQL+pgvector (`pg16`) | 100k | 64 | 4.045 s | 30.756 s | 0.050 s | 0.9985 | 1,501.6 / 0.856 ms | 5,481.6 / 10.34 ms | 137.32 MB |
| Milvus Standalone (`v2.6.20`) | 100k | 64 | 21.941 s | 8.169 s | 2.389 s | 0.9989 | 795.6 / 1.45 ms | 2,537.2 / 22.50 ms | 206.61 MB |
| TreeDB `column_graph` HNSW (in-process) | 250k | 128 | 23.005 s | 34.813 s | 0.471 s | 1.0000 | 5,095.9 / 0.242 ms | 23,051.1 / 0.711 ms | 407.42 MB |
| PostgreSQL+pgvector (`pg16`) | 250k | 128 | 9.626 s | 122.325 s | 0.059 s | 1.0000 | 453.9 / 2.64 ms | 1,790.8 / 33.74 ms | 343.27 MB |
| Milvus Standalone (`v2.6.20`) | 250k | 128 | 52.240 s | 13.186 s | 2.197 s | 0.9977 | 617.2 / 1.71 ms | 2,170.2 / 25.93 ms | 516.41 MB |

TreeDB search guardrails across all `100k`/`250k` rows (`c=1` and `c=32`):
`0.0` docs fetched, `0.0` response-owned allocs, `1.0` `hnsw_pack` route share,
`0.0` graph/typed-column/route fallbacks, and `0.0` vector scratch decodes.

---

## 11. TreeDB versus Qdrant: Same-Machine 500K Vectors

Same-machine comparison on `192.168.0.185` (`i5-11400F`, shared 6-CPU budget
`CPUs 0–5`, TreeDB `GOMAXPROCS=6` at `e78d8b40a` with `command_wal_durable`
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
| TreeDB SQ8 + packed FP32 rerank | 1 | 97.50% | 128 | 1,040.8 | 1,033.1–1,045.1 | 955.5 | 1,085.3 |
| Qdrant SQ8 + FP32 rescore | 1 | 95.55% | 128 | 873.9 | 829.8–885.8 | 1,121.4 | 1,372.0 |
| Qdrant FP32 search reference | 1 | 95.70% | 128 | 707.3 | 682.6–714.1 | 1,396.8 | 1,648.2 |
| TreeDB SQ8 + packed FP32 rerank | 6 | 97.50% | 128 | 2,521.5 | 2,437.1–2,557.0 | 2,241.0 | 4,025.5 |
| Qdrant SQ8 + FP32 rescore | 6 | 95.55% | 128 | 2,196.5 | 2,103.8–2,232.7 | 2,639.6 | 3,958.7 |
| Qdrant FP32 search reference | 6 | 95.70% | 128 | 1,619.4 | 1,533.9–1,633.3 | 3,579.5 | 5,416.0 |

At independently selected operating points exceeding 95% recall@10, TreeDB SQ8
delivered **+19.1% higher one-client QPS (`1.191×`)** and **+14.8% higher
six-client QPS (`1.148×`)** than Qdrant 1.19.0 SQ8 while achieving **+1.95 pp
higher recall@10 (`97.50%` vs `95.55%`)**, and **+47.1% (1-client) / +55.7%
(6-client)** higher QPS than Qdrant FP32 reference. Note that the September 17
report (`docs/benchmarks/treedb_vs_qdrant_cohere500k_2026-09-17.md`) ran on a
faster Ryzen 7 9700X host; this November 26 post-Sprint 16 run uses the same
`i5-11400F` host as the rest of the refresh suite.
