# TreeDB

TreeDB is a pre-alpha persistent storage engine built around a copy-on-write
B+Tree, a persistent value log, and command-WAL recovery. It includes a native
wire server, a Mongo-compatible gateway, a collection/document layer, secondary
indexes, and benchmark tooling for comparing TreeDB against MongoDB and other
engines.

TreeDB is the main focus of this repository. The repo also contains HashDB, an
older mmap-backed hash engine used for experiments and comparison; see
`HashDB/README.md`.

## Status

TreeDB is pre-alpha:

- APIs and on-disk formats may change without backward-compatibility guarantees.
- New binaries may intentionally reject old DB directories.
- Benchmark DB directories should be rebuilt from scratch unless a report says
  otherwise.

Historical evidence and archived benchmark results under
`TreeDB/docs/evidence`, `docs/evidence`, `TreeDB/docs/benchmarks`, and
`docs/benchmarks` are kept in Git. Those archives have separate module
boundaries so runtime Go module downloads remain below Go's 500 MiB source
archive limit. Clone the repository to inspect the complete archives.

## What TreeDB Provides

- Persistent B+Tree index with copy-on-write root publishing.
- Persistent value log for large values and leaf/value placement experiments.
- Command WAL for collection and raw-key redo/recovery.
- Snapshot-isolated readers and exclusive process-level DB directory locking.
- Collection/document APIs with BSON, template-v1, secondary indexes, and vector
  search experiments.
- Native wire protocol through `cmd/treedb-native-server`.
- Mongo-compatible gateway through `TreeDB/mongo_gateway`.
- Benchmark and profiling scripts for YCSB, collections, vector search, and
  storage-engine comparison.

## Quickstart

Build the primary TreeDB servers:

```sh
mkdir -p bin
go build -o bin/treedb-native-server ./cmd/treedb-native-server
go build -o bin/treedb-mongo-gateway ./TreeDB/mongo_gateway/server.go
```

Run the native server:

```sh
./bin/treedb-native-server \
  -dir /tmp/treedb-native \
  -profile command_wal_durable \
  -addr 127.0.0.1:17130
```

The native server and `cmd/treedb-document-service` use the shared COW document
owner by default. New vector indexes default to cosine/float32 `column_graph`,
the only supported serving strategy. Removed strategies, L2/inner-product ANN,
primary int8 vector indexes and BSON vector-index declarations are refused before
mutation. Mongo BSON CRUD and scalar indexes remain supported; Mongo vector
`createIndexes` requests return `BadValue` (code 2) without creating a collection
or any indexes, including scalar siblings in the same request.
Ordinary filtered ANN and vector-bearing filtered Hybrid are unavailable;
request `route=exact` explicitly for ordinary filtered dense search. Admitted
typed filtering and derived quantized score planes retain their own contracts.
Ordinary ColumnGraph requires
`command_wal_durable` under both storage owners. After deferred or raw wire vector
mutations, keyword and filtered exact reads remain available, while ANN/mixed
Hybrid require explicit graph maintenance. The HTTP service exposes Optimize;
the standalone wire server requires orderly shutdown, public owner/API Optimize
against the same directory, then restart. Flush/Checkpoint do not rebuild graphs.
Use `-legacy-storage` for a process that serves specialized typed or
normalized/quantized routes. This is an explicit
storage choice; unsupported requests do not fall back automatically. See the
[command ownership contract](TreeDB/docs/document-service-native-constructor.md#command-storage-owner).

Run the Mongo-compatible gateway:

```sh
./bin/treedb-mongo-gateway \
  -dir /tmp/treedb-mongo \
  -profile command_wal_durable \
  -document-format bson \
  -addr 127.0.0.1:27130
```

Minimal Go usage:

```go
package main

import (
	"fmt"
	"log"

	treedb "github.com/snissn/gomap/TreeDB"
)

func main() {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, "./my-db")
	db, err := treedb.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Set([]byte("key"), []byte("value")); err != nil {
		log.Fatal(err)
	}
	value, err := db.Get([]byte("key"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value))
}
```

## Profiles

The current public TreeDB profile surface is intentionally small:

- `command_wal_durable`: recommended server default; ordinary acknowledgements
  wait for a durable dependency-closed command-WAL prefix.
- `command_wal_relaxed`: ordinary acknowledgements are relaxed, while every
  explicit `*Sync` waits for a durable dependency-closed command-WAL prefix.
- `no_wal_fast`: production no-WAL profile; ordinary acknowledgements are
  relaxed and every explicit `*Sync` waits for a sealed durable root.
- `bench_unsafe`: benchmark/test-only ceiling with no durability promise. It is
  available only through the explicit benchmark constructor/parser boundary.

All three production profiles verify value-log integrity. `Flush` is a
visibility/drain operation, not a durability boundary. `Checkpoint` and clean
`Close` wait for a sealed durable root.

Legacy Go aliases are retained only for compatibility and focused low-level
tests. CLI and environment parsers accept only canonical underscore names.

More detail: `docs/TREEDB_PROFILES.md` and `docs/TREEDB_WRITE_PATHS.md`.

## Benchmarking

These checked-in reports use different workloads, profiles, and caveats. Treat
each workload as scoped evidence from its linked benchmark, not as one combined
benchmark suite.

### TreeDB versus Qdrant: same-machine 500K vectors

November 25 comparison on `mikers-B560-DS3H-AC-Y1` (`i5-11400F`, Linux
`x86_64`, commit `afcb9a21d`) with a shared six-CPU budget: 500K
768-dimensional cosine vectors, `topK=10`, and document payloads **without
vectors**. End-to-end Python clients use TreeDB native-v4 and Qdrant 1.19.0
gRPC (`qdrant-client 1.20.0`). Numbers are medians of six counterbalanced warm
10-second windows per configuration (`0` query errors across all 36 windows).

| System/query configuration | Recall@10 | One-client QPS | Six-client QPS |
| --- | ---: | ---: | ---: |
| TreeDB SQ8 + packed FP32 rerank (`ef=128`) | 97.50% | 1,039.1 | 2,496.9 |
| Qdrant SQ8 + FP32 rescore (`ef=128`) | 96.05% | 881.9 | 2,180.1 |
| Qdrant FP32 search reference (`ef=128`) | 96.20% | 695.7 | 1,618.5 |

At independently selected settings exceeding 95% recall, TreeDB delivered
**17.8% more one-client QPS and 14.5% more six-client QPS than Qdrant SQ8**
(at `97.50%` vs `96.05%` recall@10). Achieved recalls are not identical. This is
scoped application throughput on a shared workstation, not maximum server
throughput or an official 1M benchmark; the FP32 reference uses the same
SQ8-equipped Qdrant collection. See the
[November 25 README benchmark refresh](docs/benchmarks/readme_benchmark_refresh_2026-11-25.md#11-treedb-versus-qdrant-same-machine-500k-vectors)
(prior Ryzen 7 9700X [September 17 TreeDB/Qdrant report](docs/benchmarks/treedb_vs_qdrant_cohere500k_2026-09-17.md))
for latency, repeat ranges, configurations, dataset provenance, and limitations.

### VectorDBBench Cohere 1M

End-to-end VDBBench search on Cohere Medium 1M (768-dimensional cosine,
`topK=100`, IDs-only responses), with separate c6i.8xlarge client and server
hosts. The budgeted TreeDB server was 32 vCPUs and 64 GiB at exactly
$1,000/month. The two canonical TreeDB curves are FP32 HNSW traversal and
scalar-u8 traversal with FP32 reranking.

![Vector search latency and QPS at $1,000 monthly cost](docs/benchmarks/treedb_vectordbbench_cohere1m_latency_qps_1000_2026-08-21.png)

![TreeDB and self-hosted VDBBench Cohere Medium 1M QPS versus recall](docs/benchmarks/treedb_vectordbbench_cohere1m_self_hosted_focus_qps_recall_2026-08-21.png)

| TreeDB curve | about 0.98 recall | about 0.994 recall | highest measured recall |
| --- | ---: | ---: | ---: |
| u8 + FP32 rerank | 16,532 QPS @ 0.9809 | 6,969 QPS @ 0.9944 | 5,626 QPS @ 0.9961 |
| FP32 | 6,709 QPS @ 0.9808 | 2,649 QPS @ 0.9944 | 2,093 QPS @ 0.9960 |

The public curves are a directional leaderboard overlay, not same-hardware
reproductions. Full methodology, latency, profiles, supporting quantized-only
measurements, raw data, and evidence archive:
[August 21 Cohere 1M report](docs/benchmarks/treedb_vectordbbench_cohere1m_c6i_dense_curve_2026-08-21.md).

### YCSB Server Workload

The November 25 comparison on `mikers-B560-DS3H-AC-Y1` (`i5-11400F`, Linux
`x86_64`, commit `afcb9a21d`) uses the public Mongo BSON API, the external
`go-ycsb` client, 100,000 records, 10,000 operations, 16 workers, uniform keys,
95% reads / 5% updates, and `--network host` loopback parity for Docker
MongoDB 8.2.7. Three alternating fresh fixtures per arm (AB/BA/AB) have zero
operation errors. Run medians and the median of paired GoMap/MongoDB ratios
are:

| ACK class | MongoDB 8.2.7 ops/s | GoMap Mongo gateway ops/s | Paired ratio |
| --- | ---: | ---: | ---: |
| Relaxed: both `w:1`, `j` absent | 27,185.5 | 73,195.4 | 2.694336 (+169.43%) |
| Durable-B: requests differ | 20,674.8 | 32,387.5 | 1.556642 (+55.66%) |

Durable-B uses MongoDB `w:1,j:true` versus GoMap's ordinary
`command_wal_durable` ACK with `w:1`, `j` absent. Load uses `w:1`, `j` absent
on both engines (median load: `45,805.7` vs `29,402.8 ops/s` in Relaxed;
`49,139.0` vs `2,343.0 ops/s` in Durable-B, where GoMap's durable load waits on
stable command-WAL dependencies). These short, cache-warmed runs are scoped
workstation evidence, not a sustained or durability-equivalence claim. Full
per-pair numbers, latencies, and methodology:
[November 25 README benchmark refresh](docs/benchmarks/readme_benchmark_refresh_2026-11-25.md#1-ycsb-server-workload)
(prior [October final-source YCSB report](docs/benchmarks/ycsb_mongodb_gomap_final_2026-10-30.md)).

Historical June results used different whole versions and fixture/runtime
conditions. The MongoDB URI requested `w:1` with `j` absent; it is not a
journal-before-ACK baseline. The three June repeats reused one loaded database.
Keep those numbers separate from the fresh-fixture comparison above:

| Historical June target | profile | load ops/sec | run ops/sec | run avg us | run p99 us |
| --- | --- | ---: | ---: | ---: | ---: |
| MongoDB 8 | `w:1`, `j` absent | 38,755.4 | 26,494.1 | 595.0 | 1,275.0 |
| TreeDB nativewire | `command_wal_durable` | 83,217.0 | 135,318.6 | 113.0 | 367.0 |
| TreeDB Mongo gateway | `command_wal_durable` | 68,218.2 | 80,628.4 | 199.0 | 649.0 |

[June 3 historical YCSB report](docs/benchmarks/ycsb_latest_main_2026-06-03.md)
retains its original commands, repeats and limitations.

### Indexed Collection Insert Workload

Two secondary indexes, November 25 benchmark refresh (`i5-11400F`, commit
`afcb9a21d`), `100000` documents, batch size `16000`, and
`command_wal_relaxed` for TreeDB. `docs/sec` is the timed insert measurement
for the value-log outer-leaf layout. Compacted B/doc uses `exhaustive_compact`
for TreeDB template-v1 (`68.9` B/doc offline compact), `full_leafgen_pack_gc`
for TreeDB BSON and JSON, and SQLite after `VACUUM`.

| engine / format | layout | docs/sec | compacted B/doc |
| --- | --- | ---: | ---: |
| TreeDB template-v1 | data and index outer leaves in value log | 483,092 | 26.7 |
| TreeDB BSON | data and index outer leaves in value log | 520,562 | 98.6 |
| TreeDB JSON | data and index outer leaves in value log | 417,188 | 118.1 |
| SQLite native columns | WAL normal | 374,532 | 156.7 |
| SQLite JSON | WAL normal | 323,311 | 231.7 |

Source:
[November 25 README benchmark refresh](docs/benchmarks/readme_benchmark_refresh_2026-11-25.md#2-indexed-collection-insert-workload)
(prior [June 17 full benchmark refresh](docs/benchmarks/full_benchmark_report_2026-06-17.md)).

### Collection Read And Lookup Workload

Two secondary indexes, November 25 collection/SQLite matrix (`i5-11400F`,
commit `afcb9a21d`, `command_wal_relaxed`, batch size `16000`, data and index
outer leaves in value log).

| operation | TreeDB template-v1 ops/sec | TreeDB JSON ops/sec | SQLite native columns ops/sec | SQLite JSON ops/sec |
| --- | ---: | ---: | ---: | ---: |
| Primary read | 1,126,634 | 1,065,417 | 231,803 | 304,878 |
| Unique secondary lookup | 296,560 | 297,530 | 312,305 | 286,779 |
| Nonunique secondary lookup | 118,906 | 115,687 | 41,681 | 27,086 |

Source:
[November 25 README benchmark refresh](docs/benchmarks/readme_benchmark_refresh_2026-11-25.md#3-collection-read-and-lookup-workload)
(prior [April 27 collection/SQLite matrix](docs/benchmarks/collections_rewrite_vacuum_matrix_pr1075_2026-04-27.md)).

### Collection Concurrency Workload

The collection insert/read/lookup rows above are single benchmark-driver rows.
For concurrent collection-layer reads and mixed read/write evidence, use the
separate concurrency runner. In the November 25 run (`i5-11400F`, commit
`afcb9a21d`, `GOMAXPROCS=12`, `command_wal_relaxed`, two secondary indexes),
TreeDB template-v1 primary reads into a caller buffer measured `214.9 ns/op`
(`4.65M ops/sec`, `0 B/op`, `0 allocs/op`); mixed primary reads with one writer
measured `4.40M reader ops/sec` (`227.5 ns/op`) and `13.9k writer docs/sec`.

Source:
[November 25 README benchmark refresh](docs/benchmarks/readme_benchmark_refresh_2026-11-25.md#4-collection-concurrency-workload)
(prior [June 4 collection concurrency report](docs/benchmarks/collections_concurrency_main_2026-06-04.md)).

### Typed Metadata Mutation Workload

November 25 run on `mikers-B560-DS3H-AC-Y1` (`i5-11400F`, commit `afcb9a21d`),
pinned to two CPUs with `GOMAXPROCS=2`: 128 existing typed documents,
8-dimensional vectors, and a declared metadata string update. Each sample
performs 10 complete public mutations, including validation, planning,
command-WAL encoding, and publication; setup and teardown are untimed. The
Collection API fixture uses `command_wal_durable` with fsync-on-publish. The
document-service fixture uses its test backend with command WAL enabled but
without fsync-on-publish. Vector assets are not rebuilt. Medians are from five
samples:

| public boundary | publication profile | median ns/op | batches/sec | rows/sec | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Collection API | durable, fsync-on-publish | 17,008,470 | 58.79 | 7,526 | 1,537,211 | 10,965 |
| Document service | test backend, no fsync-on-publish | 14,627,988 | 68.36 | 8,750 | 1,362,241 | 9,640 |

The rows use different fixtures and publication profiles, so do not compare
them directly against each other. Reproduce with:

```sh
GOMAXPROCS=2 taskset -c 4,5 go test \
  ./TreeDB/collections ./TreeDB/documentservice \
  -run '^$' \
  -bench '^Benchmark(TypedMinima|ServiceTyped)MetadataMutation$/^rows128$/^dims8$' \
  -benchmem -benchtime=10x -count=5
```

### Vector Search Serving Workload

Tier S exact-FP32 no-document snapshot: `mikers-B560-DS3H-AC-Y1` (Intel Core
i5-11400F, `linux/amd64`), 2026-11-25, commit `afcb9a21d` (with the
`-tags usearch_bench` compile fix in `vector_usearch_bench_test.go`), `10000`
documents, `64` dimensions, `M=16`, `efConstruction=128`, `efSearch=128`,
`topK=10`, query stream length `16`, `BENCHTIME=1000x`, and `COUNT=3`. TreeDB
rows use warmed persisted `column_graph` / `hnsw_search_pack_v1` no-document
routes. USearch (`v2.25.2`) rows are pure in-memory external ANN baselines, not
persistence-equivalent storage rows.

| row | cpu | median ns/op | derived ops/sec | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB `Collection.SearchVectorIndexWithBuffer` | 1 | 67,034 | 14,918 | 0 | 0 |
| TreeDB `OpenVectorIndexSearcher` + `SearchWithBuffer` | 1 | 64,254 | 15,563 | 0 | 0 |
| TreeDB `OpenVectorIndexSearcher` + `SearchWithBuffer` parallel row | 8 | 10,278 | 97,295 | 0 | 0 |
| USearch `Search` | 1 | 76,684 | 13,041 | 136 | 3 |
| USearch `SearchParallel` | 8 | 11,888 | 84,118 | 136 | 3 |

Source and reproduction workflow:
[November 25 README benchmark refresh](docs/benchmarks/readme_benchmark_refresh_2026-11-25.md#6-vector-search-serving-workload)
and [TreeDB vs USearch vector benchmark workflow](TreeDB/docs/guides/vector-search-benchmark-workflow.md).
API chooser, route guardrails, and runnable exact-only demo:
[high-QPS collection vector-search guide](TreeDB/docs/guides/vector-search-high-qps-collection-api.md)
and [`cmd/treedb_vector_highqps_demo`](cmd/treedb_vector_highqps_demo/README.md).

### TreeDB Mongo Gateway Client-Shape Workload

November 25 run (`i5-11400F`, commit `afcb9a21d`): gateway-shaped BSON
documents, `200000` documents, batch size `1000`, 16 insert producers, two
secondary indexes (`email`, `city`), TreeDB `command_wal_relaxed` (standalone
COW), settled read state, and 16 concurrent readers for the read phases. This
benchmark keeps TreeDB storage constant while changing the client/protocol
boundary.

| TreeDB access path | load docs/sec | concurrent `_id` reads/sec | concurrent indexed `email` reads/sec | `_id` p95 us | `email` p95 us |
| --- | ---: | ---: | ---: | ---: | ---: |
| Direct collection API | 239,715 | 904,133 | 521,593 | 35 | 95 |
| Mongo raw wire over TCP | 236,864 | 197,922 | 99,365 | 127 | 144 |
| TreeDB native wire over TCP | 246,963 | 70,504 | 96,129 | 222 | 252 |
| Mongo driver raw command | 239,742 | 72,147 | 108,656 | 226 | 224 |
| Mongo driver command | 243,531 | 69,981 | 106,969 | 223 | 227 |
| Mongo driver CRUD | 224,320 | 71,861 | 106,706 | 225 | 226 |

`Direct collection API` is the TreeDB collection/storage ceiling, not a
Mongo-compatible protocol row. TreeDB rejects unacknowledged `w:0` writes
before mutation, so `driver-unack` is excluded. On the same run, TreeDB
post-load physical `du` measured `33.27 MiB` (`174.4 B/doc`) versus Docker
MongoDB 8.2.7 `driver` physical `du` `219.06 MiB` (`1,148.5 B/doc`, `0.15x`).
Use this table to compare TreeDB access-path overhead; compare against MongoDB
only when client mode and acknowledgement semantics match.

Source:
[November 25 README benchmark refresh](docs/benchmarks/readme_benchmark_refresh_2026-11-25.md#7-treedb-mongo-gateway-client-shape-workload)
(prior [June 4 fast Mongo/native client-shape matrix](docs/benchmarks/mongo_gateway_fast_client_matrix_2026-06-04.md)).

### Mongo API Full-Sweep Workload

November 25 benchmark refresh (`i5-11400F`, commit `afcb9a21d`), Docker
MongoDB 8.2.7 baseline, `100000` BSON documents, TreeDB Mongo gateway
`driver-command-raw`, `command_wal_relaxed`, standalone COW open mode. The
full-sweep load rows include the additional `age_1` range index even when the
displayed secondary-index count is `0`. TreeDB physical MiB is the post-sweep
`checkpoint` snapshot `du` (after `480,000+` concurrent updates, prior to
offline `exhaustive` compaction; `0.91x–0.92x` MongoDB physical `du`).

| secondary indexes | TreeDB load docs/sec | MongoDB load docs/sec | TreeDB `_id` r16 ops/sec | MongoDB `_id` r16 ops/sec | TreeDB physical MiB | MongoDB physical MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 160,729 | 451,420 | 118,615 | 37,065 | 195.66 | 213.38 |
| 1 | 127,376 | 251,970 | 118,486 | 36,392 | 199.33 | 217.01 |
| 2 | 109,390 | 204,153 | 118,021 | 35,836 | 198.21 | 217.58 |

In the same run, TreeDB indexed `age` range reads (`r16`) measured `98,798` /
`95,073` / `93,502 ops/sec` (`3.61x–3.79x` MongoDB 8.2.7) and concurrent `_id`
`updateOne` scaling reached `40,948–42,764 ops/sec` at 16 writers (`1.19x–1.28x`
MongoDB) and `55,408–55,905 ops/sec` at 32 writers (`1.55x–1.61x` MongoDB).

Source:
[November 25 README benchmark refresh](docs/benchmarks/readme_benchmark_refresh_2026-11-25.md#8-mongo-api-full-sweep-workload--writer-scaling)
(prior [June 17 full benchmark refresh](docs/benchmarks/full_benchmark_report_2026-06-17.md)).

### Vector Search Server Matched-Recall Matrix

Current-main Linux/amd64 snapshot for deterministic `100k` and `250k`, 128d
cosine top-10 corpora. Each row selects the lowest tested `efSearch` whose
median recall@10 is at least `.90`; each cell is the median of three serialized,
counterbalanced repetitions with 1,000 measured queries. Search QPS excludes
index construction.

| system | corpus | selected EF | recall@10 | c=1 QPS / p95 | c=32 QPS / p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB single daemon / four groups | 100k | 64 | .9525 | 2,198.0 / 0.537 ms | 10,852.5 / 4.866 ms |
| pgvector | 100k | 64 | .9251 | 1,255.6 / 0.965 ms | 3,745.6 / 15.390 ms |
| Milvus Standalone | 100k | 64 | .9210 | 644.8 / 1.931 ms | 2,256.7 / 25.410 ms |
| TreeDB single daemon / four groups | 250k | 128 | .9580 | 1,525.3 / 0.791 ms | 6,220.6 / 8.466 ms |
| pgvector | 250k | 128 | .9229 | 385.5 / 3.580 ms | 1,712.9 / 36.690 ms |
| Milvus Standalone | 250k | 128 | .9336 | 559.8 / 1.997 ms | 2,141.3 / 26.670 ms |

TreeDB uses one OS daemon with one public coordinator and four logical serving
groups; pgvector uses one PostgreSQL container; Milvus uses Standalone plus
etcd and MinIO. Full EF curves, repetition ranges, topology, resource controls,
and caveats are in the
[August 12 matched-recall report](docs/benchmarks/treedb_vector_server_matched_recall_2026-08-12.md)
and its [machine-readable result](docs/benchmarks/treedb_vector_server_matched_recall_2026-08-12.json).

On November 25 (`afcb9a21d`, same `i5-11400F` host, same pinned
`pgvector/pgvector:pg16` and `milvusdb/milvus:v2.6.20` containers), running the
checked-in `scripts/bench_vector_db_compare.sh` harness (`M=16`,
`efConstruction=128`, 1,000 queries) refreshed **in-process** TreeDB
`column_graph` HNSW (`treedb_vector_search_demo`) against containerized
PostgreSQL+pgvector and Milvus Standalone on the deterministic clustered
`100k`/`250k` 128d cosine corpora:

| system (Nov 25 `bench_vector_db_compare.sh`) | corpus | EF | recall@10 | c=1 QPS / p95 | c=32 QPS / p95 | storage |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB `column_graph` HNSW (in-process) | 100k | 64 | .9978 | 9,695.5 / 0.131 ms | 40,376.2 / 1.20 ms | 167.51 MB |
| PostgreSQL+pgvector (`pg16`) | 100k | 64 | .9985 | 1,509.3 / 0.776 ms | 5,246.3 / 11.81 ms | 137.39 MB |
| Milvus Standalone (`v2.6.20`) | 100k | 64 | .9988 | 802.7 / 1.32 ms | 2,563.9 / 22.70 ms | 206.61 MB |
| TreeDB `column_graph` HNSW (in-process) | 250k | 128 | 1.0000 | 5,077.6 / 0.242 ms | 23,781.7 / 0.648 ms | 411.42 MB |
| PostgreSQL+pgvector (`pg16`) | 250k | 128 | 1.0000 | 456.5 / 2.56 ms | 1,835.6 / 32.86 ms | 343.26 MB |
| Milvus Standalone (`v2.6.20`) | 250k | 128 | .9992 | 632.6 / 1.65 ms | 1,868.2 / 31.63 ms | 516.41 MB |

Note the measurement boundary: the November 25 table exercises in-process TreeDB
`column_graph` (`0` docs fetched, `0` fallbacks) against networked pgvector and
Milvus containers, whereas the August 12 table above measures the networked
TreeDB single-daemon / four-group server topology. See the
[November 25 README benchmark refresh](docs/benchmarks/readme_benchmark_refresh_2026-11-25.md#10-vector-search-matched-recall-comparison-bench_vector_db_comparesh)
for insert/build/reopen timings and guardrails.

TreeDB is also wired into `snissn/vectordbbench` through the document service.
Those rows measure Python/client/HTTP/service overhead and are separate from
native Go `0 B/op` no-document evidence; see the
[TreeDB VectorDBBench runbook](docs/benchmarks/treedb_vectordbbench_runbook_2026-06-11.md).

### `application.db` Offline Density Workload

Offline compacted-size comparison from the June 2 Celestia `application.db`
rerun.

| engine | compacted size | workflow |
| --- | ---: | --- |
| TreeDB | 1.690 GiB | `command_wal_relaxed`, rebuild, `CompactStorageFull`, offline index vacuum |
| PebbleDB | 2.108 GiB | snappy, 64 KiB blocks, 64 MiB target files, full compact |
| goleveldb | 2.221 GiB | snappy, 64 KiB blocks, restart interval 256, full compact |

Source:
[June 2 density rerun](docs/benchmarks/application_db_engine_matrix_2026-06-02.md).

### Indexed Text/Vector/Hybrid Insert And Search Workload

November 25 run on `mikers-B560-DS3H-AC-Y1` (`i5-11400F`, Linux `x86_64`,
commit `afcb9a21d`, `-benchtime=5x -count=3` medians): `256` JSON documents,
scalar indexes on `tenant`/`region`, a `lexical` title/body text index (new
text indexes default to `text-v2`; `text-v1` is explicit compatibility), and an
exact cosine column graph (`dims=16`, `M=8`). The insert row times
`InsertBatch` + `Flush` + `RebuildVectorIndex`; search rows build/index the
fixture before timing and then time the search API call only.

| row | timed boundary | ns/op median | ops/sec | B/op | allocs/op | key counters |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Indexed insert/readiness | 256-doc batch insert + flush + vector rebuild | 185,495,996 | 5.39 ops/sec / 1,380.1 docs/sec | 11,880,849 | 39,659 | 45,469 insert ns/doc; 479,192 flush ns/doc; 200,277 vector rebuild ns/doc |
| Text candidates (`text-v2` default) | `SearchHybridTextCandidates`, no docs | 97,733 | 10,231.96 | 115,011 | 198 | 64 text candidates; 0 docs fetched; 0 fail/fallback |
| Text candidates (`text-v1` legacy) | `SearchHybridTextCandidates`, no docs | 200,820 | 4,979.58 | 92,825 | 632 | 64 text candidates; 0 docs fetched; 0 fail/fallback |
| Vector candidates | `SearchHybridVectorCandidates`, no docs | 35,890 | 27,862.91 | 40,256 | 82 | 64 vector candidates; 0 docs fetched; 0 fail/fallback |
| Hybrid `text-v2` + scalar filter | `SearchHybrid` lexical + scalar filter, no docs | 61,521 | 16,254.61 | 47,875 | 300 | 0 docs fetched; 0 fail/fallback |

Ordinary vector+scalar filtered Hybrid is unsupported and fails closed; current
benchmark probes verify the rejection and skip those cells without success
metrics.

Source, commands, artifact paths, guardrails, and caveats:
[November 25 README benchmark refresh](docs/benchmarks/readme_benchmark_refresh_2026-11-25.md#9-indexed-textvectorhybrid-insert-and-search-workload)
and [TreeDB indexed insertion/search benchmark](docs/benchmarks/treedb_index_insert_search_benchmarks.md).

### Benchmark tools and runbooks

The dated reports above contain their own reproduction details. Start here for
current benchmark entry points:

- `docs/benchmarks/treedb_canonical_benchmark_runbook.md`
- `docs/benchmarks/collections_canonical_benchmark.md`
- `docs/benchmarks/ycsb_mongodb_treedb_current.md`
- `scripts/ycsb_compare_mongodb_treedb.sh`
- `cmd/unified_bench/README.md`
- `cmd/benchprof/README.md`

Profile capture workflow:

```sh
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)
./bin/unified-bench ... -profile-dir "$OUT"
./bin/benchprof -profiles-dir "$OUT"
```

## Documentation

- TreeDB canonical spec: `TreeDB/docs/spec/README.md`
- TreeDB guides: `TreeDB/docs/guides/README.md`
- TreeDB concepts: `docs/TREEDB_CONCEPTS.md`
- TreeDB storage format: `docs/TREEDB_STORAGE_FORMAT.md`
- TreeDB recovery: `docs/TREEDB_RECOVERY.md`
- TreeDB collection quickstart: `docs/TREEDB_COLLECTION_QUICKSTART.md`
- Contracts: `docs/contracts/README.md`
- Full docs index: `docs/README.md`

## Repo Contents

- `TreeDB/`: TreeDB storage engine, collection layer, native APIs, command WAL,
  and Mongo gateway.
- `cmd/treedb-native-server/`: native wire server.
- `TreeDB/mongo_gateway/`: Mongo-compatible TreeDB gateway.
- `cmd/unified_bench/`: cross-engine benchmark harness.
- `cmd/benchprof/`: profile/result summarizer.
- `HashDB/`: mmap-backed hash-index engine used for experiments and comparison.

## Testing

```sh
go test ./...
go test ./TreeDB/... ./cmd/treedb-native-server ./TreeDB/mongo_gateway
```

For large benchmark runs, prefer a fresh DB directory and record the exact
commit, host, profile, command, and artifact path in the report.
