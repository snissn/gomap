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

## Benchmark Highlights

These checked-in reports use different workloads, profiles, and caveats. Treat
each workload as scoped evidence from its linked benchmark, not as one combined
benchmark suite.

### YCSB Server Workload

External `go-ycsb`, local loopback TCP, `recordcount=100000`,
`operationcount=10000`, `threadcount=16`, BSON document format, and zero YCSB
operation errors. Run rows use the median total-throughput repeat from the
latest-main June 3 HST / June 4 UTC report.

| target | profile | load ops/sec | run ops/sec | run avg us | run p99 us |
| --- | --- | ---: | ---: | ---: | ---: |
| MongoDB 8 | baseline | 38,755.4 | 26,494.1 | 595.0 | 1,275.0 |
| TreeDB nativewire | `command_wal_durable` | 83,217.0 | 135,318.6 | 113.0 | 367.0 |
| TreeDB Mongo gateway | `command_wal_durable` | 68,218.2 | 80,628.4 | 199.0 | 649.0 |

Full report, commands, host context, run repeats, and artifact paths:
[June 3 latest-main YCSB report](docs/benchmarks/ycsb_latest_main_2026-06-03.md).

### Indexed Collection Insert Workload

Two secondary indexes, latest-main June 4 HST / June 4 UTC rerun, `100000`
documents, batch size `16000`, and `command_wal_relaxed` for TreeDB. `docs/sec`
is the timed insert measurement for the value-log outer-leaf layout. Compacted
B/doc uses the byte-minimized `exhaustive_compact` row for TreeDB template-v1 and
SQLite after `VACUUM`; TreeDB JSON is omitted from the README compacted-size
headline until the canonical exhaustive fixture covers that format.

| engine / format | layout | docs/sec | compacted B/doc |
| --- | --- | ---: | ---: |
| TreeDB template-v1 | data and index outer leaves in value log | 697,350 | 22.8 |
| TreeDB JSON | data and index outer leaves in value log | 475,511 | — |
| SQLite native columns | WAL normal | 332,116 | 156.7 |
| SQLite JSON | WAL normal | 282,885 | 231.7 |

Source:
[June 4 exhaustive-compact two-index insert rerun](docs/benchmarks/collections_insert_two_index_exhaustive_main_2026-06-04.md).

### Collection Read And Lookup Workload

Two secondary indexes, April 27 collection/SQLite matrix.

| operation | TreeDB template-v1 ops/sec | TreeDB JSON ops/sec | SQLite native columns ops/sec | SQLite JSON ops/sec |
| --- | ---: | ---: | ---: | ---: |
| Primary read | 771,803 | 524,843 | 357,398 | 473,634 |
| Unique secondary lookup | 815,661 | 845,785 | 484,574 | 442,478 |
| Nonunique secondary lookup | 243,625 | 257,356 | 68,815 | 39,399 |

Source:
[April 27 collection/SQLite matrix](docs/benchmarks/collections_rewrite_vacuum_matrix_pr1075_2026-04-27.md).

### Collection Concurrency Workload

The collection insert/read/lookup rows above are single benchmark-driver rows.
For concurrent collection-layer reads and mixed read/write evidence, use the
separate concurrency report. In the June 4 run, TreeDB template-v1 primary reads
into a caller buffer measured 332.3 ns/op at `GOMAXPROCS=12`; mixed primary
reads with one writer measured 2.96M reader ops/sec and 44.3k writer docs/sec.

Source:
[June 4 collection concurrency report](docs/benchmarks/collections_concurrency_main_2026-06-04.md).

### Vector Search Serving Workload

Dated Tier S exact-FP32 no-document snapshot: Apple M3 (`darwin/arm64`),
2026-06-05, commit `2feb1f0e35459d1b3d044008203d0c8afcf5630f`, `10000`
documents, `64` dimensions, `M=16`, `efConstruction=128`, `efSearch=128`,
`topK=10`, query stream length `16`, `BENCHTIME=1000x`, and `COUNT=3`. TreeDB
rows use warmed persisted `column_graph` / `hnsw_search_pack_v1` no-document
routes. USearch rows are pure in-memory external ANN baselines, not
persistence-equivalent storage rows.

| row | cpu | median ns/op | derived ops/sec | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB `Collection.SearchVectorIndexWithBuffer` | 1 | 43,049 | 23,229 | 0 | 0 |
| TreeDB `OpenVectorIndexSearcher` + `SearchWithBuffer` parallel row | 8 | 8,610 | 116,144 | 0 | 0 |
| USearch `Search` | 1 | 30,065 | 33,261 | 136 | 3 |
| USearch `SearchParallel` | 8 | 6,906 | 144,802 | 139 | 3 |

Source and reproduction workflow:
[TreeDB vs USearch vector benchmark workflow](TreeDB/docs/guides/vector-search-benchmark-workflow.md).
API chooser, route guardrails, and runnable exact-only demo:
[high-QPS collection vector-search guide](TreeDB/docs/guides/vector-search-high-qps-collection-api.md)
and [`cmd/treedb_vector_highqps_demo`](cmd/treedb_vector_highqps_demo/README.md).

### TreeDB Mongo Gateway Client-Shape Workload

Gateway-shaped BSON documents, `200000` documents, batch size `1000`,
16 insert producers, two secondary indexes (`email`, `city`), TreeDB
`command_wal_relaxed`, settled read state, and 16 concurrent readers for the
read phases. This benchmark keeps TreeDB storage constant while changing the
client/protocol boundary.

| TreeDB access path | load docs/sec | concurrent `_id` reads/sec | concurrent indexed `email` reads/sec | `_id` p95 us | `email` p95 us |
| --- | ---: | ---: | ---: | ---: | ---: |
| Direct collection API | 299,777 | 2,026,834 | 230,846 | 6 | 813 |
| Mongo raw wire over TCP | 283,472 | 276,187 | 65,365 | 81 | 627 |
| TreeDB native wire over TCP | 271,948 | 115,712 | 66,917 | 213 | 618 |
| Mongo driver raw command | 276,503 | 124,961 | 70,215 | 197 | 580 |
| Mongo driver command | 253,315 | 130,387 | 69,213 | 191 | 596 |
| Mongo driver CRUD | 228,906 | 115,262 | 62,586 | 221 | 643 |
| Mongo driver unacknowledged writes | 233,169 | 114,026 | 68,454 | 219 | 601 |

`Direct collection API` is the TreeDB collection/storage ceiling, not a
Mongo-compatible protocol row. `Mongo driver unacknowledged writes` is not a
durability-equivalent default. Use this table to compare TreeDB access-path
overhead; compare against MongoDB only when client mode and acknowledgement
semantics match.

Source:
[June 4 fast Mongo/native client-shape matrix](docs/benchmarks/mongo_gateway_fast_client_matrix_2026-06-04.md).

### Vector Search External Snapshot

Local Apple M3 snapshot for `10k x 1536` vectors, `topK=10`, `M=16`,
`efConstruction=128`, and `efSearch=128`. TreeDB rows use DB-demo no-document
search; USearch is an in-memory library comparator; pgvector is a PostgreSQL
server HNSW comparator.

| system | recall@10 | c=1 avg / QPS | c=8 avg / QPS |
| --- | ---: | ---: | ---: |
| TreeDB exact FP32 | 0.9859 | 418 µs / 2,391 | 852 µs / 9,386 |
| TreeDB scalar_u8 rerank32 | 0.9828 | 165 µs / 6,072 | 511 µs / 15,571 |
| USearch f32 HNSW | 0.8938 | 725 µs / 1,380 | 160 µs / 6,259 |
| PostgreSQL+pgvector HNSW | 0.9859 | 2.67 ms / 374 | 4.29 ms / 1,864 |

USearch c=1/c=8 averages are from batch searches with `threads=1/8`, while
TreeDB and pgvector report per-query latency samples. Source and caveats:
[June 8 vector external comparison](docs/benchmarks/treedb_vector_external_compare_2026-06-08.md).

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

- `command_wal_durable`: recommended server default; command WAL enabled with
  durable sync/checksum settings.
- `command_wal_relaxed`: command WAL enabled with relaxed sync/read-integrity
  settings for high-throughput ingest and comparative benchmarks.
- `bench`: explicit no-WAL benchmark-only ceiling.

Legacy/raw profile names are retained only for compatibility and focused
low-level tests. They should not be used as public server defaults.

More detail: `docs/TREEDB_PROFILES.md` and `docs/TREEDB_WRITE_PATHS.md`.

## Benchmarking

Current YCSB status and rerun commands:

- `docs/benchmarks/ycsb_mongodb_treedb_current.md`
- `docs/benchmarks/ycsb_latest_main_2026-06-03.md`
- `scripts/ycsb_compare_mongodb_treedb.sh`

Collection, vector-search, and engine benchmark runbooks:

- `docs/benchmarks/collections_insert_two_index_exhaustive_main_2026-06-04.md`
- `docs/benchmarks/mongo_gateway_fast_client_matrix_2026-06-04.md`
- `TreeDB/docs/guides/vector-search-high-qps-collection-api.md`
- `TreeDB/docs/guides/vector-search-benchmark-workflow.md`
- `cmd/treedb_vector_highqps_demo/README.md`
- `docs/benchmarks/treedb_canonical_benchmark_runbook.md`
- `docs/benchmarks/collections_canonical_benchmark.md`
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
