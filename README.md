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

Collection and engine benchmark runbooks:

- `docs/benchmarks/collections_insert_two_index_exhaustive_main_2026-06-04.md`
- `docs/benchmarks/mongo_gateway_fast_client_matrix_2026-06-04.md`
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
