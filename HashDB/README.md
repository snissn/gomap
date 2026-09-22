# HashDB

HashDB is a sibling storage engine in this repository. It is an mmap-backed hash
index with an append-only slab value log, optimized for high-throughput point
lookups and random I/O experiments.

TreeDB is the main current focus of the repository. HashDB remains useful for
comparison, Redis-wrapper experiments, hash-index research, and workloads that
do not need ordered scans.

## Design

HashDB is implemented as:

- an mmap-backed hash index with SwissHash-style control bytes and key metadata;
- append-only slab segment files (`slab-*`) that store key/value records;
- optional sharding for concurrent point operations;
- optional per-shard write-back cache WAL for sharded cache durability
  experiments.

HashDB does not provide ordered iteration. `ForEach` walks live entries in
arbitrary order.

## Entry Points

- Sharded, thread-safe store: `hashdb.Open(dir)` or
  `hashdb.OpenWithShards(dir, n)`.
- Single-shard store: `hashdb.OpenSingle(dir)`.
- Redis protocol wrapper: `HashDB/redisserver`.
- BTree-on-HashDB comparison engine: `HashDB/BTreeOnHashDB`.

## Minimal Usage

```go
package main

import (
	"fmt"
	"log"

	hashdb "github.com/snissn/gomap/HashDB"
)

func main() {
	db, err := hashdb.Open("/tmp/hashdb-example")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.PutSync([]byte("key"), []byte("value")); err != nil {
		log.Fatal(err)
	}
	value, err := db.Get([]byte("key"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value))
}
```

## Durability

HashDB durability is slab-log based:

- `Put` / `Delete`: fast, best-effort durability.
- `PutSync` / `DeleteSync`: fsync the active slab segment.
- `ApplyBatchSync`: crash-atomic batch mutation with explicit slab-log markers.

The mmap hash index is derived state. After an unclean shutdown, HashDB rebuilds
the index by scanning slab segments and truncating torn tail records.

For the sharded store, commits are atomic per shard but not across shards.

## Benchmarking

HashDB is covered by the unified benchmark harness alongside TreeDB, Badger, and
LevelDB:

```sh
make unified-bench
./bin/unified-bench
```

HashDB-specific benchmark binaries:

```sh
make build-hashdb
./bin/hashdb-benchmark --help
./bin/hashdb-shardbench
./bin/hashdb-loadfactorbench
./bin/hashdb-resizebench
```

Update the generated benchmark snapshot below:

```sh
make bench-readme
```

`make bench-readme` writes the unified benchmark markdown into this file so the
TreeDB root README can stay focused on the current TreeDB/YCSB headline.

<!-- BENCHMARK_START -->
_No generated benchmark snapshot is checked in here yet. Run `make bench-readme`
to insert the current unified benchmark markdown for this host._
<!-- BENCHMARK_END -->

## More Docs

- `../docs/HASHDB_CONCEPTS.md`
- `../docs/HASHDB_TUNING.md`
- `../docs/HASHDB_SNAPSHOT.md`
- `../docs/contracts/README.md`
