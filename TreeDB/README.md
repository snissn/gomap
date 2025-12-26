# TreeDB

TreeDB is a high-performance, persistent key-value store optimized for the Cosmos SDK workload. It features a B+Tree index backed by a memory-mapped file (`index.db`) and a value log (`data-*.slab`) for storing large values efficiently.

## Features

-   **ACID Transactions:** Atomic commits using Copy-On-Write (COW) and redundant superblocks (Meta Pages).
-   **Snapshot Isolation:** Lock-free concurrent readers using Multi-Version Concurrency Control (MVCC) and Reference Counting.
-   **Hybrid Storage:**
    -   **Index:** Memory-mapped B+Tree for keys and small values.
    -   **Slabs:** Append-only log for large values (Contract code, blobs) to reduce write amplification and memory pressure.
-   **Compaction:** Background compaction mechanism ("Ghost Copy") to reclaim space from dead records in slabs.
-   **Crash Recovery:** Automatic recovery from torn writes using strict write-ordering and checksum verification.
-   **Lifecycle Management:** Safe page reclamation using a Graveyard and Reader Registry to protect active snapshots.

## Architecture

### Storage Layout
-   **Pages:** 4KB fixed-size blocks in `index.db`.
-   **Nodes:** Slotted pages supporting variable-length keys.
-   **Slabs:** Append-only files (`data-0000.slab`) storing value records with CRC checksums.

### Write Path ("The Zipper")
Writes are batched and applied using a recursive "Zipper" merge algorithm. This creates a new version of the tree path (COW) without modifying existing on-disk pages, ensuring crash safety and snapshot isolation.

### Read Path
Readers acquire a `Snapshot` which pins the version of the tree and the active slab files. This guarantees a consistent view of the database even while writers are committing new versions.

## Usage

```go
package main

import (
	"errors"
	"fmt"
	"log"

	treedb "github.com/snissn/gomap/TreeDB"
)

func main() {
	// Open the database (recommended: cached wrapper)
	opts := treedb.Options{Dir: "./my-db-data"}
	database, err := treedb.Open(opts)
	if err != nil {
		if errors.Is(err, treedb.ErrLocked) {
			log.Fatal("database is already open in another process")
		}
		log.Fatal(err)
	}
	defer database.Close()

	// Set a key-value pair
	if err := database.Set([]byte("key1"), []byte("value1")); err != nil {
		log.Fatal(err)
	}

	// Get a value
	val, err := database.Get([]byte("key1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Got: %s\n", val)

	// Iterate
	it, _ := database.Iterator(nil, nil)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		fmt.Printf("%s = %s\n", it.Key(), it.Value())
	}
	
	// Atomic Batch
	batch := database.NewBatch()
	batch.Set([]byte("k2"), []byte("v2"))
	batch.Delete([]byte("key1"))
	batch.Write() // Atomic commit
}
```

## Profiles (Durable / Fast / Bench)

If you want a simple, documented “bundle” of options, start with a profile and
then override a few workload-specific knobs:

```go
opts := treedb.OptionsFor(treedb.ProfileDurable, "./my-db-data")
opts.FlushThreshold = 128 << 20 // optional tuning
db, err := treedb.Open(opts)
```

Profiles are intended to make intent explicit:

- `ProfileDurable`: safest defaults (recommended).
- `ProfileFast`: relax durability/integrity knobs for throughput.
- `ProfileBench`: deterministic benchmarking profile (not production).

Unsafe profiles require an explicit acknowledgement:

```go
opts := treedb.OptionsFor(treedb.ProfileFast, "./my-db-data")
opts.AllowUnsafe = true
db, err := treedb.Open(opts)
```

Details: `docs/TREEDB_PROFILES.md`.

## Durability & Safety Notes

- Safe defaults keep WAL, fsync, and read checksums enabled; unsafe toggles require `AllowUnsafe`.
- With `RelaxedSync` enabled, `SetSync`/`WriteSync` are crash-consistent only (no fsync) and may not survive power loss.
- Page checksums are verified once and cached until the page is rewritten; `DisableReadChecksum` disables slab/value-log CRC checks entirely.
- `GetUnsafe` on a `Snapshot` and iterator `Key()`/`Value()` return short-lived views; use `Get`, `KeyCopy`, or `ValueCopy` for stable bytes.
- TreeDB does not provide encryption-at-rest or secure deletion; deleted data may remain on disk until compacted. Use OS/disk encryption for confidentiality.
- On-disk format is considered alpha and may change without backward-compatibility guarantees.

## Tuning (Cached Mode)

`treedb.Open` defaults to cached mode (memtable + WAL + background flush). The most important knobs:

- `Options.FlushThreshold` + `Options.MaxQueuedMemtables` (throughput vs. backlog/memory)
- Adaptive backpressure: `SlowdownBacklogSeconds`, `StopBacklogSeconds`, `MaxBacklogBytes`
- Cached-mode auto checkpointing: `BackgroundCheckpointInterval`, `BackgroundCheckpointIdleDuration`, `MaxWALBytes`
- Background pruning: `PruneInterval`, `PruneMaxPages`, `PruneMaxDuration`
- Background index vacuum: `BackgroundIndexVacuumInterval`, `BackgroundIndexVacuumSpanRatioPPM`
- Optional background slab compaction: `BackgroundCompactionInterval` + related knobs
- Optional flush build parallelism: `FlushBuildConcurrency`
- Offline index vacuum (backend index): `treedb.VacuumIndexOffline(opts)` (requires the DB to be closed)

Details: `docs/TREEDB_TUNING.md`.

### Exclusive Open (Process Lock)

TreeDB acquires an **exclusive** lock on `Options.Dir`. If another process has the database open,
`treedb.Open`/`treedb.OpenBackend` returns `treedb.ErrLocked`.

## Testing

TreeDB includes a comprehensive test suite covering unit functionality, integration, fuzzing, and crash recovery.

### Unit & Integration Tests
```bash
go test -v ./...
```

### Fuzz Testing
Model-based fuzzing verifies consistency against a simple in-memory map map model.
```bash
go test -v ./db/fuzz_test.go
```

### Crash Simulation
The `verify_crash.sh` script compiles a stress tool, runs it, kills it (`kill -9`), and verifies database integrity upon restart.
```bash
./verify_crash.sh
```

## License
MIT
