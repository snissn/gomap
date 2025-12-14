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
	"fmt"

	"log"

	"github.com/snissn/gomap/GeminiTreeDB/db"
)

func main() {
	// Open the database
	opts := db.Options{Dir: "./my-db-data"}
	database, err := db.Open(opts)
	if err != nil {
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
