# Getting Started

## TL;DR

- `make test`
- `make build`
- `make unified-bench && ./bin/unified-bench`

## Who Is This For?

- New contributors and anyone trying to run benchmarks locally.

## Requirements

- Go `1.25+` (see `go.mod`)
- Linux/macOS recommended (TreeDB uses `mmap`)

## Local Workflow

- Format: `make fmt`
- Tests: `make test`
- Vet: `make vet`
- Build useful binaries: `make build`

## Minimal Usage

### TreeDB (recommended default: cached mode)

```go
package main

import (
	"log"

	treedb "github.com/snissn/gomap/TreeDB"
)

func main() {
	db, err := treedb.Open(treedb.Options{Dir: "./db"})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// For durability, use *Sync operations.
	if err := db.SetSync([]byte("k"), []byte("v")); err != nil {
		log.Fatal(err)
	}
}
```

### TreeDB (backend-only mode)

```go
opts := treedb.Options{Dir: "./db", Mode: treedb.ModeBackend}
db, err := treedb.Open(opts)
```

### HashDB (sharded; recommended)

```go
package main

import (
	"log"

	hashdb "github.com/snissn/gomap/HashDB"
)

func main() {
	db, err := hashdb.Open("./hashdb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		log.Fatal(err)
	}
}
```

## Unified Bench

Build and run:

- `make unified-bench`
- `./bin/unified-bench -keys 1000000`

See:
- `cmd/unified_bench/README.md`

