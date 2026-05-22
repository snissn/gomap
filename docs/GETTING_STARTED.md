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
- Optional race check: `make test-race`
- Vet: `make vet`
- Build useful binaries: `make build`
- Optional: install a local `gofmt` pre-commit hook: `make hooks`

## Minimal Usage

### TreeDB (recommended default: cached mode)

```go
package main

import (
	"log"

	treedb "github.com/snissn/gomap/TreeDB"
)

func main() {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, "./db")
	db, err := treedb.Open(opts)
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

	// Best-effort write:
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		log.Fatal(err)
	}

	// Durable write:
	if err := db.PutSync([]byte("k"), []byte("v2")); err != nil {
		log.Fatal(err)
	}
}
```

HashDB snapshots:
- `docs/HASHDB_SNAPSHOT.md`

## Viewing GoDocs

Local:

- Package docs: `go doc github.com/snissn/gomap/TreeDB` (or `.../HashDB`)
- List exported identifiers: `go doc -all github.com/snissn/gomap/TreeDB`

Online:

- pkg.go.dev will render docs once the module is indexed (requires a recognized OSS license; see `LICENSE`).

## Unified Bench

Build and run:

- `make unified-bench`
- `./bin/unified-bench -keys 1000000`

Profile + analyze (recommended):

- `OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)`
- `./bin/unified-bench -keys 1000000 -profile-dir "$OUT" -progress=false`
- `make benchprof`
- `./bin/benchprof -profiles-dir "$OUT"`

See:
- `cmd/unified_bench/README.md`
- `cmd/benchprof/README.md`
