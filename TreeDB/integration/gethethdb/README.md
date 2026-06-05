# TreeDB geth `ethdb` adapter

This nested module provides a reusable adapter from TreeDB to go-ethereum's
`ethdb.KeyValueStore` / `ethdb.Batch` interfaces without adding a go-ethereum
dependency to gomap's root module or core TreeDB packages.

## Package

```go
import gethethdb "github.com/snissn/gomap/TreeDB/integration/gethethdb"
```

Primary APIs:

- `Open(path string, options *OpenOptions) (*Database, error)`
- `OpenWithOptions(opts treedb.Options) (*Database, error)`
- `Wrap(db *treedb.DB) *Database`

`Open(nil options)` defaults to `treedb.ProfileCommandWALDurable`. Writable
opens reject non-command-WAL TreeDB options; this adapter is intended for geth
persistent hot-KV durability/recovery through TreeDB command WAL.

## Semantics

- No adapter-side key codec is used. TreeDB's native raw-KV parity handles empty
  keys, nil point keys, and nil values.
- DB-level `DeleteRange` calls public `TreeDB.DB.DeleteRange` directly.
- Nil range bounds remain unbounded; empty range bounds remain concrete empty
  byte-string bounds.
- `NewIterator(prefix, start)` iterates `prefix` keys starting at
  `prefix || start`.
- `SyncKeyValue` maps to `TreeDB.DB.Checkpoint()`.
- `Compact` maps to TreeDB's current whole-index `CompactIndex()` primitive;
  geth's range arguments are accepted for interface compatibility but do not
  narrow the TreeDB compaction.

## Validation

Run from this directory:

```sh
go test ./... -count=1
go test ./... -run 'TestCommandWALReopen|TestDatabaseSuite' -count=1
go test ./... -bench 'BenchmarkAdapterVsDirect/(Put|Get|BatchWrite|Iterator|DeleteRange|BatchDeleteRange)' -benchtime=100x -count=1
```

The module imports `github.com/ethereum/go-ethereum/ethdb` and currently uses a
`replace` to validate against the `snissn/go-ethereum-nitro` fork while keeping
the standard geth module import path.
