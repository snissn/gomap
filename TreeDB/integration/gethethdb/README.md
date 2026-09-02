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
persistent hot-KV durability/recovery through TreeDB command WAL. When callers
leave command-WAL growth knobs at zero, the adapter uses a 256 MiB command-WAL
frame cap, a 256 MiB active-segment rotation target, and a 512 MiB
auto-checkpoint byte trigger. The frame cap lets geth's large beacon
skeleton-header batches fit in one durable frame; the separate rotation and
checkpoint knobs keep active command-WAL growth bounded, including read-only
inspection of an existing command-WAL directory.

## Semantics

- No adapter-side key codec is used. TreeDB's native raw-KV parity handles empty
  keys, nil point keys, and nil values.
- DB-level `DeleteRange` calls public `TreeDB.DB.DeleteRange` directly.
- Nil range bounds remain unbounded; empty range bounds remain concrete empty
  byte-string bounds.
- `NewIterator(prefix, start)` iterates `prefix` keys starting at
  `prefix || start`.
- `SyncKeyValue` maps to `TreeDB.DB.Checkpoint()`.
- `Compact(nil, nil)` maps to TreeDB's high-level `CompactStorage` full-storage
  compaction sequence.
- Bounded `Compact(start, limit)` calls with non-nil `limit` are accepted as
  advisory no-ops because TreeDB does not currently expose geth-style
  range-scoped compaction. A nil `limit` also handles the final tail range in
  geth's 16/256-range compaction sweeps and runs one full TreeDB storage
  compaction.

## Validation

Run from this directory:

```sh
go test ./... -count=1
go test ./... -run 'TestCommandWALReopen|TestDatabaseSuite' -count=1
go test ./... -bench 'BenchmarkAdapterVsDirect/(Put|Get|BatchWrite|Iterator|DeleteRange|BatchDeleteRange)' -benchtime=100x -count=1
```

## Dependency strategy

The module pins resolvable requirements at their declared module paths:

- `github.com/ethereum/go-ethereum` for the standard `ethdb` interfaces.
- `github.com/snissn/gomap` for TreeDB.

No `replace` directive is required for normal consumers. When this adapter is
imported from a geth fork whose module path is `github.com/ethereum/go-ethereum`
(such as `snissn/go-ethereum-nitro`), the fork's main module supplies the
`ethdb` package. If a downstream experiment needs a local gomap checkout or a
newer untagged gomap commit, add the `github.com/snissn/gomap` replace in that
experiment's main module or Go workspace rather than relying on this module to
propagate replaces.
