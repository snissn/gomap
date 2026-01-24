# PR5: parallel active journal lanes + durability tickets

## Summary
- add `JournalLanes` option plumbed through TreeDB public and cached options; adapter flag now configures lane count
- refactor cached-mode WAL/value-log writers into per-lane state with lane-aware segment naming and tracking
- add commitlog sequence ordering for cross-lane recovery and update recovery parsing for lane segments

## Tests
- `go test ./TreeDB/caching -run "Race|Rotate|Consistency" -count=1`
- `go test ./... -count=1`
- `go test ./... -race -count=1`
  - Note: macOS linker warning building `cmd/unified_bench.test`: malformed `LC_DYSYMTAB` (matches prior runs).

## unified_bench output
`go run ./cmd/unified_bench -suite lanes_probe -dbs treedb -keys 500000 -valsize 128 -batchsize 1000 -treedb-journal-lanes 1`
```text
Unified Benchmark Runner
========================
Profile:     (none/custom)
Settings:    keys=500000 valsize=128 batchsize=1000
             range_queries=200 range_span=100
DBs:         treedb
Tests:       all
Seed:        1

       Test         TreeDB
-----------  -------------
Batch Write              -
Batch Write / TreeDB = 4,815,336
# unified_bench suite: lanes_probe

- lanes requested: 1
- keys: 500,000
- valsize: 128
- batchsize: 1000
- ops/sec: 4,815,336
- wall time: 441ms
- index.db bytes: 134,217,728
- wal bytes: 0
```

`go run ./cmd/unified_bench -suite lanes_probe -dbs treedb -keys 500000 -valsize 128 -batchsize 1000 -treedb-journal-lanes 2`
```text
Unified Benchmark Runner
========================
Profile:     (none/custom)
Settings:    keys=500000 valsize=128 batchsize=1000
             range_queries=200 range_span=100
DBs:         treedb
Tests:       all
Seed:        1

       Test         TreeDB
-----------  -------------
Batch Write              -
Batch Write / TreeDB = 4,566,634
# unified_bench suite: lanes_probe

- lanes requested: 2
- keys: 500,000
- valsize: 128
- batchsize: 1000
- ops/sec: 4,566,634
- wall time: 463ms
- index.db bytes: 134,217,728
- wal bytes: 0
```
