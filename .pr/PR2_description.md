# PR2: dictdb separate TreeDB instance (lagging dict rule)

## Summary
- add dictdb store with hash/bytes/current key scheme, dedup + immutability, and tests
- open dictdb under `<root>/dictdb` before maindb and wire the store into cached writes to freeze dictID per batch
- update layout-aware tests/bench probes for the new `<root>/maindb` paths

## Tests
- `go test ./TreeDB/internal/dictdb -count=1`
- `go test ./TreeDB -run TestCrashRecovery_DurabilityTiers -count=1`
- `go test ./... -count=1`
- `go test ./... -race -count=1`

## unified_bench output
`go run ./cmd/unified_bench -suite lanes_probe -dbs treedb -keys 100000 -valsize 128 -batchsize 1000`
```text
Unified Benchmark Runner
========================
Profile:     (none/custom)
Settings:    keys=100000 valsize=128 batchsize=1000
             range_queries=200 range_span=100
DBs:         treedb
Tests:       all
Seed:        1

       Test         TreeDB
-----------  -------------
Batch Write              -
Batch Write / TreeDB = 5,198,350
# unified_bench suite: lanes_probe

- lanes requested: 0
- keys: 100,000
- valsize: 128
- batchsize: 1000
- ops/sec: 5,198,350
- wall time: 95ms
- index.db bytes: 67,108,864
- wal bytes: 0
```

`go run ./cmd/unified_bench -suite lanes_probe -dbs treedb -keys 100000 -valsize 128 -batchsize 1000 -treedb-journal-lanes 2`
```text
Unified Benchmark Runner
========================
Profile:     (none/custom)
Settings:    keys=100000 valsize=128 batchsize=1000
             range_queries=200 range_span=100
DBs:         treedb
Tests:       all
Seed:        1

       Test         TreeDB
-----------  -------------
Batch Write              -
Batch Write / TreeDB = 5,106,611
# unified_bench suite: lanes_probe

- lanes requested: 2
- keys: 100,000
- valsize: 128
- batchsize: 1000
- ops/sec: 5,106,611
- wall time: 94ms
- index.db bytes: 67,108,864
- wal bytes: 0
```
