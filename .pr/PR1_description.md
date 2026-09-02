# PR1: journal durability abstraction + crash tests

## Summary
- rename WAL durability concept to journal durability and document payload-before-commit ordering
- fail fast on missing value-log payloads during WAL replay
- add missing-commit/missing-payload recovery tests and split value-log crash tier coverage

## Tests
- `go test ./TreeDB/caching -run TestUnifiedWAL -count=1`
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
Batch Write / TreeDB = 3,253,157
# unified_bench suite: lanes_probe

- lanes requested: 0
- keys: 100,000
- valsize: 128
- batchsize: 1000
- ops/sec: 3,253,157
- wall time: 133ms
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
Batch Write / TreeDB = 5,153,577
# unified_bench suite: lanes_probe

- lanes requested: 2
- keys: 100,000
- valsize: 128
- batchsize: 1000
- ops/sec: 5,153,577
- wall time: 89ms
- index.db bytes: 67,108,864
- wal bytes: 0
```
