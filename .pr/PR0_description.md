# PR0: unified bench lane probe harness

## Summary
- add lanes_probe suite hook + placeholder TreeDB lane flag
- add deterministic lane probe suite for TreeDB only
- document current benchmark entry points

## Benchmark entry points
- TreeDB cached bench: `go test ./TreeDB -run '^$' -bench BenchmarkWriteParallelCached` (`TreeDB/bench_test.go:121`)
- TreeDB backend bench: `go test ./TreeDB/db -run '^$' -bench BenchmarkBatch` (`TreeDB/db/bench_test.go:167`)
- unified bench lanes probe: `go run ./cmd/unified_bench -suite lanes_probe -dbs treedb -keys 100000 -valsize 128 -batchsize 1000`

## Tests
- `go test ./cmd/unified_bench -count=1`
- `go test ./... -count=1`

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
Batch Write / TreeDB = 3,669,916
# unified_bench suite: lanes_probe

- lanes requested: 0
- keys: 100,000
- valsize: 128
- batchsize: 1000
- ops/sec: 3,669,916
- wall time: 136ms
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
Batch Write / TreeDB = 5,125,971
# unified_bench suite: lanes_probe

- lanes requested: 2
- keys: 100,000
- valsize: 128
- batchsize: 1000
- ops/sec: 5,125,971
- wall time: 94ms
- index.db bytes: 67,108,864
- wal bytes: 0
```
