# PR6: recovery hardening + fuzz + crash edge cases

## Summary
- add commitlog/valuelog fuzzers with bounded allocations and error-only failures
- extend recovery tests for multi-lane ordering, partial batches, and missing dict failures
- keep crash recovery coverage updated for truncation handling

## Tests
- `go test ./TreeDB/internal/commitlog -run Fuzz -fuzz=Fuzz -fuzztime=10s`
- `go test ./TreeDB/internal/valuelog -run Fuzz -fuzz=Fuzz -fuzztime=10s`
- `go test ./TreeDB -run "CrashRecovery|Recovery" -count=1`
- `go test ./... -count=1`
- `go test ./... -race -count=1`
  - Note: macOS linker warning building `cmd/unified_bench.test`: malformed `LC_DYSYMTAB` (matches prior runs).

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
Batch Write / TreeDB = 4,854,506
# unified_bench suite: lanes_probe

- lanes requested: 0
- keys: 100,000
- valsize: 128
- batchsize: 1000
- ops/sec: 4,854,506
- wall time: 143ms
- index.db bytes: 67,108,864
- wal bytes: 0
```
