# PR7: index work flags + plumbing (no default change)

## Summary
- add opt-in index flags for columnar leaves and internal base-delta experiments
- plumb flags through zipper/bulk/index rebuild paths without changing defaults
- add columnar leaf encoding path behind the flag with a smoke test

## Tests
- `go test ./TreeDB/node -count=1`
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
Batch Write / TreeDB = 5,014,846
# unified_bench suite: lanes_probe

- lanes requested: 0
- keys: 100,000
- valsize: 128
- batchsize: 1000
- ops/sec: 5,014,846
- wall time: 116ms
- index.db bytes: 67,108,864
- wal bytes: 0
```
