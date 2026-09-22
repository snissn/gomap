# PR4: dict + dynamic-K grouped ValueLog encoding

## Summary
- add `TreeDB/internal/compression` and use it to select per-batch K for grouped frames
- switch value-log writes to grouped frames with dict IDs and wire dict lookup through reader/manager paths
- add compressible dataset test to assert stored-bytes reduction for dict-compressed ValueLog

## Tests
- `go test ./TreeDB/internal/compression -count=1`
- `go test ./TreeDB/internal/valuelog -count=1`
- `go test ./TreeDB/caching -run "Dict|K|Grouped|UnifiedWAL" -count=1`
- `go test ./... -count=1`
- `go test ./... -race -count=1`
  - Note: macOS linker warning building `cmd/unified_bench.test`: malformed `LC_DYSYMTAB` (matches prior runs).

## unified_bench output
`go run ./cmd/unified_bench -suite sload_readheavy -dbs treedb -keys 100000 -valsize 128 -batchsize 1000`
```text
Unified Benchmark Runner
========================
Profile:     (none/custom)
Settings:    keys=100000 valsize=128 batchsize=1000
             range_queries=200 range_span=100
DBs:         treedb
Tests:       all
Seed:        1

                            Test         TreeDB        LevelDB
--------------------------------  -------------  -------------
          Dataset Write (Random)              -              -
Update ForkChoice (Dataset Keys)              -              -
      Random Read (Dataset Keys)              -              -
Dataset Write (Random) / TreeDB = 1,371,346
Dataset Write (Random) / LevelDB = 455,704
Update ForkChoice (Dataset Keys) / TreeDB = 193,444
Update ForkChoice (Dataset Keys) / LevelDB = 151,531
Random Read (Dataset Keys) / TreeDB = 883,424
Random Read (Dataset Keys) / LevelDB = 755,799
# unified_bench

- keys: 100,000
- valsize: 128
- batchsize: 1000
- range-queries: 200
- range-span: 100
```
