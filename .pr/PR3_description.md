# PR3: RID join for CommitLog/ValueLog + basic recovery v1

## Summary
- add commitlog/valuelog v1 formats (CRC + RIDs) and wire cached writes to emit value-log payloads before commit intents
- implement two-pass recovery (scan valuelog -> replay commitlog) and update crash-recovery tests for truncation + RID join correctness
- clarify PR process guidance in the slab-optimization runbook (GH CLI + unified_bench outputs + sequential bases)

## Tests
- `go test ./TreeDB/internal/commitlog -count=1`
- `go test ./TreeDB/internal/valuelog -count=1`
- `go test ./TreeDB -run TestCrashRecovery -count=1`
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
Dataset Write (Random) / TreeDB = 1,329,947
Dataset Write (Random) / LevelDB = 454,717
Update ForkChoice (Dataset Keys) / TreeDB = 218,136
Update ForkChoice (Dataset Keys) / LevelDB = 136,832
Random Read (Dataset Keys) / TreeDB = 904,868
Random Read (Dataset Keys) / LevelDB = 738,341
# unified_bench

- keys: 100,000
- valsize: 128
- batchsize: 1000
- range-queries: 200
- range-span: 100
```
