# Summary
- Final Track #610 landing docs and perf evidence for `v1_leaflog`.
- Clarify outer-leaf mode semantics in canonical spec and user README.
- Add dedicated mode matrix doc with invariants and reproducible unified-bench workflows.

# Docs Updated
- `TreeDB/README.md`
- `TreeDB/docs/spec/README.md`
- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/docs/spec/outer-leaf-modes.md` (new)

# Tests
- `GOWORK=off go test ./TreeDB/docs/... -count=1`
- `GOWORK=off go test ./TreeDB -run 'TestReopenVerify_ValueLogRewrite_BatchedPointerSwap_ReopenParity_OuterLeaf(V2|V2FencePtr|V1LeafLog)|TestReopenVerify_ValueLogGC_OuterLeaf(V2|V2FencePtr|V1LeafLog)_ReopenParity|TestCrashRecovery_DeleteRangeWithoutTrailingSync_ReplaysCorrectKeys|TestCrashRecovery_DurabilityTiers' -count=1`

# Benchmarks (unified-bench)
## Command Set A: full suite, forced pointers
```bash
./bin/unified-bench -dbs treedb -profile fast -keys 500000 -progress=false -format markdown \
  -checkpoint-between-tests -test all \
  -treedb-index-outer-leaf-mode v1 -treedb-force-value-pointers=true

./bin/unified-bench -dbs treedb -profile fast -keys 500000 -progress=false -format markdown \
  -checkpoint-between-tests -test all \
  -treedb-index-outer-leaf-mode v1_leaflog -treedb-force-value-pointers=true
```

| Metric | `v1` | `v1_leaflog` | Delta |
| --- | ---: | ---: | ---: |
| Batch Write | 10,801,428 | 7,777,928 | -28.0% |
| Batch Write (Steady) | 553,422 | 552,181 | -0.2% |
| Batch Random | 5,029,891 | 3,377,711 | -32.8% |
| Random Read (Parallel) | 1,984,056 | 5,618,201 | +183.2% |
| Full Scan | 574,370 | 1,352,491 | +135.5% |
| Prefix Scan | 10,447,306 | 10,432,717 | -0.1% |
| `index.db` | 189 MiB | 188 MiB | -0.5% |

## Command Set B: small-value inline candidate
```bash
./bin/unified-bench -dbs treedb -profile fast -keys 500000 -valsize 1 -progress=false -format markdown \
  -checkpoint-between-tests -test batch_write,batch_write_steady,batch_random,random_read_parallel,full_scan,prefix_scan \
  -treedb-index-optimizations=false -treedb-force-value-pointers=false \
  -treedb-index-outer-leaf-mode v1

./bin/unified-bench -dbs treedb -profile fast -keys 500000 -valsize 1 -progress=false -format markdown \
  -checkpoint-between-tests -test batch_write,batch_write_steady,batch_random,random_read_parallel,full_scan,prefix_scan \
  -treedb-index-optimizations=false -treedb-force-value-pointers=false \
  -treedb-index-outer-leaf-mode v1_leaflog
```

| Metric | `v1` | `v1_leaflog` | Delta |
| --- | ---: | ---: | ---: |
| Batch Write | 13,028,127 | 12,797,711 | -1.8% |
| Batch Write (Steady) | 1,732,765 | 1,792,429 | +3.4% |
| Batch Random | 12,441,100 | 12,670,092 | +1.8% |
| Random Read (Parallel) | 24,276,306 | 23,865,763 | -1.7% |
| Full Scan | 15,155,630 | 15,212,936 | +0.4% |
| Prefix Scan | 16,497,118 | 16,349,153 | -0.9% |
| `index.db` | 43 MiB | 45 MiB | +4.7% |

# Notes
- These are pre-alpha comparative measurements for regression tracking.
- Results are workload and option sensitive; use command-set parity when comparing future changes.
