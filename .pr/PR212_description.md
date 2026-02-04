## Summary
Checkpoint-focused perf harness + targeted mmapped prefetch + allocation locality tweaks.

### Changes
- Add `treemap checkpoint` command to run only the durable checkpoint boundary (with optional `-cpuprofile`).
- Add `treemap checkpoint-bench` to run a deterministic write workload (batch_write + random_write) and then checkpoint, with optional `-pause-before-checkpoint` so you can attach `perf` to just the checkpoint window.
- Add pager option `PagerPrefetchOnRead` (best-effort prefetch hints via `PrefetchPage`) and unified-bench flag `-treedb-pager-prefetch-on-read`.
- Improve leaf split allocation locality by hinting from the current leaf page (`target.PageID()`), not the first leaf in the batch.

## How to use (server)
Option A: checkpoint-only on an existing DB dir
- `perf stat -e cycles,instructions,cache-misses,LLC-load-misses,dTLB-load-misses,page-faults -- ./bin/treemap checkpoint <dbdir> -rw`

Option B: write workload + checkpoint, with a pause for perf attach
- `./bin/treemap checkpoint-bench <dbdir> -rw -reset -fast -keys 2000000 -valsize 128 -batchsize 1000 -flush-threshold $((2<<30)) -pause-before-checkpoint 5s`
  (during the pause, attach perf to the printed pid)

Knobs:
- `-pager-prefetch-on-read`
- `-pager-mmap-populate` (from PR211)
- `-maintenance-ops-per-coalesce=<K>`

## Testing
- `go test ./... -count=1`

## Benchmarks

> Note: this PR branch was previously missing ~98 commits from `main`, including PR246 which changes the `fast` profile defaults (notably TreeDB `-flush-threshold`). That made local checkouts look like a huge regression due to very large checkpoint times. PR211 and PR212 have now been updated by merging `main` into the PR211 base branch (and PR211 into PR212).

Command:
- `make unified-bench && ./bin/unified-bench -test batch_write,random_write,batch_delete -dbs treedb -profile fast -keys 5000000 -format markdown -checkpoint-between-tests`

Results (macOS arm64, single runs):

`main` @ `8d1c891eaa`:
```text
        Test         TreeDB
------------  -------------
 Batch Write      2,513,100
Random Write      2,103,187
Batch Delete      3,368,751
```
```text
 Before Test    TreeDB
------------  --------
 Batch Write      58µs
Random Write  981.06ms
Batch Delete     1.30s
```

PR211+PR212 stack @ `55e113bc62`:
```text
        Test         TreeDB
------------  -------------
 Batch Write      2,528,934
Random Write      2,121,660
Batch Delete      3,375,150
```
```text
 Before Test    TreeDB
------------  --------
 Batch Write      52µs
Random Write  998.58ms
Batch Delete     1.30s
```
