## Summary
Checkpoint-focused perf harness + targeted mmapped prefetch + allocation locality tweaks.

### Changes
- Add `treemap checkpoint` command to run only the durable checkpoint boundary (with optional `-cpuprofile`).
- Add `treemap checkpoint-bench` to run a deterministic write workload (batch_write + random_write) and then checkpoint, with optional `-pause-before-checkpoint` so you can attach `perf` to just the checkpoint window.
- Add pager option `PagerPrefetchOnRead` (madvise WILLNEED once per chunk on first read) and unified-bench flag `-treedb-pager-prefetch-on-read`.
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
