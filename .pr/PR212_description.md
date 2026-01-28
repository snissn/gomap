## Summary
Checkpoint-focused perf harness + targeted mmapped prefetch + allocation locality tweaks.

### Changes
- Add `treemap checkpoint` command to run only the durable checkpoint boundary (with optional `-cpuprofile`).
- Add pager option `PagerPrefetchOnRead` (madvise WILLNEED once per chunk on first read) and unified-bench flag `-treedb-pager-prefetch-on-read`.
- Improve leaf split allocation locality by hinting from the current leaf page (`target.PageID()`), not the first leaf in the batch.

## How to use (server)
1) Produce a DB state (keep it):
- `./bin/unified-bench -test batch_write,random_write,batch_delete -dbs treedb -profile fast -keys 2000000 -format markdown -checkpoint-between-tests -keep`

2) Run checkpoint-only under perf:
- `perf stat -e cycles,instructions,cache-misses,LLC-load-misses,dTLB-load-misses,page-faults -- ./bin/treemap checkpoint /tmp/bench-treedbXXXX/maindb -rw`

Optional knobs:
- `-pager-prefetch-on-read`
- `-pager-mmap-populate` (from PR211)
- `-maintenance-ops-per-coalesce=<K>`

## Testing
- `go test ./... -count=1`
