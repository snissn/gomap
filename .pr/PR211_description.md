## Summary
Try to reduce checkpoint page-fault overhead (mmap-backed TreeDB index) via best-effort mmap hints:

- `madvise(MADV_HUGEPAGE)` for each mmapped chunk (Linux)
- optional `MAP_POPULATE` support for index.db mmaps (Linux), wired through TreeDB `Options` and unified-bench flag

Both are best-effort / platform-gated and should be safe no-ops when unsupported.

## Why
Checkpoint after random writes is dominated by minor faults / dTLB pressure while rebuilding leaves (mmap-backed index). Hints that reduce fault cost or improve TLB coverage may move wall time.

## Bench (mikers)
Baseline (already on main before this PR):
- checkpoint after random_write ("Batch Delete" row): ~3.67s
- page-faults: 931,360

With `madvise(MADV_HUGEPAGE)` only:
- checkpoint after random_write: 3.55s
- page-faults: 923,779

With `madvise(MADV_HUGEPAGE)` + `-treedb-pager-mmap-populate`:
- checkpoint after random_write: 3.63s
- page-faults: 882,432

Command:
- `perf stat -e cycles,instructions,cache-misses,LLC-load-misses,dTLB-load-misses,page-faults -- ./bin/unified-bench -test batch_write,random_write,batch_delete -dbs treedb -profile fast -keys 2000000 -format markdown -checkpoint-between-tests`
- `perf stat ... -- ./bin/unified-bench ... -treedb-pager-mmap-populate`

## Notes
- MAP_POPULATE is exposed as `Options.PagerMmapPopulate` and unified-bench flag `-treedb-pager-mmap-populate`.
- Next data point to collect: same perf stat with MAP_POPULATE enabled to see whether page-faults and checkpoint time drop further.
