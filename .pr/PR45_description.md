Builds on PR100 (compress bump + vlogprof warmup fix):
- https://github.com/snissn/gomap/pull/100

## Goal (Hotspot: `largePtrMap.SetString`)
Try to reduce lock overhead in the memtable pointer map (`largePtrMap`) on the mode3 write path.

## Change (reverted)
- Attempt: switch `largePtrMap` from `sync.RWMutex` to `sync.Mutex` and preallocate map capacity.
- Result: no consistent improvement in end-to-end write throughput; reverted in this PR (net diff is nil).

## Benchmarks (mode3 vlogprof, ultra_compressible_repeat, 1KiB values)
Commands:
- dict-on:
  - `VLOG_DICT_CPUPROFILE=/tmp/pr45_base_dicton_cpu.pprof  go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=3 -v`
  - `VLOG_DICT_CPUPROFILE=/tmp/pr45_after_dicton_cpu.pprof go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=3 -v`

Write ops/sec (3 runs):
- dict-on: `572,784 / 573,508 / 514,117` → `571,808 / 515,036 / 514,061` (no clear win; high variance)

## Notes
- pprof focus on `largePtrMap` stayed ~flat (mapassign dominates vs lock ops), so changing the lock primitive alone is unlikely to help.
