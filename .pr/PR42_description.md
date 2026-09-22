Builds on:
- PR98 (valuelog writer appendBuf EncodeAll): https://github.com/snissn/gomap/pull/98
- PR97 (dict harness + regression tests): https://github.com/snissn/gomap/pull/97

## Summary
- deps: bump `github.com/snissn/compress` to `7ad45194ecdc` (master) which includes a zstd **dictionary encoder reset** optimization.
- vlogprof: extend warmup in `TestProfileVlogDict_*` until the dict is applied (fixes flake exposed by higher throughput).

## Benchmarks
### End-to-end (mode3 write-to-disk, 1KiB ultra)
Command (base vs this branch):
- dict-on:
  - `VLOG_DICT_CPUPROFILE=/tmp/pr100_base_dicton_cpu.pprof  go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=3 -v`
  - `VLOG_DICT_CPUPROFILE=/tmp/pr100_after_dicton_cpu.pprof go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=3 -v`
- dict-off:
  - `VLOG_DICT_DISABLE=1 VLOG_DICT_CPUPROFILE=/tmp/pr100_base_dictoff_cpu.pprof  go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=3 -v`
  - `VLOG_DICT_DISABLE=1 VLOG_DICT_CPUPROFILE=/tmp/pr100_after_dictoff_cpu.pprof go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=3 -v`
- dict-off + journal compress:
  - `VLOG_DICT_DISABLE=1 VLOG_DICT_CPUPROFILE=/tmp/pr100_base_journalcompress_cpu.pprof  go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=3 -v -args -treedb-journal-compress`
  - `VLOG_DICT_DISABLE=1 VLOG_DICT_CPUPROFILE=/tmp/pr100_after_journalcompress_cpu.pprof go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=3 -v -args -treedb-journal-compress`

Write ops/sec (3 runs):
- dict-on: `514,795 / 467,180 / 515,235` → `571,427 / 573,285 / 571,112` (median +11.0%)
- dict-off: `368,311 / 286,275 / 322,429` → `468,742 / 303,008 / 428,205`
- dict-off + `-treedb-journal-compress`: `429,341 / 468,941 / 570,654` → `468,775 / 643,383 / 429,083`

### Value-log writer (CPU, dict encode)
Command:
- `go test ./TreeDB/internal/valuelog -run '^$' -bench 'BenchmarkValueLogDictCompressibilityCPU_NoIO/valsize=1024/(highly_compressible_tail64|incompressible)/dict_(on|off)$' -benchmem -count=6`

Result (benchstat):
- `dict_on` + `highly_compressible_tail64`: **-14.00% sec/op** (**+16.28% B/s**), p=0.002
- `dict_off`: ~no significant change (as expected)
