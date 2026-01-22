Builds on PR97 (dict harness + regression tests):
- https://github.com/snissn/gomap/pull/97

## Change
Bump `github.com/snissn/compress` to `7ad45194ecdc` (master) which includes a zstd **dictionary encoder reset** optimization.

## Benchmarks
### Value-log writer (CPU, dict encode)
Command:
- `go test ./TreeDB/internal/valuelog -run '^$' -bench 'BenchmarkValueLogDictCompressibilityCPU_NoIO/valsize=1024/(highly_compressible_tail64|incompressible)/dict_(on|off)$' -benchmem -count=6`

Result (benchstat):
- `dict_on` + `highly_compressible_tail64`: **-14.00% sec/op** (**+16.28% B/s**), p=0.002
- `dict_off`: ~no significant change (as expected)

### End-to-end (write-to-disk) suite
Command:
- `go run ./cmd/unified_bench -suite vlog_dict -batchsize 5000`

Key rows (dict-on, 1KiB values, batchsize=5000):
- mode3 `ultra_compressible_repeat`: 406,195 → 443,290 ops/s (+9.1%)
- mode3 `highly_compressible_tail64`: 325,232 → 367,232 ops/s (+12.9%)
- mode4 `ultra_compressible_repeat`: 734,379 → 754,280 ops/s (+2.7%)
- mode4 `highly_compressible_tail64`: 621,790 → 636,787 ops/s (+2.4%)

