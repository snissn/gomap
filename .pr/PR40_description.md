Builds on PR97 (profiling harness + corruption regression):
- https://github.com/snissn/gomap/pull/97

## Goal (Branch 3: Value-Log Writer)
Reduce dict-on value-log writer copy amplification for grouped frames without changing on-disk format or durability semantics.

## Change summary
- `TreeDB/internal/valuelog/writer.go`: dict-on `AppendFrameWithStatsInto` now builds the record directly into the writer `appendBuf` (when it fits) and runs `zstd.EncodeAll` with `appendBuf` as the destination, avoiding an extra `encoded -> appendBuf` copy.
- Keeps existing fallback paths for large frames / non-appendBuf cases.

## Perf (vlogprof harness, ultra_compressible_repeat, 1KiB values)
Mode3 dict-on (journal on):
- Command:
  - `VLOG_DICT_CPUPROFILE=/tmp/vlog_dict_mode3_ultra_1024_before_cpu.pprof go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=1 -v`
  - `VLOG_DICT_CPUPROFILE=/tmp/vlog_dict_mode3_ultra_1024_after_cpu.pprof  go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=1 -v`
- Before (base/PR97 branch): write ops/s=469,044 MB/s=458.1
- After (this branch):     write ops/s=514,717 MB/s=502.7

Mode4 dict-on (journal off): no meaningful change expected (memtable dominates in below-cutoff profiles).

## Validation
- `go test ./TreeDB/internal/valuelog -run TestDictAppendReadRoundTrip -count=1`
- `go test ./... -count=1`

