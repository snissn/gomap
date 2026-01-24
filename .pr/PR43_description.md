Builds on PR98 (Branch 3: value-log writer direct-to-appendBuf EncodeAll):
- https://github.com/snissn/gomap/pull/98

## Goal (mode3 / Branch 3 hotspot attempt)
Try removing the per-frame raw payload concatenation (`k>1`) by switching to a streaming zstd encoder write into `appendBuf` with an early-abort “no benefit” cap.

## Outcome
This regressed mode3 throughput for dict-on workloads, so the optimization commit is reverted in this PR.

## Changes
Commits:
1) `valuelog: stream multi-record dict encode (experiment)` (attempt)
2) `Revert \"valuelog: stream multi-record dict encode (experiment)\"` (revert; net code diff is nil)

## Benchmarks (vlogprof steady-state writes, mode3, 1KiB values)
Command:
- `go test -tags vlogprof ./cmd/unified_bench -run TestProfileVlogDict_Mode3_DictOn_Ultra_1024 -count=1 -v`

### Dict compression (dict on)
- Before: ops/s=513,101 MB/s=501.1
- After (experiment): ops/s=429,715 MB/s=419.6  (**regression**)

### Compression disabled (dict off)
- Before: ops/s=234,168 MB/s=228.7  (`VLOG_DICT_DISABLE=1 ...`)
- After (experiment): ops/s=223,907 MB/s=218.7

### Compression enabled (non-dict, journal zstd)
- Before: ops/s=343,115 MB/s=335.1  (`VLOG_DICT_DISABLE=1 ... -args -treedb-journal-compress`)
- After (experiment): ops/s=210,057 MB/s=205.1

Notes:
- The “journal zstd” numbers are single samples and can be noisy; the dict-on regression was consistent in quick reruns.

