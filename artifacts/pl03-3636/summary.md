# PL-03 CompactStorage shared-audit benchmark

Issue: `snissn/gomap#3636`

- Base: `7d210c971ed3df543d9fe00fd5016b5d82cc7c0d`
- Head source: branch `codex/pl03-compact-shared-audit`
- Host: Linux 6.8.0-124-generic, Intel Core i5-11400F, Go 1.26.0
- Workspace mode: `GOWORK=off`

## Fixture and method

`BenchmarkCompactStorageSharedAudit` creates 4,096 sorted keys whose 256-byte
values are forced into a retained persistent value-log segment. Index outer
leaves are enabled and written through a leaf-page log, so each audit must walk
the index, decode outer-leaf pages, count logical value pointers, account leaf
generations, and recompute filesystem debt. Planner caches are cleared outside
the timed region before each operation. The same benchmark source was copied
unchanged into a detached worktree at the pinned base.

Both base and head were measured serially with:

```sh
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkCompactStorageSharedAudit$' -benchmem -count=5
```

## Results

| Metric (median, count=5) | Base | Head | Change |
| --- | ---: | ---: | ---: |
| Audit time | 897.9 us/op | 451.3 us/op | -49.74% |
| Allocated bytes | 427.0 KiB/op | 328.4 KiB/op | -23.09% |
| Allocations | 422 allocs/op | 314 allocs/op | -25.59% |
| Legacy reference scans | 1/op | 0/op | -100% |
| Legacy live-byte scans | 1/op | 0/op | -100% |
| Legacy leaf scans | 2/op | 0/op | -100% |

The time gate requires at least a 30% median reduction and no allocation
regression greater than 10%. The measured result passes both gates.

## Profiles

Matched CPU and allocation-space profiles used a three-second benchmark window:

```sh
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkCompactStorageSharedAudit$' -benchtime=3s -count=1 \
  -cpuprofile=cpu.pprof -memprofile=mem.pprof
```

The base CPU profile is dominated by repeated iterator/live-byte collection.
The head profile contains one direct recursive audit walk; remaining hot work is
the exact grouped-record set and leaf value projection accounting.

Artifacts:

- [`base.txt`](base.txt) and [`head.txt`](head.txt): count=5 raw benchmark output
- [`benchstat.txt`](benchstat.txt): base/head comparison
- [`base_profile.txt`](base_profile.txt) and [`head_profile.txt`](head_profile.txt): profile-run output
- [`base_cpu.pprof`](base_cpu.pprof) and [`head_cpu.pprof`](head_cpu.pprof): CPU profiles
- [`base_mem.pprof`](base_mem.pprof) and [`head_mem.pprof`](head_mem.pprof): allocation profiles
- [`base_cpu_top.txt`](base_cpu_top.txt) and [`head_cpu_top.txt`](head_cpu_top.txt): CPU profile summaries
- [`base_mem_top.txt`](base_mem_top.txt) and [`head_mem_top.txt`](head_mem_top.txt): allocation profile summaries
