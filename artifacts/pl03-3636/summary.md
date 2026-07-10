# PL-03 CompactStorage shared-audit benchmark

Issue: `snissn/gomap#3636`

- Base: `9bb109dd0ed643448c065951eb72cdab99c47ac6`
- Head source: `2f1adad574d5b2b3930bb9cff71c696fab6d71f6` (branch
  `codex/pl03-compact-shared-audit`)
- Host: Linux 6.8.0-124-generic, Intel Core i5-11400F, Go 1.26.0
- Workspace mode: `GOWORK=off`

## Fixture and method

`BenchmarkCompactStorageSharedAudit` creates 4,096 sorted keys whose 256-byte
values are forced into a retained persistent value-log segment. Index outer
leaves are enabled and written through a leaf-page log, so each audit must walk
the index, decode outer-leaf pages, count logical value pointers, account leaf
generations, and recompute filesystem debt. Planner caches are cleared outside
the timed region before each operation. Returned audit counters are accumulated
only after the benchmark timer stops.

The benchmark source was copied unchanged into a detached worktree at the
pinned base. Both copies had SHA-256
`9c93177e54ee9a45d799f7273f2ef6c34b852ea200d2939bd2a9164410c40e01`.
Both base and head were measured serially with:

```sh
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkCompactStorageSharedAudit$' -benchmem -count=5
```

## Results

| Metric (median, count=5) | Base | Head | Change |
| --- | ---: | ---: | ---: |
| Audit time | 868.1 us/op | 593.2 us/op | -31.66% |
| Allocated bytes | 427.2 KiB/op | 461.7 KiB/op | +8.08% |
| Allocations | 426 allocs/op | 339 allocs/op | -20.42% |
| Legacy reference scans | 1/op | 0/op | -100% |
| Legacy live-byte scans | 1/op | 0/op | -100% |
| Legacy leaf scans | 2/op | 0/op | -100% |

The time gate requires at least a 30% median reduction and no allocated-byte
regression greater than 10%. The measured result passes both gates.

## Shared counters

The base does not expose `CompactStorageStats.Audit`, so the byte-identical
benchmark reports `audit_unavailable/op=1` and zero shared counters there.
Head reports these per-operation values in every raw sample:

| Counter | Head |
| --- | ---: |
| Pages visited | 2 |
| Pointer projections | 4,096 |
| Grouped-record dedupe hits | 0 |
| Physical bytes read | 131,072 |
| Shared scans | 1 |
| Structural reuse hits | 0 |
| Structural reuse misses | 1 |
| Revalidation retries | 0 |
| Last reuse-miss reason | `cold` (1/op) |

The zero grouped-dedupe result is expected for this fixture because each
logical value pointer refers to a distinct record. The counter remains emitted
so grouped fixtures and future benchmark changes expose dedupe behavior without
harness changes.

## Profiles

Matched CPU and allocation-space profiles used a three-second benchmark window:

```sh
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkCompactStorageSharedAudit$' -benchtime=3s -count=1 \
  -cpuprofile=cpu.pprof -memprofile=mem.pprof
```

The base CPU profile is dominated by repeated iterator/live-byte collection.
The head profile contains one physical recursive audit walk plus memoized
maintenance-root projection replay. Remaining hot work is the per-page pointer
aggregate maps and grouped-record live-byte accounting.

Artifacts:

- [`base.txt`](base.txt) and [`head.txt`](head.txt): count=5 raw benchmark output
- [`benchstat.txt`](benchstat.txt): base/head comparison including shared counters
- [`base_profile.txt`](base_profile.txt) and [`head_profile.txt`](head_profile.txt): profile-run output
- [`base_cpu.pprof`](base_cpu.pprof) and [`head_cpu.pprof`](head_cpu.pprof): CPU profiles
- [`base_mem.pprof`](base_mem.pprof) and [`head_mem.pprof`](head_mem.pprof): allocation profiles
- [`base_cpu_top.txt`](base_cpu_top.txt) and [`head_cpu_top.txt`](head_cpu_top.txt): CPU profile summaries
- [`base_mem_top.txt`](base_mem_top.txt) and [`head_mem_top.txt`](head_mem_top.txt): allocation profile summaries
