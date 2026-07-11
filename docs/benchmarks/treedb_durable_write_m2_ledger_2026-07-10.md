# TreeDB Durable-Write M2 Barrier Ledger (2026-07-10)

Issue: [#3656](https://github.com/snissn/gomap/issues/3656)

Parent: [#3652](https://github.com/snissn/gomap/issues/3652)

Base commit: `3e5543145e2f4fd0762b6bdeb1bbcbcb42599f6c`

This is an instrumentation and contract result. It does not change a
durability boundary and does not claim a throughput improvement.

## Result

- A dirty inline public `WriteSync` performs one command-frame append, one
  command-writer sync, one command-file `fsync`, and no value-log sync.
- A dirty forced-pointer public `WriteSync` currently performs two value-log
  file `fsync` calls (`materialization`, then `external_ref`) before one
  command-WAL write and one later command-file `fsync`.
- An unsynced `Write` contributes an append, kernel write, and logical flush.
  A following sync boundary covers the earlier command bytes already flushed
  to that file. An empty `WriteSync` also sweeps pending value-log lanes first.
- The state-shaped interval has two distinct durable operations per block. It
  does not perform two command-file syncs inside one batch `WriteSync`.
- Named top-level phases cover 99.923% or more of every focused `WriteSync`
  shape below. The matching state active-window phase sample covers 99.982%.
- The only measured M3 redundancy candidate is the second pointer-path
  value-log sync. Its focused ceiling is 1.35-1.46 ms per affected batch. It
  is not safe to remove without proving byte coverage and exclusion of an
  intervening writer or rotation.
- A rotated external reference is a distinct case: the current lane writer is
  synced first and each referenced non-current segment is then synced directly.
  M2 now attributes those direct attempts to both logical `external_ref` and
  aggregate physical value-log counters without changing the ordering.

The normative byte and ordering contract is in
`TreeDB/docs/spec/command-wal-durable-write-contract.md`.

## Environment and artifacts

The focused artifacts are in:

```text
/mnt/fast4tb/gomap-3656-m2-evidence-20260710
```

The benchmark binary reports Go 1.26.0, Linux/amd64. Trace decoding used the
matching Go 1.26.3 tool. The worktree is on `/dev/nvme1n1p1`, an ext4
filesystem mounted `rw,noatime` at `/mnt/fast4tb` on a Samsung SSD 990 PRO 4TB.
The host CPU is an Intel Core i5-11400F.

Primary artifacts:

| Artifact | Purpose |
| --- | --- |
| `benchmark_count3.txt` | all 12 shapes, 20 iterations, three runs |
| `benchmark_summary.tsv` | arithmetic means used in the tables below |
| `benchmark_wait_summary.tsv` | value-log lane-lock wait ledger |
| `phase_stats_overhead{,_benchstat}.txt` | diagnostic option disabled/enabled comparison |
| `always_on_overhead*.txt` | same-Go base/current default-path comparison |
| `profile_benchmark.txt`, `cpu.pprof`, `allocs.pprof` | CPU and allocation evidence |
| `block.pprof`, `mutex.pprof`, `trace.out` | blocking, mutex, and runtime trace evidence |
| `*_top.txt`, `trace_sync_top.txt` | text profile summaries |
| `strace.log`, `strace_active_window.txt` | production Linux syscall validation |
| `rotated_segment_test.txt`, `rotated_segment_strace.log`, `rotated_segment_strace_fsync.txt` | rotated command-reference counter and syscall reconciliation |
| `rotated_segment_focused_count10.txt`, `rotated_segment_race.txt`, `rotated_segment_public_regression.txt` | focused repeat, race, and public-ledger regressions |
| `perf_stat.txt`, `perf_benchmark.txt` | hardware/runtime counter sample |
| `environment.txt` | commit, Go, kernel, filesystem, device, and capacity |

Reproduction commands:

```sh
GOWORK=off go test -c ./TreeDB \
  -o /mnt/fast4tb/gomap-3656-m2-evidence-20260710/TreeDB.test

./TreeDB.test -test.run='^$' \
  -test.bench='^BenchmarkPublicCommandWALDurableTinyBatchWriteSync$' \
  -test.benchtime=20x -test.count=3

./TreeDB.test -test.run='^$' \
  -test.bench='^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=forced_pointer/shape=dirty_batch/ops=1$' \
  -test.benchtime=100x -test.count=1 \
  -test.cpuprofile=cpu.pprof -test.memprofile=allocs.pprof \
  -test.blockprofile=block.pprof -test.mutexprofile=mutex.pprof \
  -test.trace=trace.out

strace -f -yy -tt -T \
  -e trace=write,writev,fsync,fdatasync,openat,close \
  -o strace.log ./TreeDB.test -test.run='^$' \
  -test.bench='^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=forced_pointer/shape=dirty_batch/ops=1$' \
  -test.benchtime=5x -test.count=1

perf stat -d -o perf_stat.txt -- ./TreeDB.test -test.run='^$' \
  -test.bench='^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=forced_pointer/shape=dirty_batch/ops=1$' \
  -test.benchtime=100x -test.count=1
```

## Barrier ledger

The rows are means of three runs at exactly 20 iterations. `D1`, `D8`, and
`D32` are dirty batches of that many keys; `W->DWS` is an unsynced dirty batch
followed by a dirty `WriteSync`; `W->EWS` is an unsynced dirty batch followed
by an empty `WriteSync`; and `state` is `Set`, `SetSync`, then batch
`WriteSync`. Times are hook wall time in milliseconds per iteration.

`M`, `E`, and `P` mean value-log `materialization`, `external_ref`, and
`pending_barrier` logical sync paths respectively.

| Placement | Shape | Appends | command writes / bytes | Flush | Sync | command fsync / ms | value paths | value fsync / ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| inline | D1 | 1 | 1 / 134.5 | 0 | 1 | 1 / 7.816 | - | 0 / 0 |
| inline | D8 | 1 | 1 / 480.5 | 0 | 1 | 1 / 10.493 | - | 0 / 0 |
| inline | D32 | 1 | 1 / 1,680 | 0 | 1 | 1 / 6.551 | - | 0 / 0 |
| inline | W->DWS | 2 | 2 / 269 | 1 | 1 | 1 / 6.570 | - | 0 / 0 |
| inline | W->EWS | 1 | 1 / 134.5 | 1 | 1 | 1 / 6.565 | - | 0 / 0 |
| inline | state | 3 | 3 / 425.5 | 1 | 2 | 2 / 13.184 | - | 0 / 0 |
| pointer | D1 | 1 | 1 / 118.5 | 0 | 1 | 1 / 6.760 | M + E | 2 / 7.869 |
| pointer | D8 | 1 | 1 / 352.5 | 0 | 1 | 1 / 6.756 | M + E | 2 / 7.759 |
| pointer | D32 | 1 | 1 / 1,168 | 0 | 1 | 1 / 6.467 | M + E | 2 / 8.055 |
| pointer | W->DWS | 2 | 2 / 237 | 1 | 1 | 1 / 6.559 | M + E | 2 / 8.249 |
| pointer | W->EWS | 1 | 1 / 118.5 | 1 | 1 | 1 / 2.636 | P | 1 / 6.516 |
| pointer | state | 3 | 3 / 4,458 | 1 | 2 | 2 / 14.205 | M + E | 2 / 8.406 |

The command-write byte mean can be fractional because key lengths cross a
decimal digit boundary during the 20-iteration samples. Logical sync-path
counts and actual file-sync-hook counts agree for every isolated row. Segment
creation/rotation directory syncs are outside the delta window and remain
separate counters. None of the 12 measured active windows rotated a referenced
value-log segment, so the ledger values above are unchanged by the follow-up
counter fix and `file_sync.rotated_segment.calls_total` is zero for those rows.

The deterministic rotated-reference regression constructs a real command
frame whose value pointer names a deliberately rotated non-current segment. Its
delta is two logical `external_ref` calls and two aggregate value-log file-sync
attempts: one through the active lane writer and one direct old-segment sync.
The rotated-segment subcounter is exactly one. An injected direct-sync failure
increments both logical and physical error counters, while a segment-open
failure increments only the logical error counter because no file-sync hook was
reached.

## Latency, publication, and residual ledger

`iter` includes the entire named operation mix. `WS wall` is the exclusive
batch-`WriteSync` phase wall, so it excludes the preceding `Write` in the two
arrow shapes and excludes the state row's point `Set`/`SetSync`. The state
latency distribution contains both durable point and durable batch samples.

| Placement | Shape | iter ms | B/op | allocs/op | WS wall ms | preflight ms | callback ms | publish us | residual us | p50 ms | p95 ms | named % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| inline | D1 | 7.876 | 5,710 | 21.0 | 7.862 | 0.005 | 7.835 | 17.260 | 3.839 | 6.844 | 12.026 | 99.951 |
| inline | D8 | 10.575 | 9,170 | 35.0 | 10.562 | 0.005 | 10.531 | 22.682 | 3.541 | 11.076 | 12.060 | 99.966 |
| inline | D32 | 6.645 | 16,148 | 60.3 | 6.623 | 0.005 | 6.587 | 27.343 | 3.823 | 6.434 | 7.434 | 99.942 |
| inline | W->DWS | 6.648 | 10,711 | 36.0 | 6.602 | 0.004 | 6.579 | 14.070 | 5.105 | 6.375 | 7.253 | 99.923 |
| inline | W->EWS | 6.625 | 7,239 | 20.0 | 6.572 | 0 | 6.568 | 0 | 3.570 | 6.354 | 7.236 | 99.946 |
| inline | state | 13.312 | 33,811 | 38.3 | 6.484 | 0.007 | 6.455 | 18.498 | 3.984 | 6.398 | 7.271 | 99.939 |
| pointer | D1 | 15.122 | 7,143 | 37.0 | 15.106 | 6.848 | 8.231 | 23.617 | 3.755 | 15.093 | 17.224 | 99.975 |
| pointer | D8 | 15.047 | 10,807 | 51.3 | 15.027 | 6.806 | 8.189 | 27.925 | 3.677 | 15.091 | 15.752 | 99.976 |
| pointer | D32 | 15.073 | 19,476 | 78.0 | 15.048 | 7.044 | 7.972 | 28.471 | 3.396 | 15.062 | 15.315 | 99.977 |
| pointer | W->DWS | 15.277 | 12,498 | 65.7 | 14.888 | 6.808 | 8.060 | 17.723 | 2.993 | 14.993 | 16.868 | 99.980 |
| pointer | W->EWS | 9.601 | 8,386 | 36.0 | 9.161 | 0 | 9.158 | 0 | 3.510 | 9.440 | 9.940 | 99.962 |
| pointer | state | 23.154 | 8,675 | 42.3 | 15.709 | 7.053 | 8.635 | 16.898 | 3.565 | 9.421 | 20.776 | 99.977 |

The measured value-log lane-lock wait is 159 ns/op or less in every row. The
mutex profile contains only 230 microseconds total delay in the 100-operation
profile, so there is no material local lock target in this focused path.

## Linux syscall validation

Go's production file hook is `os.File.Sync`. On this Linux/Go build it maps to
`fsync(2)`. The five-iteration measured window in `strace_active_window.txt`
contains exactly:

| Operation | Writes | fsync calls | strace fsync total | in-process hook total |
| --- | ---: | ---: | ---: | ---: |
| value-log file | 5 | 10 | 37.639 ms | 37.934 ms |
| command-WAL file | 5 | 5 | 28.496 ms | 28.601 ms |

The sequence repeats as value write, value materialization `fsync`, value
external-reference `fsync`, command write, command `fsync`. The trace also
contains benchmark warmup, open, close, checkpoint, rotation, and directory
sync activity outside the timestamped active window; those calls must not be
charged to the five measured public operations.

This validates both the production mapping and the deterministic hook tests.
It also proves that the two pointer value-log sync observations are two
physical syscalls on this host.

The rotated-reference follow-up retains a separate path-specific Linux trace.
It verifies that the direct production hook for the deliberately old segment
also maps to `fsync(2)`; the in-process regression supplies the exact active
window reconciliation because test setup, rotation, and close add unrelated
directory and checkpoint calls to the whole-process trace.

## State and application interval reconciliation

The accepted state interval is explained without a duplicate sync inside one
batch call:

```text
3,595 appends
  = 1,199 entry-scan batch WriteSync frames
  + 1,198 point SetSync frames
  + 1,198 point Set frames

2,408 logical command-WAL sync observations
  = 1,199 batch WriteSync syncs
  + 1,198 point SetSync syncs
  + 11 checkpoint/publication barrier syncs
```

The 1,198 unsynced point writes account for 1,198 logical flush observations.
The benchmark's state row reproduces the per-block ratio exactly: three
appends, three command writes, one flush, two syncs, and two command-file
syncs. The aggregate application interval uses the same primitives but a
different operation mix: its 1,212 sync observations do not contain the extra
per-block point-sync family seen in state.

The original accepted interval predates the exclusive phase counters, so those
phase durations cannot be assigned retroactively. A matching M0 state active
window captured 1,187 batch `WriteSync` operations and the exclusive partition:

| Named top-level phase | Total |
| --- | ---: |
| checkpoint gate | 0.2282 s |
| preflight/materialization | 8.5386 s |
| command callback | 16.6293 s |
| cached publication/reset | 0.0276 s |
| residual | 0.00458 s |
| wall | 25.4283 s |

The first four named categories account for 99.982% of that matching active
window. Nested inside the callback, value-log external ordering contributes
8.2433 s and command sync contributes 8.3452 s; they are not added again to
the exclusive top-level sum. Sampling boundaries and the different 1,187-call
representative count are stated explicitly rather than splicing these timings
into the older 1,199-call accepted window.

## Profiles and default overhead

The 100-operation forced-pointer profile is I/O-bound. `perf stat` reports
90.37 ms task-clock over 1.197 s elapsed (0.075 CPUs) and 3,980 context
switches. The CPU profile sampled only 70 ms over 1.74 s. Block/trace profiles
are dominated by background goroutines waiting on channels, and the allocation
profile includes DB open/close setup; per-operation `B/op` and `allocs/op` are
therefore taken from the benchmark ledger above.

The existing diagnostic phase option remains disabled by default. At 100
iterations and five runs, the durable median was 6.611 ms/op disabled versus
6.613 ms/op enabled (no significant timing difference, `p=0.841`). Enabling
the detailed diagnostic partition increased 4,845 B/op and 13 allocs/op to
about 5,037 B/op and 18 allocs/op. The always-on M2 observations are atomic
counters plus clocks only at actual write/sync boundaries; they do not enable
the allocation-bearing detailed phase collector.

A same-Go 1.25 base/current comparison with diagnostics disabled found no
significant default-path change. The adjacent accepted durable reruns measured
6.491 ms/op at the base and 6.512 ms/op in M2 (`p=0.620`, seven samples), with
the same 13 allocs/op and statistically unchanged bytes. The relaxed 10,000x
comparison measured 4.857 versus 5.842 microseconds/op (`p=0.128`, seven
samples), with identical 12 allocs/op and bytes. An earlier non-interleaved
durable pair was rejected because a rerun showed its large difference tracked
device-time ordering rather than the branch; both raw and accepted rerun
artifacts are retained.

## M3 ranking and guardrail

Only one candidate is supported by the evidence:

1. Coalesce the second pointer-path `external_ref` value-log sync when, and
   only when, the implementation can prove that the successful materialization
   sync covered every referenced byte, that no writer or segment rotation can
   intervene, and that reference lookup/readability remains valid before the
   later command-WAL sync. The measured ceiling is 1.35-1.46 ms per affected
   batch, roughly 9-10% of the focused pointer batch wall. The macro effect is
   unknown and is zero for inline values and the required pending-lane empty
   barrier.

There is no evidence for removing the command-WAL sync, making acknowledgement
asynchronous, weakening the durable profile, or checkpointing the backend per
write. Materialization sync, command sync, and the empty pending-lane barrier
all carry distinct correctness obligations.
