# CompactStorage M0 evidence

`BenchmarkCompactStorageM0` is the canonical six-row maintenance matrix for
issue #3733. Fixture setup and artifact reporting are outside
`total_wall_time_nanos`; the JSON artifact also records apply-phase wall time,
leaf-pack work, exact checkpoint frontiers, stable calls, index-vacuum
disposition, maintenance allocations, and matched foreground/idle write
latencies.

Run the complete pinned collection protocol on the local NVMe filesystem:

```sh
RUN_DIR=/mnt/fast4tb/compact_storage_m0_$(date +%Y%m%d_%H%M%S) \
  scripts/compact_storage_m0_profile.sh
```

The script defaults to 12 samples, CPUs `2-3`, `GOMAXPROCS=2`, and
`GOMEMLIMIT=8GiB`. It writes environment and source metadata, raw benchmark and
`benchstat` output, one JSON artifact per fixture/sample, instrumentation
overhead evidence, CPU/allocation/block/mutex profiles, a runtime trace, and a
stable-syscall `strace` summary. Diagnostic profiles disable the test recorder
so allocation attribution describes the production maintenance path; pprof
phase labels remain available.

The allocation profile is a subtraction of `allocs` snapshots taken immediately
before and after the measured operation. Each snapshot first advances two GC
cycles so Go's allocation profile has no delayed setup samples. This excludes
fixture construction;
the script preserves both snapshots plus `allocs_top.txt` and
`allocs_objects_top.txt`. The reports focus on TreeDB maintenance stacks,
exclude the snapshot writer itself, and calculate percentages relative to the
remaining operation profile.

Durable barrier latency on local NVMe can be bimodal. Keep every sample, report
the spread, and compare candidate work in counterbalanced baseline/candidate
blocks. The overhead collection alternates pair order for this reason. Start
with 12 blocks; if the confidence interval still crosses the issue's minimum
effect, continue sampling before accepting or rejecting the candidate.

The JSON recorder reports logical production durability barriers; relaxed
userspace flushes are coordination events and are excluded from
`stable_calls`. A single logical directory or index barrier may issue multiple
kernel `fsync` or `msync` syscalls.
`syscalls/operation_stable_calls.txt` preserves the physical calls, while
`syscalls/checkpoint_stable_calls.txt` isolates checkpoint intervals. Accept a
count difference only when this grouping explains it; an immediate checkpoint
fast-path claim requires zero calls in the physical checkpoint file.

Useful overrides:

```sh
COUNT=1 scripts/compact_storage_m0_profile.sh
CPU_SET=4-5 GOMAXPROCS=2 GOMEMLIMIT=8GiB scripts/compact_storage_m0_profile.sh
```

The benchmark is fail-closed: maintenance errors, foreground write errors,
missing declared leaf-pack or value-log rewrite work, and unexpected rewrite
selection in a no-rewrite control fixture fail the run instead of emitting a
misclassified result.
