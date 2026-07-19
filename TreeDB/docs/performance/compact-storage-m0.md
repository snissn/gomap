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

The script defaults to six samples, CPUs `2-3`, `GOMAXPROCS=2`, and
`GOMEMLIMIT=8GiB`. It writes environment and source metadata, raw benchmark and
`benchstat` output, one JSON artifact per fixture/sample, instrumentation
overhead evidence, CPU/allocation/block/mutex profiles, a runtime trace, and a
stable-syscall `strace` summary.

The JSON recorder reports logical production barriers. A single logical
directory or index barrier may issue multiple kernel `fsync` or `msync`
syscalls. `syscalls/operation_stable_calls.txt` preserves the physical calls,
while `syscalls/checkpoint_stable_calls.txt` isolates checkpoint intervals.
Accept a count difference only when this grouping explains it; an immediate
checkpoint fast-path claim requires zero calls in the physical checkpoint
file.

Useful overrides:

```sh
COUNT=1 scripts/compact_storage_m0_profile.sh
CPU_SET=4-5 GOMAXPROCS=2 GOMEMLIMIT=8GiB scripts/compact_storage_m0_profile.sh
```

The benchmark is fail-closed: maintenance errors, foreground write errors, and
missing declared leaf-pack or value-log rewrite work fail the run instead of
emitting a zero-valued result.
