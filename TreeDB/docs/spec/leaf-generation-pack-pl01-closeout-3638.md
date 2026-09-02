# Leaf-generation pack PL-01 closeout 3638

This note records the local correctness and performance boundary for #3638.
The implementation moves leaf-generation copy work outside `writeMu`; it does
not change pack admission, byte budgets, leaf/value-log formats, or GC policy.

## Publication state machine

1. A pinned snapshot supplies the exact root, generation state, source files,
   leaf/value-log records, and collection descriptors used by the copy.
2. Recompressed records go to a hidden `.leaf-pack-copy-*` directory. COW index
   pages use an in-memory pager with disjoint high logical IDs and fallback reads
   through the pinned source pager. Neither staging representation changes live
   `PageCount`, the allocator, or the freelist.
3. Staged records are flushed and fsynced without `writeMu`. The append writers
   and private staging reader, including its mapped segments, are closed before
   promotion so Windows rename semantics are satisfied. Pack publication is
   durable even when the legacy pre-alpha `Sync` option is false.
4. Under `writeMu`, `publishPrepareMu`, and the value-log publication gate, the
   implementation revalidates index generation, `CommitSeq`, both roots,
   `LeafGenerationStateVersion`, and exact source-generation state/file lists.
   A mismatch discards every copy-time page and retired ID and performs one full
   retry; attempts never reuse a delta.
5. After validation, staged files are promoted and the leaf directory is synced.
   Private pages are relocated into a short live-pager append, those pages are
   durably synced, segments are tentatively registered while snapshot/refresh
   visibility is exclusive, and the alternate meta page atomically publishes
   roots plus generation state. Existing snapshots keep their immutable old set;
   no new snapshot or refresh can include candidates before publication.
6. Pre-meta failures roll back the logical live append and remove candidates
   before releasing visibility. A post-meta-write sync failure retains all
   candidates and poisons the handle with `ErrRecoveryRequired`; reopen chooses
   the highest valid durable meta page. Sources remain pinned until successful
   durable publication.

Startup removes orphan `.leaf-pack-copy-*` directories only after obtaining the
exclusive database lock. `maintenanceMu`, generation pins, and `teardownMu`
continue to serialize pack with GC and close.

## Portable page durability

`Pager.SyncPages` plans granularity-aligned mapped-view ranges using the platform
mapping allocation granularity. Platforms that require an explicit mapped-view
flush receive those aligned ranges. Linux finishes with `fdatasync`; Darwin
uses `F_FULLFSYNC`, Windows uses `FlushFileBuffers`, and unsupported stable-file
adapters fail closed. The contract is that requested pages are durable; it is
not an exact-range durability claim, and ordinary dirty-chunk bookkeeping is
unchanged.

## Performance fixture

The benchmark file is overlaid byte-for-byte on exact base
`9bb109dd0ed643448c065951eb72cdab99c47ac6` and the head worktree. The fixture
uses 1,048,576 keys with 512-byte values, rewrites 289,730,560 bytes in 7,351
frames, and runs eight writers with 64 writes each. Idle and pack modes therefore
each report 512 foreground latency samples. Odd pairs run base then head; even
pairs run head then base. Runs are pinned to CPUs 0-5 with `GOMAXPROCS=6`.

```sh
GOWORK=off GOMAXPROCS=6 TMPDIR=/mnt/fast4tb/tmp \
  taskset -c 0-5 go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkLeafGenerationPackCopyPublish$' \
  -benchtime=1x -count=1 -benchmem \
  -mutexprofile=<run>.mutex.pprof -mutexprofilefraction=1
```

Five alternating runs per revision produced these medians:

| Metric | Exact base | Head | Gate/result |
| --- | ---: | ---: | --- |
| idle foreground p95 | 1.232 ms | 1.219 ms | reference |
| pack foreground p95 | 1.254 ms | 1.234 ms | 1.01x head idle, pass |
| idle foreground p99 | 2.293 ms | 2.314 ms | reference |
| pack foreground p99 | 303.335 ms | 2.288 ms | 0.99x head idle, pass |
| idle foreground writes/s | 7.259k | 7.142k | reference |
| pack foreground writes/s | 1.357k | 7.546k | 105.7% of head idle |
| pack idle wall | 663.4 ms | 607.7 ms | -8.39%, below 5% regression gate |
| pack idle throughput | 436.8 MB/s | 476.7 MB/s | +9.15% |
| pack idle B/op | 311.7 MiB | 314.8 MiB | +1.00% |
| pack idle allocs/op | 195.5k | 195.7k | +0.09% |
| pack publish hold | unavailable | 24.41 ms | 739 metadata pages |
| contended pack wall | 515.3 ms | 722.0 ms | head includes full retry |
| contended retry copy | unavailable | 320.2 ms | one stale attempt discarded |
| contended B/op | 313.0 MiB | 630.3 MiB | two complete copies |
| contended allocs/op | 204.0k | 308.0k | two complete copies |

The contended head result deliberately forces a foreground commit between copy
and publish, so wall time and allocations include two complete attempts. It is
retry-cost evidence, not the no-conflict wall-time gate. The mutex profiles are
also dominated by fixture leaf-lane construction; after removing that common
setup signal. Across the five combined profiles, base attributes 1,578.86 ms of
mutex delay to the whole-copy `rewriteLeafRefsOnline` unlock; head attributes
9.92 ms to `rewriteLeafRefsOnline`. The explicit head publish-hold counter, not
that sampled delay, is the bounded critical-section measurement.

Raw local artifacts are in
`artifacts/pl01-3638-20260710T080225Z/`: `base.txt`, `head.txt`, `benchstat.txt`,
five mutex profiles per revision, and rendered mutex tops. The overlaid harness
SHA-256 is
`50a71982c36240dec603a88999c4c8957e8e134fb9b097eb48615535fdc29dd3`.
