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
flush receive those aligned ranges. Linux relies on its file-wide `fsync`
contract, and all platforms finish with `os.File.Sync` (`FlushFileBuffers` on
Windows). The contract is that requested pages are durable; it is not an
exact-range durability claim, and ordinary dirty-chunk bookkeeping is unchanged.

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
| idle foreground p95 | 1.275 ms | 1.242 ms | reference |
| pack foreground p95 | 1.256 ms | 1.228 ms | 0.99x head idle, pass |
| idle foreground p99 | 2.173 ms | 1.403 ms | reference |
| pack foreground p99 | 267.941 ms | 2.298 ms | 1.64x head idle, pass |
| idle foreground writes/s | 7.222k | 7.321k | reference |
| pack foreground writes/s | 1.499k | 7.121k | 97.3% of head idle |
| pack idle wall | 571.9 ms | 588.9 ms | +2.97%, below 5% gate |
| pack idle throughput | 506.6 MB/s | 492.0 MB/s | -2.9%, statistically neutral |
| pack idle B/op | 311.7 MiB | 314.8 MiB | +1.01% |
| pack idle allocs/op | 195.5k | 195.7k | +0.09% |
| pack publish hold | unavailable | 18.74 ms | 739 metadata pages |
| contended pack wall | 481.6 ms | 698.5 ms | head includes full retry |
| contended retry copy | unavailable | 316.6 ms | one stale attempt discarded |
| contended B/op | 313.0 MiB | 630.3 MiB | two complete copies |
| contended allocs/op | 204.1k | 308.0k | two complete copies |

The contended head result deliberately forces a foreground commit between copy
and publish, so wall time and allocations include two complete attempts. It is
retry-cost evidence, not the no-conflict wall-time gate. The mutex profiles are
also dominated by fixture leaf-lane construction; after removing that common
setup signal, the sampled base profile attributes 281 ms of mutex delay to the
whole-copy `rewriteLeafRefsOnline` unlock, while the corresponding head profile
contains no leaf-pack `writeMu` contention stack. The explicit head publish-hold
counter is the bounded critical-section measurement.

Raw local artifacts are in
`artifacts/pl01-3638-20260710T070752Z/`: `base.txt`, `head.txt`, `benchstat.txt`,
five mutex profiles per revision, and rendered mutex tops. The overlaid harness
SHA-256 is
`50a71982c36240dec603a88999c4c8957e8e134fb9b097eb48615535fdc29dd3`.
