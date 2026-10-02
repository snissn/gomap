# Private point-read capture qualification (#4890)

Ordinary synchronous owned backend reads reuse a private capture guard through
`Get`, `GetAppend`, `GetVersioned`, and `GetVersionedAppend`. Exported snapshots
remain fresh, single-use handles. Both routes call the same capture routine and
`Snapshot.Close`; registry entries, value-log and leaf-generation pins, publication
locks, close/poison handling, and retry-before-refresh release are unchanged.
The private guard cannot reach a caller, callback, or iterator. Before reuse it
clears the captured reader, index, pager, root, and tree reader interfaces.

## Source and environment

- Baseline: `ef22ce55b85524de2707453f22c5f8cd6ba89eaa`.
- Timed implementation: `4a21d37c3d163f51124afc2300e5cc9e7d3ce5a6`.
  Subsequent evidence-only documentation preserves those runtime/test blobs.
- Canonical public benchmark blob: `6a8e39a1e233b7c6f2276241321ffc698b6adc6a`,
  SHA256 `8d0244fab4707e68b5f96d18e4445a18dc5b419895c5c9a33e8455daeb9030e0`.
  The identical candidate benchmark file was overlaid on the baseline archive.
- Exact baseline archive SHA256:
  `3620e49e1e30fcc5f77839712e3c1e25b43e3f566214ce84f9e51a7096e7e227`.
  Candidate archive SHA256:
  `ee7c2a04b5946f129dd7918dab58cead87214efe8b3a3731df9d1cd032cb4afa`.
- Go 1.26.3 linux/amd64, explicit GOROOT
  `/home/mikers/.gvm/gos/go1.26.3`, `GOWORK=off`, `GOMAXPROCS=4`,
  `GOMEMLIMIT=1GiB`, persistent `/home/mikers/.cache/go-build` cache.
- Owned shared Linux runner: Intel i5-11400F @ 2.60GHz, 12 logical CPUs,
  Linux 6.8.0-138-generic. Light services remained active (ClickHouse, chain
  daemon, desktop/audio, monitoring); initial load averages 1.50/1.82/1.93.
  This was an exclusive graph benchmark window on a shared host, not a
  dedicated machine. Sources, fixture directories, binaries, and outputs were
  isolated under `/mnt/fast4tb/gomap-4890-reads-20261001`.

These are focused pre-merge qualification observations. Final expensive retained
collection remains subject to the reviewed/landed harness policy.

## Active public path and read semantics

`BenchmarkDBCheckpointedValueLogGet` uses the landed snapshot-read fixture:
32,768 distinct keys, 256-byte pointer values, leaf pages in the value log,
prefix/columnar/packed-pointer leaves, pointer threshold 1, and disabled background
checkpoint/vacuum/prune. It checkpoints before timing, then warms every key via
ordinary public `DB.Get`. Checkpoint drains the memtable so public `Get` selects
cached `Get` and its backend fallback, rather than measuring a memtable hit.
`GetUnsafe` follows the same owned `Get` path. `GetAppend` appends into a reused
512-byte caller buffer. Every timed read checks error, length, and its encoded
key index. Fixture creation, checkpoint, and explicit warm reads are outside the
subbenchmark timer; no explicit cold-page or cold-pool latency claim is made.

The existing backend `BenchmarkGetVersioned` calls `GetVersionedAppend` over
10,000 inline 100-byte values with non-legacy revisions, four parallel workers,
and worker-owned append buffers. It validates errors, value length, and revision.
It isolates capture overhead; it is not a substitute for the public fixture.

## Matched five-round results

Each round ran fresh test-binary processes. Odd rounds used baseline then
candidate; even rounds reversed the order. No sibling benchmark/build/test ran
in this window. Medians below use five observations, with 1-second subbenchmarks.
Required owned output copies remain included. Package allocation counters are
process-wide; residual amortized decoder/cache bytes remain visible.

| Path | Baseline ns/op | Candidate ns/op | Baseline B/op | Candidate B/op | Baseline allocs/op | Candidate allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Public checkpointed Get | 3431 | 3407 | 827 | 315 | 2 | 1 |
| Public checkpointed GetAppend | 3420 | 3285 | 571 | 59 | 1 | 0 |
| Public checkpointed GetUnsafe (owned) | 3473 | 3391 | 827 | 315 | 2 | 1 |
| Backend versioned append | 337.5 | 196.4 | 512 | 0 | 1 | 0 |

All four paths remove exactly 512 B/op and one allocation/op. Public median
latency changed by -0.7%, -3.9%, and -2.4%; these are comparable-throughput
observations on this shared host, not broad application speedup claims.
The backend capture-focused median improves 41.8%.
Raw timing/allocation observations are retained in
[private-point-read-4890.csv](private-point-read-4890.csv).

Equivalent reproduction (build each archived runtime with the same benchmark
file before timing):

```sh
export GOROOT=/home/mikers/.gvm/gos/go1.26.3
export PATH="$GOROOT/bin:$PATH"
export GOWORK=off GOMAXPROCS=4 GOMEMLIMIT=1GiB
export GOCACHE=/home/mikers/.cache/go-build
go test -c -o public.test ./TreeDB
go test -c -o backend.test ./TreeDB/db
/usr/bin/time -v ./public.test -test.run '^$' \
  -test.bench '^BenchmarkDBCheckpointedValueLogGet$' \
  -test.benchmem -test.benchtime=1s -test.count=1
/usr/bin/time -v ./backend.test -test.run '^$' \
  -test.bench '^BenchmarkGetVersioned$' \
  -test.benchmem -test.benchtime=1s -test.count=1
```

## Idle and peak memory boundary

A qualification-only identical harness repeated the checkpointed fixture, warm
reads, and 100,000 checked owned Get calls, then measured `runtime.MemStats.HeapAlloc`
with the DB kept alive after one and two explicit idle GCs. Harness SHA256:
`4aae5849125d034809c654041979b008a0e9810f4640ae991e4d1ea689976e6d`.
It adds no product counter or public API. Five alternating fresh-process rounds
used the same environment. This observes all existing pools and GC cadence,
not just the 512-byte private guard; the one-GC increase is not attributed solely
to that guard. Guard cleanup tests independently prove no captured index,
reader, registry entry, or pager remains retained.

| Observation (median) | Baseline | Candidate |
| --- | ---: | ---: |
| Live heap after one idle GC, bytes | 141787928 | 144145392 |
| Live heap after two idle GCs, bytes | 141787424 | 141787200 |
| Peak RSS of fixed-read process, KiB | 219600 | 177112 |
| Peak RSS of public timing process, KiB | 222612 | 200944 |
| Peak RSS of backend timing process, KiB | 51364 | 29752 |

The first idle GC retained about 2.25 MiB more across all candidate pools; the
second GC converged within 224 bytes at the median. Peak RSS includes fixture
creation, Go benchmark calibration, and process shutdown. It does not isolate
the timed interval. `sync.Pool` can drop a guard at GC; cold acquisition still
allocates and residency follows concurrency/GC cadence rather than per-call
retention. Raw memory observations and the temporary harness/script are retained
with the runner packet and local evidence packet.

## Correctness and lifetime evidence

The final allocation test was run first against the exact baseline with only
that test overlaid: Get/GetVersioned failed at 2 allocations/read (budget 1),
appends at 1 (budget 0). Candidate passes at 1/0. Every case counts 101 capture
and release pairs (202 acquisition epoch transitions) and zero live registry
pins. Required owned-output allocation is allowed.

Tests cover owned copies, missing/empty/nil keys, versioned results, stale closed
exported aliases, coherent publication, close/poison rejection, rejected-capture
value-log cleanup, and both exported/private leaf-generation pins across
publication/GC. All four owned APIs exercise the stale value-log refresh retry.
Existing snapshot/iterator lifetime tests and concurrent online rewrite are
included in focused race runs. Backend suite, public raw/owned/versioned checks,
forced-pointer checkpoint/reopen, rewrite/reopen, and GC/reopen passed.

Local correctness used Go 1.26.0 darwin/arm64, `GOWORK=off GOMAXPROCS=4`:

```sh
# From TreeDB:
go test ./db -count=1
go test ./db -race -run 'TestOneShotRead|TestLeafGenerationGC_RetiresPinnedGenerationUntilSnapshotCloses|TestGet_Retries|TestGet_ConcurrentStaleReadRetry|TestSnapshot' -count=1
go test ./db -race -run 'TestGet_Retries|TestValueLogRewriteOnline_DoesNotLoseConcurrentWrites' -count=1
go test . -run 'TestReopenVerify_(WALOn_Checkpoint|WALOn_WriteSync|ValueLogRewrite_BatchedPointerSwap_ReopenParity|ValueLogGC_LeafPagesInValueLog_ReopenParity)|Test.*(RawKV|GetVersioned|Owned)' -count=1
go test ./db -run '^$' -gcflags='-m=2'
```

Escape diagnostics show the captured snapshot address escapes through the
`&snap.reader` interface stored by `Tree.Reset`; merely moving the guard onto a
local stack variable would still allocate. The private pool avoids that warm
allocation while keeping all capture/release work, rather than replacing it
with another per-read heap object.

Raw logs, scripts, archive manifests, host context, and diagnostics:
`/tmp/gomap-4890-evidence/` locally and
`/mnt/fast4tb/gomap-4890-reads-20261001/results/` on the owned runner.
No new unified-bench result schema or benchprof parser/profile naming is used.
