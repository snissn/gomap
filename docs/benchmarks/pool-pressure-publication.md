# Pool pressure snapshot publication

TreeDB samples process heap pressure every 250 ms while a cached DB is open.
Expired foreground lookups and the one-second process memory sampler can also
refresh it. Published snapshots are coherent value copies: a reader retains its
copy independently of later publications. The separate pressure-level atomic
continues to serve callers that only need the current level. Sampling frequency,
pressure classification, pool trimming, and storage maintenance remain active.

This fixed-size process-global record has no batch, memtable, or returned-buffer
lifetime. It therefore uses bounded shared storage rather than an arena or pool.
Publication and cached reads should allocate no heap object for the record.
The Linux RSS scanner has separate allocation costs; zero record-publication
allocation is not a claim that all background telemetry allocates nothing.

The record uses typed atomic scalar fields and a publication sequence. Writers
serialize with a mutex, make the sequence odd, replace every field, then make it
even. A reader accepts a copied record only between equal even sequence values.
All payload reads and writes are atomic, including floating-point bits and
signed timestamps; there is no ordinary concurrent copy hidden behind a retry.
The typed 64-bit atomic fields supply alignment on 32-bit architectures. A reader
yields if a writer is still publishing; this is not a wait-free guarantee.

## Field demand and maintained callers

All eleven level-only production callers in `TreeDB/caching/db.go` use
`currentPoolPressureLevel`: zipper parallel-merge pressure; batch-arena retention
budget and hard cap; entry-slice retention budget; copy-arena chunk cap;
append-only direct-arena chunk retention; mutable append-only direct-arena trim;
and the four batch/shard/int/int64 auxiliary-pool return paths. `DB.Stats` is the
sole production full-snapshot consumer. The snapshot's additional fields are
still sampled and exposed through Stats.

The existing `currentPoolPressureLevelFast` is deliberately different: it relies
on background updates and skips foreground freshness checks. Replacing these
callers with that helper would change policy when sampling stops or expires.
The selected helper instead reads timestamp and level between equal even values
of the same publication sequence, then uses the original inclusive 250 ms
freshness check, refresh mutex, locked recheck, and real sample/publication path.
There is one record and one publisher; this split removes unused field loads and
copies from policy callers rather than introducing another cache or weakening
coherence. A full coherent copy remains available to Stats and diagnostics.

The revised component discriminator runs the maintained
`currentZipperParallelMergePressure` call on both baseline and candidate, plus
the unchanged full cached-snapshot read consumed by Stats. It predeclares three
alternating pairs (AB/BA/AB), CPU 1/4/16, and 250 ms per cell: 36 rows total. A
policy-read median regression above 5% at any CPU rejects the candidate. The full
snapshot results remain visible as a separate diagnostic cost; the earlier failed
full-copy gate is retained, not reclassified as a pass. This is a field-demand
architecture change selected after two rejected representations. The policy
component fixes its clock to isolate the fresh branch; it is not the public
PRIMARY comparison with real clocks and telemetry.

## Focused controls

Use the repository's Go toolchain and keep build/cache/temp output on the task
volume. Correctness and allocation controls exercise the real production sampler
and publisher, concurrent complete-field snapshots, and retained copies:

```sh
GOWORK=off go test ./TreeDB/caching -run '^TestPoolPressure(Real|Cached|Publication|Sampler|Snapshot|Policy)' -count=1
GOWORK=off go test -race ./TreeDB/caching -run '^Test(PoolPressure|SampleProcessMemoryPeaks)' -count=1
```

The standalone component benchmarks distinguish publication allocation from
cached-read contention. The latter fixes the clock only within that component
fixture so all parallel readers exercise the cached branch:

```sh
GOWORK=off go test ./TreeDB/caching -run '^$' -bench '^BenchmarkPoolPressure(PublishSnapshot|CachedSnapshotRead|CachedPolicy)$' -benchmem -cpu=1,4,16 -benchtime=1s -count=3
```

Capture a separate stdout/stderr pair for each exact-source fresh-process sample.
These Go benchmark outputs, and any diagnostic Go profiles, are not benchprof
inputs. A faster publisher does not establish public storage performance.

## Public guardrail and attribution

The maintained public route remains `BenchmarkCommitAtPrimaryOnlyAfterDataGeneration`
in `TreeDB/mvcc/primary_generation_routing_v6_test.go`. It opens the actual cached
DB, materializes DATA, and times 128 durable public replacements with telemetry
running. Use the existing `scripts/mvcc_primary_cost_gate.py` comparison contract
for qualification; component fixtures do not alter that harness or its gates.

Go benchmark allocation accounting measures process-wide `TotalAlloc` and
`Mallocs` between `ResetTimer` and `StopTimer`. On amd64 the old snapshot occupies
136 bytes, rounded to a 144-byte boxed heap object by `atomic.Value.Store`.
One extra publication within 128 operations contributes 1.125 bytes/op and
0.0078125 allocations/op before integer reporting. This mechanism can explain a
one-byte reported difference without changing integer allocations/op. It does
not prove that every observed byte difference came from that site: whole-process
profiles also include Open, setup, stopped-timer work, and Close.

Retain prior failed A/A and product comparisons. A new representation requires
its own baseline/candidate cached-read comparison at each CPU count, concurrent
correctness/race checks, and a newly declared public comparison. Do not disable
sampling or maintained pruning, subtract estimated telemetry bytes, change the
strict allocation gate, or selectively replace noisy samples.

The first development candidate used a single `RWMutex` around the record. Its
three-pair component diagnostic removed 144 bytes/one allocation per publication
but increased cached-read medians from 20.25 to 31.87 ns at CPU1, 5.109 to 72.46 ns
at CPU4, and 3.704 to 53.53 ns at CPU16. It was rejected before a public PRIMARY
comparison. Those 36 rows remain diagnostic evidence, not acceptance.

The atomic scalar successor passed real publication allocation, complete-field
coherence, scalar-bit round-trip, and owning pressure/RSS race controls. Its
separate three-pair diagnostic also removed the publication allocation, but full
cached-snapshot reads increased from 20.14 to 28.03 ns at CPU1, 5.274 to 7.091 ns
at CPU4, and 2.505 to 5.924 ns at CPU16. Publication increased from 52.58 to
108.3 ns at CPU1. It failed the same predeclared 5% cached-read regression limit,
so no public PRIMARY diagnostic ran. All 36 successor rows are retained
separately. These two rejected implementations require architectural
reassessment before further tuning; correctness and zero allocation alone do
not establish an acceptable replacement.


The selected field-demand split passed its declared actual-policy discriminator:
three-pair medians for the maintained zipper pressure call were 20.41 to
4.434 ns at CPU1, 5.17 to 1.44 ns at CPU4, and 2.578 to 0.7608 ns at CPU16.
The same campaign retained the full cached-snapshot input to Stats: 20.29 to
28.33 ns, 5.135 to 7.137 ns, and 2.609 to 3.737 ns respectively, regressions of
39.6%, 39.0%, and 43.2%. Full `DB.Stats` cost was not measured by this component.
The actual sampler/publication and both cached readers allocate zero record
objects in focused controls. Scalar-bit preservation, concurrent complete
records, concurrent timestamp/level coherence, shared refresh in both reader
orders, the inclusive freshness boundary, pressure policies, and owning race
controls passed. These development diagnostics select a representation; they
do not establish public PRIMARY acceptance or eliminate the separate Linux RSS
parser allocation source. No public PRIMARY comparison has run on this source.


The first public development attempt built both exact-source PRIMARY binaries
and the same actual public Stats fixture on baseline and candidate. The Stats
fixture opens the real backend and cached DB with 64 inline keys, retains normal
samplers and real clocks, and times `DB.Stats`. Its standalone command is:

```sh
GOWORK=off go test ./TreeDB/caching -run '^$' -bench '^BenchmarkPoolPressurePublicStats$' -benchmem -cpu=1 -benchtime=250ms -count=1
```

The attempt passed the candidate's actual durable PRIMARY lifecycle control, but
the maintained 60-second host-quiet acquisition exhausted before any timed row.
All twelve planned rows (three pairs each of Stats and PRIMARY) are unavailable,
not failed product measurements. Runtime, binary, harness, source bindings, and
all process observations remain in the task's `telemetry-5213/public-01` evidence.
Neither a material-public-Stats claim nor a public PRIMARY claim is established.
No selective retry, sampling change, or allocation tolerance was applied.

At 3.8 ms per PRIMARY operation, the 250 ms sampler's removed 144-byte box alone
would average about 2.19 bytes per operation. This is an attribution estimate for
that sampler, not a bound on all telemetry: the one-second process sampler also
publishes a snapshot, and RSS parsing has independent allocations. Actual timed
publication counts were not instrumented in the public diagnostic.
