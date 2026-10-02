# Shared decoder capacity repair qualification (#4889)

Successful non-empty decodes that reuse the caller's allocation now return its
original capacity. The codec still receives a destination bounded to checked
`rawLen`; errors and newly allocated output are unchanged. No storage format,
checksum, public ownership or snapshot-lifetime contract changes.

## Frozen comparison

Baseline production: `ef22ce55b85524de2707453f22c5f8cd6ba89eaa`.
Candidate runtime: `0ad9ce9728e3b758de5c9300d89156f891dc13c3`.
Later commits add documentation only. Both archives contain the same mixed-frame
benchmark blob `c5e72c7e5c96a6d72c86ffbbacc1f5ae4c7d3741`; baseline substitutes
only `TreeDB/internal/valuelog/reader.go` from the baseline commit. The temporary
public qualification source is identical in both archives, Git blob
`cd5771ad5fbe17bdc83e326398cd078b031134ca`, retained in
[public_harness.go.txt](public_harness.go.txt). It is not a shipped benchmark:
#4890 owns the canonical public snapshot fixture.

All raw benchmark text, failures, `/usr/bin/time -v` packets, source hashes,
host/load observations and collection scripts are in
[qualification.txt](qualification.txt) (SHA256
`1d7b876ead223b29615863c6dfaec8af8f84d4a8cda07cd3045882868b4b3d0a`).
Binary profiles and complete source archives remain at
`/mnt/fast4tb/codex-graph-4888-20261001/decoder-4889` on the owned Linux runner;
the local copy is `/tmp/gomap-4889-evidence/remote`.

The runner used Linux/amd64, Intel i5-11400F (6 cores/12 threads), Go 1.26.3,
explicit `GOROOT=/home/mikers/.gvm/gos/go1.26.3`, `GOMAXPROCS=4`,
`GOMEMLIMIT=1GiB`, `GOWORK=off`, persistent build cache and graph-owned TMPDIR.
This is a shared host: the observation packet includes a Runner.Worker process,
ClickHouse and a chain service. Public capture load1 ranged 1.47–2.50. Only one
timed comparison ran at a time; this was not a dedicated idle-host experiment.
Five fresh-process pairs alternated baseline/candidate order, with 500ms per
benchmark. Fixture construction, codec warmup and public checkpoint were outside
the timer. All results below are medians, not a claim about production throughput.

## Allocation objective and guardrails

The mixed fixture alternates 65,536-byte and 4,096-byte compressed frames into
one initially 65,536-byte destination. The benchmark counts changes of the
starting backing pointer. Go's integer `allocs/op` rounds the baseline's 0.5
backing allocations/op down to zero; the explicit counter and B/op resolve this.

| Mixed codec | ns/op before → after | ops/sec before → after | B/op before → after | backing allocations/op before → after |
| --- | ---: | ---: | ---: | ---: |
| LZ4 | 15,386 → 13,702 | 64,994 → 72,982 | 32,767 → 0 | 0.5 → 0 |
| Zstd | 16,235 → 13,834 | 61,595 → 72,286 | 32,871 → 0 | 0.5 → 0 |
| Snappy | 4,787 → 3,162 | 208,899 → 316,256 | 32,767 → 0 | 0.5 → 0 |
| Legacy no-dictionary Zstd | 15,704 → 13,852 | 63,678 → 72,192 | 32,778 → 0 | 0.5 → 0 |

Every candidate sample ends with visible capacity 65,536. The throughput ranges
before/after are LZ4 15,351–16,712 / 13,583–14,008; Zstd 15,709–16,385 /
13,749–14,284; Snappy 4,779–5,146 / 3,147–3,189; legacy Zstd
15,662–16,334 / 13,669–14,233 ns/op. This demonstrates the targeted mixed-frame
allocation removal. It does not establish a public point-read speedup.

| Control | ns/op before → after | ops/sec before → after | B/op before → after | allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| Existing DecodeFrameAlloc | 39.11 → 39.48 | 25,568,908 → 25,329,281 | 56 → 56 | 2 → 2 |
| Compressed file fallback, nil destination | 1,101 → 1,096 | 908,265 → 912,409 | 512 → 512 | 1 → 1 |
| Compressed file fallback, reused destination | 1,047 → 1,055 | 955,110 → 947,867 | 0 → 0 | 0 → 0 |
| Public DB.Get, mutable fixture | 2,392 → 2,400 | 418,060 → 416,667 | 280 → 280 | 1 → 1 |
| Public DB.Get, checkpointed fixture | 3,545 → 3,548 | 282,087 → 281,849 | 888 → 889 | 2 → 2 |

Existing DecodeFrameAlloc prepares an uncompressed frame; it is a parser control,
not evidence of mixed compressed scratch reuse. The mutable public fixture
predominantly returns owned memtable results and is not the sole public proof.
The checkpointed fixture creates 32,768 forced-pointer 256-byte values in an
owned temporary DB, keeps checksum defaults enabled and validates read errors,
length and the row's encoded index on every measured lookup. Candidate/public
times range 3,515–3,727 ns/op versus baseline 3,507–3,592; overlapping ranges and
0.08% median change support a guardrail, not a speed claim.

Public selection is `DB.Get → caching.DB.Get → db.DB.Get → Snapshot.Get →
Tree.Get/GetAppend → appendPointerValueForKey → ReadUnsafeAppend → File.ReadAppend`.
Compressed mmap, file fallback and batch cache misses call the shared decoder;
cache hits reuse decoded bytes. The checkpointed CPU profile samples public
Get, cached backend routing, `readGroupedCompressedFromFileToVerify` and grouped
cache reads. It has no samples named `decodeFramePayloadTo`; cache warmup and a
uniform-frame fixture make that profile unsuitable for a numeric decoder-time
claim. Both the earlier mutable profile's failed decoder listing and the final
checkpointed empty decoder listing are retained rather than interpreted as a
decoder speed result. Direct regressions and mixed-frame counters prove the
repair itself.

## Allocation, ownership and residency audit

All shared-function callers were inspected in `reader.go`, `reader_mmap.go`,
`manager.go` and `read_grouped_batch.go`. Generic/file readers copy selected
subrecords before releasing scratch. Mmap paths use exact caller-owned/cache
storage or scratch; append paths pass the shifted destination start for a
single-frame append, or copy the selected subrecord from scratch. Batch reads
copy outputs before release. Decoder output errors and required owned result
copies remain unchanged. Restoring capacity changes a slice header and adds no
copy. Newly allocated decoder output is not rebound to an insufficient caller
destination. Non-empty successful output must have the same starting pointer.

`groupedFrameCache.store/releaseRaw` continue to reserve/release **logical
`len(raw)`**, not physical capacity. A truncated slice already retains the entire
original allocation: restoring its capacity does not create a new cache backing
allocation or change that entry's logical admission. Cache byte counters are
therefore not a physical heap/RSS bound, both before and after this repair. On
eviction the restored capacity makes existing scratch class checks accurate.
Process scratch pools remain bounded to 128 small buffers of at most 256 KiB and
8 large buffers of at most 4 MiB; the per-file stash has its existing 128 KiB
byte bound. Larger buffers are dropped from scratch retention. The repair does
not add a pool, a cache owner or an uncapped retention path. It can retain a
reusable backing longer instead of allocating its replacement; this is the
intended reuse, not an assertion that truncating capacity formerly freed memory.

| Checkpointed public metric (median of five) | Before | After |
| --- | ---: | ---: |
| Scratch gets/op | 1.002 | 1.002 |
| Scratch allocations/op | 0.001751 | 0.001775 |
| Scratch allocated B/op | 50.89 | 51.59 |
| Pool retained B after two GCs | 65,564 | 65,564 |
| Live heap B after two GCs, DB kept alive | 141,798,904 | 141,815,896 |
| Whole-process maximum RSS KiB | 242,116 | 225,224 |

Counters are global deltas around the final benchmark invocation; post-timer
GC/stat collection can include associated activity. Every Go calibration
invocation builds a fresh fixture; profiles and process RSS include discarded
calibration/setup invocations. Heap/pool readings are post-two-GC retained
readings, not phase-only peaks. Whole-process RSS ranges are 214,960–282,676 /
209,724–287,644 KiB. The after maximum is slightly higher while its median is
lower; this noisy setup-inclusive peak is not evidence of a memory improvement.
The uniform public fixture's retained heap difference is 16,992 B (0.012%) and
pool bytes are identical. The mixed decoder process RSS medians are
22,424 → 13,580 KiB, also setup-inclusive. Physical-cap versus logical cache
accounting remains an explicit existing limitation; a final retained production
matrix is owned by the downstream canonical harness work, not this qualification.

## Red/green and reproduction

Before the fix, the backing-preservation regression failed at the small frame
for LZ4, Zstd, Snappy and legacy Zstd (capacity 4,096, expected 65,536). The same
source passes after the fix. Additional tests cover nil/insufficient destinations,
unchanged sentinel storage and oversized compressed payloads that must not write
beyond admitted raw length. Existing package coverage includes malformed and
bounded Zstd, dictionary/template resolution, grouped cache, mmap and concurrent
read/eviction behavior.

Passed local correctness commands (Mac, Go 1.26, all capped to four threads):

```sh
GOMAXPROCS=4 GOWORK=off go test ./TreeDB/internal/valuelog -count=1
GOMAXPROCS=4 GOWORK=off go test -race ./TreeDB/internal/valuelog -run 'Test(DecodeFramePayloadTo|GroupedFrameCache_ConcurrentReadsAndEvictions|GroupedFrameCache_StatsConcurrentAdmissions|MmapSafety_Concurrent_Remap)' -count=1
GOMAXPROCS=4 GOWORK=off go vet ./TreeDB/internal/valuelog
GOMAXPROCS=4 GOWORK=off go test ./TreeDB -run '^TestReopenVerify_WALOn_Checkpoint_CompressionModes$' -count=1
git diff --check
```

The public reopen test checks full values and scans through checkpoint, close and
reopen for all compression modes. Focused Linux red/green output is in the raw
packet. Collection scripts in that packet build binaries before measurement and
use these exact benchmark selectors:

```sh
go test ./TreeDB/internal/valuelog -run '^$' -bench '^Benchmark(ValueLogDecodeFrame(Alloc|MixedReuse)|FileReadAppendCompressedFallback)$' -benchmem -benchtime 500ms -count=1
go test ./TreeDB -run '^$' -bench '^BenchmarkDBValueLogGet$/^Get$' -benchmem -benchtime 500ms -count=1
# Temporary owned archive only, replace snapshot_read_bench_test.go with retained harness:
go test ./TreeDB -run '^$' -bench '^BenchmarkDBValueLogGetCheckpointed$' -benchmem -benchtime 500ms -count=1
```

The standalone benchmark emits Go benchmark text. Both tool READMEs and the
typed-storage guide document its name, counters, command and output contract;
it does not use unified-bench adapters or benchprof artifact names.
