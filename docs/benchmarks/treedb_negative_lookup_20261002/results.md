# #342 bounded qualification results

The opt-in filter substantially reduces exact tree work and latency in this
8192-key miss-heavy fixture. Zero remains the default. This packet is bounded
PR qualification; the larger authoritative campaign waits for landed graph
harness H. Five-sample medians and ranges describe this workload, not universal
or statistically established speedups.

## Provenance and completeness

Measured candidate: `37fb160c1ebfd21710cbce68da53d883c1e18eff`.
Reference: merged predecessor `a68a84c7195e0c5d8c339343a385b40acf818d03`.
Candidate/reference visible Go/module manifests:
`b17df9082b92bfe1833a988fa8ce569ce22d33824b2e856d1aeb6a3775ca9cd6` /
`1c7b8879978a2c6457123c750044b4ceb108f67ea78f50cb630b3e10fcbfe34f`.

[Capture](qualification-37fb160c1-v2/capture.json),
[parsed rows](qualification-37fb160c1-v2/parsed.json),
[paired medians and all timing samples](qualification-37fb160c1-v2/median-summary.json),
[all metric medians, ranges and samples](metrics-summary.json), and all 72 raw
stdout/stderr logs are retained without modification. The
[preparation manifest](binary-preparation-37fb160c1-v2/prepare.json) records four
successful compile commands and executable hashes; its SHA256 is
`54617e20f133769832cbded282986e55b53d2d81d1c26b191c72eaa785998e32`.
Binary, source and capture-script identities match before/after. The fail-closed
parser accepted all **36 processes, 930 performance samples and 160 diagnostic
rows**; an offline reparse independently matched every retained row and log hash.
No binary is included in Git; their actual hashes and compile provenance remain
bound in the retained manifests.

Capture elapsed **912.684 seconds**. Coordinator grant:
`root-negative-37fb-20261002T0625-exclusive`. Host: Linux amd64, Intel i5-11400F,
32 GB RAM, Go 1.26.3, `/mnt/fast4tb`, `GOWORK=off`, `GOMAXPROCS=4`,
`GOMEMLIMIT=1GiB`. Initial load: 1.14/1.85/2.25. Other graph timing was excluded;
shared external services remained. Preparation used two cores and 2 GiB.

The public matrix uses compressible 256-byte and seeded random 4096-byte values,
uniform/Zipf schedules, 0/50/90/99% misses, scalar/caller-buffer/64-key/callback
and retained-old-snapshot routes. Five fresh-process pairs alternate mode order;
reads use 500ms leaves, updates/bootstrap 10 iterations. The constructor checks
every byte of every original value, all interleaved misses and actual covered
trees after checkpoint/updates. Warmup and setup are outside timing; checksum
verification remains enabled. Counters run separately for 10000 iterations and
their timings never support throughput claims.

## Read latency and exact work

Batch figures below divide ns/op by 64 keys. Percentages are ratios of five-sample
medians. Complete route/distribution/miss comparisons are retained above.

| Uniform 99% misses | Off ns/key | On ns/key | Change |
| --- | ---: | ---: | ---: |
| Compressible256 Get | 1116 | 550 | -50.72% |
| Random4096 Get | 1193 | 581.2 | -51.28% |
| Compressible256 GetMany64 | 600.891 | 110.953 | -81.54% |
| Random4096 GetMany64 | 647.406 | 125.984 | -80.54% |
| Random4096 GetManyView64 | 628.547 | 128.344 | -79.58% |
| Random4096 old snapshot | 648.2 | 105.9 | -83.66% |

All 90%-miss case medians improve by 11.80–62.60%; all 99%-miss cases improve by
45.09–86.64%. At 50% misses changes range -17.05% to +3.22%; random4096 Zipf
routes can be near flat. At 0% misses changes range -3.05% to +3.50%. Scalar
random4096 uniform 99%-miss samples span 1151–1274 ns/key off versus
570.4–597.7 on; the batch samples span 608.266–685.594 versus 120.563–135.297.
These spreads and the limited fixture preclude a global gain claim.

The separate counter process reports **0.1075 exact descent entrances/key and
0.8925 rejects/key** on random4096 uniform 90%-miss routes, versus 1 and 0 off.
At 99% misses it reports 0.0184/0.9816. These count shared descent entrances,
not disk I/O, and demonstrate the causal reduction independently of timing.

Public result ownership costs remain: compressible256 uniform all-hit Get has
315 B/op, one allocation/op in both modes; GetAppend has 59 B/op, zero measured
allocations/op; GetMany64 has about 54987–54989 B/op, nine allocations/op; callback
GetManyView64 has about 264 KB/op, 195 allocations/op. The filter adds no
per-read allocation. Random4096 uniform 99%-miss GetMany64 records 10562/10561
B/op and three allocations/op off/on. Full public B/op and allocs/op samples
remain in the metric summary, including output/decode costs and variability.

The unchanged canonical six hit routes compare merged R against candidate
**default-off**, with five pairs. Median timing changes are -1.15%, -0.55%,
-0.56%, -1.02%, +0.05%, +2.54%. All six allocation counts match; bytes match
apart from snapshot GetUnsafe (4555 versus 4549 B/op). This packet demonstrates
no material default-off regression.

## Updates, bootstrap and retained memory

| Whole operation | Off median | On median | Change | Off/on allocs/op | Off/on B/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cached 64-key WriteSync acknowledgement | 7.396 ms | 8.088 ms | +9.36% | 241 / 301 | 585841 / 179970 |
| 64-key WriteSync plus checkpoint | 49.966 ms | 51.741 ms | +3.55% | 1430 / 1436 | 1626721 / 1646992 |
| Backend open/close including bootstrap | 20.146 ms | 21.331 ms | +5.88% | 5682 / 5790 | 20786608 / 21132927 |

WriteSync acknowledgement reports **zero backend publications/op** and defers
membership hashing. Its +9.36% difference cannot be attributed to membership
publication/hash cost. Samples span 5.91–7.87 ms off and 5.95–8.54 ms on.
Checkpoint reports one backend publication/op and includes storage I/O; samples
span 39.25–57.84 ms off and 39.20–57.89 ms on. Its approximately +3.6% is a
combined operation observation. Open/close is not isolated bootstrap hashing;
samples span 19.23–23.24 ms off and 20.91–24.79 ms on. Allocation/byte variation
belongs to whole operations and is not a peak-RSS or retained-memory measure.
The coordinator accepts these workload-specific opt-in costs with these limits;
they provide no reason to change the default or promise write acceleration.

The isolated normalized coverage preparation measures enabled medians
**61.65 ns / 2510 ns / 9936 ns** for 1/64/256 keys. Its logical token is 56 bytes,
measured 64 B/op and one allocation/op, independent of batch key count. Off
preparation allocates nothing. Monotonic membership probes measure zero B/op
and zero allocations/op.

Bit storage stays **10240 bytes**; standalone filter plus header is **10272
retained bytes**, shared by old snapshots. At 8192 keys this is 1.25 bit-storage
bytes/key. Empirical false-positive fractions are
0.0071/0.0076/0.0079/0.0087/0.0077: five-run **median 0.0077**, range
0.0071–0.0087. At 81920 keys, the same memory has median **0.9935**, range
0.9918–0.9948. Saturation safely erases usefulness without growing memory.
Bootstrap can refuse incomplete coverage under its work budget; maintenance
replacement can leave later views exact until reopen. No per-root bit-array
copy or corpus retention is introduced. Snapshot trees/views hold a pointer;
coherent publication adds the bounded temporary token. Peak RSS was not measured.

## Corrections and applicability

The first [e508 packet](failed-qualification-e50876abc-v1/capture.json) is
incomplete infrastructure evidence, with its original mixed logs and
[diagnosis](failed-qualification-e50876abc-v1/diagnosis.md) retained unchanged.
Standard-error dictionary diagnostics interrupted benchmark-name stdout framing.
Exact row matching rejected it. The capture-only repair separates and hashes
both streams; successful v2 is a fresh full capture, with no fragment splicing.

Independent mature review of measured 37fb found one raw-tree correctness gap:
`SetRoot` retained old exact-root coverage. Correction
`a30d4de44cbc3bf5fe28054421d2cefcf8a1683d` clears the filter in shared SetRoot and
adds one regression. The new test fails on 37fb and passes after correction,
alongside the existing point-entrance and concurrent/bounds tests. All existing
root writes are New, Reset (already clearing coverage), and SetRoot; repository
production and benchmark code have no SetRoot callers. The original timing
source stays **37fb**. The correction and subsequent sync to main
`c9a00bb631d34b62669ec1d3f4437e1c8c5df845` do not change measured constructors or
read/publication routes. Main's intervening reliability changes concern
raft/nativewire and their tests. See the
[actual-diff applicability audit](correctness/post-measurement-applicability.json).
This is reuse of recorded evidence, not new-head timing or current-head CI.

[Construction checks](correctness/provisional-validation.md) retain exact red
guard-disabled proofs, focused public/backend/tree and race results,
current-root metadata preservation/non-resurrection, full-byte constructor
checks and merged-predecessor audit. Full tree/backend suites passed; full public
suite had two transferred AppleDouble source-inventory failures, both corrected
inventories later passed. Its failed log remains retained and is not labeled
green. Existing repository 386 overflows prevented full compilation; the exact
filter core passed isolated 386 bounds/concurrency checks, without claiming
whole-TreeDB 32-bit support. Initial Mac ENOSPC was infrastructure. The final
SetRoot checks used Mac Go 1.26.0, two cores/2 GiB, and passed in 0.340s.

Independent correction review, current-head CI, review-thread inventory and
readiness remain coordinator gates. No policy, checksum, persistent pointer,
durability, recovery, close/admission or GC rule is weakened.
