# Owned persistent value-log reads (#4891)

This packet compares merged decoder base `a217ee3b2ee68918905bc5357faa0e57775c22a6`
with runtime candidate `f9d8b01d7bdb01180e71a2ef3dba55c5d6359e64`,
with final focused tests at `9631f9dbacb02e8e6d50c56561e0ad6116ffe8e5`.
Later evidence/documentation commits do not change the measured Go sources.
The reviewed provisional base `dc86c51e7f61de596affc3c788880686e00d4e09`
and merged base have identical Git trees. Production changes affect only
`manager.go`, `reader_mmap.go` and `grouped_frame_cache.go`; predecessor
`reader.go`, canonical snapshot benchmark, defaults, formats and public APIs
are unchanged.

## Behavior and correctness

Ordinary owned `DB.Get` reaches caching/backend/tree pointer resolution, then
`Set.ReadUnsafeAppend`/`Set.ReadAppend`/`File.ReadAppend`; `Manager.ReadAppend`
shares that boundary. Eligible sealed files now use the existing lazy mapping
helper after an initial mapped-read miss. Already mapped reads avoid additional
Stat/eligibility work. Unsupported, writable, closed, identity/budget/error
cases retain existing mapping-helper and file-read fallback behavior.

Grouped raw cache hits copy under the existing read lock directly into the
final owned destination, including caller prefixes. Nil or insufficient
capacity allocates once. No borrowed cache slice escapes. Template hits retain
an owned encoded copy before unlocking because lookup can evict/recycle cache
storage; template decoding then appends into the final destination. Existing
CRC, verification-mode/shape identity, cache entry/raw-byte budgets, pins,
segment identity and GC rules remain in force. Owned nil mapped reads use the
existing bounded grouped admission policy.

[Original-base red tests](failed-933a1d4/raw/correctness/red-base.txt) detect both missed lazy
mapping (`mmap=0`, fallback=1) and two allocations instead of the final output's
one allocation. Candidate full valuelog race tests pass, including codec/raw,
empty/prefix ownership, dictionaries/templates, verification identity,
eviction/close and concurrent reuse. Focused public ordinary Get tests cover
normal and readonly reopen plus mutation independence; public WAL checkpoint
and WriteSync reopen, GC/rewrite parity and stable-identity GC tests pass.
[Source-bound correctness inventory](failed-933a1d4/raw/correctness/source-manifest.json) and
[construction audit](failed-933a1d4/raw/correctness/review-audit.md) preserve the earlier
identities. The original full valuelog proofs remain source-bound; focused repair race
checks cover all three sibling readers, complete mapped hits at the cap, one
capped miss/fallback/CRC, unchanged manager-denial/remap state, cap-increase
recovery and nil-map initial admission with a saturated old count.
[Red933](raw/dead-cap-red.txt), [green repaired](raw/dead-cap-final-green-race.txt)
and [vet](raw/dead-cap-vet.txt) are retained. Vet of
valuelog and TreeDB passed. A Mac GC build failed before execution with ENOSPC;
Linux GC tests replaced that infrastructure failure.

## Capture contract

The coordinator granted exclusive Linux capture. Go1.26.3, Linux amd64,
Intel i5-11400F, `/mnt/fast4tb`, `GOWORK=off`, `GOMAXPROCS=4`,
`GOMEMLIMIT=1GiB`, `TREEDB_HOT_PATH_STATS=1`. Five fresh-process pairs alternate
base/candidate order with 1s per benchmark cell. Every process records
`/usr/bin/time -v`; setup, constructors, cloning, verification and calibration
are included in peak RSS but excluded from read timing. Source, binary,
toolchain, policy, host and harness hashes are in
[source-freeze.json](raw/source-freeze.json). The original baseline uses only
identical benchmark overlays; candidate correctness tests are not installed on
the baseline.

Rows:

- Internal `mmap_cache`: both versions pre-admit the identical 262144B raw frame
  using a reusable nonnil destination; timed nil reads isolate the owned copy.
- Internal `mmap_decode` and `fallback`: cache disabled; all fixture values
  verified untimed, exact route/CRC counts asserted. Eight 32KiB Snappy values.
- `cold_open_map`: OS-cache-warm open/map/read/cache-admission/checked-close cost,
  with observed map/fallback/store counts. This is not cold disk I/O.
- Public `BenchmarkDBOwnedValueLogRoute`: fully warmed ordinary owned Get from
  one prerecorded pointer fixture, with exact pointer/inline/transport/CRC/cache
  vector and bounded raw-length residency asserted.
- Landed compressed fallback nil/reused destination, grouped ReadUnsafeTo and
  public Get/GetAppend constructors are guardrails. Normal public constructor
  asynchronous dictionary/layout noise is disclosed rather than normalized.

The public source fixture contains 32768 keys with 256B values and forced
`PointerThreshold=1`, columnar/outer-leaf layout, and a trained dictionary.
[fixture-manifest.json](raw/fixture-manifest.json) hashes all 17 files/5,659,493B.
It was checkpointed, closed, reopened and every value verified before export.
The immutable, readonly source is never opened by a measured process. Each
process makes a private byte copy, then applies existing supported snapshot
restore order: dictionary/template side stores first, main index second.
`RebindDurableRootSnapshotLayoutV1` rewrites copied index physical dependency
identities/checksums outside timing; logical roots, pointer records, dictionary
and value-log bytes stay identical. Full verification and checked final close
are mandatory. Naive copied physical identities correctly failed startup;
that [nonqualifying failure](failed-933a1d4/raw/nonqualifying/provisional-fixture-validate-base.txt)
is retained. Before/after source inventories must remain identical.

Untimed final probes show base pointer1/inline0/cache1/CRC1/fallback1/mmap0 and
candidate pointer1/inline0/cache1/CRC1/fallback0/mmap1, each retaining 9,389,893 raw
bytes within 67,108,864 budget. Raw counter counts length, not backing capacity.
Existing scratch pools may provide oversized backing; GC heap, mmap bytes and
RSS describe different quantities. Heap deltas include other DB allocations;
RSS is a setup-inclusive peak, not isolated retained cache heap. This packet
makes no universal physical heap ceiling claim from the raw-length budget.

## Reproduction and retained results

Prepare isolated archives of the recorded commits, add the identical public
benchmark file and [internal benchmark-only overlay](baseline-benchmark-overlay.go.txt)
to the base (install overlay as `owned_constructor_bench_test.go`), and build
`valuelog-{base,candidate}.test` and `treedb-{base,candidate}.test` with the
recorded Go toolchain. Export the canonical fixture with
`TREEDB_OWNED_VLOG_FIXTURE_EXPORT`, hash it using the manifest format, make it
readonly, and store binaries/fixture/logs/tmp under one capture directory.
The retained runner directory is
`/mnt/fast4tb/treedb-quicksilver-4891-values-20261001/final-f9d8b01d`.

```sh
bash docs/benchmarks/treedb_owned_values_20261001/qualify.sh FROZEN_CAPTURE_DIR
python3 docs/benchmarks/treedb_owned_values_20261001/analyze.py RAW_LOG_DIR
```

`qualify.sh` writes 10 raw cells per revision/pair, 30 process RSS files and
fixture hash checks. `analyze.py` requires 100 samples, five samples per cell,
exact public transport vector and cache residency before calculating medians
and ranges. The `failed-933a1d4/raw/nonqualifying` constructor rehearsals are validation
history only and must not be used as performance evidence.


The [first933 qualification](failed-933a1d4/README.md) is preserved as a failed
candidate: landed small fallback nil/reused rows regressed 12.2%/10.9% across
five separated ranges. An [independent mechanism adviser](raw/values-fallback-adviser.md)
confirmed the shared lazy helper retried a stale mapping when the existing
retained-mapping cap prohibited growth. The repaired helper checks that live
cap before budget/remap/retry work. It adds no sticky denial state, and normal
mapped hits still return before this check. The lower-level remap safety guard
and pinned old mapping lifetime remain unchanged.

The authorized [allocation diagnostic](failed-933a1d4/raw/allocation-diagnostic/hot-space.txt)
is separate from throughput qualification.100000 hot ordinary Get calls
allocate 256B each for owned output, 512B each for fresh Snapshot handles, and
about 59.7B each for Node key scratch. Exact objects are ~2.93/op while Go's integer
benchmark field reports 2. Before/after profiles remove setup/calibration;
profile writing adds ~6.79MiB separately from DB.Get. Fresh snapshot allocation
is mandated by the existing handle-lifetime contract.

The isolated mapped-cache control removes an avoidable intermediate owned
copy. The baseline's public fixture already hits the file fallback cache, whose
append path returns its owned value directly for nil destinations. Therefore
public selection changes fallback→mmap while preserving the same output
allocation count; the copy repair prevents an additional mapped-cache output
allocation. Public throughput evidence alone cannot isolate that copy effect.

## Repaired qualification results

The [analysis](analysis.json) retains all 100 benchmark samples and 30 process
RSS observations; [benchstat](raw/benchstat.txt) retains statistical output.
Fixture inventories [before](raw/fixture-before.txt) and
[after](raw/fixture-after.txt) are equal. Times below are median (min–max),
ns/op; allocations are Go's integer benchmark field.

| Row | Base ns/op | Candidate ns/op | Median change | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Small fallback, nil | 1096 (1092–1099) | 1118 (1114–1149) | +2.0% | 512 → 512 | 1 → 1 |
| Small fallback, reused | 1041 (1031–1060) | 1044 (1037–1050) | +0.3% | 0 → 0 | 0 → 0 |
| OS-warm open/map/read/close | 85097 (84679–87190) | 89430 (87295–91088) | +5.1% | 299166 → 299321 | 13 → 15 |
| 32KiB fallback, cache off | 64736 (64356–66117) | 64231 (64093–65233) | -0.8% | 32773 → 32771 | 1 → 1 |
| 32KiB mapped cache, pre-admitted | 6018 (5980–6164) | 3207 (3004–3522) | -46.7% | 65536 → 32768 | 2 → 1 |
| 32KiB mapped decode, cache off | 63850 (63458–64852) | 63452 (63240–63844) | -0.6% | 32768 → 32768 | 1 → 1 |
| Grouped ReadUnsafeTo | 97.96 (94.25–106.2) | 97.31 (96.64–98.69) | -0.7% | 0 → 0 | 0 → 0 |
| Normal public Get | 2392 (2363–2455) | 2410 (2386–2503) | +0.8% | 268 → 268 | 1 → 1 |
| Normal public GetAppend | 2123 (2117–2233) | 2141 (2127–2189) | +0.8% | 0 → 0 | 0 → 0 |
| Frozen pointer public Get | 3210 (3184–3295) | 1997 (1990–2029) | -37.8% | 827 → 827 | 2 → 2 |

The controlled public row improves 37.8% in elapsed time (+60.8% byte throughput),
with unchanged owned output placement, CRC/cache counts and 827B/op. The isolated
pre-admitted cache row improves 46.7% and removes exactly one 32KiB intermediate
allocation. Cache-off decode/fallback and normal public constructor guards
have overlapping ranges. Five samples support the retained rank-test results
but do not provide a 95% confidence interval (benchstat requires at least six);
no broad hardware, cold-disk or workload claim follows from this fixture.
Benchstat is an analysis binary built with Go1.25; measured binaries use Go1.26.3.

The small nil fallback retains a 2.0% median cost (22ns/op, p=0.008, separated
ranges), with unchanged 512B/one allocation; the reused fallback has overlapping
ranges (+0.3%, p=0.841). An initial mapped miss now reads the live retained-map
cap to avoid an unsafe growth/redundant retry; this cost is disclosed separately
from the removed 12% retry regression. OS-cache-warm lifecycle cost increases
5.1% (p=0.008), 155B/op and two allocations because owned append now attempts
lazy mapping/admission before fallback. This row includes checked close and
is outside the steady read claim. These are acceptance tradeoffs for independent
review, not silently discarded samples.

The public raw-length cache counter is identically 9,389,893B with a 67,108,864B
budget. Active mapped bytes increase 236,469→873,560B (637,091B of value segments).
GC warm heap deltas are median 13,095,864B (12,875,176–13,307,352) for base and
13,083,088B (13,082,816–13,083,248) for candidate. Setup-inclusive public fixture
peak RSS spans 136,308–138,060KiB and 138,472–143,028KiB respectively. Normal public
guard process RSS spans 147,908–169,624KiB and 139,488–170,392KiB; internal process
RSS spans 15,760–90,984KiB and 15,828–92,052KiB. These mixed-process peaks include
fixture preparation and calibration. They are not isolated cache measurements.

The runtime and harness remain frozen at the recorded identities. The final
packet/documentation commit changes no Go source after9631f9d; its hashes and
all retained artifact hashes are recorded in [packet-inventory.json](packet-inventory.json).
The coordinator still owns independent final review, current-head CI and merge.

## Focused correctness reproduction

Use the recorded Go1.26.3 toolchain, `GOWORK=off GOMAXPROCS=2 GOMEMLIMIT=2GiB`.
The following reproduce the relevant checks; retained earlier raw outputs bind
to their original source identities rather than claiming a new broad run:

```sh
go test -race ./TreeDB/internal/valuelog
go test -race ./TreeDB/internal/valuelog -run 'ReadAppend|Grouped.*Cache|Sealed|DeadMapping|CurrentWritable' -count=1
go test ./TreeDB -run 'TestOwnedGetValueLogMmapCacheRouteAfterReopen|TestReopenVerify_WALOn_Checkpoint|TestReopenVerify_WALOn_WriteSync' -count=1
go vet ./TreeDB/internal/valuelog ./TreeDB
```

The first route/copy red proof uses the original base with only the new focused
test file installed. The cap red proof uses933 with only the added cap test.
Source inventories and raw outputs distinguish those red overlays from the
benchmark-only overlays used for final timing.
