# Owned persistent value-log reads (#4891)

**Candidate933 capture failed the small forced-fallback guardrail.** The full
100-cell/30-process packet is retained; the row must be repaired and requalified
before readiness. Values below describe this candidate only.

This packet compares merged decoder base `a217ee3b2ee68918905bc5357faa0e57775c22a6`
with runtime/harness candidate `933a1d4ec57f6715f2fb746fdc57136a7ddc89a6`.
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

[Original-base red tests](raw/correctness/red-base.txt) detect both missed lazy
mapping (`mmap=0`, fallback=1) and two allocations instead of the final output's
one allocation. Candidate full valuelog race tests pass, including codec/raw,
empty/prefix ownership, dictionaries/templates, verification identity,
eviction/close and concurrent reuse. Focused public ordinary Get tests cover
normal and readonly reopen plus mutation independence; public WAL checkpoint
and WriteSync reopen, GC/rewrite parity and stable-identity GC tests pass.
[Source-bound correctness inventory](raw/correctness/source-manifest.json) and
[construction audit](raw/correctness/review-audit.md) preserve the earlier
identities. Runtime/test sources remain unchanged at the final freeze. Vet of
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
[fixture-manifest.json](raw/fixture-manifest.json) hashes all17 files/5,659,493B.
It was checkpointed, closed, reopened and every value verified before export.
The immutable, readonly source is never opened by a measured process. Each
process makes a private byte copy, then applies existing supported snapshot
restore order: dictionary/template side stores first, main index second.
`RebindDurableRootSnapshotLayoutV1` rewrites copied index physical dependency
identities/checksums outside timing; logical roots, pointer records, dictionary
and value-log bytes stay identical. Full verification and checked final close
are mandatory. Naive copied physical identities correctly failed startup;
that [nonqualifying failure](raw/nonqualifying/provisional-fixture-validate-base.txt)
is retained. Before/after source inventories must remain identical.

Untimed final probes show base pointer1/inline0/cache1/CRC1/fallback1/mmap0 and
candidate pointer1/inline0/cache1/CRC1/fallback0/mmap1, each retaining9,389,893 raw
bytes within67,108,864 budget. Raw counter counts length, not backing capacity.
Existing scratch pools may provide oversized backing; GC heap, mmap bytes and
RSS describe different quantities. Heap deltas include other DB allocations;
RSS is a setup-inclusive peak, not isolated retained cache heap. This packet
makes no universal physical heap ceiling claim from the raw-length budget.

## Reproduction and retained results

Prepare isolated archives of the recorded commits, add the identical public
benchmark file and [internal benchmark-only overlay](../baseline-benchmark-overlay.go.txt)
to the base (install overlay as `owned_constructor_bench_test.go`), and build
`valuelog-{base,candidate}.test` and `treedb-{base,candidate}.test` with the
recorded Go toolchain. Export the canonical fixture with
`TREEDB_OWNED_VLOG_FIXTURE_EXPORT`, hash it using the manifest format, make it
readonly, and store binaries/fixture/logs/tmp under one capture directory.
The retained runner directory is
`/mnt/fast4tb/treedb-quicksilver-4891-values-20261001/final-a217ee3b`.

```sh
bash docs/benchmarks/treedb_owned_values_20261001/qualify.sh FROZEN_CAPTURE_DIR
python3 docs/benchmarks/treedb_owned_values_20261001/analyze.py RAW_LOG_DIR
```

`qualify.sh` writes10 raw cells per revision/pair, 30 process RSS files and
fixture hash checks. `analyze.py` requires100 samples, five samples per cell,
exact public transport vector and cache residency before calculating medians
and ranges. The `raw/nonqualifying` constructor rehearsals are validation
history only and must not be used as performance evidence.

## Candidate933 measurements (failed guardrail)

All rows have five fresh-process samples. [Analysis](analysis.json) retains every
metric/sample and ranges; [benchstat](raw/benchstat.txt) preserves statistical
comparison. Five samples are insufficient for a95% confidence interval; stable
route/allocation evidence is separate from throughput inference.

| Row | Base ns/op | Candidate ns/op | Change | Base→candidate B/op; allocations |
|---|---:|---:|---:|---|
| Pre-admitted mapped cache |6373|3091|-51.5%|65536→32768;2→1|
| Mapped decode, cache off |64810|63607|-1.9%|32768→32768;1→1|
|32KiB fallback, cache off |64934|64288|-1.0%|32773→32771;1→1|
| OS-warm open/read/close |84899|87649|+3.2%|299166→299321;13→15|
| Frozen public owned Get |3312|2015|-39.2%|827→827;2→2|
| Normal public Get |2426|2384|-1.7%|268→268;1→1|
| Normal public GetAppend |2117|2130|+0.6%|0→0;0→0|
| Landed512B fallback nil |1097|1231|+12.2%|512→512;1→1|
| Landed512B fallback reused |1040|1153|+10.9%|0→0;0→0|
| Landed grouped ReadUnsafeTo |98.61|101.4|+2.8%|0→0;0→0|

The cache-copy reduction is isolated by equal residency. Public frozen route
combines transport selection and the existing file/cache path; both revisions
have exactly one pointer/CRC/cache hit and the same raw cache length. Ordinary
public Get still needs owned output, while827B/two allocations are unchanged. The second allocation's exact
attribution requires a separate diagnostic; these counters alone do not locate it. Normal canonical public guards
have distinct outer-leaf/template conditions and constructor layout noise, so
their allocation counts are not interchangeable with the frozen route.

Public active mapped bytes rise236469→873560 (adding637091B value segments);
raw retained length stays9389893. Median GC warm heap delta is13,091,744→13,083,088B,
with candidate range12878744–13290280B. Peak setup-inclusive public-route RSS
base range136148–137232KiB; candidate138172–144212KiB. These include Open,
verification and benchmark calibration, and do not isolate physical cache
backing. Cold-open adds two allocations and modest latency for map setup.

Small fallback ranges do not overlap: nil1092–1126→1224–1278ns, reused
1032–1068→1148–1186ns. The landed guard holds a1-byte mapping at its dead-mapping
cap. New eligibility retry visits manager budget/remap then reports the same
stale mapping exists; owned caller repeats a mapped read that cannot grow.
This approximately120ns overhead is material despite the larger decode row
being unaffected. An independent read-only mechanism check precedes repair.

The authorized allocation-only diagnostic isolates100000 hot owned Get calls
with cumulative allocs snapshots before/after the loop. Both use the933 runtime
and identical fixture; the diagnostic-only harness and artifact hashes are
retained in `raw/allocation-diagnostic`. DB.Get's delta is25,600,000B/100000
objects of required owned output,51,200,000B/100000 fresh snapshot objects, and
5,970,112B/93283 key scratch objects. SnapshotPool intentionally allocates a fresh
exported handle (`db/pools.go:35`). This explains827.7B/op and~2.93objects/op;
the Go benchmark's integer allocs/op field reports2. The before-profile writer
adds about6.79MiB/31508objects separately from DB.Get's call tree. Fixture setup
and calibration precede the before snapshot. No throughput conclusion comes
from this diagnostic. An initial pprof analysis invocation used a mismatched
GOROOT; only analysis was retried with explicit Go1.26.3 GOROOT.
