# Ordinary ColumnGraph lifecycle measurements (#5338)

All six predeclared samples completed successfully: three baseline and three candidate loads in AB/AB/AB order, each a fresh process with `-test.benchtime=1x -test.count=1`. There were no failed samples; no benchmark invocation was retried or excluded. The result is a matched successful-lifecycle cost observation, not performance acceptance or a speedup claim. Timing ranges overlap and the individual phase medians move in different directions.

## Sources and execution

Baseline is exact main `d7db5409e20412f4dac4e1a3f902484b05e014e1` plus the identical lifecycle benchmark overlay; candidate is authenticated commit `9cd7557fb705b07261b801a2f42aa90dee4ce171`, tree `dff272aeed265395770cb6eba7b17eeb6463618b`. The frozen benchmark SHA-256 is `5e9c0d2922e3a6bfd163e33ea5c924a72d801ec298f9d5abe2c40127d718d7b2`; the common helper is unchanged at `45d2186bbc6ece189f56ee4fd44226250ed2a9e458d739d3dd4eb34ecaec6826`.

Both normal binaries used Go `go1.26.10`, the same verified Go executable, effective build environment/settings and no enabled race instrumentation. The separate [build-equivalence receipt](lifecycle-build-equivalence.json) checks those properties in addition to the runner's frozen source and binary bindings. Runtime settings were `GOWORK=off`, `GOMEMLIMIT=4GiB`, `GOMAXPROCS=2` and the same owned `TMPDIR`. Builds, tests and measurements were serialized.

The fixture fixes native durable `cow_btree`, 4096 documents, 128 per request and one writer. Each sample performs 32 public upserts, one public Optimize and an explicit Checkpoint. Full document/vector/metadata and serving checks, original Close, physical-pin cleanup and fresh reopen run outside the benchmark timer. Each fresh Python worker launched exactly one benchmark child; every initial child-rusage field was zero. Peak RSS therefore belongs to that sample's benchmark child, not an earlier sample or the Python coordinator.

## Timings and allocation totals

Values are median [minimum–maximum], with three complete loads per arm. Standard totals cover the three timed phases; process wall/CPU cover the entire lifecycle. Medians of separate phases need not sum to the median total.

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Seed, ms/load | 414.379 [382.717–558.269] | 415.274 [382.278–458.773] |
| Optimize, ms/load | 285.636 [274.063–389.120] | 298.372 [241.435–347.185] |
| Explicit Checkpoint, ms/load | 19.806 [15.124–20.866] | 17.045 [16.701–19.662] |
| Standard total, ms/load | 818.624 [688.161–853.200] | 716.911 [697.696–782.122] |
| Standard allocation bytes/load | 133,508,352 [133,251,656–134,859,736] | 133,435,752 [133,290,072–134,702,544] |
| Standard allocation count/load | 690,169 [690,105–690,174] | 690,004 [689,964–690,123] |
| Whole process wall time, ms | 1,107.126 [968.573–1,170.689] | 1,069.155 [1,021.129–1,077.131] |
| Whole process user CPU, s | 0.879 [0.734–0.895] | 0.768 [0.760–0.820] |
| Whole process system CPU, s | 0.215 [0.202–0.254] | 0.225 [0.212–0.292] |

Phase allocation intervals are process-wide deltas. Observation/logging and Go benchmark bookkeeping have different boundaries from the standard `B/op` and `allocs/op`; the raw intervals remain available in every sample receipt.

| Phase | Baseline bytes/load | Candidate bytes/load | Baseline allocations/load | Candidate allocations/load |
| --- | ---: | ---: | ---: | ---: |
| Seed | 107,705,928 [107,544,856–107,808,720] | 107,572,032 [107,565,136–107,714,008] | 629,222 [629,210–629,231] | 629,194 [629,151–629,220] |
| Optimize | 25,126,320 [25,118,744–26,572,984] | 25,137,968 [25,129,048–26,557,040] | 59,769 [59,691–59,780] | 59,634 [59,624–59,719] |
| Explicit Checkpoint | 580,824 [580,480–580,888] | 583,776 [580,368–588,992] | 1,183 [1,179–1,183] | 1,184 [1,179–1,186] |

## Actual path activity and completion

All six samples had the following changes in the existing counters. These are observations of public publication/physical handoff/checkpoint activity, not invented graph-build counts.

| Phase | Publications | Logical sequence | Rollovers | Handoffs | Checkpoint runs |
| --- | ---: | ---: | ---: | ---: | ---: |
| seed | 32 | 32 | 6 | 6 | 0 |
| optimize | 1 | 1 | 2 | 2 | 2 |
| checkpoint | 0 | 0 | 0 | 1 | 1 |

Each sample verified a loaded, non-stale ordinary ColumnGraph with 4096 live rows and a clean snapshot, and logged its active manifest. All retained input documents and service-query assertions passed before original Close and again in a fresh owner. The original owner's physical-pin registry had zero active pins, identities and stable-namespace links after Close; all original and reopened cleanup errors were checked. The benchmark does not separately assert a reopened pin-registry count or report final COW-budget zero. Those stronger custody assertions belong to the independent handoff/resource controls.

At the explicit checkpoint end, all samples reported `total_bytes=36,662,736`, `retired_bytes=0` and `maintenance_used_bytes=675,840`. One candidate seed-end snapshot had `total_bytes=38,362,224`; the following Optimize-start snapshot was `38,262,768`, matching every other sample at that boundary. The raw interphase accounting change is retained without attributing a particular background refund or normalizing it away.

## Memory scopes

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| COW accounted peak through measured phases, bytes | 51,983,120 [51,983,120–51,983,120] | 51,983,120 [51,983,120–51,983,120] |
| Whole benchmark process peak RSS, MiB | 106.191 [104.727–106.969] | 108.004 [105.273–108.402] |
| Post-Close process heap after GC, MiB | 14.774 [14.766–14.784] | 14.779 [14.776–14.784] |

Unforced phase-end `HeapAlloc` medians in seed/Optimize/Checkpoint order were baseline: 58.008 / 84.062 / 85.386 MiB; candidate: 58.230 / 84.338 / 85.661 MiB. These are process heap snapshots, not independently retained graph bytes. The COW peak is finite owner accounting observed through the three measured phase ends. RSS includes setup, full validation, Close and reopen. The post-Close GC sample includes the expected-input fixture and shared/runtime caches; it is neither a graph-only memory figure nor a leak proof. MiB means 1,048,576 bytes.

This successful sequential workload does not force the captured-prefix overlap, exercise every preassignment refresh schedule, attribute the historical #5332 failure or qualify broad concurrent ingestion/power loss. The deterministic handoff and lifecycle/error controls remain separate evidence. Three samples per arm and overlapping timing ranges do not justify a performance improvement conclusion.

## Retained evidence

The published [measurement summary](measurement-summary.json), [raw evidence archive](raw-evidence.tar.gz), [build-equivalence receipt](lifecycle-build-equivalence.json) and [reproduction protocol](benchmark-protocol.md) retain the result and its bindings. Inside the archive, the exact member paths are `lifecycle-source-freeze.json`, `lifecycle-measurements/bound-manifest.json`, `lifecycle-measurements/summary.json`, `lifecycle-measurements/samples.json`, `lifecycle-build-baseline/build-receipt.json` and `lifecycle-build-candidate/build-receipt.json`. Original `benchmark.stdout`, `benchmark.stderr` and `receipt.json` files are under `lifecycle-measurements/sample-01-baseline/`, `lifecycle-measurements/sample-02-candidate/`, `lifecycle-measurements/sample-03-baseline/`, `lifecycle-measurements/sample-04-candidate/`, `lifecycle-measurements/sample-05-baseline/` and `lifecycle-measurements/sample-06-candidate/`. Build stdout/stderr and build-info files remain under the two build directories; the archive also retains `native_graph_lifecycle_benchmark.py` and `build_native_graph_lifecycle_benchmark.py`.

Large executables are excluded from the published archive. The retained build commands/source/toolchain bindings support reproduction, and these SHA-256 values identify the binaries actually measured: baseline `6f22611b2cbcca15fdc4d3cda059b0dad1ed902b5f40bccabf95e041cea70443` and candidate `e822448cf6fb931766f9b278e175dbf6d254e97b18d803ea6e94ac38c4c9ece7`. The published `measurement-summary.json` is a byte-identical copy of the archive's `lifecycle-measurements/summary.json`.

| Artifact | SHA-256 |
| --- | --- |
| `lifecycle-source-freeze.json` | `0e037265f65d222e4a353ccbc51d2c2fe7403b5a8474deb2c87869f962512476` |
| `lifecycle-measurements/bound-manifest.json` | `40ac14fb94608de433274981ef4e451ca6f27f911ed7d802818f28b11547ca18` |
| `lifecycle-measurements/summary.json` | `28cca229432f9c3fbf8e24b4e743561c87cc2f6377f904ab9fcf8bedf7ef3761` |
| `lifecycle-measurements/samples.json` | `669230959e2250705bacd9aa743b2d2f4472a7ac1ea6685711e739d2cd6eb7c4` |
| `lifecycle-build-equivalence.json` | `019ff4a71ad49b341308752af060d2e587e415317a6fef731e9b4e6fcfe488cf` |
