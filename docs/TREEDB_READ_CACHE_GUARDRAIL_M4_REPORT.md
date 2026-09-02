# TreeDB read/cache guardrail M4 report

Issue: #2787. Parent tracker: #2782. Handoff consumer: #2788.

This is the **actual M4 read/cache/reopen guardrail evidence** for the
span-native/backlog/cache-admission default-readiness track. It supersedes the
earlier preflight / matrix-contract version of this report.

## Outcome

M4 found **no blocking read/scan/reopen/cache correctness regression** for the
runtime candidates carried into #2788:

- `span_native_c4_immediate`
- `span_native_c4_adaptive`
- `span_native_c16_immediate`

The diagnostic `span_native_c4_cache_disabled` row is also recorded, but it is
not a default-readiness candidate because it removes the outer-leaf read cache
axis. Treat it as a cache-axis interpretation row only.

The write/checkpoint rows still show multi-second checkpoint boundaries. Per the
#2794 coordinator decision, checkpoint debt is now an accepted analytics/model
tradeoff for this cycle rather than an automatic runtime-mitigation blocker.
M4 therefore reports checkpoint wall time and normalized write+checkpoint
throughput, but does **not** default-enable anything by itself. #2788 must make
the final default-on/default-off decision and must not decide from write
throughput alone.

## Artifact roots

Public references intentionally redact the remote host, user, and address.

- 1MM smoke root, coordinator-accepted before the 10MM run:
  `<remote-profile-root>/2787_m4_smoke_20260617_020426`
- 10MM actual M4 matrix root:
  `<remote-profile-root>/2787_m4_actual_20260617_020700`
- Candidate/base commit for the actual matrix:
  `f2b9cdfb720a4d15461803e2262078efe8439c5b`
- Generated matrix summaries:
  `<remote-profile-root>/2787_m4_actual_20260617_020700/m4_guardrail_summary.md`
  and
  `<remote-profile-root>/2787_m4_actual_20260617_020700/m4_guardrail_summary.json`

Each matrix row has row-local `command.sh`, `variant.env`, `COMMIT`,
`benchprof_results.{json,md}`, `insights.{json,md,html}`, pprof files, trace,
and unified-bench stdout/stderr artifacts.

Common 10MM shape:

```text
-dbs treedb
-keys 10000000
-valsize 128
-batchsize 8000
-path-label m8-m14-10mm-gate
-treedb-journal-lanes=1
-progress=false
```

Span-native rows additionally used:

```text
-treedb-flush-apply-min-entries=1
-treedb-flush-apply-min-spans=1
-treedb-flush-apply-min-bytes=1
-treedb-flush-apply-span-native
-treedb-flush-backlog-coalescing
```

## Focused validation

The 10MM actual run completed the required focused correctness/cache tests before
benchmark rows:

```text
git diff --check
go test ./TreeDB/db ./TreeDB/caching ./TreeDB/zipper ./cmd/unified_bench -count=1
go test ./TreeDB -run 'TestReopenVerify_(WALOn_Checkpoint|WALOn_WriteSync|WALOn_Checkpoint_LeafPagesInValueLog|LeafPageLogGroupedFrameCRCIntegrityModes)$' -count=1
go test ./TreeDB/db -run 'TestLeafPageReadCache(AdaptiveWriteAdmission|ConcurrentSetAssociativeAccess|WriteAdmissionImmediate)' -count=1
go test -race ./TreeDB/db -run 'TestLeafPageReadCache(AdaptiveWriteAdmissionSkipsWhenSlotLockContended|ConcurrentSetAssociativeAccess)' -count=1
python3 scripts/treedb_m14_matrix_summary.py --self-test
```

All focused commands passed in
`<remote-profile-root>/2787_m4_actual_20260617_020700/focused_tests/`.

## Rows and workload families

Rows carried forward:

- `default_unconfigured`
- `forced_off` rollback row for the write/checkpoint subset
- `span_native_c4_immediate`
- `span_native_c4_adaptive`
- `span_native_c16_immediate`
- `span_native_c4_cache_disabled`

Workloads run:

1. write/checkpoint subset:
   `sequential_write,batch_random,random_write` with checkpoint-between-tests.
2. settled point-read after write/checkpoint/reopen:
   `sequential_write,random_write,random_read` with checkpoint-between-tests,
   settle-before-scans, cache stats before reads, and read-require-hit.
3. settled full/prefix scan:
   `sequential_write,random_write,full_scan,prefix_scan` with
   checkpoint-between-tests, settle-before-scans, cache stats before reads,
   `-range-queries 200`, and `-range-span 100`.
4. mixed/debt read+scan:
   `sequential_write,random_write,random_read,full_scan,prefix_scan` without a
   settle/checkpoint boundary before reads/scans, with cache stats before reads,
   read-require-hit, `-range-queries 200`, and `-range-span 100`.

Note on checkpoint stage counters: settled point-read/scan rows intentionally
close/reopen before read/scan measurement. That gives the required reopen/read
boundary, but live end-of-run checkpoint stage counters are reset by the reopen.
Use their checkpoint wall table for per-row boundary timing and the
write/checkpoint subset for the full stage-counter comparison.

## Write/checkpoint subset

| Row | seq write ops/s | batch random ops/s | random write ops/s | RW checkpoint | checkpoint efficiency | write+checkpoint | Δ write+checkpoint vs default |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `default_unconfigured` | 2,498,997 | 300,434 | 138,012 | 3.16s | 3.165 Mops/s | 132.2 kops/s | +0.0% |
| `forced_off` | 2,713,454 | 448,802 | 151,633 | 12.31s | 0.812 Mops/s | 127.8 kops/s | -3.4% |
| `span_native_c4_immediate` | 2,719,463 | 713,040 | 276,140 | 9.96s | 1.004 Mops/s | 216.6 kops/s | +63.8% |
| `span_native_c4_adaptive` | 2,719,136 | 695,100 | 274,665 | 7.83s | 1.277 Mops/s | 226.0 kops/s | +70.9% |
| `span_native_c16_immediate` | 2,669,700 | 763,990 | 322,050 | 7.16s | 1.397 Mops/s | 261.7 kops/s | +97.9% |
| `span_native_c4_cache_disabled` | 2,678,978 | 700,836 | 303,645 | 7.74s | 1.292 Mops/s | 245.9 kops/s | +85.9% |

Checkpoint stage counters for the write/checkpoint subset:

| Row | flushmu wait | active wait | flush_all | reducer publish | backend boundary | leaf_value_log_sync |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `default_unconfigured` | 34.6s | 34.6s | 26.0s | 23.7s | 1.614s | 12.422s |
| `forced_off` | 39.1s | 39.1s | 20.2s | 17.9s | 0.054s | 12.427s |
| `span_native_c4_immediate` | 28.7s | 28.7s | 16.2s | 14.4s | 1.845s | 0.224s |
| `span_native_c4_adaptive` | 22.4s | 22.4s | 16.1s | 14.3s | 0.944s | 0.202s |
| `span_native_c16_immediate` | 22.4s | 22.4s | 17.5s | 13.8s | 0.565s | 2.145s |
| `span_native_c4_cache_disabled` | 31.3s | 31.3s | 16.0s | 14.3s | 0.891s | 0.190s |

Interpretation:

- The candidate rows materially improve write+checkpoint throughput even though
  the measured checkpoint boundary remains multi-second.
- `span_native_c16_immediate` is the strongest write+checkpoint row in this run
  and is therefore a plausible #2788 candidate, but it still needs the read/scan
  evidence below and a default-policy decision in #2788.
- `span_native_c4_adaptive` preserves most of the write/checkpoint gain while
  reducing write-side cache stores and evictions substantially.

## Settled point-read after checkpoint/reopen

| Row | random write ops/s | RW checkpoint | write+checkpoint | random_read ops/s | Δ random_read vs default | cache hits | cache misses | cache stores | cache evictions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `default_unconfigured` | 409,924 | 1.08s | 392.5 kops/s | 257,149 | +0.0% | 892,574 | 9,107,426 | 665,408 | 632,640 |
| `span_native_c4_immediate` | 564,902 | 819.72ms | 539.9 kops/s | 258,872 | +0.7% | 890,483 | 9,109,517 | 665,973 | 633,205 |
| `span_native_c4_adaptive` | 593,254 | 819.01ms | 565.8 kops/s | 258,747 | +0.6% | 892,189 | 9,107,811 | 665,923 | 633,155 |
| `span_native_c16_immediate` | 663,759 | 783.19ms | 631.0 kops/s | 260,003 | +1.1% | 891,630 | 9,108,370 | 666,547 | 633,779 |
| `span_native_c4_cache_disabled` | 601,747 | 782.96ms | 574.7 kops/s | 259,517 | +0.9% | 0 | 0 | 0 | 0 |

Interpretation: the settled read-after-reopen guardrail passed for c4 immediate,
c4 adaptive, and c16 immediate. Read throughput is flat to slightly positive,
and cache hit/miss/store/eviction behavior is effectively unchanged for the
cache-enabled candidates. The cache-disabled row has zero cache counters by
construction and remains diagnostic only.

## Settled full/prefix scans

| Row | random write ops/s | RW checkpoint | full_scan ops/s | Δ full_scan | prefix_scan ops/s | Δ prefix_scan | scan cache hits | scan cache misses | scan cache stores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `default_unconfigured` | 304,353 | 1.76s | 4,053,999 | +0.0% | 4,655,751 | +0.0% | 1 | 1,317,353 | 1 |
| `span_native_c4_immediate` | 583,923 | 824.78ms | 4,407,977 | +8.7% | 4,598,515 | -1.2% | 1 | 1,317,161 | 1 |
| `span_native_c4_adaptive` | 606,990 | 809.92ms | 4,399,775 | +8.5% | 4,646,039 | -0.2% | 1 | 1,317,157 | 1 |
| `span_native_c16_immediate` | 717,713 | 389.73ms | 4,505,723 | +11.1% | 4,618,022 | -0.8% | 1 | 1,277,790 | 1 |
| `span_native_c4_cache_disabled` | 600,189 | 817.50ms | 4,817,798 | +18.8% | 4,756,412 | +2.2% | 0 | 0 | 0 |

Interpretation: settled scan guardrails passed for cache-enabled candidates.
Full-scan throughput improved; prefix-scan throughput was within roughly one
percent of default for the candidates and did not show a blocking regression.
The scan cache counters show the expected range-scan pattern: almost all misses
are skipped by read-miss admission rather than filling the point-read cache.

## Mixed/debt read+scan without a settle boundary

| Row | random write ops/s | random_read ops/s | Δ random_read | full_scan ops/s | Δ full_scan | prefix_scan ops/s | Δ prefix_scan | cache hits | cache misses | cache stores | cache evictions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `default_unconfigured` | 330,458 | 234,868 | +0.0% | 4,264,279 | +0.0% | 4,235,835 | +0.0% | 826,394 | 14,026,111 | 5,497,848 | 5,465,080 |
| `span_native_c4_immediate` | 548,959 | 240,767 | +2.5% | 4,764,528 | +11.7% | 4,707,240 | +11.1% | 831,094 | 13,481,517 | 4,947,251 | 4,914,483 |
| `span_native_c4_adaptive` | 595,158 | 240,620 | +2.4% | 4,721,474 | +10.7% | 4,388,716 | +3.6% | 954,435 | 13,358,911 | 2,082,349 | 2,049,581 |
| `span_native_c16_immediate` | 565,981 | 240,848 | +2.5% | 4,640,803 | +8.8% | 4,983,189 | +17.6% | 869,746 | 13,586,573 | 5,106,840 | 5,074,072 |
| `span_native_c4_cache_disabled` | 545,242 | 239,785 | +2.1% | 4,658,708 | +9.2% | 4,885,721 | +15.3% | 0 | 0 | 0 | 0 |

Interpretation: the mixed/debt read+scan guardrail passed. Candidate rows did
not merely move write work into later reads/scans in this shape; point reads and
scans are flat to faster than default while writes remain faster.

## Cache admission and cache-axis interpretation

Adaptive admission effects are visible in the write-heavy rows:

| Workload / row | write admission attempts | write stores | write skips | total cache stores | total evictions |
| --- | ---: | ---: | ---: | ---: | ---: |
| write/checkpoint `span_native_c4_immediate` | 0 | 0 | 0 | 12,410,955 | 12,378,187 |
| write/checkpoint `span_native_c4_adaptive` | 12,144,189 | 1,035,107 | 11,109,082 | 1,035,107 | 1,002,339 |
| mixed/debt `span_native_c4_immediate` | 0 | 0 | 0 | 4,947,251 | 4,914,483 |
| mixed/debt `span_native_c4_adaptive` | 4,322,127 | 1,429,239 | 2,892,888 | 2,082,349 | 2,049,581 |

The settled point-read row shows adaptive does not starve read-miss admission:
`span_native_c4_adaptive` recorded 665,923 read-miss admission stores and
8,441,888 read-miss admission skips, essentially matching the default and
immediate rows.

The cache-disabled row is useful because it proves the benchmark can separate
cache effects, but it should not be treated as a default candidate: it records
zero read-cache hits/misses/stores/evictions and removes the telemetry/read-cache
behavior that #2787 is guarding.

## Flush/apply, span-native, backlog, and storage counters

Write/checkpoint subset:

| Row | old-leaf decode B/op | leaf merges/op | replacement pages/op | append frames/op | span-native used ops | fallback close/checkpoint ops | backlog admitted runs | backlog extra ops |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `default_unconfigured` | 1,639.8 | 0.362 | 0.469 | 0.362 | 0 | 0 | 0 | 0 |
| `forced_off` | 1,609.0 | 0.357 | 0.463 | 0.358 | 0 | 0 | 0 | 0 |
| `span_native_c4_immediate` | 1,403.3 | 0.320 | 0.415 | 0.321 | 29,688,493 | 218,612 | 2 | 986,896 |
| `span_native_c4_adaptive` | 1,367.8 | 0.313 | 0.406 | 0.314 | 29,686,164 | 218,612 | 2 | 986,896 |
| `span_native_c16_immediate` | 1,406.2 | 0.320 | 0.416 | 0.321 | 29,196,563 | 711,003 | 2 | 986,896 |
| `span_native_c4_cache_disabled` | 1,367.7 | 0.313 | 0.406 | 0.314 | 29,686,165 | 218,612 | 2 | 986,896 |

No unexpected span-native fallback family dominated. The expected
close/checkpoint fallback is still visible because checkpoint drains remain a
non-span-native boundary in this cycle.

Storage footprint highlights:

| Workload / row | index.db | leaf_vlog | WAL | value_vlog |
| --- | ---: | ---: | ---: | ---: |
| write/checkpoint `default_unconfigured` | 632 MiB | 3.7 GiB | 12 B | 0 B |
| write/checkpoint `span_native_c4_adaptive` | 440 MiB | 3.3 GiB | 12 B | 0 B |
| write/checkpoint `span_native_c16_immediate` | 444 MiB | 3.3 GiB | 12 B | 0 B |
| settled point-read `default_unconfigured` | 38 MiB | 1.3 GiB | 12 B | 0 B |
| settled point-read `span_native_c4_adaptive` | 38 MiB | 1.3 GiB | 12 B | 0 B |
| settled scan `default_unconfigured` | 125 MiB | 1.3 GiB | 12 B | 0 B |
| settled scan `span_native_c4_adaptive` | 128 MiB | 1.3 GiB | 12 B | 0 B |
| mixed/debt `default_unconfigured` | 128 MiB | 1.4 GiB | 12 B | 0 B |
| mixed/debt `span_native_c4_adaptive` | 36 MiB | 1.3 GiB | 12 B | 0 B |

## Profile signals

The full CPU, allocation, block, and mutex profile tops are in each row's
`insights.{json,md,html}`. Important M4 signals:

- Random-write CPU in the write/checkpoint subset is still dominated by leaf-log
  LZ4 decode/compress and memory movement. Totals: default 100.66s,
  c4 immediate 76.64s, c4 adaptive 75.15s, c16 immediate 95.29s,
  cache-disabled c4 73.86s.
- Random-write alloc-space is higher for span-native rows and should be carried
  into #2788 as a performance tradeoff: default 2702.98MB, c4 adaptive
  3644.06MB, c16 immediate 3.38GB, cache-disabled c4 3594.13MB.
- Random-write block/mutex contention improves materially for the c4 rows:
  default block/mutex totals 1754.51s / 703.59s; c4 adaptive 655.99s / 38.68s;
  c16 immediate 764.60s / 96.52s.
- Settled random-read CPU totals are flat across cache-enabled rows: default
  39.73s, c4 immediate 39.45s, c4 adaptive 39.53s, c16 immediate 39.27s.
  LZ4 decode remains the top read signal.
- Cache-disabled point-read CPU shifts further toward LZ4 decode, as expected
  for a diagnostic row with no outer-leaf read cache.

## #2788 handoff

M4 gives #2788 enough evidence to consider a default-on decision, subject to the
#2788 final policy gate:

- The actual 10MM same-host matrix ran after the #2794/#2795 unblock.
- Focused reopen/read-integrity/cache tests passed on the candidate head.
- Settled point-read after checkpoint/reopen did not regress for c4 immediate,
  c4 adaptive, or c16 immediate.
- Settled full/prefix scans did not show a blocking regression for cache-enabled
  candidates.
- Mixed/debt point-read and scan rows did not show a read/scan regression caused
  by deferred write work.
- Adaptive cache admission substantially reduced write-side cache churn while
  preserving settled read-miss admission behavior.
- Checkpoint boundaries remain multi-second and must be reported as part of any
  default decision, but normalized write+checkpoint throughput matches the
  accepted #2794 model direction.
- Random-write allocation volume is higher for the span-native rows and should
  remain visible in #2788's risk/tradeoff section.

Recommended #2788 framing:

1. Do not default-enable from write throughput alone.
2. If selecting c4, prefer the adaptive cache-admission axis unless #2788 has a
   separate reason to keep immediate write admission; M4 shows adaptive reduces
   cache churn without hurting settled reads.
3. If selecting c16, treat it as plausible based on M4's write+checkpoint and
   read/scan results, but keep the c16 checkpoint and allocation tradeoffs
   explicit.
4. Do not treat `span_native_c4_cache_disabled` as a default candidate.
5. Keep public issue/PR text redacted to `<remote-profile-root>/...` paths.
