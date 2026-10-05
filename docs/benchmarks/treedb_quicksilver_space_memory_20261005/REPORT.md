# TreeDB space, memory, and maintenance: provisional 3M evidence

This packet records the baseline and bounded failure diagnostics for [#5001](https://github.com/snissn/gomap/issues/5001). Candidate acceptance and final publication remain pending. One profiled primary run supports these observations; it is not an unprofiled throughput comparison.

<!-- BEGIN summary -->
Full reduced apparent WAL-excluded bytes by 47.39%, from 2,832,310,293 to 1,490,201,793. All three completion flags remained false. Default Exhaustive reached its 1800-second limit, so minimum attainable size remains unknown.
<!-- END summary -->

## Storage boundaries

All table values are bytes. Cells show apparent / allocated (`st_blocks * 512`). The directory census includes dictionary storage; WAL is separated from persistent value and outer-leaf storage.

<!-- BEGIN storage_table -->
| State | Dictionary | Outer leaves | User values | Index | Metadata | WAL | Total excluding WAL |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| final-before-verify | 786,927 / 811,008 | 1,671,795,127 / 1,672,167,424 | 1,072,949,478 / 1,072,992,256 | 86,769,664 / 86,773,760 | 9,097 / 24,576 | 12 / 4,096 | 2,832,310,293 / 2,832,769,024 |
| before-maintenance | 786,927 / 811,008 | 1,671,795,127 / 1,672,167,424 | 1,072,949,478 / 1,072,992,256 | 86,769,664 / 86,773,760 | 9,097 / 24,576 | 12 / 4,096 | 2,832,310,293 / 2,832,769,024 |
| full-before-oracle | 786,927 / 811,008 | 370,316,345 / 370,368,512 | 1,072,949,478 / 1,072,992,256 | 46,137,344 / 46,141,440 | 11,699 / 24,576 | 12 / 4,096 | 1,490,201,793 / 1,490,337,792 |
| full-after | 786,927 / 811,008 | 370,316,345 / 370,368,512 | 1,072,949,478 / 1,072,992,256 | 46,137,344 / 46,141,440 | 11,699 / 24,576 | 12 / 4,096 | 1,490,201,793 / 1,490,337,792 |
<!-- END storage_table -->

`final-before-verify` is retained post-workload storage. `before-maintenance` follows the initial writable public oracle. `full-before-oracle` follows compactor close; `full-after` follows the full writable oracle and close. These are distinct boundaries even where byte totals agree.

<!-- BEGIN oracle_delta -->
The Full oracle changed total apparent bytes by 0 and allocated bytes by 0. All-value verification covered 3,000,000 present keys and 6,040,000 misses.
<!-- END oracle_delta -->

## Maintenance costs and limits

<!-- BEGIN maintenance_table -->
| Attempt | Outcome | GNU time elapsed (s) | Receipt elapsed (s) | Process RSS high-water bytes | Sampled apparent / allocated disk maximum, including WAL |
| --- | ---: | ---: | ---: | ---: | ---: |
| full | completed | 41.76 | 42.085 | 2,018,893,824 | 3,497,238,380 / 3,497,709,568 |
| exhaustive | timed_out | unavailable | 1800.264 | unavailable | 1,947,537,066 / 1,947,705,344 |
| restored-copy Full, batch 8192 | failed | 11.39 | 12.015 | 1,041,219,584 | 2,366,509,623 / 2,366,685,184 |
<!-- END maintenance_table -->

<!-- BEGIN debt -->
Full reported leaf-GC debt of 28,854,392 bytes in 1 generation. `fully_compacted=false`, `policy_fully_compacted=false`, and `byte_minimized=false` are preserved. Debt is not automatically safe to delete: recoverable roots and stable resources remain protected.
<!-- END debt -->

<!-- BEGIN failed -->
The failed 8192 attempt used a restored copy: its initial writable oracle changed apparent WAL-excluded bytes from 1,947,537,054 to 1,849,486,263. It failed with an incompatible duplicate dictionary stable identity; its 11.39-second failure time cannot be compared as a successful batch-speed result. Both the timed-out original and failed copy passed read-only full-fixture verification without census changes. R's causal regression and production repair remain pending in this packet.
<!-- END failed -->

Disk observations sample owned DB files once per second, so maxima are sampled rather than exact instantaneous peaks; observation cost is included. Receipt elapsed includes polling/reaping overhead. Exhaustive emitted no completed JSON result or GNU-time summary after process-group termination; unavailable metrics remain unavailable.

## Memory observations

<!-- BEGIN memory_table -->
| Profiled phase | HeapAlloc before | HeapAlloc after | Allocated bytes in bracket | Bytes per operation |
| --- | ---: | ---: | ---: | ---: |
| quicksilver_hits | 350,708,568 | 732,844,368 | 4,104,913,008 | 684.152 |
| quicksilver_misses | 455,516,800 | 513,771,816 | 58,255,016 | 9.709 |
| quicksilver_mixed | 456,571,960 | 475,933,000 | 441,744,048 | 73.624 |
| quicksilver_concurrent | 462,206,744 | 910,211,824 | 705,703,208 | 140.998 |
<!-- END memory_table -->

<!-- BEGIN rss -->
The baseline observer retained 291 samples of one process. Maximum observed kernel VmHWM was 3,762,454,528 bytes; maximum sampled RSS was 3,762,454,528. At that RSS sample, anonymous/file/shared bytes were 1,215,500,288 / 2,546,954,240 / 0. Independent component maxima remain separate in RESULTS.json and must not be added together.

The separate 256MiB outer-leaf mapping-budget diagnostic retained 333 samples, with observed kernel VmHWM 2,351,124,480 bytes and sampled RSS peak 2,351,124,480. Its frozen producer chain and full-fixture oracle are bound here. This single profiled setting run does not establish a repeatable memory or throughput improvement.
<!-- END rss -->

HeapAlloc endpoints are not peaks. Profiled StatsBefore follows explicit pre-profile GC plus setup; StatsAfter precedes post-profile GC. Concurrent allocation measurement ends at reader join and excludes the writer-only tail included in composition time. Delta inuse-space profiles are net sampled changes, not total retained cache heap. RSS has no phase markers and cannot establish phase-specific RSS peaks. RESULTS.json preserves reported cache slots, capacity, entries, and retained payload bytes at both boundaries; these statistics are not an atomic cache-manager snapshot or a complete metadata-heap measurement.

Known harness footprints are 3,000,000 oracle-state bytes, 9,375,000 distinct-tracking bytes, and 8,000,000 sample bytes; they are not directly subtractable from RSS. GOMEMLIMIT=2GiB is a soft Go memory goal. User and outer-leaf mapping budgets are separate controls, not process memory limits. The separate leaf-budget run is a setting diagnostic; the matched public candidate matrix remains pending.

## Bounded diagnosis and operation

<!-- BEGIN diagnostic -->
The intentionally terminated 60-second diagnostic CPU profile attributed 95.41% cumulatively to candidate external-reference closure scans and 73.46% to nested Zstd decoding. These overlapping cumulative shares must not be added. They describe this interval on a rebound diagnostic copy, not an accepted optimization or full-run scaling result.
<!-- END diagnostic -->

Close the public owner before supported offline maintenance. Preserve failed originals; use owned copies and read-only verification for diagnosis. Do not clear recovery/dictionary gates to reclaim reported debt. Scheduler defaults do not prove observed execution or a hard storage bound; qualified churn curves are pending. See [value-log lifecycle](../../../TreeDB/docs/spec/value-log-lifecycle.md) and [recoverable-root maintenance](../../../TreeDB/docs/spec/recoverable-root-set-maintenance-3681.md).

## Provenance and remaining gates

<!-- BEGIN source -->
Frozen source `fb42d5ba229d2cbfba8727272af02f9544a33389` has whole-tree equality with landed `a6b383b6c0d6269a598390032ecce9fd619a4eac`. Hash-bound source/build/native/runner receipts and the actual collector command are retained under raw/maintenance. The publication does not claim that its documentation head was measured. The original pre-measurement maintenance build receipt is missing: a separate post-measurement reconstruction from frozen source produced a bit-identical treemap binary (`1d0f9222676a15c59fbd09214d0b158ff56ed0979bf06d50c7cfefe60cf23406`). That reconstruction supplements preserved command logs; it does not become a pre-measurement receipt.
<!-- END source -->

The generic-v1 primary fixture uses seed 24, 3M keys, 6M aggregate reads, 40k updates, four readers, an eight-second concurrent bracket, 90% configured misses, and GOMAXPROCS=12. A shared Linux i5-11400F was used because a dedicated runner was unavailable; root owned one native measurement process at a time. Public candidate pairs and diagnostic overlays remain distinct.

Raw paths and exact SHA-256 values are in inputs.json and RESULTS.json. Larger profiles remain at their recorded native paths with digests. Gzipped raw inputs preserve exact original bytes and bind both compressed and original digests. REPORT.md supplies hash-pinned prose around generated numerical blocks. Reproduce extraction with `python3 extract.py --check`; run bounded validation with `python3 selfcheck.py` and `python3 -O selfcheck.py`. No command here builds binaries or opens a database.

<!-- BEGIN pending -->
- M: three matched public durable/fast/holdout pairs, profiled primary pairs, and cache metadata distribution
- C: qualified default and opt-in maintenance churn curves
- R: causal restored-copy regression, production repair, and qualified post-repair maintenance
- Accept the missing original maintenance build receipt as a documented reconstruction limitation
- Independent artifact review, latest-head CI, mature Codex review, and final source reconciliation
<!-- END pending -->
