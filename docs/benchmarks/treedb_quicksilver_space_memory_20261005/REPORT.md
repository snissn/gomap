# TreeDB space, memory, and maintenance: provisional 3M evidence

This packet records the baseline and bounded failure diagnostics for [#5001](https://github.com/snissn/gomap/issues/5001). Candidate acceptance and final publication remain pending. One profiled primary run supports these observations; it is not an unprofiled throughput comparison.

<!-- BEGIN summary -->
Full reduced apparent WAL-excluded bytes by 47.39%, from 2,832,310,293 to 1,490,201,793.
<!-- END summary -->

All three completion flags remained false. Default Exhaustive reached its 1800-second limit, so minimum attainable size remains unknown.

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
The failed 8192 attempt's initial writable oracle changed apparent WAL-excluded bytes from 1,947,537,054 to 1,849,486,263.
<!-- END failed -->

The restored-copy attempt failed with an incompatible duplicate dictionary stable identity; its failure time cannot be compared as a successful batch-speed result. Both the timed-out original and failed copy passed read-only full-fixture verification without census changes. R's causal regression and production repair remain pending in this packet.

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
The diagnostic CPU profile attributed 95.41% cumulatively to candidate external-reference closure scans and 73.46% to nested Zstd decoding.
<!-- END diagnostic -->

The profile covers an intentionally terminated 60-second interval on a rebound diagnostic copy. These overlapping cumulative shares must not be added; they do not establish an accepted optimization or full-run scaling result.

Close the public owner before supported offline maintenance. Preserve failed originals; use owned copies and read-only verification for diagnosis. Do not clear recovery/dictionary gates to reclaim reported debt. Scheduler defaults do not prove observed execution or a hard storage bound; qualified churn curves are pending. See [value-log lifecycle](../../../TreeDB/docs/spec/value-log-lifecycle.md) and [recoverable-root maintenance](../../../TreeDB/docs/spec/recoverable-root-set-maintenance-3681.md).

## Maintenance system audit

The frozen runtime retains resources for both recovery-selectable durable slots, plus pending or ambiguous publications and registered protected roots ([recoverable closure](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/db/recoverable_root_set.go#L224)). Applied leaf GC also honors snapshot-generation and [stable-resource pins](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/db/leaf_generation_gc.go#L359); a recovery-only generation remains retiring ([GC decision](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/db/leaf_generation_gc.go#L496)). The final compact audit derives debt from visible/protected reachability and generation pins, so reported debt can include recovery-retained bytes that applied GC cannot yet delete ([audit](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/db/compact_storage_audit.go#L863)). This distinction preserves the false completion flags; the observed debt alone does not demonstrate a leak. Checkpoint seals previously published state without advancing CommitSeq, so repeated checkpoint need not replace an older fallback ([checkpoint contract](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/db/db.go#L3918)).

| Background mechanism | Frozen source defaults | Qualification boundary |
| --- | --- | --- |
| User-value generation rewrite | Total-byte trigger 4GiB; stale-ratio trigger 20%; rewrite budget 128MiB/s; 30s minimum rewrite interval and 15s staged confirmation | Admission and stale-segment selection still apply; thresholds are scheduling policy |
| Outer-leaf generation pack | Opt-in `TREEDB_ENABLE_LEAF_GENERATION_PACK_MAINTENANCE`; 256MiB copied and 32 generations per pass; 30s interval and timeout; at least two candidates and one commit of age; 25% reclaim per copied byte | Requires outer leaves and quiet/admitted work; skipped on GC ticks |

These policies are separate ([rewrite constants](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/caching/db.go#L10157), [pack limits](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/caching/db.go#L2432), [pack admission](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/caching/db.go#L31147)). The scheduler offers work periodically, but foreground admission, copy cost, stale selection, and the recovery horizon determine whether bytes drain. Neither policy is a hard bound on directory size. Default and opt-in churn curves must establish sustained reclamation, temporary disk, RSS, and foreground latency before a default-policy change is justified.

The ranked algorithm target is repeated whole-closure capture during outer-leaf value rewriting: its default batch is 256 swaps, outer-leaf mode disables the simple logical-reference delta, and each successful publication can scan the reachable roots again ([batch default](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/db/vlog_rewrite.go#L374), [delta gate](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/db/vlog_rewrite.go#L2700), [closure scan](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/db/durable_root_runtime.go#L520)). The retained 60s diagnostic confirms dominance in that interval, while the source-derived scaling hypothesis is approximately `O(N * ceil(M/B))` for N reachable entries, M rewritten pointers, and batch B. It is not a measured full-run scaling curve.

The first performance experiment after R proves restored-copy correctness is the existing `-rewrite-batch-size` option on an isolated offline copy ([CLI](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/cmd/treemap/main.go#L507)). The retained 8192 attempt failed before providing an accepted result. A matched default/larger-batch experiment should count successful publications and closure scans while measuring elapsed time, RSS, cancellation boundaries, temporary disk, and final pre-oracle census. Further closure reuse needs exact logical deltas and certified raw-leaf dependencies, with an exact final closure and existing fallback retained. Recovery slots, pins, dictionary identity, and namespace epochs remain correctness gates. Zero-dead packing can still recompress or consolidate live pages; the rehearsal's equal-size repack does not justify a universal skip. No larger online default, epoch clearing, or automatic pack default change is established by this packet.

## Pending matrix input format

The three actual collector plans are retained and hash-bound; the rows below are expected inventory, not measured candidate results.

<!-- BEGIN planned_inputs -->
| Pending plan | Expected rows | Imported result rows | Class |
| --- | ---: | ---: | ---: |
| offsets-3m-matched-plan.json | 18 | 0 | unprofiled public pairs |
| offsets-3m-profile-paired-plan.json | 6 | 0 | profiled public pair |
| offsets-3m-structural-diagnostic-plan.json | 2 | 0 | profiled structural diagnostic |
<!-- END planned_inputs -->

Each imported run will retain its actual `run.json`, stdout, stderr, qualified manifest, and source/build/native/runner receipts. Extraction will authenticate the complete raw inventory and manifest/collector/command/source/runtime bindings before interpreting a row, then match repeat and cell identity to the pinned plan and verify final values and misses. Public matched rows supply throughput and latency comparisons; profiled public pairs supply heap/allocation/cache observations; the two instrumented structural-K diagnostics remain diagnostic. Candidate rows will be added only when the coordinator supplies the concrete packets and frozen-source qualification; completion and gate wording remain provisional until that evidence exists.

## Provenance and remaining gates

<!-- BEGIN source -->
Frozen source `fb42d5ba229d2cbfba8727272af02f9544a33389` has whole-tree equality with landed `a6b383b6c0d6269a598390032ecce9fd619a4eac`. The separate post-measurement reconstruction produced a bit-identical treemap binary (`1d0f9222676a15c59fbd09214d0b158ff56ed0979bf06d50c7cfefe60cf23406`).
<!-- END source -->

Hash-bound source/build/native/runner receipts and the actual collector command are retained under raw/maintenance. The publication does not claim that its documentation head was measured. The original pre-measurement maintenance build receipt is missing. The separate reconstruction supplements preserved command logs; it does not become a pre-measurement receipt.

The generic-v1 primary fixture uses seed 24, 3M keys, 6M aggregate reads, 40k updates, four readers, an eight-second concurrent bracket, 90% configured misses, and GOMAXPROCS=12. A shared Linux i5-11400F was used because a dedicated runner was unavailable; root owned one native measurement process at a time. Public candidate pairs and diagnostic overlays remain distinct.

Raw paths and exact SHA-256 values are in inputs.json and RESULTS.json. Larger profiles remain at their recorded native paths with digests. Gzipped raw inputs preserve exact original bytes and bind both compressed and original digests. REPORT.md supplies hash-pinned prose around generated numerical blocks. Reproduce extraction with `python3 extract.py --check`; run bounded validation with `python3 selfcheck.py` and `python3 -O selfcheck.py`. No command here builds binaries or opens a database.

<!-- BEGIN pending -->
- M: three matched public durable/fast/holdout pairs, profiled primary pairs, and cache metadata distribution
- C: qualified default and opt-in maintenance churn curves
- R: causal restored-copy regression, production repair, and qualified post-repair maintenance
- Accept the missing original maintenance build receipt as a documented reconstruction limitation
- Independent artifact review, latest-head CI, mature Codex review, and final source reconciliation
<!-- END pending -->
