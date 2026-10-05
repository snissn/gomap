# TreeDB space, memory, and maintenance: provisional 3M evidence

This packet records the baseline, 18 public matched runs, and bounded failure diagnostics for [#5001](https://github.com/snissn/gomap/issues/5001). Candidate acceptance and final publication remain pending. The maintenance and initial memory observations use one profiled primary run; the public throughput comparison uses three unprofiled repeats per frozen source and workload.

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

The first performance experiment after R proves restored-copy correctness is the existing `-rewrite-batch-size` option on an isolated offline copy ([CLI](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/cmd/treemap/main.go#L507)). The retained 8192 attempt failed before providing an accepted result. The writer already caps pending raw value-frame batches at 4MiB, apart from a single outlier, and limits retained decode scratch to 1MiB ([buffer caps](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/db/vlog_rewrite.go#L49), [pre-append flush](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/TreeDB/db/vlog_rewrite.go#L4945)). An 8192 swap batch therefore does not automatically retain 8192 one-MiB values; key, candidate, and swap lists still scale with batch size, so actual memory measurement is required. A matched default/larger-batch experiment should count successful publications and closure scans while measuring elapsed time, RSS, cancellation boundaries, temporary disk, and final pre-oracle census. Further closure reuse needs exact logical deltas and certified raw-leaf dependencies, with an exact final closure and existing fallback retained. Recovery slots, pins, dictionary identity, and namespace epochs remain correctness gates. Zero-dead packing can still recompress or consolidate live pages; the rehearsal's equal-size repack does not justify a universal skip. No larger online default, epoch clearing, or automatic pack default change is established by this packet.

## Public matched comparison: 18 validated runs

Baseline `fb42d5ba229d2cbfba8727272af02f9544a33389` and frozen cache-offset candidate `90219d64ced3539accff5d0e50d9b5fa6424ed20` each ran three repeats of durable primary, fast primary, and durable holdout. Primary uses uniform access and 90% configured misses; holdout uses a 20% working set and 70% misses. All 18 rows passed the complete initial/final present and absent fixture oracles. Receipt `VALIDATED_ALL` describes the row inventory and correctness checks; performance acceptance remains pending. The manifest's original `provisional=true` remains unchanged. During import, all 2,143 baseline and 2,144 candidate compiled project-file entries were independently checked against their frozen Git blobs; extraction authenticates the preserved receipts and complete raw inventory.

Cells show median [minimum, maximum] across three repeats. Throughput changes divide candidate median by baseline median; paired ranges divide corresponding candidate/baseline repeat values. Reader concurrent throughput excludes writer completion after reader join; whole-command time includes setup, all phases, writer completion, reopen, and full verification. No significance or noise-dismissal claim is made on this shared host.

<!-- BEGIN public_throughput -->
| Workload | Phase | Baseline ops/s | Candidate ops/s | Ratio of medians change | Paired repeat change range |
| --- | ---: | ---: | ---: | ---: | ---: |
| durable primary | hits | 984,015 [975,236, 996,385] | 977,601 [903,934, 1,023,990] | -0.65% | -9.28% to +4.06% |
| durable primary | misses | 1,872,360 [1,806,317, 1,872,949] | 1,851,588 [1,840,121, 1,993,489] | -1.11% | -1.75% to +10.36% |
| durable primary | mixed | 1,778,507 [1,765,795, 1,791,367] | 1,730,768 [1,730,140, 1,735,651] | -2.68% | -3.42% to -1.98% |
| durable primary | concurrent | 659,197 [641,311, 659,708] | 635,644 [634,532, 661,751] | -3.57% | -3.74% to +3.19% |
| fast primary | hits | 930,455 [913,421, 994,164] | 976,592 [920,198, 979,825] | +4.96% | -1.77% to +5.31% |
| fast primary | misses | 1,748,400 [1,740,129, 1,773,305] | 1,831,704 [1,737,657, 1,861,841] | +4.76% | -0.61% to +5.26% |
| fast primary | mixed | 1,637,029 [1,627,161, 1,640,242] | 1,701,273 [1,631,574, 1,760,792] | +3.92% | +0.27% to +7.56% |
| fast primary | concurrent | 673,147 [666,091, 682,874] | 686,337 [681,292, 690,814] | +1.96% | -0.23% to +3.71% |
| durable holdout | hits | 1,186,406 [1,151,232, 1,187,331] | 1,186,150 [1,154,852, 1,223,515] | -0.02% | -0.10% to +3.13% |
| durable holdout | misses | 1,885,834 [1,844,280, 1,907,164] | 1,902,648 [1,890,153, 1,979,713] | +0.89% | -0.24% to +4.98% |
| durable holdout | mixed | 1,680,902 [1,612,395, 1,683,616] | 1,613,738 [1,585,191, 1,653,716] | -4.00% | -4.00% to -1.69% |
| durable holdout | concurrent | 644,921 [613,421, 654,209] | 634,810 [625,590, 639,676] | -1.57% | -2.97% to +1.98% |
<!-- END public_throughput -->

Mixed and concurrent tails retain both distributions and matched repeat ratios. A ratio above one means higher latency. Durable primary and holdout mixed p99 increase alongside their lower mixed/concurrent median throughput; fast-profile gains do not establish blanket regression freedom. Other phase tails and allocation brackets remain in RESULTS.json and original stdout.

<!-- BEGIN public_tails -->
| Workload | Phase | Baseline p99 us | Candidate p99 us | Paired p99 ratio | Baseline p999 us | Candidate p999 us | Paired p999 ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| durable primary | mixed | 5.769 [5.753, 5.816] | 6.126 [5.946, 6.165] | 1.060 [1.034, 1.062] | 15.428 [15.357, 15.982] | 16.187 [16.033, 16.380] | 1.039 [1.013, 1.067] |
| durable primary | concurrent | 12.426 [12.364, 12.609] | 12.634 [12.036, 13.078] | 1.022 [0.955, 1.052] | 22.848 [22.753, 22.892] | 23.166 [22.657, 24.349] | 1.018 [0.992, 1.064] |
| fast primary | mixed | 6.480 [6.381, 6.506] | 6.036 [5.731, 6.084] | 0.939 [0.881, 0.946] | 16.554 [16.393, 16.561] | 15.826 [15.467, 15.851] | 0.956 [0.944, 0.958] |
| fast primary | concurrent | 12.413 [12.377, 12.681] | 12.066 [12.004, 12.159] | 0.975 [0.947, 0.980] | 23.714 [23.630, 23.739] | 23.031 [22.625, 23.110] | 0.975 [0.953, 0.975] |
| durable holdout | mixed | 5.229 [5.222, 5.579] | 5.544 [5.418, 5.565] | 1.038 [0.997, 1.060] | 12.834 [12.121, 13.131] | 13.015 [12.847, 14.069] | 1.060 [0.991, 1.096] |
| durable holdout | concurrent | 11.769 [11.641, 12.440] | 11.903 [11.883, 12.250] | 1.010 [0.985, 1.023] | 20.636 [20.345, 21.581] | 20.256 [20.134, 21.377] | 0.991 [0.976, 0.996] |
<!-- END public_tails -->

The guardrails below retain process RSS, command/load time, and final runtime file sizes. These file sizes are apparent bytes after final checkpoint and close, before the final writable reopen/full oracle; they are not allocated-byte censuses or post-oracle retention measurements ([capture boundary](https://github.com/snissn/gomap/blob/fb42d5ba229d2cbfba8727272af02f9544a33389/cmd/unified_bench/suite_quicksilver.go#L932)). Per-domain initial/final totals are in RESULTS.json. GNU-time RSS high-water is a whole-process observation; observer samples retain anonymous/file/shared components at the sampled RSS peak and separate component maxima. These public rows do not exercise the earlier diagnostic's 256MiB leaf-mapping override. Neither unprofiled heap endpoints nor RSS differences establish a cache-memory improvement: GC state is uncontrolled and the six profiled public rows and two structural diagnostics remain pending.

<!-- BEGIN public_guardrails -->
| Cell | Load s | Whole command GNU time s | Process RSS high-water bytes | Final pre-oracle apparent bytes excluding WAL |
| --- | ---: | ---: | ---: | ---: |
| baseline-durable-primary | 39.241 [39.045, 42.310] | 135.520 [135.330, 140.150] | 3,679,145,984 [3,671,318,528, 3,690,573,824] | 2,910,196,799 [2,903,260,870, 2,963,895,068] |
| candidate-durable-primary | 40.775 [39.147, 41.555] | 134.960 [133.400, 142.150] | 3,550,736,384 [3,429,773,312, 3,574,214,656] | 2,833,755,639 [2,776,900,141, 2,918,048,616] |
| baseline-fast-primary | 3.842 [3.830, 3.862] | 97.000 [95.600, 97.170] | 3,354,918,912 [3,269,255,168, 3,356,184,576] | 2,499,116,135 [2,495,247,457, 2,571,017,639] |
| candidate-fast-primary | 3.882 [3.799, 3.923] | 95.460 [94.580, 96.130] | 3,300,360,192 [3,293,716,480, 3,309,543,424] | 2,569,017,202 [2,496,996,333, 2,580,722,971] |
| baseline-durable-holdout | 38.108 [37.122, 38.464] | 127.650 [127.250, 129.700] | 2,880,114,688 [2,861,318,144, 2,884,460,544] | 2,392,947,613 [2,386,816,095, 2,401,534,923] |
| candidate-durable-holdout | 37.451 [37.099, 37.636] | 127.240 [126.920, 128.290] | 2,798,075,904 [2,785,476,608, 2,814,353,408] | 2,414,049,467 [2,404,328,456, 2,417,783,613] |
<!-- END public_guardrails -->

<!-- BEGIN public_barriers -->
| Cell | Initial checkpoint ms | Final checkpoint ms | Initial reopen ms | Final reopen ms | Maximum concurrent checkpoint ms | Maximum update batch ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline-durable-primary | 1,990.730 [1,743.328, 1,994.680] | 52.130 [44.444, 53.009] | 1,229.052 [1,199.576, 1,358.453] | 1,295.381 [1,291.622, 1,364.028] | 16,050.687 [15,856.905, 16,590.274] | 61.831 [61.731, 62.118] |
| candidate-durable-primary | 1,995.092 [1,231.164, 2,321.849] | 48.060 [46.502, 50.777] | 1,214.191 [1,162.835, 1,237.035] | 1,228.936 [1,226.684, 1,345.527] | 15,740.194 [14,314.285, 16,254.757] | 62.279 [53.754, 72.213] |
| baseline-fast-primary | 3,930.625 [3,644.085, 4,520.475] | 0.015 [0.014, 0.016] | 548.916 [537.482, 559.498] | 602.881 [587.720, 627.451] | 11,518.299 [11,196.182, 12,183.053] | 29.354 [28.538, 29.420] |
| candidate-fast-primary | 4,395.134 [3,640.789, 4,788.497] | 0.018 [0.016, 0.018] | 548.561 [544.892, 575.981] | 616.400 [597.742, 628.548] | 11,801.870 [11,085.279, 11,924.555] | 33.883 [28.565, 34.549] |
| baseline-durable-holdout | 1,897.802 [1,235.925, 2,002.070] | 56.703 [47.741, 93.000] | 1,047.347 [1,046.184, 1,049.798] | 1,112.187 [1,107.133, 1,148.280] | 14,730.118 [14,682.124, 15,788.786] | 50.198 [49.215, 88.006] |
| candidate-durable-holdout | 1,510.429 [1,367.821, 1,685.475] | 51.577 [48.824, 65.557] | 1,053.419 [1,036.408, 1,069.910] | 1,111.718 [1,104.043, 1,163.655] | 15,180.516 [14,919.491, 15,306.846] | 50.763 [49.214, 72.675] |
<!-- END public_barriers -->

The last two columns summarize each run's maximum among four concurrent checkpoints and 40 update batches, then report median [minimum, maximum] of those three per-run maxima. All raw batch/checkpoint values are retained. The observer's 4,323 samples are bound by executable, PID, and recorded run interval; native library hashes and per-run loader output are checked before accepting row metrics.

## Remaining matrix inputs

The three original collector plans remain hash-bound. The public matched plan is now populated above; only the following result inventories remain pending.

<!-- BEGIN planned_inputs -->
| Pending plan | Expected rows | Imported result rows | Class |
| --- | ---: | ---: | ---: |
| offsets-3m-profile-paired-plan.json | 6 | 0 | profiled public pair |
| offsets-3m-structural-diagnostic-plan.json | 2 | 0 | profiled structural diagnostic |
<!-- END planned_inputs -->

Each imported run retains its actual `run.json`, stdout, stderr, qualified manifest, and source/build/native/runner receipts. Extraction authenticates the complete raw inventory and manifest/collector/command/source/runtime bindings before interpreting a row, then matches repeat and cell identity to the pinned plan and verifies final values and misses. Public matched rows supply throughput and latency comparisons; profiled public pairs supply heap/allocation/cache observations; the two instrumented structural-K diagnostics remain diagnostic. The two remaining profiled packets will be added from concrete supplied raw; completion and acceptance wording remain provisional.

## Provenance and remaining gates

<!-- BEGIN source -->
Frozen source `fb42d5ba229d2cbfba8727272af02f9544a33389` has whole-tree equality with landed `a6b383b6c0d6269a598390032ecce9fd619a4eac`. The separate post-measurement reconstruction produced a bit-identical treemap binary (`1d0f9222676a15c59fbd09214d0b158ff56ed0979bf06d50c7cfefe60cf23406`).
<!-- END source -->

Hash-bound source/build/native/runner receipts and the actual collector command are retained under raw/maintenance. The publication does not claim that its documentation head was measured. The original pre-measurement maintenance build receipt is missing. The separate reconstruction supplements preserved command logs; it does not become a pre-measurement receipt.

The generic-v1 primary fixture uses seed 24, 3M keys, 6M aggregate reads, 40k updates, four readers, an eight-second concurrent bracket, 90% configured misses, and GOMAXPROCS=12. A shared Linux i5-11400F was used because a dedicated runner was unavailable; root owned one native measurement process at a time. Public candidate pairs and diagnostic overlays remain distinct.

Raw paths and exact SHA-256 values are in inputs.json and RESULTS.json. Larger profiles remain at their recorded native paths with digests. Gzipped raw inputs preserve exact original bytes and bind both compressed and original digests. REPORT.md supplies hash-pinned prose around generated numerical blocks. Reproduce extraction with `python3 extract.py --check`; run bounded validation with `python3 selfcheck.py` and `python3 -O selfcheck.py`. No command here builds binaries or opens a database.

<!-- BEGIN pending -->
- M: six profiled public primary rows, two structural cache diagnostics, performance acceptance, and final source qualification
- C: qualified default and opt-in maintenance churn curves
- R: causal restored-copy regression, production repair, and qualified post-repair maintenance
- Accept the missing original maintenance build receipt as a documented reconstruction limitation
- Independent artifact review, latest-head CI, mature Codex review, and final source reconciliation
<!-- END pending -->
