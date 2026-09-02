# TreeDB leaf-log append lanes M5 final gate report

## Scope

This report closes the #2925 direct leaf-log append-lane sprint and feeds the
result back into #2916/#2899. It evaluates TreeDB at latest main commit
`06366c0f9f4ff03dc33807f7896e36cc53e8ed24` after #2930 merged.

The sprint goal was not to increase `ops/span` for uniform random writes; #2919
reclassified low `ops/span` as the expected fine-grained workload shape. The
sprint goal was to make the one-op/few-op span case write leaf pages through
true parallel lane-owned output without a single collector/append queue and
without losing checkpoint/reopen/GC/rewrite correctness.

## Policy outcome

- Direct multi-lane leaf-log output is **allowed on the admitted span-native
  path**. Under the current `FlushAdmissionPolicyAuto`, TreeDB normalizes to the
  measured conservative c4 span-native/backlog candidate when durability and
  host-concurrency guardrails admit it.
- Plain `AppendLeafPage` remains lane 0. Selected-lane appends are still an
  internal/opt-in `LeafPageLogLaneProvider` path used by span-native workers;
  code that does not ask for a selected lane keeps the default lane.
- `FlushAdmissionPolicyOff` remains the rollback knob. It disables span-native
  apply/backlog/concurrency and therefore avoids selected-lane leaf-log output;
  no data migration is required because value-log/leaf-log pointers remain
  persistent storage.
- c8/c16 remain explicit tuning/diagnostic rows, not a new default, because the
  final gate found a remaining checkpoint-wall blocker (#2943).

## Verdict

The #2925 leaf-log lane sprint **passes its leaf-log-lane gate**:

- no global collector/queue was introduced;
- selected-lane distribution used every worker-owned lane in c4/c8/c16;
- close/checkpoint span-native fallbacks stayed zero;
- leaf-log sync time became tiny relative to checkpoint wall;
- read/cache/scan and no-cache guardrails remained green;
- checkpoint/reopen/GC/rewrite focused tests passed.

Do **not** close #2916/#2899 from this result alone. The final matrix improved
write throughput at c8/c16, but checkpoint total wall is still dominated by
active-background/barrier wait and backend-boundary work rather than leaf-log
sync. The remaining parent blocker was split to #2943.

## Final c4/c8/c16 matrix

Artifacts:

- c4: `<remote-profile-root>/treedb_2931_final_c4_20260622_064144`
- c8: `<remote-profile-root>/treedb_2931_final_c8_20260622_064314`
- c16: `<remote-profile-root>/treedb_2931_final_c16_20260622_064436`

| row | sequential_write | batch_random | random_write | random_write vs M0 | ops/span | single-op spans | close/checkpoint fallback |
|---|---:|---:|---:|---:|---:|---:|---:|
| M0 c4 | — | — | 257,889/s | baseline | 2.92 | 65.2% | 218,612 |
| final c4 | 2,576,573/s | 760,796/s | 253,460/s | -1.7% | 3.28 | 60.5% | 0 |
| M0 c8 | — | — | 266,084/s | baseline | 3.03 | 62.9% | 218,612 |
| final c8 | 2,523,140/s | 736,904/s | 323,691/s | +21.7% | 3.19 | 60.5% | 0 |
| M0 c16 | — | — | 251,864/s | baseline | 3.09 | 62.1% | 218,612 |
| final c16 | 2,512,762/s | 786,721/s | 333,241/s | +32.3% | 3.19 | 60.5% | 0 |

Interpretation: c8/c16 finally turn extra span-native workers into useful
random-write throughput; c4 is effectively flat against the old M0 throughput
and improved against the immediate #2934/#2930 predecessor baselines used for
hot-path PR gating.

## Leaf-log output and lane distribution

| row | selected worker lanes used | append lanes configured/active/used | lane tasks | lane task max | append wait | reservation wait | lane lock wait | lane lock hold |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| c4 | 4/4 | 5/5/5 | 5,488 | 1,372 | 52.91s | 1.40s | 0.29s | 7.12s |
| c8 | 8/8 | 9/9/9 | 10,976 | 1,385 | 65.54s | 1.73s | 0.36s | 8.90s |
| c16 | 12/12 | 13/13/13 | 16,464 | 1,397 | 69.84s | 1.85s | 0.36s | 7.45s |

The selected-lane worker distribution is the important correctness/performance
signal here: dynamic scheduling no longer lets one worker collapse all output
onto one selected lane, and the final implementation avoids the static-striding
load-imbalance regression found during #2930 review.

## Worker scheduling counters

| row | workers max | busy | idle | wait | ready tasks | dispatched/completed | task spans max | task ops max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| c4 | 4 | 126.30s | 5.41s | 34.13s | 5,827 | 5,827/5,827 | 10,058 | 32,768 |
| c8 | 8 | 198.66s | 10.49s | 27.59s | 11,315 | 11,315/11,315 | 3,665 | 32,768 |
| c16 | 12 | 274.09s | 17.30s | 25.79s | 16,803 | 16,803/16,803 | 2,441 | 32,768 |

## Checkpoint stages

| row | checkpoint total | active-background wait | flush_all | backend_boundary | reducer_publish | leaf_value_log_sync | wal_rotate |
|---|---:|---:|---:|---:|---:|---:|---:|
| M0 c4 | 42.51s | 23.22s | 17.43s | 0.75s | — | 0.16s | — |
| final c4 | 43.65s | 20.81s | 16.04s | 5.52s | 0.61s | 0.03s | 1.12s |
| M0 c8 | 39.45s | 20.95s | 13.41s | 3.91s | — | 0.42s | — |
| final c8 | 43.60s | 24.94s | 15.66s | 1.77s | 0.55s | 0.04s | 0.46s |
| M0 c16 | 32.73s | 12.48s | 18.38s | 0.74s | — | 0.92s | — |
| final c16 | 43.01s | 22.61s | 17.94s | 1.24s | 0.53s | 0.04s | 0.52s |

Leaf-log sync is no longer the checkpoint wall. The remaining issue is
checkpoint coordination/drain: active-background/barrier wait plus backend
boundary/root publication still dominate enough to block #2916/#2899 closure.
That follow-up is #2943.

## Storage footprint and segment counts

| row | index.db | leaf_vlog | leaf_vlog files | rotations | mmap active/current/sealed segments |
|---|---:|---:|---:|---:|---:|
| c4 | 405 MiB | 3.2 GiB | 208 | 104 | 104/5/99 |
| c8 | 412 MiB | 3.2 GiB | 216 | 108 | 108/9/99 |
| c16 | 412 MiB | 3.2 GiB | 224 | 112 | 112/13/99 |

The footprint shape is bounded by data plus active lane headroom. c16 has more
current segments, as expected, but sealed segment count stays stable.

## Read/cache/scan guardrails

Artifacts:

- default cache: `<remote-profile-root>/treedb_2931_default_cache_read_scan_20260622_064807`
- cache disabled: `<remote-profile-root>/treedb_2931_cold_nocache_read_scan_20260622_064822`

Both rows used c4 explicit span-native/backlog settings, wrote 1M keys, then
settled by close/reopen before reads/scans.

| row | sequential_write | random_read | random_read_parallel | random_read_batch | full_scan | prefix_scan |
|---|---:|---:|---:|---:|---:|---:|
| default cache | 2,249,294/s | 302,640/s | 2,856,480/s | 2,053,300/s | 8,955,255/s | 7,373,625/s |
| cache disabled | 2,242,401/s | 106,344/s | 793,954/s | 644,499/s | 7,305,514/s | 4,633,643/s |

These are in line with the #2933 guardrail rows and do not show a read/scan
regression from the final lane implementation.

## Correctness validation

Commands run at `06366c0f9f4ff03dc33807f7896e36cc53e8ed24`:

```sh
GOWORK=off go test ./TreeDB/caching ./TreeDB/db ./TreeDB/zipper ./cmd/unified_bench -count=1

GOWORK=off go test -race ./TreeDB/caching ./TreeDB/db ./TreeDB/zipper \
  -run 'Test(CachingSpanNativeLeafLogOutputUsesSelectedLanes|CachingLeafPageLogLaneSnapshotsAggregateAndMarkPerLane|LeafPageLogLanes_FlushSyncCloseTouchAllLanes|LeafPageLogLanes_ReadSurfacesNoCacheAndReadOnlyOpen|LeafPageLogLanes_IteratorSnapshotSurvivesMultiLaneGC|LeafPageLogLanes_SmallSegmentCurrentSetAndRefreshBounded|PersistPreparedLeafPageBatchDataUsesChildRefBatcher|PersistLeafPageBatchDataDetectsChildRefPreparedBatcher|SpanNativeApplyLeafLogOutputRoutesWorkerRangesToSelectedLanes)' \
  -count=1

GOWORK=off go test ./TreeDB/db ./TreeDB/caching \
  -run 'Test(FlushApplySpanNativePreparedLeafLogOutputReopens|LeafPageLogLanes_CheckpointReopenPublishesEveryCurrentLane|LeafGenerationGC_RemovesUnreachableMultiLaneSegmentsAfterReopen|LeafGenerationGC_RetainsCurrentUnreachableLeafLogLaneSegment|LeafPageLogLanes_ReadSurfacesNoCacheAndReadOnlyOpen|LeafPageLogLanes_IteratorSnapshotSurvivesMultiLaneGC|ValueLogRewriteOnline_ProtectedPathsDoNotKeepHistoricalRewriteLanes|ValueLogRewriteOffline_PreservesLeafPagesInValueLogFormatConfig|CachingLeafPageLogLaneSelectionAppendsUniqueReadablePtrs|CachingLeafPageLogLaneProviderFlushesSelectedLaneForLiveRead|CachingLeafPageLogLanes_FlushSyncCloseTouchAllLanes|CachedGenerationalMaintenance_LeafRefsRemainReopenable|CachedRepeatedRewriteVacuumLeafRefsRemainReopenable)' \
  -count=1 -v
```

## Benchmark command

c4/c8/c16 matrix command, changing only `-treedb-flush-apply-concurrency`:

```sh
OUT=$(mktemp -d /tmp/treedb_2931_final_XXXXXX)
./bin/unified-bench -dbs treedb -test sequential_write,batch_random,random_write \
  -keys 10000000 -valsize 128 -batchsize 8000 -checkpoint-between-tests \
  -treedb-flush-admission-policy=explicit -treedb-flush-apply-span-native \
  -treedb-flush-backlog-coalescing -treedb-flush-apply-min-entries=1 \
  -treedb-flush-apply-min-spans=1 -treedb-flush-apply-min-bytes=1 \
  -treedb-flush-apply-concurrency=<4|8|16> \
  -profile-dir "$OUT" -progress=false
./bin/benchprof -profiles-dir "$OUT"
```

Read/cache/scan guardrail command, with optional
`-treedb-leaf-page-read-cache-entries=-1` for the no-cache row:

```sh
OUT=$(mktemp -d /tmp/treedb_2931_read_scan_XXXXXX)
./bin/unified-bench -dbs treedb \
  -test sequential_write,random_read,random_read_parallel,random_read_batch,full_scan,prefix_scan \
  -keys 1000000 -valsize 128 -batchsize 8000 -checkpoint-between-tests \
  -settle-before-scans -treedb-flush-admission-policy=explicit \
  -treedb-flush-apply-span-native -treedb-flush-backlog-coalescing \
  -treedb-flush-apply-min-entries=1 -treedb-flush-apply-min-spans=1 \
  -treedb-flush-apply-min-bytes=1 -treedb-flush-apply-concurrency=4 \
  -profile-dir "$OUT" -progress=false
./bin/benchprof -profiles-dir "$OUT"
```
