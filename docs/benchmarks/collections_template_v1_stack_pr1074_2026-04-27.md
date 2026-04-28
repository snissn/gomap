# Collections Template-v1 Stack Benchmark Report

Run date: 2026-04-27 HST / 2026-04-28 UTC

This run was captured on branch `opt/template-v1-collection-optimizations` at
commit `4f3a9fcf22`. The PR stack already includes PR 1072 through merge commit
`169acfd3b68fa887bb70542435d282d2d4563bf6`; that merge is an ancestor of the
current PR 1074 head.

## Executive Summary

TreeDB collections are ahead of SQLite on the indexed document workloads in this
run. The two-index template-v1 insert path is `943.70 ns/doc`
(`1,059,659 docs/sec`), compared with SQLite JSON at `2,567.00 ns/doc`
(`389,560 docs/sec`) and SQLite native columns at `2,361.67 ns/doc`
(`423,430 docs/sec`). With checkpointing, template-v1 is `1,552.00 ns/doc`
(`644,330 docs/sec`) versus SQLite JSON at `3,062.33 ns/doc`
(`326,548 docs/sec`) and SQLite native columns at `2,654.00 ns/doc`
(`376,790 docs/sec`).

Template-v1 is doing the job it was meant to do: it cuts indexed extraction and
read materialization overhead versus JSON. Two-index primary reads improve from
`1,706.33 ns/op` (`586,052 ops/sec`, `8,482 B/op`, `4 allocs/op`) for JSON to
`1,286.67 ns/op` (`777,202 ops/sec`, `4,816.7 B/op`, `3 allocs/op`) for
template-v1. The isolated index-state extraction probe improves from
`274.07 ns/op` (`3,648,747 ops/sec`) for JSON to `111.80 ns/op`
(`8,944,544 ops/sec`) for template-v1.

The biggest remaining indexed-write cost is no longer document parsing. For the
two-index template-v1 insert path, publish/root merge is `568.93 ns/doc`
(`1,757,675 docs/sec`), secondary run construction is `158.90 ns/doc`
(`6,293,266 docs/sec`), index-state extraction is `81.22 ns/doc`
(`12,312,238 docs/sec`), and prepare is `38.14 ns/doc`
(`26,216,901 docs/sec`). With checkpointing, publish rises to `684.37 ns/doc`
(`1,461,205 docs/sec`) and sync adds `494.73 ns/doc`
(`2,021,291 docs/sec`). The next material gains are therefore in publish/index
write amplification, secondary run construction, and the read buffer path.

Disk usage is competitive. At two indexes, TreeDB template-v1 reports
`155.93 B/doc`, TreeDB JSON reports `153.70 B/doc`, SQLite JSON reports
`262.23 B/doc`, and SQLite native columns report `175.73 B/doc`. The TreeDB
index delta dominates total storage: the template-v1 zero-index row is only
`27.64 B/doc`, while the derived two-index component is `128.29 B/doc`.

The raw TreeDB engine is not the current ceiling for collections. The unified
bench anchors report raw TreeDB `batch_write` at `66.72 ns/op`
(`14,988,385 ops/sec`) for `fast` and `151.93 ns/op`
(`6,582,069 ops/sec`) for `wal_on_fast`, well above the collection two-index
template-v1 insert rate. The indexed collection path is spending its budget in
collection/index coordination and backend publish work, not raw B-tree ingest.

## Run Context

Command:

```sh
scripts/bench_collections_harness.sh \
  --out /tmp/gomap_collections_1074_20260427_194034 \
  --count 3 \
  --benchtime 1s \
  --batch-size 8000 \
  --include-sqlite \
  --include-unified \
  --unified-keys 100000
```

Harness artifacts generated:

- Matrix summary: `/tmp/gomap_collections_1074_20260427_194034/collections_matrix_summary.md`
- Matrix HTML: `/tmp/gomap_collections_1074_20260427_194034/collections_matrix_summary.html`
- Disk TSV: `/tmp/gomap_collections_1074_20260427_194034/collections_disk_usage_summary.tsv`
- TreeDB JSON report and pprof: `/tmp/gomap_collections_1074_20260427_194034/collections_json_shapes/`
- TreeDB template-v1 report and pprof: `/tmp/gomap_collections_1074_20260427_194034/collections_template_v1_shapes/`
- SQLite report and pprof: `/tmp/gomap_collections_1074_20260427_194034/sqlite_wal_normal_shapes/`
- Unified `fast` profile-dir: `/tmp/gomap_collections_1074_20260427_194034/unified_fast/`
- Unified `wal_on_fast` profile-dir: `/tmp/gomap_collections_1074_20260427_194034/unified_wal_on_fast/`

The generated reports include adjacent throughput columns for latency columns,
and the unified profile dirs include CPU, allocation, block, mutex, checkpoint
CPU, and trace artifacts.

## Headline Throughput

| Workload | ns/op or ns/doc | Ops/sec or docs/sec | B/op | allocs/op | Disk B/doc |
| --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB JSON insert, 2 indexes | 1,039.67 | 961,847 | 811.7 | 0 | 153.70 |
| TreeDB template-v1 insert, 2 indexes | 943.70 | 1,059,659 | 1,061.7 | 0 | 155.93 |
| SQLite JSON insert, 2 indexes | 2,567.00 | 389,560 | 192 | 7 | 262.23 |
| SQLite native insert, 2 indexes | 2,361.67 | 423,430 | 368 | 10 | 175.73 |
| TreeDB JSON checkpoint insert, 2 indexes | 1,644.33 | 608,149 | 854 | 0 | 154.00 |
| TreeDB template-v1 checkpoint insert, 2 indexes | 1,552.00 | 644,330 | 1,023.7 | 0 | 153.73 |
| SQLite JSON checkpoint insert, 2 indexes | 3,062.33 | 326,548 | 192 | 7 | 262.33 |
| SQLite native checkpoint insert, 2 indexes | 2,654.00 | 376,790 | 368 | 10 | 175.73 |
| TreeDB JSON primary read, 2 indexes | 1,706.33 | 586,052 | 8,482 | 4 | - |
| TreeDB template-v1 primary read, 2 indexes | 1,286.67 | 777,202 | 4,816.7 | 3 | - |
| SQLite JSON primary read, 2 indexes | 2,110.67 | 473,784 | 632 | 18 | - |
| SQLite native primary read, 2 indexes | 2,782.33 | 359,411 | 856 | 33 | - |
| TreeDB JSON unique secondary lookup | 1,172.00 | 853,242 | 407 | 10 | - |
| TreeDB template-v1 unique secondary lookup | 1,160.00 | 862,069 | 642.7 | 10 | - |
| SQLite JSON unique secondary lookup | 2,285.00 | 437,637 | 576 | 20 | - |
| SQLite native unique secondary lookup | 2,066.33 | 483,949 | 576 | 20 | - |
| TreeDB JSON nonunique secondary lookup | 3,816.33 | 262,032 | 5,522 | 79 | - |
| TreeDB template-v1 nonunique secondary lookup | 3,771.00 | 265,182 | 5,463.3 | 79 | - |
| SQLite JSON nonunique secondary lookup | 25,328.67 | 39,481 | 3,552 | 208 | - |
| SQLite native nonunique secondary lookup | 14,672.67 | 68,154 | 3,552 | 208 | - |

## Disk Usage Matrix

Disk rows are generated from the harness matrix summary. Collection/index splits
use the zero-index delta where the engine does not expose an exact object split.

| Engine/format | Indexes/doc | Total B/doc | Collection B/doc | Index B/doc |
| --- | ---: | ---: | ---: | ---: |
| TreeDB JSON | 0 | 28.95 | 28.95 | 0.00 |
| TreeDB JSON | 1 | 78.87 | 28.95 | 49.92 |
| TreeDB JSON | 2 | 153.70 | 28.95 | 124.75 |
| TreeDB JSON | 3 | 183.80 | 28.95 | 154.85 |
| TreeDB template-v1 | 0 | 27.64 | 27.64 | 0.00 |
| TreeDB template-v1 | 1 | 77.98 | 27.64 | 50.34 |
| TreeDB template-v1 | 2 | 155.93 | 27.64 | 128.29 |
| TreeDB template-v1 | 3 | 182.90 | 27.64 | 155.26 |
| SQLite JSON | 0 | 146.30 | 146.30 | 0.00 |
| SQLite JSON | 1 | 227.30 | 146.30 | 81.00 |
| SQLite JSON | 2 | 262.23 | 146.30 | 115.93 |
| SQLite JSON | 3 | 306.90 | 146.30 | 160.60 |
| SQLite native | 0 | 100.00 | 100.00 | 0.00 |
| SQLite native | 1 | 148.90 | 100.00 | 48.90 |
| SQLite native | 2 | 175.73 | 100.00 | 75.73 |
| SQLite native | 3 | 211.40 | 100.00 | 111.40 |

The single-string JSON collection shape remains a useful lower-bound sanity
check: no-index bulk insert is `143.53 ns/doc` (`6,967,023 docs/sec`) and
`13.62 B/doc`; one indexed string value is `475.97 ns/doc`
(`2,100,987 docs/sec`) and `50.53 B/doc`.

## Template-v1 Phase Breakdown

| Phase | 2-index bulk ns/doc | 2-index bulk docs/sec | 2-index checkpoint ns/doc | 2-index checkpoint docs/sec |
| --- | ---: | ---: | ---: | ---: |
| Total operation | 943.70 | 1,059,659 | 1,552.00 | 644,330 |
| Insert phase | - | - | 1,057.00 | 946,074 |
| Sync phase | - | - | 494.73 | 2,021,291 |
| Prepare documents | 38.14 | 26,216,901 | 38.10 | 26,249,016 |
| Index-state extraction | 81.22 | 12,312,238 | 82.78 | 12,079,726 |
| Duplicate preflight | 7.94 | 125,928,724 | 7.80 | 128,166,788 |
| Unique preflight | 22.67 | 44,111,160 | 22.79 | 43,878,894 |
| Primary run build | 8.79 | 113,787,218 | 8.95 | 111,777,637 |
| Index-state run build | 39.26 | 25,469,055 | 39.34 | 25,419,420 |
| Secondary runs build | 158.90 | 6,293,266 | 156.47 | 6,391,138 |
| Publish root group | 568.93 | 1,757,675 | 684.37 | 1,461,205 |

The two-index template-v1 shape built one sorted secondary run and one unsorted
secondary run per batch, with `63 secondary_key_bytes/doc`. The three-index
shape built two sorted runs and one unsorted run per batch, with
`92 secondary_key_bytes/doc`.

## Planning Diagnostics

| Cell | Benchmark | ns/op | Ops/sec | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| TreeDB JSON cell | JSON index-state extraction | 271.20 | 3,687,316 | 144 | 3 |
| TreeDB JSON cell | Template-v1 index-state extraction | 120.30 | 8,312,552 | 144 | 3 |
| TreeDB JSON cell | Indexed JSON planner | 486.30 | 2,056,344 | 486.7 | 0 |
| TreeDB JSON cell | Indexed template-v1 planner | 355.63 | 2,811,885 | 513.3 | 0 |
| TreeDB JSON cell | Precomputed-state planner | 253.97 | 3,937,525 | 343.3 | 0 |
| TreeDB JSON cell | No-index planner | 32.52 | 30,750,308 | 117.3 | 0 |
| TreeDB template-v1 cell | JSON index-state extraction | 274.07 | 3,648,747 | 144 | 3 |
| TreeDB template-v1 cell | Template-v1 index-state extraction | 111.80 | 8,944,544 | 144 | 3 |
| TreeDB template-v1 cell | Indexed JSON planner | 479.83 | 2,084,057 | 484.7 | 0 |
| TreeDB template-v1 cell | Indexed template-v1 planner | 354.70 | 2,819,284 | 512.3 | 0 |
| TreeDB template-v1 cell | Precomputed-state planner | 252.63 | 3,958,306 | 342.3 | 0 |
| TreeDB template-v1 cell | No-index planner | 32.39 | 30,873,726 | 115.3 | 0 |

## Unified TreeDB Raw Engine Anchors

Unified bench used `keys=100000`, `valsize=128`, `batchsize=8000`,
`key_shape=be8`, `val_pattern=zero`, and TreeDB only.

| Profile | Test | ns/op | Ops/sec |
| --- | --- | ---: | ---: |
| fast | Sequential write | 296.56 | 3,372,051 |
| fast | Random write | 231.02 | 4,328,637 |
| fast | Batch write | 66.72 | 14,988,385 |
| fast | Batch random | 155.74 | 6,420,752 |
| fast | Random read | 2,917.37 | 342,774 |
| fast | Random read parallel snapshot/key | 1,293.98 | 772,812 |
| fast | Full scan | 188.58 | 5,302,717 |
| fast | Prefix scan | 368.86 | 2,711,037 |
| wal_on_fast | Sequential write | 414.05 | 2,415,148 |
| wal_on_fast | Random write | 436.74 | 2,289,703 |
| wal_on_fast | Batch write | 151.93 | 6,582,069 |
| wal_on_fast | Batch random | 232.16 | 4,307,305 |
| wal_on_fast | Random read | 3,057.58 | 327,056 |
| wal_on_fast | Random read parallel snapshot/key | 1,327.16 | 753,489 |
| wal_on_fast | Full scan | 191.20 | 5,230,229 |
| wal_on_fast | Prefix scan | 217.22 | 4,603,610 |

## Profile Observations

The whole-cell CPU profiles include benchmark setup and multiple benchmark
shapes, so they are directional rather than a focused steady-state profile.
Still, they match the phase counters:

- Template-v1 CPU top includes `readViaMmapView`, `Tree.loadNodeViewWithLoadKind`,
  `Builder.AddInternalChild`, `templateV1BuildState.encodeFields`,
  `Collection.catalogForSnapshot`, and `Collection.FindByIndex`.
- Template-v1 alloc top is dominated by `readViaMmapView` with `132,884 MB`
  flat (`39.41%`) and `Collection.Get` with `151,700 MB` cumulative. This
  reinforces the `GetInto` or owned-buffer read fast-path target.
- Insert allocation still includes collection planner work:
  `planInsertBatchWithPreflight` is `49,966 MB` cumulative, `encodeFields` is
  `26,213 MB` cumulative, and `emitSecondaryRuns` is `6,908 MB` cumulative.
- Compression/dictionary setup appears in the whole-cell allocation profile
  (`BuildDict`, `ensureHist`, and trainer paths), so future lower-noise runs
  should separate warm-up from steady-state.

## Next Optimization Targets

1. Add focused 10s+ phase profiles for `BenchmarkCollectionShapeInsertBatch/indexes_2`,
   `BenchmarkCollectionShapeInsertBatchCheckpoint/indexes_2`,
   `BenchmarkCollectionShapeReadPrimary/indexes_2`, and
   `BenchmarkCollectionMixedReadWritePrimary`.
2. Attack read allocation first. The current primary read path is still
   `4,816.7 B/op` and `3 allocs/op` for template-v1, and whole-cell allocs
   point at `readViaMmapView` and `Collection.Get`.
3. Reduce index-state/write amplification. The two-index template-v1 disk delta
   is `128.29 index B/doc` versus `27.64 collection B/doc`, and publish is the
   largest phase timer.
4. Optimize secondary run construction for the common sorted-index case. The
   phase counter is `158.90 ns/doc` (`6,293,266 docs/sec`) for two-index bulk
   inserts and the harness now reports sorted versus unsorted run counts.
5. Separate compression/dictionary warm-up from steady-state results with
   pre-warmed DBs, compression/dictionary toggles, and longer steady-state
   windows, then include vlog rewrite/vacuum scenarios in the same harness.
