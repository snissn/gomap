# Collections Rewrite and Vacuum Matrix Report

Run date: 2026-04-27 HST / 2026-04-28 UTC

This run was captured from branch `bench/collections-rewrite-vacuum-matrix`.
The measured code commit was `d8c44fea4f`, stacked on PR 1074
(`opt/template-v1-collection-optimizations`) at `606717571a`.

## Executive Summary

TreeDB collections remain materially ahead of SQLite for the two-index document
insert shapes in this run. TreeDB template-v1 inserts with the default index leaf
placement are `926.10 ns/doc` (`1,079,797 docs/sec`), TreeDB JSON is
`1,040.67 ns/doc` (`960,922 docs/sec`), SQLite JSON is `2,533.67 ns/doc`
(`394,685 docs/sec`), and SQLite native columns are `2,221.33 ns/doc`
(`450,180 docs/sec`).

The new index-outer-leaf-in-value-log probes show a clear disk/throughput trade:
two-index TreeDB JSON drops from `153.70 B/doc` to `106.90 B/doc`, while insert
throughput moves from `960,922 docs/sec` to `825,309 docs/sec`. Two-index
template-v1 drops from `155.10 B/doc` to `104.90 B/doc`, while throughput moves
from `1,079,797 docs/sec` to `941,029 docs/sec`. That is a roughly one-third
disk reduction for a roughly 13-14% write-throughput cost on this shape.

The value-log rewrite measurements did not reclaim space on the insert-only
TreeDB workloads. For two-index default TreeDB JSON, total size moved from
`153,688,757 B` to `153,689,087 B`; after GC it remained `153,689,087 B`. For
two-index default template-v1, total size moved from `198,803,276 B` to
`198,803,606 B`; after GC it remained `198,803,606 B`. The same pattern held for
the index-vlog probes. This is the expected result for append-only insert data:
there is no dead value-log payload for rewrite or GC to remove. The next matrix
needs explicit update/delete/churn workloads to evaluate compaction value.

SQLite `VACUUM` did reclaim meaningful space. SQLite JSON two-index data moved
from `134,714,709 B` to `118,872,747 B`, and SQLite native columns moved from
`112,710,997 B` to `100,324,693 B`. These totals are not directly comparable to
TreeDB totals because the Go benchmark rows stored different document counts.
The comparable normalized numbers are SQLite JSON at `231.40 B/doc` after
vacuum, SQLite native columns at `156.40 B/doc` after vacuum, and TreeDB
template-v1 with index outer leaves in the value log at `104.90 B/doc` after
rewrite/GC.

Template-v1 is still a strong read-path improvement over JSON. Primary reads at
two indexes are `1,295.67 ns/op` (`771,803 ops/sec`, `4,862 B/op`,
`3 allocs/op`) for template-v1 versus `1,905.33 ns/op` (`524,843 ops/sec`,
`8,482 B/op`, `4 allocs/op`) for JSON. Parallel primary reads are
`665.13 ns/op` (`1,503,458 ops/sec`) for template-v1 versus `1,086.00 ns/op`
(`920,810 ops/sec`) for JSON.

The raw TreeDB engine remains well above the collection layer. The unified
`fast` anchor reports batch writes at `13,958,032 ops/sec`; `wal_on_fast`
reports batch writes at `6,735,197 ops/sec`. The indexed collection path is
still spending its budget in collection/index coordination, secondary/index
state writes, and publish/root work rather than raw B-tree throughput.

## Harness Command

```sh
OUT="/tmp/gomap_collections_1075_20260427_204054"
TREEDB_COLLECTION_HARNESS_REPORT_VLOG_REWRITE=true \
TREEDB_COLLECTION_HARNESS_REPORT_SQLITE_VACUUM=true \
scripts/bench_collections_harness.sh \
  --out "$OUT" \
  --count 3 \
  --benchtime 1s \
  --batch-size 8000 \
  --include-sqlite \
  --include-unified \
  --unified-keys 100000 \
  2>&1 | tee "$OUT/harness_stdout.log"
```

Harness settings:

- Benchmark engine: `production_fast`
- Benchmark count: `3`
- Benchmark time: `1s`
- Collection batch size: `8000`
- SQLite included: `true`
- Unified bench included: `true`
- TreeDB value-log rewrite reporting: `true`
- SQLite vacuum reporting: `true`
- Additional index-vlog probes: `BenchmarkCollectionShapeInsertBatch/indexes_2`

## Full Harness Output

Top-level artifacts:

- Harness README: `/tmp/gomap_collections_1075_20260427_204054/README.md`
- Harness stdout: `/tmp/gomap_collections_1075_20260427_204054/harness_stdout.log`
- Matrix index: `/tmp/gomap_collections_1075_20260427_204054/harness_matrix_index.tsv`
- Matrix report: `/tmp/gomap_collections_1075_20260427_204054/collections_matrix_summary.md`
- Matrix HTML: `/tmp/gomap_collections_1075_20260427_204054/collections_matrix_summary.html`
- Matrix TSV: `/tmp/gomap_collections_1075_20260427_204054/collections_matrix_summary.tsv`
- User-story TSV: `/tmp/gomap_collections_1075_20260427_204054/collections_user_story_summary.tsv`
- Disk usage TSV: `/tmp/gomap_collections_1075_20260427_204054/collections_disk_usage_summary.tsv`
- Maintenance compaction TSV: `/tmp/gomap_collections_1075_20260427_204054/collections_maintenance_summary.tsv`
- Unified index: `/tmp/gomap_collections_1075_20260427_204054/harness_unified_index.tsv`

Per-cell reports and profiles:

- TreeDB JSON shapes: `/tmp/gomap_collections_1075_20260427_204054/collections_json_shapes/`
- TreeDB template-v1 shapes: `/tmp/gomap_collections_1075_20260427_204054/collections_template_v1_shapes/`
- TreeDB JSON, index outer leaves in value log: `/tmp/gomap_collections_1075_20260427_204054/collections_json_index_vlog_insert2/`
- TreeDB template-v1, index outer leaves in value log: `/tmp/gomap_collections_1075_20260427_204054/collections_template_v1_index_vlog_insert2/`
- SQLite WAL normal shapes: `/tmp/gomap_collections_1075_20260427_204054/sqlite_wal_normal_shapes/`
- Unified `fast` profile-dir: `/tmp/gomap_collections_1075_20260427_204054/unified_fast/`
- Unified `wal_on_fast` profile-dir: `/tmp/gomap_collections_1075_20260427_204054/unified_wal_on_fast/`

Each collection cell includes `collections_report.md`, `collections_report.html`,
benchmark output, CPU pprof, allocation pprof, and the generated JSON summary.
The unified profile dirs include `benchprof_results.md`, `benchprof_results.json`,
CPU profiles, allocation profiles, checkpoint CPU profiles, block profile, mutex
profile, and trace output.

## Two-index Insert Matrix

| Engine/format | Layout | ns/doc | docs/sec | B/op | allocs/op | Disk B/doc |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB JSON | default index leaves | 1,040.67 | 960,922 | 810.0 | 0 | 153.70 |
| TreeDB template-v1 | default index leaves | 926.10 | 1,079,797 | 1,059.0 | 0 | 155.10 |
| TreeDB JSON | index outer leaves in value log | 1,211.67 | 825,309 | 1,152.7 | 0 | 106.90 |
| TreeDB template-v1 | index outer leaves in value log | 1,062.67 | 941,029 | 1,028.7 | 0 | 104.90 |
| SQLite JSON | WAL normal | 2,533.67 | 394,685 | 192.0 | 7 | 262.30 |
| SQLite native columns | WAL normal | 2,221.33 | 450,180 | 368.0 | 10 | 175.77 |

## Two-index Checkpoint Insert Matrix

| Engine/format | ns/doc | docs/sec | insert ns/doc | insert docs/sec | sync ns/doc | sync docs/sec | Disk B/doc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB JSON | 1,603.00 | 623,830 | 1,133.00 | 882,613 | 470.40 | 2,125,850 | 153.00 |
| TreeDB template-v1 | 1,452.33 | 688,547 | 1,024.33 | 976,245 | 428.07 | 2,336,085 | 153.00 |
| SQLite JSON | 3,052.33 | 327,618 | 2,383.00 | 419,639 | 669.67 | 1,493,280 | 262.30 |
| SQLite native columns | 2,690.00 | 371,747 | 2,146.67 | 465,839 | 543.37 | 1,840,378 | 175.73 |

## Maintenance Compaction

TreeDB measurements run value-log rewrite, checkpoint, then value-log GC and
checkpoint. SQLite measurements run `VACUUM` and a WAL checkpoint.

| Engine/format | Layout | Stored docs | Maintenance | ns/op | ops/sec | GC ns/op | GC ops/sec | Before bytes | After bytes | After GC bytes | After GC B/doc | Delta after GC |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TreeDB JSON | default index leaves | 1,000,000 | vlog rewrite + GC | 97,042,875 | 10.30 | 332,028 | 3,011.79 | 153,688,757 | 153,689,087 | 153,689,087 | 153.69 | +330 |
| TreeDB template-v1 | default index leaves | 1,281,732 | vlog rewrite + GC | 123,363,986 | 8.11 | 369,680 | 2,705.04 | 198,803,276 | 198,803,606 | 198,803,606 | 155.10 | +330 |
| TreeDB JSON | index outer leaves in value log | 981,699 | vlog rewrite + GC | 141,203,000 | 7.08 | 344,917 | 2,899.25 | 104,923,856 | 104,924,289 | 104,924,289 | 106.90 | +433 |
| TreeDB template-v1 | index outer leaves in value log | 1,000,000 | vlog rewrite + GC | 137,128,125 | 7.29 | 332,194 | 3,010.29 | 104,862,258 | 104,862,588 | 104,862,588 | 104.90 | +330 |

| Engine/format | Stored docs | Maintenance | ns/op | ops/sec | Before bytes | After bytes | Delta bytes | Before B/doc | After B/doc |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite JSON | 513,627 | VACUUM | 247,588,889 | 4.04 | 134,714,709 | 118,872,747 | -15,841,963 | 262.30 | 231.40 |
| SQLite native columns | 641,271 | VACUUM | 199,786,611 | 5.01 | 112,710,997 | 100,324,693 | -12,386,304 | 175.77 | 156.40 |

## Disk Usage Comparison

| Engine/format | Layout | Total B/doc | Collection B/doc | Index B/doc |
| --- | --- | ---: | ---: | ---: |
| TreeDB JSON | default, two indexes | 153.70 | 28.87 | 124.83 |
| TreeDB template-v1 | default, two indexes | 155.10 | 27.60 | 127.50 |
| TreeDB JSON | index outer leaves in value log, two indexes | 106.90 | - | - |
| TreeDB template-v1 | index outer leaves in value log, two indexes | 104.90 | - | - |
| SQLite JSON | before VACUUM, two indexes | 262.30 | 146.30 | 116.00 |
| SQLite JSON | after VACUUM, two indexes | 231.40 | - | - |
| SQLite native columns | before VACUUM, two indexes | 175.77 | 100.00 | 75.77 |
| SQLite native columns | after VACUUM, two indexes | 156.40 | - | - |

## Reads and Mixed Read/Write

| Engine/format | Workload | ns/op | ops/sec | B/op | allocs/op | writer docs/sec |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB JSON | primary read, two indexes | 1,905.33 | 524,843 | 8,482.0 | 4 | - |
| TreeDB template-v1 | primary read, two indexes | 1,295.67 | 771,803 | 4,862.0 | 3 | - |
| SQLite JSON | primary read, two indexes | 2,111.33 | 473,634 | 632.0 | 18 | - |
| SQLite native columns | primary read, two indexes | 2,798.00 | 357,398 | 856.0 | 33 | - |
| TreeDB JSON | primary read parallel, two indexes | 1,086.00 | 920,810 | 8,482.0 | 4 | - |
| TreeDB template-v1 | primary read parallel, two indexes | 665.13 | 1,503,458 | 4,745.7 | 3 | - |
| TreeDB JSON | mixed primary read/write | 1,347.00 | 742,390 | 8,797.3 | 6 | 52,225 |
| TreeDB template-v1 | mixed primary read/write | 1,112.63 | 898,769 | 5,126.0 | 6 | 57,040 |
| TreeDB JSON | mixed unique-secondary read/write | 627.33 | 1,594,049 | 557.7 | 11 | 65,472 |
| TreeDB template-v1 | mixed unique-secondary read/write | 562.53 | 1,777,672 | 683.0 | 12 | 70,145 |

## Secondary Lookup

| Engine/format | Workload | ns/op | ops/sec | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| TreeDB JSON | unique secondary lookup | 1,182.33 | 845,785 | 407 | 10 |
| TreeDB template-v1 | unique secondary lookup | 1,226.00 | 815,661 | 625 | 10 |
| SQLite JSON | unique secondary lookup | 2,260.00 | 442,478 | 576 | 20 |
| SQLite native columns | unique secondary lookup | 2,063.67 | 484,574 | 576 | 20 |
| TreeDB JSON | nonunique secondary lookup | 3,885.67 | 257,356 | 5,505.3 | 79 |
| TreeDB template-v1 | nonunique secondary lookup | 4,104.67 | 243,625 | 5,421.3 | 79 |
| SQLite JSON | nonunique secondary lookup | 25,381.33 | 39,399 | 3,552 | 208 |
| SQLite native columns | nonunique secondary lookup | 14,531.67 | 68,815 | 3,552 | 208 |

## Unified TreeDB Anchors

Unified bench used `keys=100000`, `valsize=128`, `batchsize=8000`,
`key_shape=be8`, `val_pattern=zero`, and TreeDB only.

| Profile | Test | ops/sec |
| --- | --- | ---: |
| fast | Sequential write | 3,354,054 |
| fast | Random write | 3,404,043 |
| fast | Batch write | 13,958,032 |
| fast | Batch random | 5,908,972 |
| fast | Random read | 487,195 |
| fast | Random read parallel snapshot/key | 762,988 |
| fast | Full scan | 6,659,324 |
| fast | Prefix scan | 5,071,958 |
| wal_on_fast | Sequential write | 2,043,470 |
| wal_on_fast | Random write | 2,485,344 |
| wal_on_fast | Batch write | 6,735,197 |
| wal_on_fast | Batch random | 4,011,211 |
| wal_on_fast | Random read | 300,515 |
| wal_on_fast | Random read parallel snapshot/key | 805,914 |
| wal_on_fast | Full scan | 5,394,670 |
| wal_on_fast | Prefix scan | 3,149,379 |

## Interpretation and Next Targets

The index-vlog layout is promising for disk usage. It reduces two-index TreeDB
disk usage to roughly `105 B/doc`, below SQLite native columns even after
VACUUM at `156.40 B/doc`. The raw SQLite total can be lower in the maintenance
table because that benchmark row stored about `641k` documents rather than
TreeDB template-v1 index-vlog's `1M` documents. The cost is lower write
throughput, so the next focused profile should compare default and index-vlog
two-index insert profiles to find whether the extra time is in value-log append,
publish/root delta construction, or readback of outer leaves.

The value-log rewrite result is not evidence that rewrite is ineffective. It is
evidence that this insert-only matrix is the wrong workload for reclaim analysis.
The harness should add churn shapes: insert N, update indexed values, update
large document payloads, delete a configurable percentage, then measure before
rewrite, after rewrite, after GC, and steady-state read/write throughput.

Read allocation remains a high-value optimization. Template-v1 already reduces
primary-read allocation substantially, but the primary read path is still
thousands of bytes per operation. An internal `GetInto` or owned-buffer fast path
and another audit of grouped-frame decode/copy behavior should remain near the
top of the implementation list.

Secondary lookup is a strong TreeDB result versus SQLite, especially for the
nonunique shape. That suggests the next write-side work should preserve the
current lookup shape while targeting run construction and index-state write
amplification.

SQLite should remain a sanity baseline, not the target ceiling. TreeDB is already
faster for the indexed insert shapes, faster for these primary-read shapes, much
faster for nonunique secondary lookups, and smaller on disk with the index-vlog
layout.
