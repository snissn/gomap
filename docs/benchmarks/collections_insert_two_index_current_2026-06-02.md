# Two-Index Collection Insert Rerun

Date: 2026-06-02 HST / 2026-06-02 UTC

This rerun refreshes the README benchmark claim for the two-index collection
insert workload. The earlier README row cited the April 27 matrix and mixed a
post-insert disk number into a high-level benchmark highlight, which made the
TreeDB storage result easy to misread. This report records the canonical
compacted storage rows and a follow-up timed layout comparison used for the
current README headline.

## Command

```sh
OUT=/tmp/collections_insert_pr2186_$(date +%Y%m%d_%H%M%S)
USE_BUILT_BIN=1 ./scripts/bench_collections_canonical.sh \
  -out-dir "$OUT" \
  -formats template-v1,json \
  -indexes 2 \
  -docs 100000 \
  -batch-size 16000 \
  -count 1 \
  2>&1 | tee "$OUT.stdout.log"
```

Measured run:

```text
/tmp/collections_insert_pr2186_20260602_071510
```

Measured code:

```text
branch: codex/readme-treedb-ycsb-headline
commit: 32f17af9af42133f41094caea085bce161795e58
```

Generated artifacts:

- `/tmp/collections_insert_pr2186_20260602_071510/benchmark_summary.md`
- `/tmp/collections_insert_pr2186_20260602_071510/benchmark_results.json`
- `/tmp/collections_insert_pr2186_20260602_071510/benchmark_matrix.csv`
- `/tmp/collections_insert_pr2186_20260602_071510/timed_matrix/collections_matrix_summary.md`

## Timed Insert Rows

These rows are benchmark-timed post-insert measurements for two secondary
indexes, `100000` documents, batch size `16000`, and `command_wal_relaxed` for
TreeDB. The B/doc column is the post-insert footprint after the benchmark flush
or checkpoint. It is not a compacted-state number.

The canonical runner uses `storage-cells=index-vlog`, which is the TreeDB layout
that writes data and index outer leaves to the value log. These timings should
not be compared directly with the April 27 "default index leaves" rows, which
used a legacy `production_fast` profile and a different index-leaf placement.

| engine / format | layout | ns/doc | docs/sec | post-insert B/doc |
| --- | --- | ---: | ---: | ---: |
| TreeDB JSON | data and index outer leaves in value log | 3,730 | 268,097 | 233.2 |
| SQLite native columns | WAL normal | 4,026 | 248,385 | 176.1 |
| TreeDB template-v1 | data and index outer leaves in value log | 4,451 | 224,669 | 214.8 |
| SQLite JSON | WAL normal | 4,512 | 221,631 | 262.6 |

## Follow-up Timed Layout Comparison

After the README table was reviewed, the timed insert comparison was rerun with
both current TreeDB layout cells. This isolates the layout/profile difference
from compaction storage accounting. The `docs/sec` column is still only the
timed insert phase; no compaction time is included.

```sh
OUT=/tmp/collections_insert_layout_compare_$(date +%Y%m%d_%H%M%S)
go run ./cmd/collection_bench_matrix \
  -out-dir "$OUT" \
  -formats template-v1,json \
  -engine command_wal_relaxed \
  -storage-cells mainline,index-vlog \
  -tree-bench-pattern '^BenchmarkCollectionShapeInsertBatch$/^indexes_2$' \
  -sqlite-bench-pattern '^(BenchmarkSQLiteShapeInsertBatchJSON|BenchmarkSQLiteShapeInsertBatchNativeColumns)$/^indexes_2$' \
  -batch-size 16000 \
  -benchtime 100000x \
  -count 1 \
  -leaf-segment-target-bytes 1048576 \
  -leafgen-pack-frame-k 16 \
  2>&1 | tee "$OUT.stdout.log"
```

Measured run:

```text
/tmp/collections_insert_layout_compare_20260602_083113
```

Measured code:

```text
branch: codex/readme-treedb-ycsb-headline
commit: b8863137de27
```

| engine / format | layout | ns/doc | docs/sec | post-insert B/doc |
| --- | --- | ---: | ---: | ---: |
| TreeDB template-v1 | data outer leaves in value log, index outer leaves in pager | 1,292 | 773,994 | 237.9 |
| TreeDB template-v1 | data and index outer leaves in value log | 1,595 | 626,959 | 214.8 |
| TreeDB JSON | data outer leaves in value log, index outer leaves in pager | 1,927 | 518,941 | 255.5 |
| TreeDB JSON | data and index outer leaves in value log | 2,384 | 419,463 | 233.2 |
| SQLite native columns | WAL normal | 3,023 | 330,797 | 176.1 |
| SQLite JSON | WAL normal | 3,310 | 302,115 | 262.6 |

The root README uses the value-log outer-leaf TreeDB rows from this follow-up
timed run, paired with the compacted storage values below. That keeps the
headline compact while avoiding the impression that compacted storage time was
included in the throughput measurement.

## Compacted Storage Rows

These rows use the canonical compacted-state comparison basis: TreeDB compacted
phases are compared with SQLite after `VACUUM`.

| engine / format | phase | B/doc | comparison basis |
| --- | --- | ---: | --- |
| TreeDB template-v1 | `full_leafgen_pack_gc` | 22.2 | full leaf generation pack/GC plus offline index vacuum |
| TreeDB JSON | `full_leafgen_pack_gc` | 30.4 | full leaf generation pack/GC plus offline index vacuum |
| TreeDB template-v1 | `offline_compact` | 49.7 | high-level `treemap compact <dir> -rw` |
| SQLite native columns | `sqlite_vacuum` | 156.7 | SQLite `VACUUM` |
| SQLite JSON | `sqlite_vacuum` | 231.7 | SQLite `VACUUM` |

Derived comparison ratios from `benchmark_results.json`:

| TreeDB row | vs SQLite native columns after `VACUUM` | vs SQLite JSON after `VACUUM` |
| --- | ---: | ---: |
| template-v1 `offline_compact` | 3.2x smaller | 4.7x smaller |
| template-v1 `full_leafgen_pack_gc` | 7.1x smaller | 10.4x smaller |
| JSON `full_leafgen_pack_gc` | 5.1x smaller | 7.6x smaller |

## Guardrail Notes

The canonical runner emitted one warning and one info note:

- `phase.online_one_pass.partial`: online one-pass maintenance is partial and
  should not be described as full compaction.
- `raw_shape_labeled`: raw TreeDB rows are labeled separately and should not be
  mixed with collection rows.
