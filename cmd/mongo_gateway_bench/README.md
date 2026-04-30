# Mongo Gateway Benchmark Harness

`mongo_gateway_bench` runs the same MongoDB-driver workload against either the
TreeDB Mongo gateway or a MongoDB server. It is intended to make the first
Mongo-compatible product benchmarks reproducible, especially ops/sec and disk
usage comparisons.

## Build

```sh
GOWORK=off go build ./cmd/mongo_gateway_bench
```

## TreeDB Target

```sh
GOWORK=off go run ./cmd/mongo_gateway_bench \
  -target treedb \
  -treedb-dir /tmp/treedb-mongo-bench \
  -keep-treedb-dir \
  -documents 10000 \
  -reads 10000 \
  -range-reads 1000 \
  -updates 1000 \
  -concurrent-readers 8 \
  -concurrent-reads 10000 \
  -concurrent-writers 4 \
  -concurrent-writes 1000 \
  -secondary-indexes 2 \
  -format json
```

For `-target treedb`, the command starts an in-process TreeDB Mongo gateway on a
loopback listener, connects with the official MongoDB Go driver, and reports
the sum of regular file sizes for the TreeDB directory after load, after a final
checkpoint, and by default after the full TreeDB maintenance stack. If
`-treedb-dir` is provided and already exists, the harness
removes and recreates it before loading deterministic fixtures so repeated runs
are reproducible. Obvious unsafe reset targets such as root, the current
checkout, the temp directory itself, a home directory, or an immediate child of
a home directory are rejected.

The TreeDB benchmark target defaults are intended to match the optimized
collection benchmark profile:

- `-treedb-profile wal_on_fast`
- `-treedb-document-format template-v1`
- `-treedb-data-root-storage compressed`
- `-treedb-index-state-root-storage compressed`
- `-treedb-index-root-storage compressed`
- `-treedb-maintenance full`

The TreeDB target always opens with outer leaves in the leaf value log and the
cached leaf-log backend, so collection and secondary-index roots exercise the
same leaf-vlog path as the optimized collection benchmarks. The `full`
maintenance mode reports each post-load compaction step: value-log rewrite,
value-log GC, leaf-generation pack, leaf-generation GC, and index vacuum. Use
`-treedb-maintenance checkpoint` to reproduce the older checkpoint-only disk
metric, or `none` to skip final TreeDB disk reporting.

## MongoDB Target

```sh
GOWORK=off go run ./cmd/mongo_gateway_bench \
  -target mongo \
  -mongo-uri mongodb://127.0.0.1:27017 \
  -documents 10000 \
  -reads 10000 \
  -range-reads 1000 \
  -updates 1000 \
  -concurrent-readers 8 \
  -concurrent-reads 10000 \
  -concurrent-writers 4 \
  -concurrent-writes 1000 \
  -secondary-indexes 2 \
  -format json
```

For `-target mongo`, the command connects to the supplied URI, drops the
benchmark database by default, and reports `dbStats` fields after load and at
the end of the run.

## Reusable Comparison Harness

Use `scripts/mongo_gateway_compare.sh` when you want a complete
TreeDB-vs-MongoDB matrix bundle instead of hand-running each target. The
harness builds `mongo_gateway_bench`, runs matching TreeDB and MongoDB cells,
writes raw JSON for every target, records physical `du` bytes where available,
and generates a Markdown report plus TSV summary.

```sh
scripts/mongo_gateway_compare.sh \
  --out /tmp/gomap_mongo_gateway_compare \
  --docs "1000 10000" \
  --indexes "0 2" \
  --concurrent-readers 8 \
  --concurrent-reads 10000 \
  --concurrent-writers 4 \
  --concurrent-writes 1000 \
  --mongo-mode docker
```

Docker mode starts a fresh MongoDB container and isolated data directory per
matrix cell, which makes MongoDB physical disk usage reproducible enough for
local comparisons. If you already have a MongoDB server, use:

```sh
scripts/mongo_gateway_compare.sh \
  --mongo-mode external \
  --mongo-uri mongodb://127.0.0.1:27017
```

The bundle contains:

- `report.md`: reviewable Markdown with highlights, disk bytes/doc, ops/sec
  ratios, and raw input paths.
- `summary.tsv`: machine-readable per-phase comparison rows.
- `matrix.tsv`: target/document/index/raw-json/physical-byte index.
- `raw/*.json`: unmodified `mongo_gateway_bench -format json` output.
- `treedb_data/` and, in Docker mode, `mongodb_data/`: final data directories
  for post-run inspection.

The first checked-in bundle is
`docs/benchmarks/mongo_gateway_compare_2026-04-29/`.

To regenerate only the report from an existing bundle:

```sh
GOWORK=off go run ./cmd/mongo_gateway_compare_report \
  -matrix /tmp/gomap_mongo_gateway_compare/matrix.tsv \
  -report /tmp/gomap_mongo_gateway_compare/report.md \
  -summary /tmp/gomap_mongo_gateway_compare/summary.tsv
```

Useful overrides:

- `DOCS_LIST="1000 10000 100000"`
- `INDEXES_LIST="0 1 2"`
- `READS=50000`, `RANGE_READS=5000`, `UPDATES=5000`
- `DELETES=1000`
- `CONCURRENT_READERS=16`, `CONCURRENT_READS=50000`
- `CONCURRENT_WRITERS=8`, `CONCURRENT_WRITES=10000`
- `BATCH_SIZE=1000`
- `MONGO_IMAGE=mongo:8`

For a larger MongoDB comparison that keeps the 1M-doc cell bounded enough for a
local run, use explicit operation counts:

```sh
BATCH_SIZE=5000 scripts/mongo_gateway_compare.sh \
  --out /tmp/gomap_mongo_gateway_compare_large \
  --docs "100000 1000000" \
  --indexes "2" \
  --reads 50000 \
  --range-reads 5000 \
  --updates 5000 \
  --concurrent-readers 16 \
  --concurrent-reads 50000 \
  --concurrent-writers 8 \
  --concurrent-writes 10000 \
  --timeout 120m
```

## Manual Matrix

Run both targets with the same values and keep the raw JSON outputs:

```sh
for docs in 1000 10000 100000; do
  for indexes in 0 1 2; do
    GOWORK=off go run ./cmd/mongo_gateway_bench \
      -target treedb \
      -treedb-dir "/tmp/treedb-mongo-${docs}-${indexes}" \
      -keep-treedb-dir \
      -documents "$docs" \
      -reads "$docs" \
      -range-reads "$((docs / 10))" \
      -updates "$((docs / 10))" \
      -concurrent-readers 8 \
      -concurrent-reads "$((docs / 10))" \
      -concurrent-writers 4 \
      -concurrent-writes "$((docs / 20))" \
      -secondary-indexes "$indexes" \
      -format json > "treedb-${docs}-${indexes}.json"

    GOWORK=off go run ./cmd/mongo_gateway_bench \
      -target mongo \
      -documents "$docs" \
      -reads "$docs" \
      -range-reads "$((docs / 10))" \
      -updates "$((docs / 10))" \
      -concurrent-readers 8 \
      -concurrent-reads "$((docs / 10))" \
      -concurrent-writers 4 \
      -concurrent-writes "$((docs / 20))" \
      -secondary-indexes "$indexes" \
      -format json > "mongo-${docs}-${indexes}.json"
  done
done
```

The initial workload phases are:

- `load_insert_many`: batched document inserts.
- `id_find_one`: point lookup by `_id`.
- `email_find_one`: point lookup by the `email` field; emitted only when the
  email secondary index is part of the cell.
- `age_range_limit_10`: bounded range query with `limit: 10`; operations count
  range queries, not returned documents.
- `id_update_set`: `$set` update by `_id`.
- `concurrent_id_find_one_rN`: total `_id` point reads split across `N`
  goroutines.
- `concurrent_id_update_set_wN`: total `$set` updates split across `N`
  goroutines.
- `id_delete_one`: optional deletes; disabled unless `-deletes` is non-zero.

Latency samples are per MongoDB driver call. Insert ops/sec is normalized by
document count, while insert latency percentiles are per `InsertMany` call.
Range-query samples include cursor materialization with `cursor.All`.
Use `-timeout 0` to run without an overall benchmark deadline.

The package test `TestTreeDBProfileSmokeFastAndWALOnFast` runs a small write-only
TreeDB gateway smoke against both `fast` and `wal_on_fast` to catch large
profile regressions without making the smoke a replacement for the full matrix.

## Interpreting Results

`-secondary-indexes 2` creates `email_1` and `city_1`; the age range phase is
currently a bounded scan in the gateway and is included to make that cost
visible.

For TreeDB, prefer `treedb_disk_after_maintenance.total_bytes` when present, and
use `treedb_disk_after_checkpoint.total_bytes` for checkpoint-only runs. The
child path breakdown, especially `index.db`, `leaf_vlog`, and `value_vlog`,
shows where bytes landed. These are logical bytes: the sum of regular file
sizes, not allocated physical disk usage including block allocation, sparse-file
effects, filesystem compression, or metadata. Capture `du` separately when
physical on-disk usage is the comparison target.

For MongoDB, compare `mongodb_stats_final.storageSize`,
`mongodb_stats_final.indexSize`, and `mongodb_stats_final.totalSize`. If the
MongoDB server is local, also capture a filesystem `du` of the database path for
the final report. The comparison harness treats isolated physical `du` as the
preferred local disk metric because small WiredTiger workloads can report
`dbStats.totalSize` values that are much smaller than the actual data directory.

The benchmark intentionally keeps BSON format questions visible. If TreeDB load
or read phases spend a meaningful amount of time re-encoding documents in future
profiles, that is evidence for adding a native BSON collection document format
beside JSON and template-v1.
