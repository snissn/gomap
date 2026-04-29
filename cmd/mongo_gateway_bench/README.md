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
  -secondary-indexes 2 \
  -format json
```

For `-target treedb`, the command starts an in-process TreeDB Mongo gateway on a
loopback listener, connects with the official MongoDB Go driver, and reports
the sum of regular file sizes for the TreeDB directory after load and after a
final checkpoint. If `-treedb-dir` is provided and already exists, the harness
removes and recreates it before loading deterministic fixtures so repeated runs
are reproducible. Obvious unsafe reset targets such as root, the current
checkout, the temp directory itself, or a home directory are rejected.

## MongoDB Target

```sh
GOWORK=off go run ./cmd/mongo_gateway_bench \
  -target mongo \
  -mongo-uri mongodb://127.0.0.1:27017 \
  -documents 10000 \
  -reads 10000 \
  -range-reads 1000 \
  -updates 1000 \
  -secondary-indexes 2 \
  -format json
```

For `-target mongo`, the command connects to the supplied URI, drops the
benchmark database by default, and reports `dbStats` fields after load and at
the end of the run.

## Suggested Matrix

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
      -secondary-indexes "$indexes" \
      -format json > "treedb-${docs}-${indexes}.json"

    GOWORK=off go run ./cmd/mongo_gateway_bench \
      -target mongo \
      -documents "$docs" \
      -reads "$docs" \
      -range-reads "$((docs / 10))" \
      -updates "$((docs / 10))" \
      -secondary-indexes "$indexes" \
      -format json > "mongo-${docs}-${indexes}.json"
  done
done
```

The initial workload phases are:

- `load_insert_many`: batched document inserts.
- `id_find_one`: point lookup by `_id`.
- `email_find_one`: point lookup by the `email` field.
- `age_range_limit_10`: bounded range query with `limit: 10`; operations count
  range queries, not returned documents.
- `id_update_set`: `$set` update by `_id`.
- `id_delete_one`: optional deletes; disabled unless `-deletes` is non-zero.

Latency samples are per MongoDB driver call. Insert ops/sec is normalized by
document count, while insert latency percentiles are per `InsertMany` call.
Range-query samples include cursor materialization with `cursor.All`.

## Interpreting Results

`-secondary-indexes 2` creates `email_1` and `city_1`; the age range phase is
currently a bounded scan in the gateway and is included to make that cost
visible.

For TreeDB, compare `treedb_disk_after_checkpoint.total_bytes` and the child
path breakdown, especially `index.db`, `leaf_vlog`, and `value_vlog`. These are
logical bytes: the sum of regular file sizes, not allocated physical disk usage
including block allocation, sparse-file effects, filesystem compression, or
metadata. Capture `du` separately when physical on-disk usage is the comparison
target.

For MongoDB, compare `mongodb_stats_final.storageSize`,
`mongodb_stats_final.indexSize`, and `mongodb_stats_final.totalSize`. If the
MongoDB server is local, also capture a filesystem `du` of the database path for
the final report.

The benchmark intentionally keeps BSON format questions visible. If TreeDB load
or read phases spend a meaningful amount of time re-encoding documents in future
profiles, that is evidence for adding a native BSON collection document format
beside JSON and template-v1.
