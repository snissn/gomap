# Native Collection Workload Bench

`collection_workload_bench` measures TreeDB collection operations without the
MongoDB compatibility stack. It is intended to answer whether an operation is
slow before Mongo gateway, wire protocol, cursor, driver, and BSON `_id`
primary-key encoding overhead.

The harness uses native `collections.Collection` calls:

- `InsertBatch`
- `GetInto`
- `FindByIndexValueLimit`
- `FindByIndexRange`
- `Update`
- `Delete`

Document IDs are external native `[]byte` keys such as `doc-000000000001`.
Stored documents still include a normal `id` payload field, but the primary key
is not derived from Mongo `_id` encoding.

## Storage Formats

Use `-formats` to run one or more collection storage formats:

```bash
go run ./cmd/collection_workload_bench \
  -formats json,template-v1,bson \
  -indexes 0,1,2 \
  -read-states buffered,flushed,checkpointed \
  -documents 16000 \
  -batch-size 16000
```

`collections-v1` is accepted as an alias for `template-v1`.

## Index Shapes

The index count is deliberately native and small:

- `indexes_0`: primary root only
- `indexes_1`: unique `email` index
- `indexes_2`: `email` plus non-unique `age`
- `indexes_3`: `email`, `age`, and non-unique `city`

`email_find_one` is skipped for `indexes_0`. `age_range_indexed_limit_10` is
skipped until `indexes_2`. `age_range_scan_limit_10` always runs and measures a
native primary-key scan floor using deterministic IDs and `GetInto`.

## Read State

Each matrix row creates a fresh TreeDB directory, loads the fixture, then applies
one read state:

- `buffered`: read before an explicit collection flush
- `flushed`: run `CollectionManager.FlushAll()` before reads
- `checkpointed`: run `FlushAll()` and `DB.Checkpoint()` before reads

This separates memtable/overlay reads from settled backend reads.

## Output

Text output includes both latency and throughput for every non-skipped phase:

```text
id_find_one        1200.0 ns/op    833333 ops/sec
```

JSON output is available with `-format json` and includes selected TreeDB stats
deltas for each phase.

## Profiling

For now, capture pprof around this command with normal Go tooling, or use the
existing `unified-bench -profile-dir` flow for end-to-end report artifacts. If
this command grows a built-in `-profile-dir`, keep the artifact names compatible
with `cmd/benchprof`.
