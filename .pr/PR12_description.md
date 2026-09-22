# PR12: decouple journal off from value-log (enable 1/2/3 bench matrix)

## Summary

- Add `DisableJournal` option (redo/journal off without disabling value-log pointers).
- Make cached-mode flush persist `ValuePtr` directly into the backend (value log becomes the durable value store for large values).
- Update backend WAL replay to apply `OpSetRID` as `SetPointer` (no value materialization) and keep `value-*.log` segments when value-log pointers are enabled.
- Keep “fail fast” safety for missing dictionaries by validating dict existence while scanning value-log segments during replay.

## Tests

- `go test ./... -count=1`
- `go test ./... -race -count=1` (macOS linker warning building `cmd/unified_bench.test`: malformed `LC_DYSYMTAB`)

## unified_bench output (write-path matrix)

Method:

- Built binary once: `go build -o /tmp/unified_bench ./cmd/unified_bench`
- Bench hygiene: `RUNS=5`, `SLEEP_S=5`, keep middle 3 (drop min/max).
- Workload: `-suite lanes_probe -dbs treedb -keys 1000000 -valsize 1024 -batchsize 1000`

### Case 1: `DisableWAL=1` (values → `backend_flush`)

Command:

`/tmp/unified_bench -suite lanes_probe -dbs treedb -keys 1000000 -valsize 1024 -batchsize 1000 -treedb-disable-wal -treedb-allow-unsafe`

Results:

- runs ops/sec: `2,946,024 / 2,998,833 / 2,925,590 / 2,913,562 / 2,911,058`
- keep3 avg ops/sec: `2,928,392`
- typical sizes: `index.db=134,217,728` `wal bytes=0`

### Case 2: `DisableJournal=1` (values → `value_log`)

Command:

`/tmp/unified_bench -suite lanes_probe -dbs treedb -keys 1000000 -valsize 1024 -batchsize 1000 -treedb-disable-journal -treedb-memtable-value-log-pointers -treedb-split-value-log -treedb-allow-unsafe`

Results:

- runs ops/sec: `768,054 / 827,890 / 845,296 / 854,036 / 876,695`
- keep3 avg ops/sec: `842,407`
- typical sizes: `index.db=67,108,864` `wal bytes=1,072,000,000`

### Case 3: journal on (values → `value_log`)

Command:

`/tmp/unified_bench -suite lanes_probe -dbs treedb -keys 1000000 -valsize 1024 -batchsize 1000 -treedb-memtable-value-log-pointers -treedb-split-value-log -treedb-allow-unsafe`

Results:

- runs ops/sec: `901,674 / 862,631 / 742,792 / 537,508 / 512,846`
- keep3 avg ops/sec: `714,310`
- typical sizes: `index.db=67,108,864` `wal bytes=1,072,000,000`

