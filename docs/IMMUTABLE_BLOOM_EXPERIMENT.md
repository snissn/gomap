# TreeDB (cached): Immutable Bloom Filter Experiment

Date: 2026-01-27

This is an experiment to evaluate whether adding a per-immutable Bloom filter
helps point-read throughput under flush debt (deep immutable queue), beyond the
shard-only immutable lookup fix.

## Implementation summary

- Adds an option `Options.ImmutableBloomFilter` (default false).
- When enabled, the caching layer wraps immutable **hash_sorted** memtables with
  a Bloom filter built at rotation time.
- Point reads (`Get`/`GetAppend`/`Has`) consult the filter before doing the
  immutable lookup; if the filter is negative the immutable is skipped.
- Unified bench flag: `-treedb-immutable-bloom`.

## Bench methodology

All runs below are TreeDB only:

- `-profile fast`
- mixed workload (no checkpoint between tests)
- `-treedb-cache-stats-before-reads` to record queue/backlog

### Run A (all tests)

Baseline:

```bash
go run ./cmd/unified_bench -dbs treedb -profile fast -keys 900000 -valsize 128 -batchsize 1000 \
  -test all -treedb-cache-stats-before-reads -progress=false
```

Candidate (immutable bloom enabled):

```bash
go run ./cmd/unified_bench -dbs treedb -profile fast -keys 900000 -valsize 128 -batchsize 1000 \
  -test all -treedb-cache-stats-before-reads -treedb-immutable-bloom -progress=false
```

Raw outputs are in:
Baseline (ops/sec):

```
Sequential Write:      9,846,244
Random Write:          1,488,381
Dataset Write (Random):3,074,501
Dataset Write (Sorted):3,227,284
Batch Write:           4,361,116
Batch Random:          1,506,147
Batch Delete:          2,300,202
Batch Small Seq:       3,166,544
Random Delete:         1,622,010
Random Read:             640,669
Full Scan:             1,925,874
Prefix Scan:             425,214
```

Candidate (ops/sec):

```
Sequential Write:      9,803,032
Random Write:          1,504,210
Dataset Write (Random):2,611,270
Dataset Write (Sorted):2,831,067
Batch Write:           4,384,260
Batch Random:          1,493,257
Batch Delete:          2,224,492
Batch Small Seq:       2,381,720
Random Delete:         1,763,584
Random Read:             597,334
Full Scan:             1,527,160
Prefix Scan:             204,610
```

Key deltas (candidate vs baseline):

- `Random Read`: -6.76%
- `Sequential Write`: -0.44%
- `Batch Random`: -0.86%
- `Batch Small Seq`: -24.78%

### Run B (forced deep queue)

This increases rotation frequency and disables backpressure so the immutable
queue becomes very deep:

```bash
go run ./cmd/unified_bench -dbs treedb -profile fast -keys 2000000 -valsize 128 -batchsize 1000 \
  -test sequential_write,random_read -treedb-cache-stats-before-reads \
  -treedb-flush-threshold 4194304 -treedb-max-queued-memtables 1000 \
  -treedb-slowdown-backlog-seconds 1000000000 -treedb-stop-backlog-seconds 1000000000 \
  -treedb-max-backlog-bytes 1099511627776 -progress=false
```

Raw output:
Baseline (ops/sec):

```
Sequential Write: 9,180,481
Random Read:      1,049,809
```

Observed: `queue_len=440` with `Random Read` still ~1.05M ops/sec (skiplist memtable mode),
so the Bloom filter (hash_sorted-only) does not engage here.

## Conclusion (so far)

On these runs, the immutable Bloom filter does **not** improve `random_read` and
can regress other throughput numbers (notably `batch_small_seq` and scans).

It may still be worth revisiting if/when we have:
- a benchmark that stresses **miss-heavy** point lookups, or
- a workload where `memtable_mode=hash_sorted` persists while `queue_len` is very deep.
