# Benchmark Specification

The primary benchmarking tool is `cmd/unified_bench`, which runs a consistent workload across multiple engines (HashDB, TreeDB, Badger, LevelDB).

## Methodology

- **Cold Start**: Each test run creates a fresh, empty DB directory (unless `--keep` is used).
- **Preloading**: For read-only tests (`read_rand`, `full_scan`), the DB is pre-populated with sequential keys if no write test preceded it.
- **Timing**: Throughput (ops/sec) is calculated as `Count / Duration`. Duration includes the full loop but excludes DB open/close time.
- **Keys**: 8-byte big-endian integers (lexicographically sorted).
- **Values**: 128 bytes (default) of uniform random data (or zeros if faster allocation is needed; currently zeroed buffer for speed).

## Test Definitions

### Point Operations

1.  **Sequential Write** (`write_seq`)
    - Writes `N` keys from `0` to `N-1` in increasing order.
    - Simulates bulk loading or log-structured ingestion.

2.  **Random Write** (`write_rand`)
    - Writes `N` keys randomly selected from `[0, 10*N)`.
    - The sparse keyspace (10x) forces internal fragmentation and defeats simple append-only optimizations.

3.  **Random Read** (`read_rand`)
    - Reads `N` keys randomly selected from `[0, N)`.
    - Targets the populated keyspace.

4.  **Random Delete** (`delete_rand`)
    - Deletes `N` keys randomly selected from `[0, N)`.

### Batch Operations

1.  **Batch Write** (`batch_write`)
    - Writes `N` keys sequentially in batches of size `1000` (default).
    - Tests the amortized write path (WAL commit / Transaction commit).

2.  **Batch Random** (`batch_write_random`)
    - Writes `N` keys randomly selected from `[0, 10*N)` in batches.
    - Tests batch processing under fragmentation pressure.

### Scans

1.  **Full Scan** (`full_scan`)
    - Iterates over **all** keys in the database `[0, ∞)`.
    - Measures pure iteration throughput (Next/Value overhead).

2.  **Prefix Scan** (`prefix_scan`)
    - Performs `M` short range queries (default 200 queries of 100 keys each).
    - Measures "seek + short scan" performance, critical for range queries.

## Comparison Baselines

- **TreeDB (Cached)**: `treedb`
  - Default mode. Memtable + WAL -> Async Flush.
- **TreeDB (Backend)**: `treedbbackend`
  - Direct B+Tree writes. No memtable.
- **HashDB**: `hashdb`
  - Mmap-based hash index. No ordered scans.
- **BadgerDB**: `badger` (v4)
  - Pure Go LSM tree.
- **LevelDB**: `leveldb` (goleveldb)
  - Classic LSM tree.

## Running

```bash
# Run all tests on all DBs with 1 million keys
./bin/unified-bench -keys 1000000

# Run specific tests
./bin/unified-bench -tests write_seq,read_rand -dbs treedb,hashdb
```