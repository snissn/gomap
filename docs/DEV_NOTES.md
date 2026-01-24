# Dev Notes – Incremental Rehash & Perf Tuning

This note summarizes the main engine and benchmark changes made during the recent performance tuning session.

## Hashing

- Switched the primary hash function from `segmentio/fasthash/fnv1` (32-bit) to `cespare/xxhash/v2` (64-bit) in `hashindex.go`.
- The `Hash` stored in `Key.hash` now uses the full 64 bits of `xxhash`, reducing collision risk for large keyspaces and sharding.
- On‑disk indexes created with the old FNV hash are not compatible with the new hash; existing databases should be rebuilt via `Recover()` or reinitialized.

## Slab I/O

- Eliminated per-write `Stat()` syscalls on slab segments:
  - Added `Hashmap.activeSegmentSize` and now track segment size in memory.
  - `writeSlab`, `writeSlabAndRotate`, and `addManySlabs` rotate segments using `activeSegmentSize + len(buf) > MaxSegmentSize` and update `activeSegmentSize` after each write.
  - `openSlabSegments` and `Recover` initialize `activeSegmentSize` from the last segment’s file size.
- Behavior of slab layout and `Recover()` remains the same; only syscall overhead is reduced.

## Resize Threshold & Load Factor Benchmark

- Added `Hashmap.resizeThreshold` and `SetResizeThreshold(percent uint64)`:
  - `checkResize()` now uses `resizeThreshold` (default 65%) instead of a hard-coded constant.
  - Allows experiments with different load factors.
- New CLI benchmark: `cmd/loadfactorbench/main.go`
  - Preloads a single `Hashmap` to specific load factors (0.25–0.95).
  - Runs a mixed 80% GET / 20% SET workload and reports RPS and average latency per load factor.

## Incremental Per-Shard Rehash

- Implemented incremental (online) rehash per shard instead of full stop-the-world index rebuild:
  - New fields on `Hashmap` in `types.go`:
    - `rehashInProgress`, `rehashOldMapFile`, `rehashOldMap`, `rehashOldCapacity`, `rehashOldKeys []Key`, `rehashIdx`.
  - `resize.go`:
    - `startRehash()` allocates a new index at 2× capacity, switches `hashMap`/`Keys`/`Capacity`, and records the old table as rehash state.
    - `rehashStep(maxToMove)` migrates up to `maxToMove` buckets from the old table to the new table by reading keys from the slab and inserting into the new table without changing `Count`.
    - `finishRehash()` closes/unmaps the old index and removes stale `hashkeys-*` files so only `hashkeys-<current Capacity>` remains.
    - `rehashBucketsPerWrite` is currently fixed at 8, and `checkResize()` will not start a new rehash while one is in progress.
    - `resize()` now uses `startRehash()` + `rehashStep()` in a tight loop to complete rehash synchronously (for tests and explicit callers).
- Read/write behavior with rehash in progress:
  - `Get` (`db.go`):
    - Probes the current (new) table first.
    - If `rehashInProgress` and not found, also probes the old table (`rehashOldKeys`).
  - `Delete`:
    - Deletes/tombstones in the new table, writes a delete slab record, and decrements `Count` once.
    - If `rehashInProgress`, also tombstones any matching bucket in the old table so it cannot be resurrected by migration.
  - `Put` / `PutMany`:
    - Insert into the current table; if `rehashInProgress`, call `rehashStep(rehashBucketsPerWrite)` under the shard’s write lock to migrate a few old buckets per write.
- `closeFPs` was updated to cleanly close any old rehash table if still in progress.
- `Recover()` clears rehash state before rebuilding the index from the slab, ensuring recovery always yields a single-table state.

## Index File Cleanup & Leak Test

- After a completed rehash, `finishRehash()` now scans the DB folder and removes any `hashkeys-*` files that do not match the current capacity.
- `TestResizeLeak` (`rehash_leak_test.go`) now explicitly calls `obj.resize()` after driving resizes so the incremental rehash completes and the test can assert that exactly one `hashkeys-*` file exists.

## Sharded API & MGET

- Added `HashDB.GetMany(keys [][]byte)`:
  - Groups key indexes per shard, acquires one read lock per shard, and calls `DB.Get` for all shard-local keys.
  - Returns values and per-key errors aligned with input.
- `redisserver/hashdbredis` now implements Redis `MGET` via `GetMany`, reducing lock churn and improving multi-key read performance.

## Compression Control

- `DB.SetCompression(enabled bool)` already existed.
- New `HashDB.SetCompression(enabled bool)`:
  - Iterates shards under their write locks and forwards the setting to each `DB`.
  - Used to quickly toggle compression for the entire distributed map.
- `hashdbredis.NewRedisServer` now calls `store.SetCompression(false)`:
  - Disables value compression by default for the gomap Redis server used in benchmarks.
  - This avoids compression overhead on write-heavy and 1KB-value scenarios.

## Incremental Resize Latency Benchmark

- New CLI: `cmd/resizebench/main.go`
  - Runs a single `DB` through a fixed number of `Put` operations (default 200k).
  - Measures per-op latency distribution (p50, p95, p99, max) and overall RPS across one or more resize events.
  - Intended to compare stop-the-world vs incremental rehash behavior by running it on different versions.

## Pipeline and Large Value SET Batching

- `redisserver/hashdbredis` now supports optional batched SETs per connection:
  - `RedisServer` holds `batchSets bool`, enabled via `HASHDB_BATCH_SETS=1` (or legacy `GOMAP_BATCH_SETS=1`).
  - Connection state (`connState`) accumulates `hashdb.Item{Key, Value}` in `pending`.
  - Once `pending` reaches `setBatchSize` (currently 16), it calls `HashDB.PutMany(pending)` and then writes `+OK` once per item.
  - This is specifically tuned for `redis-benchmark -P16` workloads.
- Benchmark harness integration:
  - `benchmark/runner.go` sets `GOMAP_BATCH_SETS=1` in the hashdb server environment only for:
    - `Scenario.Name == "Pipeline16"` or `"LargeVal1KB"`.
  - Standard and RandomKeys scenarios, and non-benchmark hashdbredis usage, keep the original one-SET-per-Put behavior.

## Possible Next Steps

- **Zero/low-copy slab reads**
  - Explore returning slices into an mmap-backed slab region (or shard-local reusable buffers) for read-heavy workloads to reduce allocations and memcpy in `Get`.
  - Requires careful handling of file growth/rotation and ensuring callers do not retain references across compaction or close.

- **Vectorized I/O / deeper MGET optimizations**
  - Build on `HashDB.GetMany` to batch slab reads per shard.
  - On Linux, optionally experiment with `preadv`/`readv` or io_uring for large MGETs to further reduce syscalls.

- **Compaction correctness & performance**
  - Fix `Compact()` + `TestCompaction` to be correct with the segmented slab design.
  - Once correct, tune compaction to be more streaming and less stop-the-world, and consider background compaction per shard.

- **Index layout / key size experiments**
  - Prototype a variant where `Key` is reduced from 16 bytes to 8 bytes (e.g., 32-bit chunked offsets + 32-bit hash) with a clear maximum DB size.
  - Use `loadfactorbench` and `resizebench` to compare cache behavior, probe lengths, and latency at large capacities.

- **Compression policy tuning**
  - Systematically benchmark different compression thresholds and possibly alternative codecs for large values (e.g., only compress >4KB or user-configurable).
  - Expose per-DB or per-shard compression modes (off / fast / aggressive) and document recommended defaults for common workloads.

- **Durability and WAL improvements**
  - If stronger durability is desired, add an explicit small WAL (separate from the slab segments) with configurable fsync policies.
  - Keep the current slab-based log as the primary recovery mechanism, but allow stricter durability modes for users who need them.

## Compression Bench Notes (s2)

HashDB compresses values using `snissn/compress/s2` when enabled and when it is beneficial.
There is a minimum size threshold (`minValueBytesForCompression`, currently 32 bytes) to avoid overhead on tiny values.

Example run (Apple M3, darwin/arm64):

```text
go test -run=^$ -bench BenchmarkCompressionMatrix -benchmem ./HashDB -count=1

BenchmarkCompressionMatrix/zeros-32-8      1000000000  1.198 ns/op  26703.10 MB/s  0 B/op    0 allocs/op
BenchmarkCompressionMatrix/zeros-64-8      1823733     650.3 ns/op  98.42 MB/s     80 B/op   1 allocs/op
BenchmarkCompressionMatrix/patterned-128-8 1292676     906.1 ns/op  141.26 MB/s    144 B/op  1 allocs/op
BenchmarkCompressionMatrix/patterned-1024-8 877576     1322 ns/op   774.48 MB/s    1152 B/op 1 allocs/op
```
