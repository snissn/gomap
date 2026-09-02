# HashDB Performance + Tuning

This doc collects practical knobs and tradeoffs for HashDB.

## Sharding

- Default shard count is optimized for concurrency; you can choose explicitly with `hashdb.OpenWithShards(dir, n)`.
- More shards increases parallelism and reduces lock contention, but increases overhead (more files, more background flush loops).

## Sharded Cache WAL (Optional)

The sharded engine uses a write-back cache per shard.

- Default: no cache WAL (highest throughput; pending writes are lost on crash before flush).
- Optional: enable a per-shard cache WAL via `hashdb.OpenWithOptions` / `hashdb.OpenWithShardsAndOptions`:
  - `CacheWALFsyncOnSync`: fsync only when a higher-level `*Sync` call requests it.
  - `CacheWALFsyncAlways`: fsync every cache write (stronger durability, slower).

This is primarily a durability knob; it can reduce recovery surprise at the cost of extra write I/O.

## Compression

HashDB can compress values in the slab log (s2/snappy-style).

- Toggle: `SetCompression(true/false)` on `*hashdb.DB` or `*hashdb.HashDB`.
- Values smaller than `minValueBytesForCompression` are not considered for compression.
- Compression is only used if the compressed payload is smaller than the original.

For a quick micro benchmark run:

```text
go test -run=^$ -bench BenchmarkCompressionMatrix -benchmem ./HashDB -count=1
```

## GetMany / MGET Workloads

`HashDB.GetMany(keys)` is optimized for multi-key reads:

- Keys are grouped per shard and processed in parallel.
- Backend slab reads are coalesced per segment to reduce `ReadAt` syscalls (best-effort locality win).

Benchmark:

```text
go test -run=^$ -bench BenchmarkGetManyMatrix -benchmem ./HashDB -count=1
```

## Segment Size

Slab segments rotate at `hashdb.MaxSegmentSize` (default 64MB).
Smaller segments can reduce worst-case recovery scan work but increase file count.

## Memory Policy (Index)

HashDB can apply OS memory policies to index mmaps:

- Lock control bytes (best-effort or strict) for faster probe scans.
- Apply `madvise` hints for key array access patterns.

See `HashDB/index_memory.go` for `IndexMemoryPolicy`.

To configure for the sharded engine, pass `HashDBOptions.IndexMemoryPolicy` to:

- `hashdb.OpenWithOptions(dir, opts)`
- `hashdb.OpenWithShardsAndOptions(dir, shards, opts)`
