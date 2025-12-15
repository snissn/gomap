# HashDB Concepts

HashDB is a persistent key/value store designed for high throughput point lookups.
It is implemented as:

- an **mmap-backed hash index** (SwissHash-style control bytes + key metadata), and
- an append-only **slab value log** (segment files `slab-*`) that stores the actual key/value records.

The sharded store (`*hashdb.HashDB`) is the recommended entrypoint for most use cases.

## Entry Points

- Sharded, thread-safe: `hashdb.Open(dir)` / `hashdb.OpenWithShards(dir, n)` → `*hashdb.HashDB`
- Single shard (not goroutine-safe): `hashdb.OpenSingle(dir)` → `*hashdb.DB`

## On-Disk Layout (per DB directory)

Index (mmap):
- `hashctl-<capacity>`: control bytes (small; frequently accessed)
- `hashkeys-<capacity>`: key metadata array (`Key` entries, includes hash + slab offset)

Value log (append-only):
- `slab-0`, `slab-1`, ...: segmented slab files that store records:
  - `keyLen` + `valLen` header
  - `key` bytes
  - `value` bytes (optionally compressed)

Metadata:
- `metadata`: small mmap that stores the current append offset and counters

Lock:
- `LOCK`: exclusive cross-process lock file (`hashdb.ErrLocked` on contention)

## Durability Model

HashDB durability is based on the slab value log:

- `Put` / `Delete`: fast, but **best-effort** durability (no fsync)
- `PutSync` / `DeleteSync`: fsync the active slab segment so the operation survives a crash
- `ApplyBatchSync`: crash-atomic batches with explicit BEGIN/COMMIT markers in the slab log

The mmap index is treated as a **derived cache**:
after an unclean shutdown, HashDB rebuilds it by scanning the slab log and truncating torn tail records.

## Iteration and Snapshots

HashDB does not provide ordered iteration.

- `ForEach`: iterates the whole keyspace in **arbitrary order**.
  - Sharded `ForEach` takes an exclusive snapshot of the store (blocks writers and flushes caches).
- `Export` / `Restore`: streaming snapshot format built on `ForEach` + `ApplyBatchSync`.

See `docs/HASHDB_SNAPSHOT.md`.

## Sharded Store Notes

`*hashdb.HashDB` uses per-shard `CachedDB` instances:

- The cache is a write-back buffer (no WAL).
- Durable writes (`*Sync`) flush the cache first and then commit durably to the backend shard.

## Related Docs

- `docs/HASHDB_TUNING.md`
- `docs/HASHDB_SNAPSHOT.md`
- `docs/contracts/README.md`
