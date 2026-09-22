// Package hashdb implements HashDB, a mmap-backed hash index with a slab value log.
//
// The recommended entrypoint is Open/OpenWithShards, which returns *HashDB: a
// thread-safe sharded store (formerly "gomap_distributed").
//
// For single-shard usage (not thread-safe), open DB directly or use OpenSingle.
//
// HashDB acquires an exclusive process lock on the database directory. If the
// directory is already open in another process, Open/OpenWithShards/OpenSingle
// return ErrLocked.
//
// Note on durability:
// HashDB is tuned for performance and uses an append-only slab value log.
//
//   - Put/Delete are not guaranteed durable (no fsync).
//   - PutSync/DeleteSync fsync the slab value log so the operation survives a crash/power loss.
//   - ApplyBatch applies a batch atomically in-process; ApplyBatchSync is the durable, crash-atomic variant.
//   - Export/Restore provide a simple snapshot mechanism built on ForEach + ApplyBatchSync.
//     Snapshot iteration order is arbitrary.
//
// The mmap index files are treated as a derived cache; after an unclean shutdown
// HashDB rebuilds the index by scanning the slab log (and truncates torn tail records).
//
// Note on sharded caching:
// The sharded *HashDB implementation uses a per-shard write-back cache. By default
// the cache has no WAL (pending writes are volatile until flushed). For advanced
// usage, a per-shard cache WAL can be enabled via OpenWithOptions/OpenWithShardsAndOptions.
package hashdb
