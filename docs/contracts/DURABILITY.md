# Durability

## TL;DR

- TreeDB provides explicit durability calls (`SetSync`, `Batch.WriteSync`) and coherent crash recovery with WAL on/off semantics.
- HashDB provides explicit durability calls (`PutSync`, `DeleteSync`) backed by the slab value log and crash recovery; non-sync writes may be lost on power loss.

## Who Is This For?

- Anyone who needs to understand what survives a crash / power loss.
- Anyone building a replication/consensus layer where “committed” must be durable.

## TreeDB

TreeDB exposes a single cached-mode engine with selectable durability semantics
via `Options.Durability`.

### Durable (default) (`Durability = DurabilityDurable`)

Cached mode writes to the journal (and the value log for large values), then
eventually flushes to the backend.

- `Set`: appends to the journal (and value log if needed) but does not `fsync`; not guaranteed durable on power loss.
- `Batch.Write`: appends to the journal (and value log if needed) but does not `fsync`; not guaranteed durable on power loss.
- `SetSync`: appends to the journal and `fsync`s it; durable.
- `Batch.WriteSync`: appends the entire batch to the journal as a single checksummed segment and `fsync`s it; **atomic and durable**. On recovery, either the entire batch is applied or none of it is.

Crash recovery:
- On open, any journal segments in `Dir/maindb/wal/` are replayed into the backend with synced commits, then removed.

### WAL on, relaxed (`Durability = DurabilityWALOnRelaxed`)

WAL stays enabled, but `*Sync` operations do not `fsync`. This is
crash-consistent (process crash) but not guaranteed durable on power loss.

- `Set` / `Batch.Write`: not guaranteed durable on power loss.
- `SetSync` / `Batch.WriteSync`: crash-consistent only (no `fsync`).

### WAL off (`Durability = DurabilityWALOffRelaxed`)

WAL-off disables the journal/redo log while keeping the value log enabled. This
improves write throughput but sacrifices durability for the most recent writes
since the last checkpoint.

- `Set` / `Batch.Write`: not guaranteed durable (no redo log).
- `SetSync` / `Batch.WriteSync`: crash-consistent only (no redo log + no `fsync`).
- Use `Checkpoint()` to establish a durable boundary.

### Operational notes

- Always `Close()` iterators and DB handles to release pinned resources and directory locks.
- Multiple processes must not open the same directory concurrently (exclusive lock).

## HashDB

HashDB is designed as a high-performance mmap-backed hashmap engine with a slab value log.

Durability contract:
- `Put` / `Delete` are not guaranteed durable (no fsync); writes go through the OS page cache and may be lost on power loss.
- `PutSync` / `DeleteSync` fsync the slab value log (and the DB directory on slab segment rotation where supported) so the operation survives a crash/power loss.
- `ApplyBatch`: atomic in-process, but not guaranteed durable (no fsync).
- `ApplyBatchSync`: atomic and durable; on recovery, either the entire batch is applied or none of it is.
- The mmap index (`hashctl-*`, `hashkeys-*`) is treated as a derived cache; on open after an unclean shutdown HashDB rebuilds the index by scanning the slab log and truncates torn tail records.

Notes:
- HashDB’s sharded `Open/OpenWithShards` entrypoint uses a write-back cache. By default there is no cache WAL, so pending cache writes are volatile until flushed. `PutSync`/`DeleteSync` flush the cache and then perform a durable backend write.
- Optional: a per-shard cache WAL can be enabled via `hashdb.OpenWithOptions` / `hashdb.OpenWithShardsAndOptions` (`HashDBOptions.CacheWAL`). Depending on the fsync policy, non-`*Sync` cache writes may be recoverable after a crash.
- For the sharded `*hashdb.HashDB` entrypoint, `ApplyBatchSync` is atomic per shard, but not atomic across shards.
- If you need fully integrated journal-based durability with stronger corruption detection, use TreeDB `*Sync` operations today.
