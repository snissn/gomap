# Durability

## TL;DR

- TreeDB provides explicit durability calls (`SetSync`, `Batch.WriteSync`) and coherent crash recovery across cached vs backend opens.
- HashDB provides explicit durability calls (`PutSync`, `DeleteSync`) backed by the slab value log and crash recovery; non-sync writes may be lost on power loss.

## Who Is This For?

- Anyone who needs to understand what survives a crash / power loss.
- Anyone building a replication/consensus layer where “committed” must be durable.

## TreeDB

TreeDB has two modes behind one API:

- **Cached mode (default)**: a write-back layer (`memtable + WAL + background flush → backend`).
- **Backend-only mode**: the base B+Tree engine without the cached write-back layer.

### Backend-only mode (`opts.Mode = treedb.ModeBackend`)

- `Set`: not guaranteed durable (no fsync).
- `Batch.Write`: not guaranteed durable (no fsync).
- `SetSync`: durable (fsync of slab + index at commit).
- `Batch.WriteSync`: durable (fsync of slab + index at commit).

Crash recovery:
- On open, TreeDB validates redundant meta pages and truncates the active slab tail to avoid torn writes.

### Cached mode (default)

Cached mode writes to a WAL first, then eventually flushes to the backend.

- `Set`: appends to WAL but does not `fsync` it; not guaranteed durable on power loss.
- `Batch.Write`: appends to WAL but does not `fsync` it; not guaranteed durable on power loss.
- `SetSync`: appends to WAL and `fsync`s it; durable.
- `Batch.WriteSync`: appends to WAL and `fsync`s it; durable.

Crash recovery:
- On open (cached or backend), any WAL segments in `Dir/wal/` are replayed into the backend with synced commits, then removed.
- This makes recovery coherent: reopening as cached vs backend yields the same recovered state.

### Operational notes

- Always `Close()` iterators and DB handles to release pinned resources and directory locks.
- Multiple processes must not open the same directory concurrently (exclusive lock).

## HashDB

HashDB is designed as a high-performance mmap-backed hashmap engine with a slab value log.

Durability contract:
- `Put` / `Delete` are not guaranteed durable (no fsync); writes go through the OS page cache and may be lost on power loss.
- `PutSync` / `DeleteSync` fsync the slab value log (and the DB directory on slab segment rotation where supported) so the operation survives a crash/power loss.
- The mmap index (`hashctl-*`, `hashkeys-*`) is treated as a derived cache; on open after an unclean shutdown HashDB rebuilds the index by scanning the slab log and truncates torn tail records.

Notes:
- HashDB’s sharded `Open/OpenWithShards` entrypoint uses a write-back cache without a WAL; pending cache writes are volatile until flushed. `PutSync`/`DeleteSync` flush the cache and then perform a durable backend write.
- If you need fully integrated WAL-based durability with stronger corruption detection, use TreeDB `*Sync` operations today.
