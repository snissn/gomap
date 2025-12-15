# Durability

## TL;DR

- TreeDB provides explicit durability calls (`SetSync`, `Batch.WriteSync`) and coherent crash recovery across cached vs backend opens.
- HashDB is optimized for performance and does not currently provide an explicit durability API contract (no “fsync on commit” operation).

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

Current durability contract:
- HashDB does not yet provide an explicit “durable commit” API (no `PutSync`/`Sync` semantics).
- Writes go through the OS page cache and may be lost on power loss.
- Recovery is slab-log-based, but it should be treated as “best effort” until a stricter durability mode is implemented.

If you need strict durability (e.g. for a replicated commit path), use TreeDB `*Sync` operations today.
