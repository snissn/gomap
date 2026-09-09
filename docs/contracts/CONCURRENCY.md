# Concurrency

## TL;DR

- TreeDB is designed around **single-writer / multi-reader** semantics.
- HashDB’s primary entrypoint (`*hashdb.HashDB`) is sharded and goroutine-safe; the single-shard `*hashdb.DB` is not.
- Iterators represent a point-in-time view and must be `Close()`d to release resources.

## Who Is This For?

- Engineers writing concurrent services using these DBs.
- Anyone implementing replication/consensus where concurrent reads/writes are common.

## TreeDB

### Writer model

- Writes are effectively single-writer: concurrent writers are serialized.
- Reads can proceed concurrently with writes.

### Iterators and snapshots

- A TreeDB iterator is a point-in-time view of the DB as of iterator creation.
- The iterator must be closed to release pinned resources.

### Online index vacuum

Online index vacuum preserves admitted typed graph authority by replacing
immutable publication coordinates through the certified physical root mapping.
Held read views retain their old generation. At cutover, a busy typed-publication
or snapshot gate defers vacuum without cancelling accepted writes. These gates
are not held during the copy phase. Collections without typed publication keep
their existing schema-backfill retry after pager replacement. Stale logical
authority remains a snapshot mismatch rather than being repaired by relocation.
Concurrent root publication may grow the live pager during vacuum; materializing
a prepared root therefore ensures its high-water mark without shrinking a
newer allocation, then validates that the physical tail remains allocator-owned.

## HashDB

### Sharded (recommended)

- `*hashdb.HashDB` is sharded and intended to be safe for concurrent use.
- Cross-shard operations (e.g. `GetMany`) are implemented by grouping work per shard to reduce lock churn.
- `ForEach` takes an exclusive snapshot of the store (blocks writers) so iteration sees a stable view.

### Single-shard

- `*hashdb.DB` (opened by `hashdb.OpenSingle`) is not goroutine-safe.
- Use it only when single-threaded access is guaranteed.
