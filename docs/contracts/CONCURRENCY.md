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

## HashDB

### Sharded (recommended)

- `*hashdb.HashDB` is sharded and intended to be safe for concurrent use.
- Cross-shard operations (e.g. `GetMany`) are implemented by grouping work per shard to reduce lock churn.
- `ForEach` takes an exclusive snapshot of the store (blocks writers) so iteration sees a stable view.

### Single-shard

- `*hashdb.DB` (opened by `hashdb.OpenSingle`) is not goroutine-safe.
- Use it only when single-threaded access is guaranteed.
