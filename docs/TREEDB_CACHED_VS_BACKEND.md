# TreeDB: Cached vs Backend

## TL;DR

- Use **cached mode** (`treedb.Open(...)`) for workloads dominated by many small random writes.
- Use **backend-only mode** (`opts.Mode = treedb.ModeBackend`) when you want the simplest engine path, or when scans are the hot path.
- For durability, use `*Sync` operations in either mode.

## Who Is This For?

- Anyone choosing between TreeDB modes for a real workload.
- Benchmark authors trying to interpret “why is this faster/slower?”

## What “Cached” Means Here

Cached TreeDB is not a read-cache. It’s a write-back layer:

`memtable + journal (+ value log for large values) + background flush → backend`

It reduces write amplification by batching/deferring expensive tree merges into larger flushes.

## Practical Guidance

### Cached mode (default)

Best when:
- You have many small random writes (e.g. “state updates” style workloads).
- You want higher throughput by batching work into flushes.

Costs:
- Additional journal I/O and value-log appends for large values.
- Scan/iterator paths may be slower than backend-only mode because they can merge multiple sources (memtable + queue + backend).

### Backend-only mode

Best when:
- You want the simplest storage stack (no journal/memtable layer).
- Scans dominate and you want to avoid merging multiple iterator sources.
- You already batch writes explicitly (use `NewBatch` and `Batch.WriteSync`).

Costs:
- Many small writes can be significantly slower due to write amplification in the tree commit path.

## Durability

- `SetSync` / `Batch.WriteSync`: durable
- `Set` / `Batch.Write`: higher throughput, not guaranteed durable on power loss

See `docs/contracts/DURABILITY.md` for details.

Related:

- `docs/TREEDB_CONCEPTS.md`
- `docs/TREEDB_RECOVERY.md`
- `docs/TREEDB_TUNING.md`
