# TreeDB Concepts

TreeDB is a persistent ordered key/value store built around a B+Tree. The public API is
`github.com/snissn/gomap/TreeDB` (package `treedb`).

TreeDB has two modes behind one public handle:

- **Cached mode (default)**: `treedb.Open(...)` wraps the backend with a write-back layer (`memtable + WAL + background flush → backend`).
- **Backend-only mode**: `opts.Mode = treedb.ModeBackend` (or `treedb.OpenBackend`) opens just the backend engine.

This doc is intentionally high-level; the normative behavior is captured in `docs/contracts/`.

## Key Ordering

- Keys are arbitrary byte slices.
- Ordering is lexicographic by bytes (`bytes.Compare`).
- Iterators (and range scans) use the half-open interval `[start, end)` with `nil` meaning unbounded.

See: `docs/contracts/ITERATION.md`.

## Public API Surface

The stable surface is `treedb.Open` returning `*treedb.DB`:

- Point ops: `Get`, `Has`, `Set`, `Delete` (+ `*Sync` variants).
- Iteration: `Iterator`, `ReverseIterator`.
- Batching: `NewBatch` returning a `treedb.Batch` (`Write`/`WriteSync`).
- Introspection: `Stats`, `Print`.

See: `docs/API_STABILITY.md`.

## Backend Engine (B+Tree)

At a conceptual level, the backend engine stores:

- A memory-mapped index file (`Dir/index.db`) containing B+Tree pages and metadata.
- One or more slab/value files under `Dir/` used to store larger values efficiently.

Backend writes are “commit-like”: a batch updates pages + slabs and then updates the active meta page.

## Cached Mode (Write-Back Layer)

Cached mode is not a read cache. It is a write-back layer used to batch work:

- Incoming writes go to:
  - an in-memory memtable, and
  - a WAL segment under `Dir/wal/` (for durability / crash recovery).
- A background flusher periodically writes memtables into the backend using a backend batch.

Practical effects:

- Many small random writes are often faster than backend-only mode.
- Iterators/scans can be slower than backend-only mode because they merge multiple sources (memtables + backend).

See: `docs/TREEDB_CACHED_VS_BACKEND.md`.

## Durability (Set vs SetSync)

TreeDB exposes explicit durability choices:

- `Set` / `Batch.Write`: higher throughput, not guaranteed durable on power loss.
- `SetSync` / `Batch.WriteSync`: forces durability at commit boundaries.

Importantly, crash recovery is coherent across modes: reopening as cached vs backend yields the same recovered state.

See: `docs/contracts/DURABILITY.md` and `docs/TREEDB_RECOVERY.md`.

## Concurrency Model

- TreeDB is designed around **single-writer / multi-reader** semantics.
- An exclusive cross-process lock is held for the lifetime of the DB handle.
- Iterators are point-in-time views and must be `Close()`'d.

See: `docs/contracts/CONCURRENCY.md` and `docs/contracts/LOCKING.md`.

