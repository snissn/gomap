# TreeDB Concepts

TreeDB is a persistent ordered key/value store built around a B+Tree. The public API is
`github.com/snissn/gomap/TreeDB` (package `treedb`).

TreeDB has two modes behind one public handle:

- **Cached mode (default)**: `treedb.Open(...)` wraps the backend with a write-back layer (`memtable + journal + value log + background flush → backend`).
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
- One or more backend slab files under `Dir/` used to store larger values efficiently.

Backend writes are “commit-like”: a batch updates pages + slabs and then updates the active meta page.

### Files and directories

- `Dir/index.db`: the pager file (chunked mmap) containing:
  - B+Tree pages (internal + leaf nodes)
  - freelist / lifecycle metadata
  - redundant meta (“superblock”) pages used for recovery
- `Dir/data-*.slab`: append-only backend slab segments used for storing larger values efficiently.
- `Dir/wal/`: cached-mode logs (journal + value log):
  - `commit-*.log` (journal / redo log)
  - `value-*.log` (value log for large values, when enabled)
- `Dir/LOCK`: the cross-process exclusive-open lock file.

### Inline vs slab values

TreeDB stores small values inline in leaf pages up to an internal threshold (currently `256` bytes).
Larger values are stored out-of-line in the slab log and referenced by a pointer stored in the tree.

### Copy-on-write and the “zipper” merge

The backend commit path applies a batch via a copy-on-write merge:

- It walks the tree from the root down, allocating new pages along the modified paths.
- Unchanged pages are reused by existing readers/snapshots.
- When a node overflows, it is split and the split key is promoted upward.

In code this is implemented by `TreeDB/zipper`, which applies the batch and returns:
the new root page ID plus the set of pages eligible for retirement once readers are done.

### Meta pages and crash recovery

TreeDB maintains redundant meta pages (a “superblock” style design). On open it:

- reads/validates both meta pages (checksums),
- chooses the newest valid commit sequence,
- and truncates torn tail state where required.

See: `docs/TREEDB_RECOVERY.md`.

## Cached Mode (Write-Back Layer)

Cached mode is not a read cache. It is a write-back layer used to batch work:

- Incoming writes go to:
  - an in-memory memtable, and
  - a journal segment under `Dir/wal/` (for durability / crash recovery), plus
  - the value log for large values when enabled.
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
