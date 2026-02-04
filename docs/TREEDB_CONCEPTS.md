# TreeDB Concepts

TreeDB is a persistent ordered key/value store built around a B+Tree. The public API is
`github.com/snissn/gomap/TreeDB` (package `treedb`).

TreeDB has one engine with WAL on/off semantics:

- **WAL on (default)**: write-back layer (`memtable + journal + value log + background flush`).
- **WAL off**: journal disabled (unsafe), value log still enabled.

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

## Storage Engine (B+Tree)

At a conceptual level, the backend engine stores:

- A memory-mapped index file (`Dir/maindb/index.db`) containing B+Tree pages and metadata.
- A value log under `Dir/maindb/wal/` used to store larger values efficiently.
- A dictionary store under `Dir/dictdb/` used to persist compression dictionaries (when enabled).

Writes are “commit-like”: a batch updates pages + value-log pointers and then updates the active meta page.

### Files and directories

- `Dir/maindb/index.db`: the pager file (chunked mmap) containing:
  - B+Tree pages (internal + leaf nodes)
  - freelist / lifecycle metadata
  - redundant meta (“superblock”) pages used for recovery
- `Dir/maindb/wal/`: journal + value-log segments (internal naming may change).
- `Dir/maindb/LOCK`: the cross-process exclusive-open lock file.
- `Dir/dictdb/`: dictionary store DB used by value-log compression.

### Inline vs value-log values

TreeDB stores small values inline in leaf pages up to an internal threshold (currently `256` bytes).
Larger values are stored out-of-line in the value log and referenced by a pointer stored in the tree.

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

## Write-Back Layer (Cached Mode)

Cached mode is not a read cache. It is a write-back layer used to batch work:

- Incoming writes go to:
  - an in-memory memtable, and
  - a journal segment under `Dir/maindb/wal/` (for durability / crash recovery), plus
  - the value log for large values when enabled.
- A background flusher periodically writes memtables into the index using a backend batch.

Practical effects:

- Many small random writes are faster than direct index writes because they batch.
- Iterators/scans merge multiple sources (memtables + index).

## Durability (Set vs SetSync)

TreeDB exposes explicit durability choices:

- `Set` / `Batch.Write`: higher throughput, not guaranteed durable on power loss.
- `SetSync` / `Batch.WriteSync`: forces durability at commit boundaries.

Importantly, crash recovery is coherent across WAL on/off: reopening yields the same recovered state for the last checkpoint.

See: `docs/contracts/DURABILITY.md` and `docs/TREEDB_RECOVERY.md`.

## Concurrency Model

- TreeDB is designed around **single-writer / multi-reader** semantics.
- An exclusive cross-process lock is held for the lifetime of the DB handle.
- Iterators are point-in-time views and must be `Close()`'d.

See: `docs/contracts/CONCURRENCY.md` and `docs/contracts/LOCKING.md`.
