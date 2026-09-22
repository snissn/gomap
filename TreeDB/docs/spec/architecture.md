# TreeDB Architecture

## 1. Engine Model

TreeDB exposes one primary public engine (`treedb.Open`) with a cached write-back layer enabled by default.

The runtime consists of:

- B+Tree index in `index.db` (mmap pager, copy-on-write commits).
- Value log (`maindb/value_vlog/value-l*.log`) for out-of-line large values.
- Optional split leaf log (`maindb/leaf_vlog/value-l*.log`) for outer leaf
  generations.
- Commit log / journal (`maindb/wal/commit-l*.log`) for cached-mode replay;
  future user-command WAL frames extend this same segment family.
- Optional side stores:
  - `dictdb/` for persistent dictionary bytes,
  - `templatedb/` for template compression definitions.

TreeDB's value log is the only value storage path for values.

## 2. Component Responsibilities

### 2.1 Index (B+Tree)

- Stores keys in lexicographic order.
- Stores either:
  - inline value bytes, or
  - a fixed-size `ValuePtr` reference into the value log.
- Uses copy-on-write page rewrites and dual meta pages for commit visibility.

### 2.2 Value Log

- Append-only segment files.
- Stores large values and grouped frames.
- Is persistent storage, not a transient write-ahead buffer.

### 2.3 Commit Log (Journal/WAL)

- Redo metadata stream used by cached-mode recovery.
- Replays legacy raw `set inline`, `set rid`, and `delete` operations.
- Target user-command WAL adds typed command frames to the same journal rather
  than adding collection-specific WAL files.
- Is independent from value-log lifetime.

### 2.4 Cached Layer

- Buffers writes in mutable memtables.
- Rotates immutable memtables and flushes them into backend batches.
- Writes commit-log/value-log data as part of ingest path.
- Applies adaptive backpressure and optional background checkpointing.

### 2.5 Backend Layer

- Owns pager, freelist/lifecycle, commit sequence, and snapshots.
- Applies batches via zipper merge into new page generations.
- Maintains active value-log segment set for reads.

## 3. Directory Layout

Public `treedb.Open(opts)` treats `opts.Dir` as a root.

Default root layout:

- `<root>/maindb/`
  - `index.db`
  - `LOCK`
  - `wal/`
    - `commit-l<lane>-<seq>.log`
  - `value_vlog/`
    - `value-l<lane>-<seq>.log`
  - `leaf_vlog/`
    - `value-l<lane>-<seq>.log`
- `<root>/dictdb/`
  - `index.db`
  - `LOCK`
- `<root>/templatedb/` (only when template mode enabled)
  - `index.db`
  - `LOCK`

If `DisableSideStores=true`, the main DB is opened directly at `<root>`.

## 4. Open and Locking Model

### 4.1 Read-write open

- Backend DB acquires an exclusive lock file at `<maindb>/LOCK`.
- Side stores (if enabled) acquire their own exclusive lock files.
- Only one read-write process may open a given DB directory.

### 4.2 Read-only open

- Read-only open does not acquire write locks.
- Read-only open does not run mutating recovery steps.
- Required directories must already exist.

## 5. Snapshot and Reader Model

- Readers use snapshots (`AcquireSnapshot`) with a pinned commit sequence.
- Snapshots pin the index generation and referenced value-log set.
- In cached mode, snapshots and iterators also include buffered memtable writes by reading from immutable queued memtables (newest-first) plus a backend snapshot.
- Iterators are point-in-time views and must be closed.
- Writers are serialized; readers run concurrently.

## 6. Write Path Modes

Durability mode controls WAL/journal behavior, not whether value log exists.

- `DurabilityDurable`: WAL on, sync on sync APIs.
- `DurabilityWALOnRelaxed`: WAL on; legacy sync APIs are relaxed, while command-WAL explicit sync APIs opt up to a durable V2 prefix.
- `DurabilityWALOffRelaxed`: WAL off, value log still used for pointers.

See `TreeDB/docs/spec/write-path-and-durability.md`.

## 7. Architectural Constraints for Reimplementations

A compatible implementation should preserve:

1. Lexicographic ordered index semantics.
2. Long-lived value-pointer semantics.
3. Separation of commit log durability from value-log storage lifetime.
4. Coherent recovery order:
   - index meta selection,
   - value-log RID scan,
   - commit-log replay,
   - replay log cleanup.
5. Single-writer + multi-reader snapshot behavior.
