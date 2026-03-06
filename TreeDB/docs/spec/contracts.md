# TreeDB Behavioral Contracts

This document specifies externally-observable behavior expected by callers.

## 1. Key Model

- Keys are byte slices.
- Key order is lexicographic (`bytes.Compare` semantics).
- The tree is ordered and range-addressable.

## 2. Read Contracts

### 2.1 `Get`

- `Get(key)` returns `(value, nil)` when key exists.
- `Get(key)` returns `(nil, nil)` when key is absent.
- Returned bytes are safe copies.

### 2.2 `GetUnsafe`

- Public `DB.GetUnsafe` currently aliases `Get` behavior (safe copy).
- Zero-copy reads are available through snapshot/iterator internals, not through this API.

### 2.3 `Has`

- `Has(key)` returns `(true, nil)` only for a visible non-deleted key.
- Deleted/missing keys return `(false, nil)`.

### 2.4 Cached-mode visibility

When the cached layer is enabled (default `treedb.Open` behavior):

- Point reads (`Get`, `GetMany`, `Has`, `GetAppend`) MUST reflect writes buffered in memtables (mutable + queued), even if they have not been flushed to the backend B+Tree yet.
- Newer memtable entries MUST shadow older backend state ("newest wins"), including tombstones.

## 3. Write Contracts

### 3.1 Point ops

- `Set`, `Delete` are non-sync writes.
- `SetSync`, `DeleteSync` request sync durability boundary subject to durability mode.

### 3.2 Batches

- `NewBatch` accumulates operations.
- `Write` commits without strict sync guarantee.
- `WriteSync` commits with sync guarantee only in durable mode.

For WAL replay, commit-log batches are treated atomically at replay boundaries.

## 4. Range and Iterator Contracts

### 4.1 Ordering

- `Iterator(start, end)` yields ascending lexicographic keys.
- `ReverseIterator(start, end)` yields descending order over the same bound domain.

### 4.2 Bounds

- Range domain is half-open: `[start, end)`.
- `nil` start means unbounded lower bound.
- `nil` end means unbounded upper bound.
- If both bounds are non-nil and `start >= end`, iterator is immediately invalid.

### 4.3 Iterator lifetime

- Iterators are point-in-time views.
- `Key()`/`Value()` data is valid until next movement/close.
- `KeyCopy`/`ValueCopy` provide stable copies.
- Iterator must be closed.

### 4.4 Cached-mode iterator semantics

When the cached layer is enabled:

- Iterators MUST include buffered memtable writes (queued + rotated mutable state) and be snapshot-isolated.
- Iterators merge multiple sorted sources: immutable memtables (newest first) + a backend snapshot.
- When the same key exists in multiple sources, the newest entry wins; tombstones suppress older versions of the key.
- `ReverseIterator` follows the same visibility rules but yields keys in descending order.

## 5. Snapshot Contracts

- Snapshots are point-in-time readers and MUST be closed to release retention pressure.
- In cached mode, snapshots MUST include buffered memtable writes and MUST be snapshot-isolated (writes after snapshot acquisition are not visible through the snapshot).
- `Snapshot.Get` / `Snapshot.GetAppend` return `ErrKeyNotFound` for missing/tombstoned keys (unlike `DB.Get`, which returns `(nil, nil)` on miss).

## 6. Concurrency and Locking

### 6.1 Process-level locking

- Read-write open acquires exclusive directory lock.
- Concurrent read-write opens on same DB directory are not allowed.

### 6.2 In-process concurrency

- Effective model is single-writer / multi-reader.
- Concurrent writers are serialized.
- Readers can run concurrently with writer via snapshot semantics.

## 7. Durability Contract Summary

Durability mode controls guarantees for sync calls:

- `DurabilityDurable`:
  - `*Sync` methods use fsync durability boundaries.
- `DurabilityWALOnRelaxed`:
  - WAL remains on, but `*Sync` is relaxed (no fsync boundary).
- `DurabilityWALOffRelaxed`:
  - WAL off, relaxed sync; durability boundary is typically checkpoint-based.

Detailed semantics are in `TreeDB/docs/spec/write-path-and-durability.md`.

## 8. Read-only Open Contract

When opened read-only:

- write operations must fail,
- no mutating recovery steps run,
- no background maintenance mutates on-disk state.

## 9. Collection Contracts (Experimental)

Collections are a higher-level document API layered on top of TreeDB keyspaces.
The current implementation stores collection schema and named-root descriptors
in the system root, primary documents in dedicated named roots, and
secondary-index entries in dedicated named roots.

### 9.1 Collection lifecycle and metadata

- `treedb.NewCollectionManager(db)` on a public `treedb.Open(...)` handle is
  the default collection entrypoint and exercises cached TreeDB semantics.
- `collections.NewCollectionManager(db)` remains the explicit backend-direct
  debugging/profiling hook when the caller is intentionally using `TreeDB/db`.
- `CreateCollection(meta)` is idempotent for the same normalized schema and
  rejects incompatible redefinitions.
- `CreateCollection(meta)` assigns deterministic primary and index-state root
  names and persists both root descriptors in the system root.
- `OpenCollection(name)` returns `errCollectionNotFound` when metadata is absent.
- `OpenCollection(name)` rejects legacy version-1 shared-keyspace metadata.
- `ListCollections()` returns collection metadata sorted by collection name.
- `DropCollection(name)` removes collection metadata, auto-id sequence state,
  root descriptors, and secondary-index entries for that collection.

### 9.2 ID modes and document identity

- Default `CollectionOptions.IDMode` is caller-provided.
- `collections.IDModeCallerProvided` requires a non-empty caller-supplied `_id`.
- `collections.IDModeAuto` allocates an 8-byte big-endian monotonically
  increasing id per collection and persists the last issued value in the system
  root.

### 9.3 Primary document writes

- Default `CollectionOptions.StorageMode` is
  `collections.CollectionStorageModeOuterLeafInValueLog`.
- `Insert(id, document)` behaves as an upsert keyed by document id.
- New-version collections store primary documents in a dedicated TreeDB root
  keyed directly by `_id` bytes, not under the legacy shared `col:d:` prefix.
- The primary root descriptor's format bits control live writes independently
  of DB-global `Options.IndexOuterLeavesInValueLog`; outer-leaf-vlog
  collections keep writing leaf pages through the value log even when the DB's
  user root is pager-backed.
- `Get(id)` returns the raw stored document bytes or `(nil, nil)` on miss.
- `Delete(id)` removes the primary document and any derived secondary-index
  entries for that document.
- When indexes are configured, collection writes also maintain a dedicated
  per-collection index-state root keyed by document id so indexed deletes do
  not need to reread and reparse the primary document.

### 9.4 Secondary indexes

- `CreateIndex(collection, def)` persists index metadata and backfills entries
  for currently visible documents.
- `CreateIndex(collection, def)` assigns a deterministic secondary root name and
  persists a root descriptor for that index in the system root.
- `CreateIndex(collection, def)` also backfills the collection's index-state
  root for all currently visible documents using the full post-create index set.
- `DropIndex(collection, name)` removes index metadata and its root descriptor;
  the detached index root becomes unreachable from collection reads.
- Secondary-index root descriptors disable value-bearing entries and outer-leaf
  value-log writes for those roots even when DB-global
  `Options.IndexOuterLeavesInValueLog` is enabled.
- `FindByIndex(indexName, value)` returns matching document ids sorted in
  lexicographic order.
- Unique indexes reject writes that would make two different document ids share
  the same encoded indexed value.
- Indexed writes update the dedicated primary root, every affected dedicated
  secondary root, the collection's dedicated index-state root, and the
  system-root root descriptors in one commit boundary, so partial visibility is
  not allowed for normal document mutations.

### 9.5 Root-format and maintenance guarantees

- Primary documents and secondary indexes both use dedicated named roots.
- Named-root descriptor format bits are enforced on live writes, online/offline
  rewrite, and online/offline vacuum, so per-root storage mode does not drift
  toward the DB-global user-root setting.
- DB-wide maintenance scans and rebuilds dedicated named roots alongside the
  user/system roots, so GC, rewrite, and vacuum preserve collection data and
  secondary-index coherence across reopen.

### 9.6 Diagnostics and maintenance visibility

- `ListIndexes()` returns a defensive copy of the current index definitions for
  an opened collection.
- `Stats()` reports metadata version, id mode, storage mode, document count,
  per-index entry counts, and user/system root page ids when the underlying DB
  exposes stats.
- `CheckConsistency()` is read-only. It recomputes expected index entries from
  visible documents and reports counts of missing and orphaned secondary-index
  entries; it does not repair them.
