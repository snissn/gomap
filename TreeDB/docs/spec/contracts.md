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
- `Update`, `UpdateSync` are single-key read-modify-write helpers. The callback
  receives the current value as a safe copy, or `nil` when the key is absent, and
  returns `Set`, `Delete`, or `Noop` intent through `UpdateResult`.
- Concurrent `Update`/`UpdateSync` calls for the same key on the same cached or
  backend `DB` handle are serialized around the read-modify-write sequence. This
  prevents lost updates for logical single-key mutations such as set-membership
  updates when competing writers use the update primitive.
- Point `Set`/`SetSync`/`Delete`/`DeleteSync` calls participate in the same
  single-key serialization on the same handle, but they remain unconditional
  writes. Batch writes and multi-key atomicity remain outside this contract.

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
