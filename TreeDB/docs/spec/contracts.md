# TreeDB Behavioral Contracts

This document specifies externally-observable behavior expected by callers.

Status:

- Current shipped contract: sections that describe existing key/value, cached,
  read-only, and collection write-domain behavior are normative for current
  code.
- PR1 collection WAL target contract: section 7.1 defines the public semantics
  that collection WAL implementation must satisfy before WAL-on collection
  durable-at-ack can be advertised or enabled by default.

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
- Under the planned collection WAL contract, collection scans and snapshots that
  can read pending collection-local state use a `CollectionReadView`. The view
  pins backend snapshot state, pending mutable/queued/publishing collection
  units, derived index views, and reachable side refs until the read closes.

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

### 7.1 Collection Write Acknowledgement Contract

Terms:

- `visible`: observable by collection reads, scans, secondary-index lookups,
  unique-index checks, update planners, delete planners, schema/index barriers,
  or pending-state merges in the serving process.
- `recoverable`: after process crash and read-write reopen, TreeDB can make the
  write visible without relying on pre-crash memory.
- `published`: root descriptors for the collection state have been committed to
  backend roots.
- `checkpointed`: the backend durability boundary includes the published roots,
  collection WAL applied watermarks, and required side-ref reachability
  metadata.
- `fsynced`: the selected durable-mode fsync boundary has completed. Non-sync
  collection APIs do not imply fsync.

Collection data mutators:

| Method | `DurabilityDurable` and `DurabilityWALOnRelaxed` target contract | `DurabilityWALOffRelaxed` contract |
|---|---|---|
| `Collection.Insert` | Successful return means the whole mutation has a committed collection WAL transaction and is recoverable after process crash. It may still be unpublished pending state and is not necessarily checkpointed or fsynced. | Successful return means process-local visibility according to the path that executed it. Crash recovery is promised only after `Flush`, `FlushAll`, `Checkpoint`, or `Close` covers the mutation. |
| `Collection.InsertBatch` | Successful return means the whole batch is recoverable as one logical mutation boundary. | Successful return is process-local visibility until an explicit persistence boundary covers the batch. |
| `Collection.Update` / `UpdateBatch` | Successful return means the resulting primary, secondary, template/index-state, and tombstone deltas are recoverable as one mutation boundary. Update callbacks must be safe for retry before commit. | Successful return is process-local visibility until a public persistence boundary. |
| `Collection.Delete` / `DeleteBatch` | Successful return means the whole delete mutation, including secondary-index deletes and tombstones, is recoverable. | Successful return is process-local visibility until a public persistence boundary. |

Batch mutators are all-or-nothing at the durable/recovery boundary. An ordinary
pre-commit error means no item from that batch became visible or recoverable. A
post-commit failure must be reported as commit-ambiguous for the whole batch.

Collection metadata mutators:

| Method | Contract |
|---|---|
| `CollectionManager.CreateCollection` | Successful return means the collection catalog entry is published and recoverable under the selected non-sync durability mode. It is not fsynced unless followed by a sync-capable barrier. Retrying creation with identical metadata is idempotent; retrying with incompatible metadata fails without changing the existing collection. |
| `Collection.CreateIndex` and index-drop methods | These methods establish an index/schema barrier. They drain collection writes admitted before the barrier before taking the backfill/planning snapshot, or are encoded as ordered collection WAL transactions. Successful `CreateIndex` means the index definition, root descriptors, and backfilled index contents for the barrier snapshot are published atomically and recoverable. A unique-index conflict is an ordinary pre-commit error and must not expose partial schema/index state. |

Barrier methods:

| Method | Contract |
|---|---|
| `Collection.Flush` | Publishes all pending collection write-domain state for that collection admitted before the flush cut, waits for in-flight async indexed publishing for that collection, and advances the collection WAL applied watermark for the published transactions. It does not imply fsync unless composed with a sync/checkpoint boundary that requires fsync. |
| `CollectionManager.FlushAll` | Applies the `Flush` contract to every collection write domain known to the manager at the flush cut. Future backend-owned collection WAL services must use a global domain registry when the contract needs to cover all collection handles, not only one manager instance. |
| `DB.Checkpoint` | Successful return is a database-wide durability/cleanup boundary for all collection writes and collection metadata admitted before the checkpoint cut. With the current `Checkpoint() error` signature, `nil` means no pre-cut committed collection WAL debt remains unpublished or required for recovery. If publication/watermark/checkpoint coverage cannot be completed, `Checkpoint` must return an error unless a future result type explicitly exposes nonzero collection WAL debt. |
| `DB.Close` | Establishes a close admission cut. Every collection write racing with the cut either fails before visible install with `ErrClosed` or is included in the close drain. If `Close` returns `nil`, every included successful collection mutation is visible after read-write reopen. Physical cleanup may remain a safe leak, but WAL/side refs needed for recovery must not be removed. |

Public durability errors:

- `ErrDurabilityUnavailable`: a requested durability boundary cannot be
  satisfied before any logical commit.
- `CommitAmbiguousError` / `ErrCommitAmbiguous`: the mutation may already be
  committed or recoverable. Callers must not blindly retry non-idempotent
  operations.
- `ErrRecoveryRequired`: read-only open found committed collection WAL that
  requires mutating recovery before collection APIs can be served.

Retry guidance:

- TreeDB does not provide exactly-once mutation semantics without a durable
  idempotency key.
- After timeout, disconnect, or commit-ambiguous error, duplicate document ID or
  unique-index conflict can mean the prior attempt committed.
- Updates are safe to retry only when the update function is idempotent, guarded
  by a version/compare predicate, or protected by a durable idempotency key.

Mongo gateway:

- Until explicit Mongo `writeConcern` support exists, gateway write success is
  the underlying TreeDB collection API success for the configured durability
  mode. It must not imply Mongo-compatible writeConcern or fsync semantics.
- Ordered update/delete commands must document whether earlier items can remain
  applied after a later item error. If partial ordered semantics are intentional,
  responses must report the applied prefix; otherwise validation must complete
  before applying any item.
- Gateway collection auto-create and `createIndexes` inherit the collection
  metadata mutator contracts above.

## 8. Read-only Open Contract

When opened read-only:

- write operations must fail,
- no mutating recovery steps run,
- no background maintenance mutates on-disk state.
- if committed unapplied collection WAL exists, read-only open must fail with
  `ErrRecoveryRequired` unless an explicit stale read-only mode is selected.

## 9. Collections Native Fast Path

Collections runtime code uses the native ordered-root publish path as the
default execution path. The historical oracle branch is an external comparison
artifact only; it is not a runtime selector or dependency in the collections
package.

Collection root physical policy is explicit per root:

- document/data roots are the production-mainline roots and benchmark defaults
  prioritize value-log-backed outer leaves,
- collection index-state roots follow the collection data-root policy,
- secondary index roots support both pager-backed fast mode and value-log-backed
  compressed mode,
- benchmark artifacts must label the storage-policy cell being measured.

Secondary indexes are typed. Every index definition must declare one of
`string`, `bool`, `int64`, or `double`; missing or unknown value types are
schema errors. Ordered secondary keys store the typed value component followed by
the document ID, so old untyped secondary-index metadata and key layouts are
intentionally incompatible with this format.

Native collection writes must publish primary, index-state, secondary, and root
descriptor updates through grouped ordered-root publish primitives. They must not
route the steady-state runtime path through oracle selectors, detached replay,
overlay state, or other translation-only hooks.

Indexed collection writes use collection-local write memtables by default.
Pending indexed writes are visible through the owning collection manager before
they are published to persisted roots. Primary reads, secondary index lookups,
unique checks, and update/delete planning must merge write-domain state with
explicit newest-to-oldest precedence: current mutable runs, queued immutable
flush units, in-flight async publishing units, then persisted roots.

`BufferedIndexedAsyncFlush` is a throughput feature, not a durable-at-ack
mutation log. The current contract is flush-boundary durable: callers may treat
`Collection.Flush`, `CollectionManager.FlushAll`, backend `DB.Close`, or a
threshold-triggered synchronous publish as durability boundaries when those
operations return successfully. Background async publish may complete earlier,
but acknowledged writes that remain only in mutable, queued, or publishing
write-domain state must not be advertised as crash-durable.

Operations that need persisted roots as planning input, including schema/index
changes, must drain pending indexed write-domain state and wait for in-flight
async publish units before taking their planning snapshot.

Under the planned collection WAL contract, WAL-on collection visibility implies
recoverability. No read, uniqueness check, update/delete planner, or pending
merge may observe a mutation before its collection WAL transaction is
committed/recoverable.

Detailed indexed collection write-domain semantics are in
`TreeDB/docs/spec/collections-write-domain.md`.
