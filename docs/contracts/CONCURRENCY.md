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

### Captured command-WAL epochs

The public coordinator fixes an exact complete-frame prefix and retains its
active file and original dependency owner before releasing append authority.
Its physical file sync permits successor appends and complete COW cut installs;
successors require their own later epoch before durable ACK. A waiting caller or
the bounded logical-MVCC completion worker elects the next leader after the
earlier capture and all retained pins finish. Submit installs each aggregate
before returning; Wait only observes the DB-owned final ACK. Close stops queue
admission and drains accepted work even when its futures are abandoned. No
second accepted-prefix or WAL owner is introduced. Earlier V2
external dependency and rotation synchronization remains serialized before a
durable marker can be emitted.

### Iterators and snapshots

- A TreeDB iterator is a point-in-time view of the DB as of iterator creation.
- The iterator must be closed to release pinned resources.

### Value-log manager shutdown

A value-log manager closes retry admission under its manager lock, cancels
owned deletion backoff, and joins every admitted worker outside that lock before
closing tracked files. Concurrent and repeated closes wait for the same result.
Shutdown never overrides a stable resource pin. Leaf-manifest revision GC uses
existing snapshot admission and held-generation pins; it does not add a
foreground snapshot resource-acquisition route.

### Immutable memtable foundation

The internal immutable memtable foundation uses writer-private preparation and
separate immutable read headers with owned strings. Retained views/cursors keep
their source generation and concrete resource owners alive until Close; budget
Close denies new admissions, including root Retain and external allocation
leases, while existing views remain valid. External leases reserve caller
storage before allocation and close idempotently after owned cleanup.
Both default and `treedb_safe` builds avoid key-conversion allocation during
estimation/refusal and lookup/seek; safe rank search costs O(log N * height).
Release callbacks
are deferred outside publication/admission locks. The foundation itself does
not select a DB mode. See
[the precise ownership contract](../../TreeDB/docs/spec/cow-memtable-ownership.md).

### Explicit immutable COW cache

Explicit `MemtableMode="cow_btree"` integrates one complete immutable cut of
fixed shard roots, bounded frozen sources and a backend snapshot basis. Capture
pins that cut under `cutMu`; tree traversal, pointer decoding and copying run
after release. A command sequencer covers all changed-root preparation, canonical
metadata/dependency finalization and one cut install. It releases the command-WAL
barrier before `cutMu`; install performs no allocation, IO or callback.

Checkpoint uses `flushMu` then exclusive write admission for a brief frontier
cut, releasing writer/admission ownership before durability IO/backend publish.
Handoff obtains backend state without cache writer locks, then takes writer
ownership and `cutMu` only to swap the complete successor. Maintenance callback
and basis refresh share the flush fence. Cleanup runs outside writer/admission/
publication locks; it may perform existing physical-owner release work.
During frontier capture, an already-assigned root-neutral WAL barrier may be
joined through the accepted FIFO condition while writer exclusion remains held.
Its installer needs neither cache writer lock; the join rechecks the original
poison and Close state and cannot include a later root-changing publication.

Snapshot/iterator owners preserve exact old reads while the DB remains open.
DB Close refuses new reads and drains admitted reads before teardown. Finite
capacity can refuse writes while old cuts remain pinned; it cannot revoke them.
`GetManyView` callbacks run outside read/writer locks and can reenter supported
operations. The resolved public atomic MVCC read-cut capability allows Store
groups to share floor admission through Write/WriteSync ACK and cleanup. Reads
validate the floor and pin one physical cut under that admission, then seek,
construct iterators and decode after releasing it. Floor advancement excludes
active commits but need not wait for materialization on already-pinned cuts.
Legacy successors retain their publication/prune fences. COW pruning refuses
before floor/WAL effects; bounded maintenance and final C3 qualification remain
separate requirements. See
[COW cached publication](../../TreeDB/docs/spec/cow-cache-publication.md).

### Typed graph reads

An ordinary typed graph read may use the previous coherent generation while an
immediate write is in progress. Admission requires the snapshot catalog and
immutable publication to match; a changed publication or an outstanding
publication-installation gap permits retry. Acknowledged buffered writes retain
their visibility drain. Schema and storage maintenance remain exclusive, and
caller-held views retain their generation pins.

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

An ordinary TreeDB Snapshot that retains a PRIMARY root also retains the existing
physical PRIMARY owner through its original last-read finalizer. DB.Close may end
public operation admission while that independent cleanup edge remains live.
Failed original cleanup preserves the exact Snapshot at that owner; private
one-shot reads may be reused only after successful cleanup. This custody rule
does not add a public Snapshot read-after-DB.Close guarantee.

Qualified public TreeDB COW transactions are goroutine-confined handles. Their
body reads one retained opening view plus staged writes; commit validates R∪W
against the bounded engine mutation history and applies to latest roots.
`ErrTxnTooOld` is a retryable transaction-history expiration, distinct from a
changed dependency. Open transaction iterators reject further mutation with
`ErrTxnIteratorOpen`; commit/close invalidates children. See
`TreeDB/docs/spec/raw-cow-transactions.md` for the supported ownership fence.

### Backend-root and native constructor lifetimes

`OpenBackendWithCachedLeafLog` and its stats/maintenance variants return backend
publication authority plus original cached persistent-log services. Requested
physical/profile policy is preserved; requested `cow_btree` uses actual `btree`
cache services and does not bind a logical COW owner. Stats report the actual
cache mode. Keep the returned cleanup callback and invoke it to close services
and side stores in their original order. Native semantic activation refuses on
this backend-only lifecycle.

For restricted native semantics, use ordinary `Open` with `cow_btree`, then
`NativeSemanticBackend()`. The method returns a backend only after existing
activation succeeds, with all first-activation quiescence checks intact. Its
handle borrows the public DB lifetime: close the public DB rather than closing
the borrowed backend. Raw cache writes and native publications share that same
owner; ordinary direct backend publication aliases remain fenced. There is no
lazy attachment or migration between these constructor lifecycles.

Native standalone scalar JSON index DDL validates the entire captured primary
range and writes the collection's existing catalog-name conflict effect before
canonical assignment. Native data transactions retain that catalog dependency
for every bound collection, including read-only inputs to a writing transaction;
schema ABA therefore invalidates a stale plan. Held read views keep the original
immutable schema and index roots. Pure drops preserve surviving name-keyed
index domains without scanning documents; they retain both conflict barriers.
Unrelated physical checkpoint progress does not itself constitute a logical
schema conflict.
