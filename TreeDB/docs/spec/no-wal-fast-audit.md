# Authoritative no-WAL fast audit (#4980)

`no_wal_fast` is a production profile for authoritative persistent data. Ordinary
successful writes are volatile acknowledgements. Recovery may lose recent
volatile writes after the last covered explicit boundary; it must not recover torn batches, mixed roots, or roots with missing
persistent dependencies. Independently buffered collection domains do not
provide a global ordinary-ACK-order prefix: an autonomous seal may retain a
later published root while an earlier local collection write is still volatile.
An explicit registered boundary covers all previously completed local writes.
Explicit sync operations, `Checkpoint`, and successful clean `Close` remain durable.
The value log is persistent storage, including when the command WAL is disabled.

## Boundaries and callers

| Operation / profile | ACK and kernel boundary | Stable-media boundary / recovery |
| --- | --- | --- |
| `no_wal_fast` ordinary public KV `Set`, `Delete`, batch `Write` | Cached memtable visibility or direct backend root visibility. Persistent value/outer-leaf append and userspace flush can happen before ACK; neither kernel acceptance nor visibility implies durability. | No durable ACK promise. A later explicit sync/checkpoint/close covers all previously completed registered writes. |
| Collection insert/update batch under `no_wal_fast` | Local write-domain tables, overlays, queued or publishing indexed units can acknowledge before backend root publication. | Registered managers drain before a database checkpoint or a public raw backend synchronous batch; earlier acknowledged collection operations therefore cannot be omitted by a later successful KV sync. |
| Public cached `SetSync`, `DeleteSync`, batch `WriteSync` | Complete the mutation, then `syncBarrierAfterWrite` invokes cached checkpoint in WAL-free mode. | Flush the cache, drain registered collection managers, then seal the captured backend root. |
| Raw backend `SetSync`, `DeleteSync`, `UpdateSync`, batch `WriteSync`, conditional `CommitSync` | Point/combiner writes converge on the public batch entry. Synchronous nonphysical WAL-free batches drain registered manager barriers before acquiring publication leases. Empty synchronous batches use checkpoint. | The resulting root and its persistent dependencies are sealed before successful return. WAL-free no-op `UpdateSync` and empty/read-only `CommitSync` also checkpoint. Conditional transactions validate their read basis after the drain and retain their existing conflict behavior. |
| `Checkpoint` | Drain pending registered collection managers outside teardown/write/publication locks, rerun all barriers, then capture backend state. | Persistent dependencies and namespace, index, seal/alternate meta, then meta sync. An error propagates; no clean result may hide a failed local drain. |
| Clean `Close` | Manager close hooks stop admission/combiners and flush local state while backend writes remain available; cached close drains remaining memtables. | Seal and sync backend state before resource owners close. Failed Close is not a durable acknowledgement. |
| `command_wal_relaxed` ordinary writes | Command frames/dependencies may remain userspace buffered. Deferred root publication does not grant durable ACK. | Explicit synchronous WAL barrier closes a dependency-complete command prefix; checkpoint additionally publishes durable roots and cleanup metadata. |
| `command_wal_durable` ordinary writes | Durable command frame plus dependency closure precede acknowledgement, with normal executor apply for visibility. | Recovery replays the durable command prefix; later checkpoint may reclaim only root-covered command ranges. |
| `bench_unsafe` | Explicit benchmark-only ceiling; may skip read CRC and use writable-current-segment mmap. | Not a production authoritative-data profile. Its benchmark numbers are not production-fast measurements. |

Source paths:

- [Public methods and checkpoint](../../public.go): `SetSync`, `DeleteSync`,
  public batch wrappers and `Checkpoint`; [cached path](../../caching/db.go):
  `syncBarrierAfterWrite`, `Batch.write`, `checkpointContext`, `Close`.
- [Raw point/combiner convergence](../../db/commit_combiner.go),
  [batch and conditional preflight](../../db/batch.go),
  [registered barriers and unlocked handoff](../../db/command_wal_barrier.go),
  [checkpoint loop](../../db/db.go): `checkpointTeardownPinned` captures only
  after every barrier has rerun following its unlocked drain.
- [Collection buffering and close](../../collections/api.go):
  `canBufferNoIndexInsertBatchAck`, `canBufferDirectUpdateAck`, `FlushAll`,
  `closeForBackend`, `SyncForStandaloneWriteConcern`;
  [manager barrier](../../collections/command_wal_pending.go):
  `hasPendingCheckpointWrites` observes local tables, indexed units/runs,
  asynchronous publication, and registered dirty native vector indexes.

Internal physical batches and ordered-root publications deliberately do not
enter the public raw synchronous drain. `FlushAll` uses those paths, which
prevents recursion while publishing the drained collection state. WAL-free hooks
and their handoffs run outside teardown entirely; command-WAL hooks preserve their existing admission/raw locking and release teardown on
PendingDrain before acquiring collection mutation/vector admission or waiting
for async work; it does not extend command-WAL admission policy.
Concurrent operations still use their existing admission and publication cuts;
a boundary covers operations acknowledged before it begins, not writes admitted
later during the drain.

## Persistent closure and retention

[The durable storage transaction](../../db/durable_root_runtime.go)
(`executeDurableRootStorageTransactionV1`) flushes and syncs all captured resource
frontiers and namespace dependencies before index sync and alternate-meta
publication. A failure before meta publication is retryable; an ambiguous
post-meta failure poisons publication until reopen. Production profile resolution
in [profiles.go](../../profiles.go) keeps CRC verification enabled and
`CurrentWritableMmap=false`.

[Recovery](../../db/durable_root_recovery.go) validates each meta slot against
its complete manifest and namespace, then selects a complete valid generation.
It never combines newer metadata with older dependencies. The
[recoverable root set](../../db/recoverable_root_set.go) includes both sealed
meta generations, current/pending ambiguous roots where required, and resource
pins. [Value-log GC](../../db/vlog_gc.go) unions those roots with live scans and
revalidates before destructive deletion. [Rewrite](../../db/vlog_rewrite.go)
publishes new pointer assets through the same root machinery; old segment
reclamation still uses the recoverable set. A newer visible deletion or rewrite
is not authority to delete the older recoverable generation's persistent assets.

## Confirmed gap and regression evidence

Before this change, `RegisterCommandWALRawPublishBarrier`, its runner, and the
collection callback returned early with the command WAL disabled. An
acknowledged `InsertBatchValidatedBSON` remained buffered after successful
backend `Checkpoint` (`domain.count=1`). A later raw synchronous KV batch also
could seal its own root without those earlier collection writes. The fix uses
the existing `PendingDrain` retry mechanism at checkpoint and public synchronous
batch entry; it adds no new hook registry and changes no ordinary publication.

[Boundary regression](../../collections/no_wal_checkpoint_test.go) uses the
existing power-loss oracle's stable image and identity overrides, multiple
managers, indexed and unindexed collections, point/batch/conditional/empty sync,
and clean close. It takes the image before cleanup can repair missing state.
The async regression holds an indexed publisher open and proves checkpoint
waits. [Vector regression](../../collections/vector_index_persist_test.go) proves
checkpoint clears dirty native graph state before Close and reopens it.
[Drain failure regression](../../db/no_wal_sync_barrier_test.go) proves the drain
runs outside teardown/write locks, propagates failure, and does not publish the
later KV mutation. Existing command-WAL lease/unregister tests cover the shared
registry semantics.

A plain file-copy diagnostic was discarded as a durability witness: stable
manifests bind resource identities, and copying outer-leaf files changes those
identities. The oracle materializer's identity scope is required. It remains a
diagnostic, not evidence of a production recovery defect.

Relevant existing checks include `TestProfileFast_*`, forced-pointer production
profile tests, `TestReopenVerify_WALOff_NoJournal`, value-log GC/rewrite reopen,
production outer-leaf append-cut oracle tests, missing-closure/chunked-sync
counterexamples, `TestRecoverableRootSet*`, and the GC tests retaining older
recoverable roots and pending appenders. Exact certification replay commands
must supply the existing cut/variant/seed selectors; running selector-gated
certification tests without them merely skips the witnesses.

## Hot path, allocation audit, and qualification limits

This change removes no ordinary ACK work and claims no throughput improvement.
The existing WAL-free deferred-value append guards in cached point/batch writes
are conditional on `!memtableValueLogPointers`; the current constructor enables
memtable value pointers, so those guards do not establish an active production
coalescing optimization. Changing that setting requires separate profiling and
broad pointer/reopen/GC qualification.

The new manager inspection and drain execute only at checkpoint/public sync
boundaries. They use existing domain locks and pending checks; native-vector
inspection can allocate an index snapshot, the existing barrier runner snapshots
hooks, and `FlushAll` allocates slices of domains/handles. Their cost scales with registered managers/domains at the
explicit boundary. Ordinary insert/update buffering and raw ordinary writes
receive no new slice allocation or scan. Moving benchmark `fast` from unsafe
CRC-skipping to verified `no_wal_fast` may add read CRC cost and removes writable
mmap behavior; any comparison must record that resolved profile change.

The next qualification packet (#4982) should separately measure ordinary ACK,
sync ACK, checkpoint tails, allocations, negative lookups, mixed reads/writes,
reopen correctness, and physical storage growth. Use the existing collection
shape benchmark with `TREEDB_COLLECTION_BENCH_ENGINE=no_wal_fast` and
`-benchmem`, plus an explicitly labelled `bench_unsafe` ceiling. Large runner
campaigns and performance claims require frozen landed-source evidence.

The unified benchmark prints/stores the resolved TreeDB profile, durability and
integrity. An explicit checksum-skipping override resolves to `bench_unsafe`,
even if the requested preset was `fast`; consumers must use the resolved mode.
The legacy benchmark `unsafe` alias also selects the explicit ceiling.
RocksDB's [adapter](../../../cmd/unified_bench/adapter_rocksdb.go) aliases
`CommitSync` to `Commit`, using configured write options; LMDB's
[adapter](../../../cmd/unified_bench/adapter_lmdb.go) commits the same transaction
under configured environment flags for both methods. Their `fast` native no-sync
flags do not imply equivalence to TreeDB's explicit synchronous boundary. In
particular, the [LMDB 0.9.19 header](https://raw.githubusercontent.com/LMDB/lmdb/LMDB_0.9.19/libraries/liblmdb/lmdb.h)
warns that `MDB_NOSYNC` can permit corruption on system crash unless the
filesystem preserves write ordering and `MDB_WRITEMAP` is absent. Native
durable configurations are the primary comparison; native fast settings are
separately disclosed sensitivities, not equivalent authoritative-data profiles.

External #1242 / PR #4901 and PR #4933 remain outside this issue's ownership.
Their admission/resource changes may interact with these shared boundaries,
so restacked heads require the relevant lease and recovery tests again.
