# Collections Write-Domain Contract

Status:

- Current implementation: flush-boundary durable for pending collection-local
  write-domain state.
- PR1 collection WAL target: WAL-on collection mutator success is process-crash
  recoverable before visibility; WAL-off remains flush-boundary.

This document specifies collection-local write-domain behavior for indexed
collections. It distinguishes the current shipped contract from the PR1
collection WAL target contract.

## Indexed Write Memtables

Indexed collections use collection-local write memtables by default.

The write domain stages the ordered runs for one collection together:

- the primary document root,
- the index-state/template root when the document format uses one,
- each affected secondary index root,
- pending unique-index lookup helpers.

The `DisableIndexedWriteMemtables` option is a debugging and benchmark baseline
escape hatch. It is not the production-mainline path.

## Visibility

Pending indexed writes are visible through the collection manager that owns the
write domain.

Reads and checks MUST merge these layers with the following newest-to-oldest
precedence:

1. current mutable indexed runs,
2. queued immutable indexed flush units,
3. in-flight async publishing units,
4. persisted backend roots from the current collection catalog.

This applies to:

- primary point reads,
- secondary index lookups,
- unique-index conflict checks,
- update/delete planning that reads buffered state.

Tombstones in pending runs MUST suppress older values from persisted roots or
older pending runs.

## Flush Units

Threshold-triggered indexed writes rotate mutable state into immutable flush
units. A flush unit owns the root-local tables, root policies, base root ids,
unique-value helper tables, and accounting for the staged documents.

Flush units are a visibility and publish-amortization mechanism. They are not a
separate durable log.

## Async Indexed Flush

`BufferedIndexedAsyncFlush` allows threshold-triggered indexed flush units to be
published by a background worker.

The async worker may move a queued immutable unit into the publishing state
before root publication completes. Publishing units remain visible to reads,
unique checks, schema-change barriers, and explicit flush barriers.

`BufferedIndexedAsyncFlushMaxQueuedUnits` bounds queued immutable flush units.
When the queue is full and a publish is already in flight, writers MUST apply
backpressure by waiting for the in-flight publish to complete before draining or
rescheduling more queued work. The queue limit must not be bypassed by repeatedly
rotating new immutable units while the background publisher is busy.

## Durability Boundary

The current async indexed write-domain contract is flush-boundary durable.

An acknowledged collection write that is still only in mutable, queued, or
publishing write-domain state is visible in-process through the owning manager,
but callers MUST NOT treat that acknowledgement as a crash-durable boundary.

Durability is established when one of these barriers returns successfully:

- `Collection.Flush`,
- `CollectionManager.FlushAll`,
- backend `DB.Close` invoking the collection manager close hook,
- a threshold-triggered synchronous publish path.

A background async publish may complete before an explicit flush, but that is an
implementation outcome, not a durable-at-ack API guarantee.

If TreeDB later needs durable-at-ack async collection writes, it MUST add the
collection WAL root-delta recovery mechanism before advertising that stronger
contract.

In WAL-on modes, write-domain mutable/queued/publishing state is a visibility
and publication-amortization layer over already committed collection WAL
transactions. It is not the first durable record. No read, unique check,
update/delete planner, schema/index barrier, queued unit, publishing unit, or
pending-state merge may observe a mutation until its collection WAL transaction
is committed and recoverable.

In WAL-off relaxed mode, write-domain state is not backed by collection WAL.
Acknowledged pending writes are process-local until published. `Flush`,
`FlushAll`, `Checkpoint`, and `Close` are the public persistence boundaries.

During private planning, root deltas, side refs, uniqueness reservations,
publish inputs, and schema/index barrier state are not reachable from any read,
scan, uniqueness check, update/delete planner, queued unit, publishing unit, or
pending-state merge. After side refs are prepared and protected, the writer
appends the collection WAL commit marker. Only then may it make
mutable/queued/publishing state visible to reads and unique-index helpers. Async
flush remains a publication optimization over already-logged transactions; it is
not the first durable record for those writes.

In WAL-on modes, visibility implies recoverability. If collection WAL commit
fails, the mutation must leave no read-visible pending state and no uniqueness
reservation. A concurrent reader or planner must never observe a write whose WAL
transaction is not committed/recoverable.

## Barrier Semantics

Operations that require persisted roots as their planning input MUST first drain
the collection write domain.

This includes schema/index changes and other operations that take a fresh
snapshot after calling the flush barrier. Those barriers MUST wait for in-flight
async publishing units; silently skipping publishing units can make a new index
backfill miss documents that were already acknowledged in the write domain.

Under the collection WAL plan, schema/index changes must also respect
collection-local WAL progress. They either publish and watermark all lower
collection WAL sequences before becoming visible, or are encoded as their own
collection WAL transaction that depends on the previous sequence and carries the
schema/root descriptor changes atomically.

`CreateIndex`, `DropIndex`, `DropIndexes`, `DropAllIndexes`, collection
creation, and future schema mutations are public barriers. They must state
whether they drain lower collection sequences, become their own WAL transaction,
or fail before exposing schema changes.

## Read Views

The current implementation often protects pending state by copying point values
under domain locks or by holding write-domain read locks for scans. The
collection WAL plan makes this a named retention contract:
`CollectionReadView`.

A collection read view pins the backend snapshot, collection catalog/root
descriptor view, immutable or refcounted pending mutable/queued/publishing
units, derived secondary-index views, and side refs reachable from those units.
Flush, async publish completion, rollback, overlay compaction, collection WAL
cleanup, value-log GC/rewrite, leaf-log GC, and future column-file cleanup must
not reset, reuse, delete, rewrite, or unprotect objects reachable from a live
read view. A single point read can avoid a read view only when it copies the
result and retains no pending-state or side-ref handles after dropping locks.

## Close And Reopen

Backend close runs the collection manager close hook while writes are still
available. The hook MUST drain pending collection write-domain state before the
backend closes.

After successful close and reopen, collection primary and secondary indexes MUST
reflect all writes that were visible before close returned.

Under the collection WAL plan, close also establishes an admission cut. After
that cut, every collection mutator either fails before visible install or is
included in the close drain. No write may return success during a close race and
then be absent after reopen.
