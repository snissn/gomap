# Collections Write-Domain Contract

Status: normative for the current implementation.

This document specifies the current collection-local write-domain behavior for
indexed collections. It intentionally does not promise a durable-at-ack
collection mutation log; that is a future architecture option, not the current
contract.

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

Reads and checks enumerate pending runs in active, queued, then mutable order.
Lookups and merged iterators use newest-wins shadowing, so effective precedence
is:

1. current mutable indexed runs,
2. queued immutable indexed flush units, in FIFO order,
3. the active in-flight async publishing batch, in original FIFO unit order,
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

The async worker may move queued immutable units into one active coalesced flush
batch before root publication completes. The batch preserves the original FIFO
unit boundaries and uses a mechanical merged view only for ordered-root publish.
Active publishing units remain visible to reads, unique checks,
schema-change barriers, and explicit flush barriers.

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

If TreeDB later needs durable-at-ack async collection writes, it MUST add a
replayable collection mutation log or equivalent recovery mechanism before
advertising that stronger contract.

## Barrier Semantics

Operations that require persisted roots as their planning input MUST first drain
the collection write domain.

This includes schema/index changes and other operations that take a fresh
snapshot after calling the flush barrier. Those barriers MUST wait for in-flight
async publishing units; silently skipping publishing units can make a new index
backfill miss documents that were already acknowledged in the write domain.

## Close And Reopen

Backend close runs the collection manager close hook while writes are still
available. The hook MUST drain pending collection write-domain state before the
backend closes.

After successful close and reopen, collection primary and secondary indexes MUST
reflect all writes that were visible before close returned.
