# Collections Write-Domain Contract

Status:

- Current implementation: flush-boundary durable for pending collection-local
  write-domain state.
- Target durable-at-ack behavior: user-command WAL frames in the shared
  commit-log WAL plus root publication and `AppliedLSN` advancement make
  accepted deterministic collection commands durable before visibility. Indexed
  write-domain durable-at-ack remains a later implementation gate; WAL-off
  remains flush-boundary.

This document specifies collection-local write-domain behavior for indexed
collections. It distinguishes the current shipped contract from the later
user-command WAL target contract.

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

Indexed schemas enable `BufferedIndexedAsyncFlush` by default, so
threshold-triggered indexed flush units are normally published by a background
worker. `DisableBufferedIndexedAsyncFlush` opts a collection back into
foreground threshold publish for debugging and baseline comparisons.

The async worker may move a queued immutable unit into the publishing state
before root publication completes. Publishing units remain visible to reads,
unique checks, schema-change barriers, and explicit flush barriers.

`BufferedIndexedAsyncFlushMaxQueuedUnits` bounds queued immutable flush units.
When the queue is full and a publish is already in flight, writers MUST apply
backpressure by waiting for the in-flight publish to complete before draining or
rescheduling more queued work. The queue limit must not be bypassed by repeatedly
rotating new immutable units while the background publisher is busy.

## Durability Boundary

The current indexed write-domain contract is flush-boundary durable regardless
of whether threshold publish is foreground or background.

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

If TreeDB later needs durable-at-ack async collection writes, it MUST add
user-command WAL support for the relevant command category before advertising
that stronger contract.

In the V1 WAL-on contract, write-domain mutable/queued/publishing state is
process-local state, not a durable pending overlay. A WAL-supported collection
command may return success or become owner-visible only after its command frame
and required external refs are recoverable and the normal executor has installed
the command in the process-visible write domain. Durable-at-ack requested for a
collection command whose command kind is not `WAL-supported` must fail before
staging.

In WAL-off relaxed mode, write-domain state is not backed by command WAL.
Acknowledged pending writes are process-local until published. `Flush`,
`FlushAll`, `Checkpoint`, and `Close` are the public persistence boundaries.

During private planning, canonical command payloads, external refs, uniqueness
reservations, publish inputs, and schema/index barrier state are not reachable
from any read, scan, uniqueness check, update/delete planner, queued unit,
publishing unit, or pending-state merge. The writer prepares and protects
required external refs, appends and syncs the typed command WAL frame through the
shared commit-log journal, and applies through the normal executor. Only after
the frame is recoverable may the mutation become visible or return success in V1
WAL-on modes. Checkpoint, flush, close, synced ack, and recovery boundaries
publish roots plus `AppliedLSN` before WAL cleanup. Durable pending overlays
that survive without replay are a future feature, not part of V1.

In WAL-on modes for enabled command WAL capabilities, visibility implies
recoverability. If command WAL append/commit fails, the mutation must leave no
read-visible pending state and no uniqueness reservation. A concurrent reader or
planner must never observe a write whose command frame is not
committed/recoverable.

## Barrier Semantics

Operations that require persisted roots as their planning input MUST first drain
the collection write domain.

This includes schema/index changes and other operations that take a fresh
snapshot after calling the flush barrier. Those barriers MUST wait for in-flight
async publishing units; silently skipping publishing units can make a new index
backfill miss documents that were already acknowledged in the write domain.

Under the user-command WAL plan, schema/index changes must also respect shared
commit-log progress. Supported catalog changes, such as PR6
`CatalogCreateCollection`, publish `AppliedLSN` covering all lower commands and
the catalog descriptor change before becoming visible. Unsupported index DDL
remains a pre-frame WAL-on rejection until its own catalog command frame carries
the schema/root descriptor changes atomically.

`CreateIndex`, `DropIndex`, `DropIndexes`, `DropAllIndexes`, collection
creation, and future schema mutations are public barriers. They must state
whether they drain lower command LSNs, become their own command WAL frame,
or fail before exposing schema changes.

Under command WAL, catalog changes that affect replay identity are durable
barriers. Until schema-change command frames are implemented, `CreateIndex`,
`DropIndex`, collection drop/recreate, rename, document-format changes,
template root descriptor resets/evolution, and future column descriptor changes
must publish `AppliedLSN` covering all lower command LSNs before becoming
visible. A later schema-change command frame must carry descriptor ops and
advance `AppliedLSN` in the same durable root commit.

Every collection has stable replay identity independent of its display name:
`CollectionUID`, `CollectionGeneration`, `CatalogEpoch`, `SchemaEpoch`,
`LogicalCatalogDigest`, and `LocalReplayCatalogDigest`. `CollectionName` is
lookup/display text only. Every index has `IndexUID`, `IndexGeneration`, and a
definition digest; unique and multikey helper state is keyed by `IndexUID`, not
index name. Every root has a stable `RootUID`, `RootKind`, generation,
descriptor epoch, and descriptor digest. Pending write-domain units and future
WAL-backed flush units must carry these guards so a drop/recreate or
same-name index recreate cannot replay into the wrong catalog incarnation.

## Read Views

The current implementation often protects pending state by copying point values
under domain locks or by holding write-domain read locks for scans. The
command WAL plan makes this a named retention contract:
`CollectionReadView`.

A collection read view pins the backend snapshot, collection catalog/root
descriptor view, immutable or refcounted pending mutable/queued/publishing
units, derived secondary-index views, and external refs reachable from those
units. Flush, async publish completion, rollback, overlay compaction, command
WAL cleanup, value-log GC/rewrite, leaf-log GC, and future column-file cleanup must
not reset, reuse, delete, rewrite, or unprotect objects reachable from a live
read view. A single point read can avoid a read view only when it copies the
result and retains no pending-state or external-ref handles after dropping
locks.

## Close And Reopen

Backend close runs the collection manager close hook while writes are still
available. The hook MUST drain pending collection write-domain state before the
backend closes.

After successful close and reopen, collection primary and secondary indexes MUST
reflect all writes that were visible before close returned.

Under the command WAL plan, close also establishes an admission cut. After
that cut, every collection mutator either fails before visible install or is
included in the close drain. No write may return success during a close race and
then be absent after reopen.
