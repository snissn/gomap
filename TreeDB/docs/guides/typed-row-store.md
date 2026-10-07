# Native indexed typed rows

Use this route for a local collection with explicit application IDs, required
string fields, complete-row reads, and scalar indexes. This guide accompanies
the selected [R1 contract](../spec/r1-indexed-row-contract.md),
[read contract](../spec/r1-row-reads.md),
[mutation contract](../spec/r1-indexed-mutations.md), and
[lifecycle contract](../spec/r1-row-lifecycle.md). The read and mutation integration
landed in [PR #5065](https://github.com/snissn/gomap/pull/5065), and the maintenance
prerequisite landed in [PR #5071](https://github.com/snissn/gomap/pull/5071).
The [integrated evidence](../evidence/r1-row-store-5061/README.md) records accepted
complete-row read, indexed mutation, and finite lifecycle qualification on those
landed sources. TreeDB is pre-alpha; APIs and disk formats may change without
migration guarantees.

## Run the example

From the repository root with Go 1.26 or later:

```sh
GOWORK=off go run ./examples/typed_rows
```

The program creates a temporary database, prints its directory, and retains it
for inspection. To choose its location:

```sh
GOWORK=off go run ./examples/typed_rows -dir /path/to/new-empty-directory
```

A missing directory is created; an existing nonempty directory is rejected.
The example verifies complete rows and current and removed secondary postings
through insert, indexed update, mixed upsert, delete, captured reads, an ordinary
bounded range, checkpoint, and reopen. It stops on errors. See the
[source and example notes](../../../examples/typed_rows/README.md).

## Choose the layout and durable profile

The example opens `treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)` with
`treedb.OpenBackendWithCachedLeafLog`, creates a `CollectionManager`, and declares
JSON documents with retained non-column JSON. Its four declared strings,
`email`, `city`, `name`, and `bio`, have the authoritative owner
`TypedStorageOwnerRowAsset` (`typed_row_asset`). Every row supplies every string;
none can be null or missing. The email index is unique and the city index is
nonunique. The [example's collection configuration](../../../examples/typed_rows/main.go)
is the complete runnable setup.

Pass external IDs, residual JSON bytes, and one `TypedColumnBatch` per declared
column. For this layout, fill each column's `Strings` slice in the same order as
the IDs. Each slice has exactly one value per ID. Keep declared paths out of
residual JSON so each field has one authoritative owner.

Residual JSON can retain `id`, numeric `age`, `score`, and `revision`, boolean
`active`, and nullable or optional fields. Explicit `"optional":null` remains
different from omitting `optional`. The example uses a smaller application row;
the R1 benchmark fixture defines its own exact fields and bytes. A schema
declaration alone does not provide a numeric, boolean, nullable, or missing native
carrier: `TypedColumnBatch` currently carries required strings and FP32 vectors.
Choose a retained document for a more flexible schema, or typed-column storage
for an applicable scan/aggregate workload; see the
[layout quickstart](collections-quickstart.md).

## Read current rows or reuse a captured view

`Get` and `GetInto` return the complete reconstructed JSON row, including declared
strings and residual fields. `GetInto(id, dst)` may reuse `dst` capacity; keep the
returned slice for reuse and clone it before the next buffer reuse if retaining
that result. For a missing or deleted ID, `Get` returns `(nil, nil)` and
`GetInto` returns `found=false`.

`FindDocumentsByIndexRange` returns owned IDs and complete documents in ascending
index order. Supply a positive `Limit` and inspect `truncated` for more visible
results. `ScanBorrowedDocumentsByIndexRange` instead lends ID/document slices
only during the callback. Do not retain or modify them, reenter the collection,
or perform blocking work in that callback. Descending document ranges are
unsupported.

For repeated batches, open one `CollectionReadView`, call `FetchDocumentsByID`,
and close the view when finished. Opening drains pending collection writes and
captures a publication; later mutations are invisible to it. Opening and first
fetch have setup costs distinct from reuse. Use one view per worker or external
synchronization. Fetch results own their bytes after later fetches and view close;
IDs from `VisitIndexValueIDs` are borrowed until callback return and need copying
when retained. Closing releases the view's resource pins.

Select exact-value index IDs and fetch them through the same held view when an
old captured result is intended. An ordinary index query followed by a separately
opened view does not establish one shared snapshot under concurrent mutation.
Use the complete-document range API for its combined visibility. Ordinary reads
include same-manager pending writes and tombstones; a captured view remains at
its captured state.

## Mutate indexed rows

| Intent | API and result |
| --- | --- |
| Insert complete native rows | `InsertTypedBatchWithStats`: returned IDs follow input order. |
| Replace existing rows | `ReplaceTypedBatch`: per-input `Matched` and `Modified`; missing IDs are unmatched. |
| Insert missing and replace existing rows atomically | `UpsertTypedBatch`: count of existing IDs, including unchanged rows. |
| Change selected fields | `UpdateBatch`: complete-document callback and per-input `Matched`/`Modified`. |
| Delete | `DeleteBatch`: count of previously present deleted IDs. |

Native replacement and upsert supply all declared columns plus complete residual
JSON. `UpdateBatch` reconstructs a complete current row and accepts a complete
replacement or a no-op; it can change indexed strings and residual null/missing
fields. That route incurs materialization and replacement work.
`UpdateTypedMetadataByID` is restricted to `meta.*`, so use the generic callback
for the selected top-level fields.

Duplicate inputs or unique conflicts reject the batch without partial visible
primary/index changes. Unique ownership handoffs within a valid batch use its
final owners. Atomicity is collection-local. Use `ReplaceTypedSourceByID` for
atomic explicit old-ID removal and native insertion; separate delete/insert
calls are separate commands. Retained-document atomic public mixed upsert is
an unsupported comparison cell and cannot be replaced by that two-call sequence.

## Acknowledge, reconcile, and maintain

`command_wal_durable` makes a successful mutation ACK cover local crash/reopen
recovery of the complete command. It is not a replicated commit; physical
power-loss behavior also depends on the filesystem and device. `FlushAll` drains
pending publication and `Checkpoint` publishes durable roots; neither replaces
the ordinary durable ACK contract. Typed writes on this route require durable
command-WAL admission; relaxed-profile rejection is an explicit unsupported cell.

An error wrapping `collections.ErrCommitAmbiguous` can occur after the command
is recoverable. Stop using the affected handle, reopen, and reconcile every ID
and affected index against the intended whole command before deciding on another
mutation. Do not automatically retry or infer rollback. Duplicate/unique errors
after an uncertain ACK do not prove exactly-once delivery. See the
[write domain](../spec/collections-write-domain.md) and
[recovery contract](../spec/recovery.md).

Release read views promptly, checkpoint as appropriate, and use the existing
value-log GC/rewrite and typed asset GC/rewrite APIs under their
[maintenance contract](../spec/typed-asset-maintenance-1788.md). The value log is
persistent storage; never remove segments by age. Old readers and selectable
recovery roots can keep source segments reachable after replacement or rewrite.
Reader release, completed rewrite, and actual reclamation are separate events.
Inspect completed work, retained/protected bytes, and remaining debt; a successful
no-op does not establish reclaimed space. The
[lifecycle spec](../spec/r1-row-lifecycle.md) describes the tested release,
recovery-retention, and lawful reclamation boundaries.

For the selected scalar layout, manual `Collection.ColumnStoreCompact` folds
latest-visible live rows into one insert-only generation and resets logical
mutation parts. It requires an eligible recovery-authoritative manifest and
maintenance readiness; unsupported vector-graph state is refused. Inspect its
actual row/part/ref statistics. A logical fold does not establish a physical
storage bound or delete superseded segments. `ColumnAssetRewrite` can remap live
refs out of eligible mixed segments; `ColumnAssetGC` can then reclaim only
recovery-safe, unpinned candidates. Complete plans, rewrite debt, dry-run
eligibility, checkpoint state, and retained/protected bytes determine useful
work. Old views and selectable recovery roots remain protected.

Backend value-log, leaf-log, and index maintenance are separate responsibilities.
Whole-segment GC cannot shrink a partially live value/leaf segment; rewrite or
leaf-generation packing is needed where eligible. Backend `DB.CompactStorage`
provides high-level orchestration of rewrite, GC, leaf packing, index vacuum,
and a final audit; `CompactStoragePlan` reports debt without performing that
work. Use the documented ownership/readiness checks and respect refusal or
retention rather than forcing reclamation. This orchestration is not a substitute
for logical typed history folding or proof that this row workload is bounded.
See the [value/leaf-log lifecycle](../spec/value-log-lifecycle.md) and the
[R1 maintenance scope](../spec/r1-row-lifecycle.md).

Immutable leaf-manifest revision GC keeps at most 16 selected deletion handles,
one scan child or quarantine placeholder, and one temporary link-validation
handle (at most 18 GC child descriptors, separate from existing parent/manager
and other process descriptors). It validates the full directory for each batch
while holding writer and snapshot-admission locks for the whole explicit GC call.
Worst-case repeated scanning is O(N²/16); footprint admission limits are not
cumulative work or pause budgets. Assess the recorded finite-workload maintenance
cost and remaining physical bytes before claiming sustained capacity.

## Interpret measurements

The [R1 source-bound benchmark methodology](../spec/r1-indexed-row-contract.md)
defines equivalent fixtures, verified TreeDB/SQLite ACK settings, complete owned
output, setup/flush and materialization costs, repetitions/noise, and exact
runtime/harness identities. The lifecycle spec defines a separate sustained
diagnostic, its bounded repeated working set, timer scope, and maintenance
attribution. Its packets are not interchangeable with the comparator matrix.
The example and comparator use `OpenBackendWithCachedLeafLog`; the lifecycle
benchmark uses `OptionsFor(ProfileCommandWALDurable)` plus `OpenBackend`, with
background prune disabled and its supported persisted/effective format checked
at creation and reopen. Its same-live-backend vacuum and exhaustive
`CompactStorage` stages record actual owner admission, phase work and debt; a
final fallback refresh precedes typed and leaf GC. These costs exclude
cached-wrapper checkpoint/reconciliation overhead. Full-root file censuses
include side stores and immutable manifest metadata. The
[integrated evidence](../evidence/r1-row-store-5061/README.md) accepts the finite
five-process/five-epoch lifecycle and its measured component growth and
maintenance costs. It does not qualify equivalence with the cached-leaf-log route
or a duration-unbounded storage bound.
Use reviewed, landed tooling and frozen sources for retained evidence. This
example and guide establish usage, with no measured speedup or capacity claim.
