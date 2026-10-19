# Native collection integration

This is the provisional collection implementation for [TreeDB native MVCC
#5139](https://github.com/snissn/gomap/issues/5139), including the responsibilities
of [P3](https://github.com/snissn/gomap/issues/5142). The
[rolling development checkpoint](https://github.com/snissn/gomap/issues/5139#issuecomment-6272276174)
records the current candidate, source-bound results and remaining work. Scoped
public tests do not establish complete-engine qualification or merge readiness.

## Opening and lifetime

Open the public database with `Options.MemtableMode = "cow_btree"`, obtain
`database.NativeSemanticBackend()`, then construct either
`collections.NewNativeJSONManager(handle)` or
`collections.NewCollectionManager(handle)`. Explicit native activation binds the
original cache/backend owner. Ordinary `Open` alone does not activate it. Existing
incompatible legacy producer registrations prevent activation; later legacy
aliases cannot acquire independent publication authority.

Keep the original public database open while transactions, collection managers
and captured reads use it. Close those objects before `database.Close()`. The
backend capability belongs to the database and is not closed separately. Raw
methods on the public database continue through the same owner. Backend-only
cached-leaf-log constructors do not provide this native capability.

## Public operations and eligibility

The ordinary collection manager supports eligible JSON `CreateCollection` and
`OpenCollection`. Native collection handles route ordinary insert, batch insert,
update and delete, point `Get`/`GetInto`, and full-document captured fetches through
the common native engine. Scalar secondary and unique indexes support the native
ordered JSON mutation path; supported scalar types are string, bool, int64 and
double. Compound and multikey indexes, array index values, text/vector index
creation and non-JSON document formats remain outside this route.

Eligible typed collections also route `InsertTypedBatchWithStats`,
`ReplaceTypedBatch`, `UpsertTypedBatch`, `ReplaceTypedSourceByID` and `DeleteBatch`
through the same transaction and immutable typed projection. Current eligibility
is one top-level column without secondary indexes or aggregate metadata:

- Int64, String and finite Float32Vector ColumnPart with noncolumn
  semantic-stream-v1 retained payload.
- Nonnullable String RowAsset or finite Float32Vector ColumnPart with noncolumn
  JSON retained payload.

Raw, eligible scalar-indexed JSON and cross-collection JSON target durable WAL,
relaxed WAL and production NoWAL profiles. Typed writes and transactions binding
typed collections require `ProfileCommandWALDurable`; relaxed WAL and NoWAL
refuse before mutation admission. Existing eligible NoWAL typed-read configurations
do not imply typed-write support. A value log remains persistent value storage
when WAL is disabled.

These are explicit eligibility limits of the provisional implementation. Broad
producer migration remains required: prepared insert completion, assigned/apply
and Raft producers, import/source controls, multi-column and additional typed
layouts, standalone schema/index/drop/rename, and derived-index lifecycle still
need their complete public-path integration. Staged transactional DDL and
approximate vector/hybrid or asynchronous text predicates in writing transactions
remain excluded by the accepted capability contract.

## Transactions and visibility

Use `manager.Begin("a", "b")`, then `InsertBatchInto`, `UpdateBatchInto`,
`DeleteBatchFrom` and `GetFrom` for bound collections. Single-collection callers
can use `InsertBatch`, `UpdateBatch`, `DeleteBatch` and `Get`. `GetRaw`/`SetRaw`
use the original conditional batch; the selected composite frontend accepts one
inline raw point Set. `UpdateBatchInto` is the scalar JSON path; typed
replacement operations use the existing typed Collection methods. Existing
member/payload and original memory limits apply.
Always close the transaction, including after a staging or commit error.

The transaction retains one original opening cut, collection incarnation and
read dependencies. Reads observe that opening plus private staged effects. Inputs
and public returned values are independently owned. Missing-row reads and
catalog identities participate in conflict validation before canonical assignment.
Commit composes the original immutable mutations onto the latest compatible
complete state; it does not rerun callbacks, retry the body or capture a different
opening. Staging failure either preserves prior state or terminally invalidates
the transaction.

Ordered scalar JSON operations use the maintained old/new index effect machinery.
Unique checks validate against prior effects, without borrowing a future removal.
Typed stages retain identity-free row projections and explicit tombstones before
LSN assignment; they do not build a full-document JSON shadow. Canonical member
LSN and ordinal bind the accepted immutable effects.

Captured collection read views retain their exact complete state across newer
commits and physical checkpoints. Typed locators, manifests, retained data and
asset opens resolve through that captured state. Lazy reads do not switch to a
current manager or catalog. Physical point reads reuse bounded traversal and
supported codec buffers owned by the exact immutable backend basis. Results copy
out while the basis mutex is held; old views retain the original basis and its
buffers until their last owner closes.

## WAL, physical construction and failure

A multi-member transaction appends one canonical bounded nonrecursive composite
command and installs one complete logical successor. Its raw member binds the
actual revisioned bytes from the original finalizer. Startup validates the whole
aggregate before applying members; child preparation does not append, publish or
advance a frontier. Recovery reuses the original RootPublicationBuildGroup and
DB-minted replay identity for one outer publication.

For canonical-inline typed effects, accepted logical state owns the immutable
row/retained backing. Captured-prefix physical construction later materializes
maintained row, column, manifest and locator assets off the publication gate.
Original memory receipts cover retained and operation-local backing before
allocation. The original build group receives the complete resource/requirement
closure; a newer suffix and held old views retain their own state and custody.
Required external-reference prerequisites still close before recoverable
acceptance; the deferred rule applies only to reconstructible inline effects.

A preappend semantic, capability, conflict or capacity refusal consumes no
canonical identity. An incomplete assigned logical-finalization failure poisons
continuation and installs no partial view. A later accepted physical-build or
asset-closure failure retains the complete accepted raw/collection cut and the
original terminal build/custody. It installs no partial disk basis or handoff,
refuses further writes and preserves the failure through Close. A fresh owner
replays the original canonical aggregate.

Equivalent physical rewrite or maintenance rebinds descriptors using the exact
before/after snapshots and original owner. It preserves logical provenance,
held views and pending effects. This capability is separate from a single-use
captured-prefix handoff and does not grant generic backend aliases publication
authority.

## Development verification and remaining qualification

Maintained public controls cover raw/cross-collection JSON, indexed ordered
mutations and unique effects, typed families and actual typed APIs, held old
views, caller/result mutation, checkpoint/reopen and genuine unapplied aggregate
replay. The accepted-build failure control checks original partial-output custody
and complete logical visibility after a real missing-obligation failure. The
rolling checkpoint binds each result to its tested source; historical results
are not silently rebound to a later candidate.

Complete-engine acceptance additionally requires every supported producer and
layout, sustained checkpoint/rotation/rewrite/GC and old-pin release, constrained
memory and useful concurrent progress, terminal cleanup, full costs, representation
retirement, current documentation and mature current-head review/CI. Those
obligations remain open. DATA is selected for new stores; explicit or persisted
PRIMARY compatibility remains until actual consumers migrate and the retirement
gates pass. This document does not retire that path or claim a performance gain.
