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

The ordinary collection manager supports eligible JSON, BSON and template-v1 `CreateCollection` and
`OpenCollection`. Native collection handles route ordinary insert, batch insert,
update and delete, point `Get`/`GetInto`, and full-document captured fetches through
the common native engine. Scalar secondary and unique indexes support the native
ordered document mutation path; supported scalar types are string, bool, int64 and
double. BSON additionally supports the maintained scalar BSONOrderedV2 encoding,
including distinct missing/null and canonical numeric equality. Raw BSON bytes
remain authoritative; replacement preserves the original BSON `_id`. Default and
explicit BSON fetches return owned BSON; an explicit request format must match
the captured collection, and JSON projection remains unavailable for BSON.
Scalar multikey indexes and the existing `AllowArrayValuesInIndex` option use
that same planner: arrays are encoded as sorted distinct value sets, preserving
the format's existing missing/null and dotted-path semantics. Final unique
ownership is checked across the whole transaction, including value handoffs.
BSONOrderedV2 compound scalar indexes reuse the same tuple encoder, old/new
planner and final unique ownership checks. Components may use dotted paths and
ascending or descending directions; arrays remain rejected for compound keys.
Text/vector index creation remains outside this route. Eligible BSON collections also support top-level `UpdateBSONSet` and
`UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges`, using the maintained BSON
transformer and canonical full replacement documents. `_id`, dotted or invalid
field names and malformed raw values retain their existing refusal contracts.
The transformer borrows input fields only during the synchronous operation;
retained documents and canonical frames own their output.

The existing conditional `UpdateBatchIfNoSecondaryUniqueIndexes` and
`UpdateBatchIfNoSecondaryUniqueIndexChanges` routes share the same captured
metadata and old/new scalar encoder. A schema-based decline does not invoke
callbacks. A unique-value-change decline returns zero-valued results and
`batched=false`, without assigning a canonical identity or publishing private
changes. BSONOrderedV2 compares its encoded numeric value, so changing the BSON
integer width alone does not imply a unique-value change.

Template-v1 uses the maintained TD1I/TD1H preparation and compact TD1D primary
representation. New template records, hash bindings and next-ID updates share the
same immutable transaction and domain 2 as the document/index effects. Canonical
members retain their original input envelopes, including intermediate members
whose documents are later deleted. Persisted template hash/next-ID reads have
ordinary OCC dependencies. The public scoped encoder learns numeric IDs only
after successful publication and remains bound to the original backend and
collection. Its compact stored inputs cannot cross that scope. A stored-document
JSON materializer retains its exact native capture and resolves template records
from that same cut, including pending records; returned JSON is caller-owned.
Its temporary parser/object backing is admitted by the original read allocator
and released after owned copyout. Scalar index DDL reuses the same captured
resolver and original complete-cut backfill. No template cache or on-disk format
is added. The deterministic native FSM adapter also accepts canonical hash-keyed
TD1D requests with TD1I embedded definitions or a TemplateRecords sidecar. Its
original native stage admits the envelope/header/parser backing, translates root
hash references to TD1H inputs, and reuses this same batch resolver and finalizer.
Definitions carried by absent replacements remain available to matched rows,
including hash references preceding their definitions; accepted canonical members
own the definitions required for startup replay. Preflight releases its captured
plan without reserving a local WAL or distributed apply identity. External packets
never authorize collection-local numeric template IDs. Prepared template producers
remain outside this ordinary family.

Scalar JSON index reads support `FindByIndexRange`, `ScanIndexRange`,
`FindDocumentsByIndexRange` and `ScanBorrowedDocumentsByIndexRange`. Ascending
scans seek directly to the maintained scalar bounds and merge pending,
captured and persisted winners from one immutable cut. Document reads resolve
primary values from that same cut. Limits report truncation only when an
additional live match exists; a callback stop is not truncation. Multikey scans
emit each document once at its first matching encoded value, and limits count
unique documents. Their operation-local deduplication backing is admitted before
growth under the original finite read budget and is released when traversal ends.
Missing indexes are empty scans. Document ranges require a positive limit, and
borrowed-document callbacks retain their existing no-retention/no-reentry contract.

Eligible typed collections also route `InsertTypedBatchWithStats`,
`ReplaceTypedBatch`, `UpsertTypedBatch`, `ReplaceTypedSourceByID` and `DeleteBatch`
through the same transaction and immutable typed projection. Current eligibility
requires top-level declared columns without secondary indexes or aggregate metadata:

- One Int64, String or finite Float32Vector ColumnPart with noncolumn
  semantic-stream-v1 retained payload.
- One or more nonnullable String RowAsset and/or finite Float32Vector ColumnPart
  columns with noncolumn JSON retained payload. Each row supplies every declared
  column through the maintained typed projection or ordinary JSON input. Pending
  reads and physical reconstruction use the same complete schema; canonical typed
  frames retain every column in schema-name order.

Column-dependent source, value, payload and reconstruction backing is admitted
before allocation by the original transaction/read/build owner. Columns share the
existing operation-local physical codec; captured TCIM sections are inspected once
per asset, rather than once per column. Existing 32 MiB maintenance and 64 MiB
in-flight limits remain in force; wider schemas do not receive extra credit.

Raw, eligible scalar-indexed JSON/BSON and cross-collection document transactions target durable WAL,
relaxed WAL and production NoWAL profiles. Typed writes and transactions binding
typed collections require `ProfileCommandWALDurable`; relaxed WAL and NoWAL
refuse before mutation admission. Existing eligible NoWAL typed-read configurations
do not imply typed-write support. A value log remains persistent value storage
when WAL is disabled.

`Collection.PrepareInsertBatchOwned(...).Commit()` supports the eligible no-index
Int64/String ColumnPart semantic-stream layout in the durable WAL profile. Prepare
stages the original transaction's identity-free typed projection and canonical
input copies; it advances no publication frontier. Conflicts against its opening
may fail preparation early. Commit consumes the token once and revalidates its
captured dependencies against the latest compatible complete state before effects,
and uses its original admitted exact physical seal receipt. It returns after
physical completion, including when a sibling publication and checkpoint changed
the basis after preparation. Ordinary transaction Commit retains logical completion.
Abandon and refused commits close the original transaction and its receipts.
`maxOwnedBytes` includes conservative request/control, parser, WAL, materializer
and publisher credit. Shared opening state and unrelated checkpoint work keep their
own engine receipts. The ordinary owned-input contract and prepared scalar/input
limits still apply; native staging also independently copies caller inputs.

Ordinary manager `ListCollections` and `ListCollectionsBounded` read the current
captured catalog, including pending creates and index DDL. Returned schemas are
owned copies in name order; positive limits bound names inspected/copied and use
the immutable directory count to report truncation. A nil engine first requires
the existing full physical-catalog import, outside that names-traversal bound.

These are explicit eligibility limits of the provisional implementation. Broad
producer migration remains required beyond the ordinary JSON/BSON deterministic apply slice: typed/derived assigned and Raft producers,
import/source controls, additional typed layouts, standalone schema/drop/rename, non-scalar index DDL and derived-index lifecycle still
need their complete public-path integration. Staged transactional DDL and
approximate vector/hybrid or asynchronous text predicates in writing transactions
remain excluded by the accepted capability contract.

## Transactions and visibility

Use `manager.Begin("a", "b")`, then `InsertBatchInto`, `UpdateBatchInto`,
`DeleteBatchFrom` and `GetFrom` for bound collections. Single-collection callers
can use `InsertBatch`, `UpdateBatch`, `DeleteBatch` and `Get`. `GetRaw`/`SetRaw`
use the original conditional batch; the selected composite frontend accepts one
inline raw point Set. `UpdateBatchInto` is the scalar JSON path; typed
replacement operations use the existing typed Collection methods. Ordered typed
stages can span several bound collections in the same transaction, including
insert/delete/reinsert of one key. They share the original physical build workspace
sequentially and install one complete resource closure. Existing member/payload
and original memory limits apply.
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

`CollectionManager.SyncForStandaloneWriteConcern` synchronizes the same cache
owner's complete captured raw/collection prefix with its existing checkpoint.
It reports `physicalSync=true` on success, including a repeated sync. Command-WAL
profiles synchronize the covered command/dependency prefix; NoWAL synchronizes
its physical checkpoint. This does not acquire a legacy publisher or change the
profile-dependent acknowledgement returned by native `Commit`; physical
checkpoint completion remains a separate boundary.

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
allocation. Successful materialization returns the ended operation workspace
credit after codecs close and borrowed inputs are dropped. Retained outputs and
handoff promises remain charged; failed builds retain their original custody.
The original build group receives the complete resource/requirement
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

## Standalone scalar index DDL

Activated ordinary JSON and BSON handles support `CreateIndex`, `DropIndex`,
`DropIndexes` and `DropAllIndexes` through one immutable catalog transition.
Creation backfills the complete captured primary cut, including pending rows,
using the maintained scalar encoder and uniqueness checks. A duplicate unique
backfill refuses before canonical assignment. Scalar multikey backfill reuses the
same complete-cut extraction and normalized value sets. BSON compound scalar
backfill uses the same captured-cut tuple extraction. Typed, text and vector
index DDL remain outside this capability.

Every transition writes the existing catalog-name conflict effect. Native
transactions depend on that name even when the collection is only read to derive
another write, so schema ABA invalidates the original transaction. Backfill also
retains a complete primary range dependency: concurrent document changes cannot
silently escape the new index. A directly registered singleton preparation has
no reprepare and keeps the stricter original exact-state basis check. Physical
progress alone can rebase the captured DDL plan onto the latest compatible
immutable state. Primary/template domains and their resources remain shared;
creation rebuilds schema-derived index domains. Pure drops retain surviving
domains and remove only dropped names. Held views retain their old schema and
index interpretation.

Catalog index mutation uses command kind 201 and payload format 17; create
collection remains kind 200. Startup validates the prior schema/incarnation and
performs the same backfill or name-based removal in the original single replay
build group.
The command contains no predicted LSN, physical roots or new schema epoch.
Temporary schema/document construction backing is admitted before normalization and
encoding and released after synchronous publication drops its borrowed aliases.
This is a bounded operation allowance, not a claim that Go's existing global
JSON encoder cache disappears at receipt release.

## Deterministic document apply

The deterministic collection apply producer routes eligible JSON/BSON catalog create,
by-ID insert, replace, delete and top-level BSONSet through the same native
conditional/catalog preparation and original canonical publisher. BSONSet stages
the maintained transformer and emits the existing full-replacement update member;
it does not introduce a patch frame or a new replay parser. Frames retain their
existing singleton kinds and payload formats. Dependency validation precedes assignment;
replace counts matched rows separately from byte-changing rows, and delete frames
retain all requested IDs. A same-metadata create or empty mutation still appends
its canonical frame and advances one logical successor.

This producer requests the original admitted exact seal receipt. It completes
physical coverage before computing/storing the deterministic result and progress,
while the request's sync flag retains its command-WAL durability meaning. The
legacy append seam cannot preassign native intents; custom legacy append seams
refuse on this route. Unsupported typed and colocated-vector deterministic
mutations remain refused before assignment. Legacy preassigned-intent BSONSet
preparation/apply APIs remain unsupported on native handles: the live adapter
must stage before assignment through the original publisher.

External deterministic preflight uses the same identity-free native row staging
and final unique-owner validation, then closes its captured transaction and all
partial plans. It does not reserve the plan or assign a WAL identity, install a
logical successor, seal, or persist apply progress/results. Empty and unchanged
mutations validate without creating a no-effect successor. Native create
preflight uses the same metadata eligibility as apply, including its ordinary
JSON/BSON role. Request document format must match the captured collection before
staging. Apply captures again and revalidates its original catalog/data
dependencies under the publication gate; a successful advisory check is not a
promise that a later conflicting apply will succeed. Legacy preflight remains on
its existing admitted wrapper.

The request-local post-append fault observer runs synchronously after append and
receipt recording, before native finalization/visibility. It must be nonblocking
and must not reenter the database or acquire locks. Its failure uses the existing
assigned-failure custody and poisons the original publication owner; it creates
no result or progress record. Startup replay recovers the original canonical
frame, without fabricating a deterministic apply result. Ordinary writers have
no observer.

Public primary ID/document scans retain one complete native capture, merge its
pending/captured/physical primary sources with the maintained successor seek,
and resolve document pointers through that same capture. Limits count live rows;
truncation requires an additional live row. Callback buffers are caller-owned,
and callbacks run outside the read mutex while the original capture remains
retained. JSON materialization copies bytes without a legacy resolver. Logical
vector receipt queries prove absence only for a current captured non-column
schema; native typed roles continue to refuse those unsupported receipt APIs.

BSON activation reuses the existing BSON validator, scalar encoders, canonical
member formats and physical builders; it adds no on-disk format. Startup replay
validates every intermediate BSON image under its captured schema before reducing
ordered effects, including images later overwritten or deleted. BSONOrderedV2
key admission includes the exact escaped ID suffix and maintained numeric digit
limit; temporary query codec backing remains with the original captured read
through encoding, traversal and owned ID copyout. Existing JSON charges and key
bytes are unchanged.

## Captured compound ranges

`FindByCompoundIndexRange` reads one captured native schema and index domain.
Forward and reverse scans seek one bounded candidate per original pending,
captured and DATA source; equal keys retain newest-source precedence including
tombstones. Reverse reads use caller-admitted fixed predecessor scratch, not a
materialized interval. Every source probe debits `MaxInspected` before work.
The maintained bounds and complete logical tie-group sorter preserve missing/null
normalization, canonical numeric equivalence, ID ordering and fail-closed
truncation. Stable ties still require at most one unfixed component. Query
key/group/result backing is admitted before copying or growing, and temporary
aliases end before refund. Only independently owned result IDs escape. Default
resource limits and the tuple/command formats are unchanged.
