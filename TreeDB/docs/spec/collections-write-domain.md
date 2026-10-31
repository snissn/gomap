# Collections Write-Domain Contract

Status:

- Supported command-WAL collection writes use the resolved production profile's
  acknowledgement contract. `command_wal_durable` makes the complete command
  frame recoverable before acknowledged write-domain visibility; root
  publication and `AppliedLSN` may follow later.
- Pending write-domain state itself is process-local. Without a durable command
  frame, ordinary acknowledgement is not a crash-durable boundary. `Flush`
  drains state; only the canonical profile's sync/checkpoint/close boundary
  establishes durability.

This document specifies collection-local write-domain behavior for indexed
collections. The normative profile/API matrix is owned by
`write-path-and-durability.md` section 0.2; this document does not define a weaker
independent durability profile. Unsupported command kinds fail closed before
admission rather than silently downgrading acknowledgement guarantees.

## Shared COW collection authority

The ordinary standalone Mongo opener owns a public `treedb.DB` in `cow_btree`
mode and creates its collection manager from `NativeSemanticBackend`. The
backend is borrowed for that owner's lifetime. The ordinary collection facade
routes supported mutations through the same immutable native collection/catalog
cut and command-WAL completion as direct native callers; it does not stage a
second manager-local write domain. Reads capture one complete cut. Legacy
backend-only constructors retain the write-domain path described below.

A native domain distinguishes its logical physical-read base (`baseRoot`) from
its authenticated persisted catalog descriptor (`catalogRoot`). A full named or
schema-derived replacement preserves catalog provenance while discarding the
superseded logical base and sources. Equivalent physical maintenance remaps that
provenance without reattaching the old image. If an older captured checkpoint
publishes after a newer full replacement, its handoff updates only that newer
replacement's matching catalog provenance; the replacement's interpretation,
source generation, pending image and zero logical base remain unchanged. Both
headers and the captured build's provenance are covered by their existing
size-derived admission, with no new resource owner or publication boundary.

Ordinary native `InsertBatch` and `InsertBatchWithStats` use the same original
native row planner and publication. Successful ordinary facade inserts replace
`LastInsertStats`; `WithStats` additionally returns an independent request-owned
copy. Failed or commit-ambiguous attempts leave the last successful diagnostics
unchanged and return no success statistics. Empty batches capture the current
schema for the index count without publishing a mutation. ColumnStore
`WithStats` remains unsupported here; plain typed insertion and the separate
typed diagnostics API retain their previous behavior.

Native counts come from the captured scalar declarations and actual prepared
primary/additional logical domain runs, including encoded secondary entry/key
sizes. They count prepared work, not physical checkpoint roots. `AlreadySorted`
describes each sorted prepared secondary run. `PrepareDocuments` measures row
staging and `Publish` measures the existing native commit including its required
acknowledgement wait. Native final planning and other legacy phase timings are
unmeasured; zero values do not claim those phases took no time. No new capture,
decode, retry, journal, durability profile, or vector notification is introduced
for a nonempty insert.

Diagnostic array/name construction is admitted on the original transaction
staging lease before allocation. On success it becomes handle-owned output,
like returned document IDs, outside the retained native engine state. A plain
facade insert retains one per-index array and cloned names; `WithStats` adds one
independent array copy. The direct native manager keeps the existing no-sink
path. There is no native performance acceptance claim for the added diagnostics.

Public native read views reuse the existing JSON projection validator and
projector, including include/exclude precedence and materialization counters.
Projection reads the view's captured document cut through physical handoff.
BSON and template-v1 keep the existing stored-format contract; the shared
projection validator permits JSON documents only.

Supported vector definitions declared in `CreateCollection` select ColumnGraph;
unsupported or retired definitions are rejected before catalog publication.
Typed/ColumnGraph source imports keep their existing construction route. JSON materialization clones captured JSON,
converts captured BSON, and invokes the captured template resolver only for
template-v1 documents, including exact vector-search fallback.

The native text adapter publishes the maintained V1 reducer's three domains
or V2 reducer's seven domains with primary and scalar changes in one successor.
V2 status and V1 shared-statistics dependencies authenticate the planning cut;
search borrows one complete cut for postings, metadata and documents. V1 retains
its maintained phrase/proximity refusal. Status/storage diagnostics use captured
schema and version-specific validators. Temporary reducer tables use an admitted
unpooled BTree; nested analysis reserves both serial decoder cohorts on the
original operation lease before token/table construction. See
`collection-text-v2-contract.md` for ownership and refusal details.

For ordinary JSON, BSON or template-v1 under `no_wal_fast`, native `CreateTextIndex` backfills V1 or
V2 from one captured primary, including physical and pending rows when no
physical root ID exists. The existing native index-family owner publishes the
schema and complete three/seven-domain replacement atomically. `DropTextIndex`
removes its logical domains and both maintained physical descriptor families;
older read views retain their original schema and domains. Unchanged registered
HNSW receivers use the same admitted schema-change payload preservation as
scalar/vector DDL. Preparation refusal installs no schema/domain successor;
all temporary backfill sources, decoder/table cohorts and outgoing rows stay on
the original operation receipt until private COW sources own their copies.
Command-WAL text DDL retains the maintained preassignment rejection. This slice
does not complete the text family: rewrite/maintenance, typed eligibility and
server/default adoption still require native proofs. Stored-format normalization
and the narrow inherited encoding/json cache boundary are specified in
`collection-text-v2-contract.md`; primary bytes and dictionary authority remain
owned by the original capture/transaction.

Each accepted native mutation installs one complete logical successor; a shared
physical durable group preserves each command's outcomes and original LSN.
Logical sequence, physical applied LSN and durable-prefix coverage are separate
identities. Durable requests wait for their own completion, including persistent
dependencies. Existing solo-sync eligibility avoids an extra barrier for an
uncontended mutation. See `write-path-and-durability.md` for the ACK contract.

For the existing ordinary Full+JSON ColumnGraph layout, a captured graph-free
segment of at least two insert-only members may use one maintained physical
column publication plan. Its assets, manifest generation and row locators use
the last original member's LSN in that collection segment as physical coverage;
they do not create an aggregate command or claim the enclosing global frontier.
Every original command identity, conditional outcome and logical successor is
preserved. The existing accepted-prefix cut still certifies contiguous journal
coverage, and held reads retain their original semantic rows and paired physical
basis through the existing handoff. Graph recipes remain barriers. Mixed
operations, deletes, source imports, compaction and other layouts retain their
member publication path. The span borrows immutable row backing and uses the
original construction receipt and shared codec workspace; eligibility checks
its aggregate source promise against the original member promises before asset
construction. Build errors retain the existing work's custody and do not retry
through the member path.

Mongo insert coalescing bounds combined stored documents and ID bytes by the
configured message envelope. A staged invalid indexed request may be isolated
only on an explicit pre-assignment rejection marker or duplicate-key outcome;
a generic failure is not proof that application callbacks can safely retry.
Assigned uncertain failures retain their identity and poison further writes
until recovery.

Native live producers run shared pressure preparation before collection locks,
captures, private staging and callbacks. Active pending catalog and collection
domain generations also request preparation when their exact generation history
(including native source metadata) plus outstanding preparation reservations
reaches the three-quarter watermark. This scans sparse pending directories,
not captured old generations or the full catalog. It reserves no headroom:
an unusually large request or concurrent suffix can still receive the original
authoritative capacity refusal. Held old reads remain owned and readable.
The existing three-quarter byte watermark or an exhausted finite source/generation
counter also requests maintenance. Each preparation joins at most one coalesced
checkpoint or captured-prefix drain. Mutation admission permits at most one
additional preparation for a changed pressured observation, then leaves the
normal final admission authoritative instead of chasing concurrent suffixes.
Pending typed construction uses the same watermark against
both finite maintenance byte limits and the same source ceiling. Its inventory
includes the original builder, handoff, typed workspace, root-key and catalog
projection promises; exact final admission remains required. It then joins refunds already queued on the existing semantic
cleanup worker through a fixed completion frontier; later producers and held
read cuts are outside that wait. Cleanup failure retains its original custody
and error. Reentry from an active publication/cleanup callback refuses before
waiting. Read capture, replay/apply and publication itself do not initiate this
maintenance. Lock-held facade openings reuse the original transaction opening
without a second pressure check. Preparation reserves no capacity: multi-domain
work and readers retaining old sources can still reach ordinary finite
admission refusal without assignment or a callback retry.


Eligible native ordinary retained-JSON ColumnGraph rebuilds capture one worker
ceiling before admission and bind the existing offline builder to it. Frozen
prefix batches retain their 16-row cuts and graph topology. The original recipe
reserves source rows, IDs and adjacency, captured manifest and source backing,
and the peak of the constructor phases. Asset authority and generated Config
metadata start after private graph construction returns. Pending source images
remain charged while the graph consumes them. The workspace stays reserved
through publication; completion does not acquire a new construction receipt.

The shared graph vector and inverse-norm readers use the maintained scan-only typed image
projection. Whole-asset integrity, image identity, all declared column schemas,
stored codecs and vector layout certification remain checked. It uses captured
collection row locators rather than decoding unused image locator, dictionary,
aggregate, statistics or pruning metadata. Native construction retains the fixed
ZSTD decoder promise for captured data sections that can need it and for pending
producers whose physical codecs are not yet known. Other typed readers retain
their existing full metadata decoding.
Ordinary unquantized status reserves payload workspace only for the inverse-norm asset it opens;
reference and configuration validation retain their metadata charges. A status
failure after successful rebuild completion is reported as an accepted ambiguous
result, so a capacity error does not imply that the rebuild was refused.

Ordinary native ColumnGraph search and lifecycle warming share the maintained
exact prepared search pack. Setup uses the original native capture's physical
manifest and catalog; unresolved source state cannot be replaced with older
physical roots. The cache identity includes native publication identity, and
its mapped or heap-backed assets retain independent admission until their final
handles close. Dense and hybrid materialization keep that original capture
through document fetch. Full retained JSON uses the existing admitted primary
reader. Scalar-filtered dense search keeps its document-scan exact route.
Ordinary vector-bearing Hybrid queries with scalar filters fail closed with
`ErrHybridSearchUnsupported`; they do not use an ANN postfilter fallback.
Text-only Hybrid scalar filtering remains supported. Maintained typed graph
serving and eligible typed quantized rerank routes retain their explicit
admission policies.

The operation-local native text catalog lends one owned pointer-projection workspace across sequential scalar, document-ID and posting phases. Each actually encountered ordinary or PRIMARY backing is admitted before allocation and remains charged to the original operation through its final alias. A cursor holds its loan until Close; overlapping cursors use separately admitted independent storage because inline page bytes can remain borrowed outside the read mutex. Close clears tree, page, directory, key and callback aliases before reuse. Query specialization stays local to the text request, and abandoned or partially constructed posting iterators close before fallback or error return. No pool, new capture, owner or admission authority is introduced.

Native collection scans traverse the same captured finite-source merge with an
operation-owned forward cursor. They materialize each selected entry directly
through the point reader's existing pending-typed, retained JSON and pointer
rules. Cursor storage, its retained reader callback and physical projection are
admitted before creation. IDs and documents become independent copies before
the read mutex is released for application callbacks. Reentrant point reads,
nested scans and mutations cannot replace the outer scan's cut or page storage;
a callback Close defers capture release until visits finish. Cursor cleanup
precedes its operation receipt and captured-read release.

For ordinary ColumnGraph, the document service retains one search collection
owner across cold Dense, Hybrid and prepared queries, including after reopening. Its existing search
cache read lease spans search and final document conversion, excluding cache
invalidation and service Close. Native collection-manager opens remain separate
facades; their per-handle prepared assets must not accumulate through repeated
service opens. Serving admits record backing and both untrusted TVIS decode
envelopes without reserving the source-requirement stream it never constructs.

Ordinary rebuilds derive durable requirements from their complete manifest.
They preserve the captured untrusted TVIS codec envelope without constructing
the source importer's streaming leaf slots. Source import keeps that original
streaming reader and its full bounds. Fresh graph metadata has separately
reserved decoder slots; its known shape does not weaken stored-state limits.
Segment ID selection uses bounded name scans, with exclusive creation retaining
allocation authority. Reachability reports and GC retain their identity checks.

## Indexed Write Memtables

Legacy backend-only indexed collections use collection-local write memtables by default.

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

For typed indexed fields, this precedence also governs old-value maintenance.
Scalar keys may reuse the persisted index-state root; text maintenance can use
the existing row-locator root and a snapshot-scoped typed point reader. Do not
substitute retained non-column JSON for missing typed fields or scan every
physical row for each changed ID. A point reader must share the mutation's
planning snapshot and keep asset handles alive until its borrowed data has been
consumed or copied. It must not force a per-row flush to recover visibility.

Tombstones in pending runs MUST suppress older values from persisted roots or
older pending runs.

## Flush Units

Threshold-triggered indexed writes rotate mutable state into immutable flush
units. A flush unit owns the root-local tables, root policies, base root ids,
unique-value helper tables, and accounting for the staged documents.

Flush units are a visibility and publish-amortization mechanism. They are not a
separate durable log.

## Bounded Typed Durable Grouping

Ordinary typed source replacement currently drains pending writes and publishes
one command-WAL/root/asset transaction per API call. The #1242 throughput slice
may instead admit several concurrent typed calls into one bounded FIFO
write-domain group. This is a collection write-domain responsibility, not a
document-service queue or a second durability mode.

The group reuses the existing command-WAL frames, contiguous coverage intent,
typed publication receipts, `indexedFlushUnit` ownership, and ordered-root
publisher. It does not add a WAL format or another durable overlay. One group
may close one durable command prefix and publish one coherent root/asset state
for several calls; each call retains its own result and error boundary.

The admission states are:

1. **Private:** validation inputs are caller-owned, have no LSN, and are neither
   visible nor recoverable.
2. **Queued:** the write domain accepts FIFO responsibility for validated,
   immutable request inputs and their required lifetimes. Borrowed inputs cannot
   outlive the synchronous call; anything retained beyond that lifetime must be
   owned. Queue admission alone assigns no LSN and cannot return success.
3. **Publishing:** one write-domain leader evaluates each request against one
   coherent base plus preceding successful requests. It rejects semantic
   failures before assigning that request an LSN, then finalizes and appends one
   command frame per changing request and publishes the coalesced roots/assets
   with contiguous LSN coverage. An all-unchanged typed upsert retains the
   existing no-frame/no-publication shortcut; explicit source replacement does
   not.
4. **Installed:** the durable command prefix and coherent collection state are
   installed. Only now may each covered request return success.

Duplicate IDs, same-ID writes, uniqueness checks, and per-request update counts
must match serial FIFO execution. Insert wins within one source replacement as
it does today; later successful requests win across requests. Readers must never
observe a partial request or a group whose durable prefix is incomplete.

Cancellation before queued admission returns without an LSN. After admission,
the write domain completes the request. Semantic rejection before append is
definitive. A failure after an affected request's LSN has been assigned may
leave that mutation durable; that outcome is commit-ambiguous and poisons the
normal handle for recovery. It must not report a rollback or invite a blind
retry, and every affected waiter must be released.

Group admission accounts for retained request bytes, command bytes, and
in-flight ownership, and applies bounded backpressure using the existing
write-domain limits where applicable. Group formation has a bounded wait; a lone
valid request progresses without another arrival. Oversized requests retain
existing atomicity and pre-LSN rejection semantics.

Schema changes, fold, reconciliation, vacuum, explicit flush, checkpoint, and
close establish an admission cut in the relevant scope and drain accepted work
through that frontier. Lower interleaved raw or sibling-collection command LSNs
must be applied, or excluded before group admission, before collection coverage
advances; a group must never claim a non-contiguous prefix.

The first product gate is two concurrent changing ordinary typed calls that
share one command-WAL file-sync boundary in a controlled no-rotation cell and
one coherent root-group publication; neither may acknowledge before
installation. Required external-resource and column-asset durability closure
remains mandatory and is counted separately. A service-only scheduler, hidden
post-ack drain, or an implementation that never groups temporally overlapping
calls does not satisfy this contract.

The first implementation must also permit ordinary same-service typed callers
to overlap while preserving generation and maintenance exclusion. Passing
`false` to `AppendStagedCommandWALIntent` is not a grouping mechanism because a
durable-profile intent still requests sync-on-publish; use the backend prefix
and resource machinery without a relaxed-profile workaround.

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

Foreground versus background threshold publication does not select the public
durability guarantee. Supported indexed collection commands already use the
shared command-WAL machinery: durable-profile intents request stable complete
frame closure before staged visibility, while `AppliedLSN` advances with the
corresponding root publication. Replay applies unapplied accepted commands
through the collection executor. The indexed staging and replay tests in
`TreeDB/collections/command_wal_test.go` cover this separation.

`Collection.Flush` and `CollectionManager.FlushAll` drain mutable, queued and
publishing state. They do not independently promise file or directory sync.
Checkpoint and clean backend close are sealed-root durability boundaries;
explicit sync operations follow the canonical profile matrix. A background
publish completing early does not strengthen relaxed acknowledgement.

In `command_wal_durable`, write-domain mutable/queued/publishing state is
process-local state, not a durable pending overlay. A WAL-supported collection
command may return success or become owner-visible only after its command frame
and required external refs are recoverable and the normal executor has installed
the command in the process-visible write domain. Durable-at-ack requested for a
collection command whose command kind is not `WAL-supported` must fail before
staging.

In WAL-off relaxed mode, write-domain state is not backed by command WAL.
Acknowledged pending writes may lead sealed-root publication. `Flush` and
`FlushAll` drain visibility; `Checkpoint`, clean `Close`, and explicit sync
operations establish the canonical sealed-root persistence boundary. Typed
column writes require their supported command-WAL capability unless an explicit
benchmark-only unsupported-production mode says otherwise.

During private planning, canonical command payloads, external refs, uniqueness
reservations, publish inputs, and schema/index barrier state are not reachable
from any read, scan, uniqueness check, update/delete planner, queued unit,
publishing unit, or pending-state merge.

For eligible no-secondary-index JSON collections using semantic-stream retained
payload, `PrepareInsertBatchOwned` can move ID sorting, duplicate preflight,
retained block encoding, and declared-row extraction ahead of the ordered
publisher. It owns the input buffers and has no command LSN or publication
lease. The `maxOwnedBytes` argument screens the input and partitions a separate
credit for each retained block before workers start. Each block checks its
raw size before encoding and its retained output before joining the prepared
batch. Rare JSON paths grow only as values arrive rather than reserving every
row in the block. The final capacity check rejects a prepared payload whose
charged backing plus a commit reserve exceeds the limit; `OwnedBytes` reports
the retained backing and `ReservedBytes` includes the commit reserve. The
caller must transfer complete, non-aliased ID/document backing whose capacity
describes the whole allocation, with no unrelated buffers in unused outer
slice slots. Full-sliced views into larger allocations are outside the
owned-byte accounting contract. The prepared token also charges an identity-free
typed scalar batch and its stable schema copy. Before the typed batch is built,
an additional scalar-vector/dictionary allowance is checked. Ordered commit
reserves disjoint credit for materialization, WAL/frame/run buffers, typed
image, row asset, sidecars, and catalog copies. These credits cover incremental
pipeline-owned buffers; they are not heap telemetry or a bound on baseline
pager/zipper/Manager work. The prepared path also preflights the existing
manifest before WAL append, allowing at most 4,096 inline records and 1 MiB of
combined key/value bytes. This whole-collection ceiling can reject a small
batch against a sufficiently large existing collection. The
input-derived preparation and commit reserves remain under allocation-site
audit for encoder workspace and typed/aggregate builders; the strict
incremental-byte gate is not yet qualified. Prepared mode rejects document IDs
over 1,024 bytes, documents over 128 KiB, or input beyond the cursor's depth and descriptor
limits. Prepared mode admits one to five declared columns with paths of at most
1,024 bytes and Int64 or String values, plus an asset namespace of at most
512 bytes. Wider schemas and other scalar types use the ordinary path because
they currently take a full-batch extractor before the bounded cursor.
Resource and structural limits return `ErrPreparedInsertResourceLimit`,
which is distinct from `ErrPreparedInsertIneligible`, so a bounded caller fails
closed by default. Unsupported collection configurations return
`ErrPreparedInsertIneligible` and can use ordinary `InsertBatch`. A later
`Commit` rechecks persisted conflicts and schema under the normal insert path
before WAL admission. One caller can
prepare batch N+1 while a single committer publishes N; neither a queued
object nor an abandoned object is acknowledged or visible. Preparation also
builds an identity-free typed scalar batch and finalizes its string dictionaries.
The ordered publisher assigns the part identity and builds the typed part and
image, then performs asset sync and root installation. GC authority stays with
the ordered publication path. The on-disk format and replay rules are unchanged.

The writer prepares and protects required external refs, appends the typed
command WAL frame through the shared commit-log journal, and applies through the
normal executor. In the
durable profile, stable complete frame closure precedes visibility and success;
the relaxed profile may lead sync as specified by the canonical matrix. WAL
cleanup requires sealed-root coverage plus `AppliedLSN` and protection of both
recoverable roots; a synced acknowledgement need not publish a root. Durable pending overlays
that survive without replay are a future feature, not part of V1.

In `command_wal_durable` for enabled command WAL capabilities, visibility implies
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

## Vector Rebuild Publication Handoff

Public command-WAL vector rebuilds take schema-read ownership first and pin the
index generation before acquiring publication locks. Capture drains the lower
command prefix under the existing teardown/admission/raw guard, then acquires
collection mutation ownership. If mutation is busy, capture releases the raw
guard before waiting and retries the acquisition order. A newer snapshot after
the drain remains protected by the earlier stable-generation pin.

Graph construction retains mutation ownership and the pinned source, but does
not hold the database-wide publication guard. Before final publication, rebuild
releases mutation, drains the command prefix, and reacquires publication then
mutation using the same nonblocking protocol. It revalidates all collection
authority roots, metadata and captured typed-base identity before appending a
rebuild frame. An intervening collection change rejects the candidate; unrelated
raw or sibling-collection publications may proceed. No automatic rebuild retry
is implied. Existing staged publishers inherit raw/teardown ownership instead
of recursively acquiring those guards or invoking barriers with mutation held.
Replay retains its existing assigned-LSN path and does not run public barriers.

This ordering applies to ordinary, normalized, empty/no-op and native rebuild
publication. Checkpoint may retain a selected write domain whose pending state
was already drained: it must still be able to acquire that domain's mutation
ownership. Skipping a busy domain or dropping lower-command coverage is not a
valid way to break the cycle.

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
