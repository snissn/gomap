# SPEC: Collection WAL Durability and Root-Group Recovery

Status: normative implementation gate once Milestones 0-7 are complete.
Sections marked MUST/SHOULD define the production collection durability
contract; explicitly marked future-work sections remain non-normative. Open
questions in Section 15 must be resolved before any milestone that depends on
them can pass.

This document defines the minimum implementation contract for durable-at-ack
collection writes. Until the requirements and crash tests in this document pass,
TreeDB must not merge production persistent column-store collection APIs, column
part descriptor roots, column-file side refs in published roots, or
crash/reopen safety claims for column-store writes.

Target repository studied: `https://github.com/snissn/gomap/tree/7431ba92f7a1a456c15f70ac019314090e22af31`
Primary code paths studied: `TreeDB/collections`, `TreeDB/db`,
`TreeDB/caching`, `TreeDB/internal/commitlog`, `TreeDB/internal/valuelog`  
Related specs:

- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/recovery.md`
- `TreeDB/docs/spec/collections-write-domain.md`
- `TreeDB/docs/spec/native-wire-protocol.md`
- `TreeDB/docs/spec/native-query-raft-roadmap.md`
- `TreeDB/docs/spec/native-wire-r2-closeout.md`

## 1. Summary

TreeDB collection writes currently have a different durability shape from normal
cached key/value writes. The base cached write path has a commit log and
value-log RID fence. The collection write domain can acknowledge writes while
they remain only in collection-local mutable, queued, or publishing state. Those
writes are visible through the owning collection manager, but they are not
durable-at-ack under the current contract.

This spec proposes a shared collection WAL and root-group recovery protocol.
The goal is to make acknowledged collection writes recoverable under WAL-on
profiles without building a separate durability model for future column-store
collections.

The proposed architecture is a root-delta transaction WAL:

1. Collection write planning builds the exact root-local deltas for the primary
   root, template/index-state root, secondary roots, delete roots, overlay
   roots, and future column-store descriptor roots.
2. Any external payloads, such as value-log values or future column files, are
   prepared before the transaction is committed.
3. The collection WAL transaction records collection identity, schema/root
   generation guards, per-collection sequence dependencies, ordered root deltas,
   required side-file references, and a replayable system-root delta template.
4. Recovery replays complete transactions by publishing the same root group and
   advancing an applied collection-sequence watermark in the same system-root
   commit that updates collection descriptors.

This is a hard prerequisite for production column-store collection writes.
Before this milestone passes, column-store work may proceed only for docs,
benchmarks, pure codecs, filters, and isolated format encode/decode tests that
do not publish persistent collection roots.

PR1 makes these decisions normative:

- use per-write collection WAL transactions with per-collection dependency
  chains and replay-side accumulation;
- reject true multi-collection collection WAL transactions before writing side
  refs or WAL records;
- use a stable persisted `CollectionUID`, mandatory
  `CollectionGeneration`, `SchemaEpoch`, `CatalogDigest`, and per-root
  `RootGeneration`/root descriptor epochs as replay guards;
- include a versioned `SystemDeltaTemplate` in every replayable transaction;
- fail recovery on any complete collection WAL transaction with a missing
  required side ref in WAL-on modes;
- maintain a protected side-ref index, rebuilt from WAL during recovery, for
  PR1 GC/rewrite safety;
- require a side-ref prepare guard that blocks GC/rewrite/cleanup while side
  refs are being prepared but not yet committed to WAL;
- require segment metadata and durable cleanup manifests before missing
  collection WAL segments can be treated as safely cleaned;
- make `DB.Checkpoint` call registered collection checkpoint hooks and force
  publication of replayable collection WAL before reporting a clean collection
  WAL state;
- encode overlay compaction as a collection WAL maintenance transaction when it
  can race with user-visible collection writes.

## 2. Current Facts

### 2.1 Base WAL and Recovery

Current TreeDB cached-mode writes already have a WAL model:

- `Options.Durability` chooses durable, WAL-on relaxed, or WAL-off relaxed
  behavior.
- Commit-log records live under `maindb/wal`.
- Value-log records live in persistent value-log storage. The value log is not
  an ephemeral WAL.
- Commit-log batches can carry a nonzero `Seq`.
- Sequence-numbered commit batches form a fence for RID-backed records.
- Recovery scans value-log segments, builds a `RID -> ValuePtr` map, reads
  commit-log batches, sorts sequence-numbered batches by `Seq`, and replays
  complete batches with `WriteSync`.
- If a sequence-numbered commit batch references a missing RID, recovery skips
  that batch instead of publishing phantom pointers.
- Replayed commit-log files are removed only after successful replay.
- Value-log files remain persistent storage after replay.

This model is strong enough for normal key/value writes because the commit log
contains user-root key/value mutations. It does not currently encode a
collection root group as a first-class transaction.

### 2.2 Backend Root-Group Publishing

Collections publish several B-tree roots as one logical group. Relevant backend
APIs include:

- `PublishOrderedRootGroup`
- `PublishOrderedRootGroupWithSystemBuilder`
- `PublishOrderedRootDeltaGroupWithSystemBuilder`
- `PublishOrderedRootDeltaGroupWithSystemDeltaBuilder`
- `PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder`

The indexed collection flush path materializes root-local delta batches, then
calls `PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder`. The system-root
delta records the produced root ids in collection descriptors.

That backend publish path is useful and should be retained. The missing piece is
an ahead-of-publish collection transaction log that lets recovery finish or
discard in-flight collection root groups coherently.

### 2.3 Collection Write Domain

Current collection write-domain facts:

- Indexed collections use collection-local write memtables by default.
- Pending writes are visible in-process through the collection manager.
- Reads, unique checks, updates, and deletes merge mutable, queued, publishing,
  and persisted roots newest-to-oldest.
- Flush units are visibility and publish-amortization units, not durable log
  records.
- `BufferedIndexedAsyncFlush` can move queued units into a publishing state
  before backend root publication completes.
- `Collection.Flush`, `CollectionManager.FlushAll`, backend close hooks, and
  threshold-triggered synchronous publish paths drain pending work.
- `DB.Checkpoint()` currently does not flush a pending collection-local
  no-index insert. The pending document remains visible in-process and only
  publishes when `Collection.Flush()` runs.
- Existing tests verify that `Close` and `FlushAll` drain queued indexed flush
  units before reopen.

The current documented contract is flush-boundary durable, not durable-at-ack.
That contract is understandable for today's row collections, but it is too weak
as a foundation for column-store collections with external part files.

Current code also has a clear separation between what can be reused and what
must be added:

- backend ordered root-group publication is the right executor for final
  publish and recovery replay;
- current commit-log records are key/value records, not collection
  transactions;
- current collection write-domain state contains enough planning information to
  build root-local deltas, but it does not preserve those deltas as durable
  transaction bytes before acknowledgement;
- current `Flush`, `FlushAll`, close hooks, and checkpoint behavior are
  flush-boundary mechanisms, not collection-WAL transaction cleanup mechanisms;
- current recovery has a chronological slot for collection WAL replay after
  normal cached WAL replay and before state exposure, but no collection WAL
  replayer API.

### 2.4 Native Wire and Raft Planning

Native-wire R0-R2 work now establishes a separate logical replication layer:

- The native wire protocol is not the Raft log.
- Deterministic command entries are canonical logical command bytes for future
  Raft replication.
- Deterministic entries include metadata and collection mutations such as
  create collection, create index, drop index, insert batch, replace batch, and
  delete batch.
- Deterministic entries intentionally exclude transport frame ids, stream ids,
  deadlines, trace context, negotiated compression, response shaping, and local
  physical storage artifacts.
- `flush_collection`, `flush_all`, `checkpoint`, reads, cursors, stats,
  value-log GC, value-log rewrite, and physical maintenance remain local-only in
  v1 unless a future distributed barrier command gives them explicit consensus
  semantics.
- `ack_policy=raft_committed` is reserved for distributed mode and is rejected
  by the current single-node native-wire server.
- Raft storage, Raft apply, distributed reads, and persistent idempotency record
  storage are still unimplemented.

Those changes do not replace this collection WAL. They create an upstream
source of logical commands that, when applied on a node, must still use the
local collection WAL/root-delta recovery path before the node can safely claim
that the committed command is locally recoverable.

## 3. Definitions

Collection WAL transaction:

An atomic durable record describing one ordered collection root-group mutation.
It is not a logical SQL/JSON operation. It contains the root-local deltas needed
to publish the mutation after recovery.

Root group:

The set of B-tree roots that must change together for one collection mutation.
For existing collections this can include primary, template, index-state, and
secondary index roots. For future column-store collections this also includes
part descriptor, locator, delete, filter, and schema roots.

Side file:

A durable file or byte range referenced by a root group but not stored inline in
the collection WAL transaction. Current examples are value-log records and
large root-delta payloads. Future examples include column part files, filter
files, dictionaries, and delete bitmap files. Leaf-log/page-log records created
while publishing replayed B-tree pages are publish outputs, not collection WAL
side refs.

Applied watermark:

System-root metadata that records the highest contiguous collection sequence
published into backend roots for one `CollectionUID`. Recovery uses the watermark
to avoid double-applying transactions after a crash that occurs after root
publish but before WAL cleanup.

Durable-at-ack:

Under WAL-on profiles, once a collection write API returns success, recovery can
make that write visible after process crash. This does not imply an fsync power
loss guarantee unless the caller used a sync barrier and the selected durability
mode requires fsync.

Visible:

Observable by any collection read path, scan, uniqueness check, update planner,
delete planner, schema/index barrier, or pending-state merge. In WAL-on modes,
visibility implies recoverability.

Recoverable:

After process crash and read-write recovery, the transaction can be made visible
from backend roots or from a complete committed collection WAL transaction
without relying on pre-crash memory. For `DurabilityWALOnRelaxed`, the complete
collection WAL frame and every required side ref have reached a file-writer
flush boundary sufficient for a fresh process to open the files, read the exact
ranges, and verify recorded checksums. Bytes accepted only into in-memory writer
buffers are not recoverable.

WAL append commit point:

The exact point at which a collection WAL frame and commit marker are complete,
checksummed, ordered by `WALLSN`, and recoverable under the selected local
durability mode.

Side-ref prepared:

The referenced external bytes have been written, made readable by a new file
reader at the selected side-ref durability boundary, and verified against the
recorded checksum over exactly `[offset, offset+size)`.

Side-ref protected:

GC, rewrite, cleanup, compaction, and side-file maintenance must treat the ref as
reachable.

Published:

The transaction's root effects are represented in backend collection root
descriptors.

Checkpointed:

The backend checkpoint/meta boundary durably contains the root descriptors,
applied watermarks, and side-ref reachability tracking needed for cleanup.

Cleanable:

A collection WAL segment or side ref may be removed because every transaction
that references it is applied, checkpointed, and no live snapshot/read view can
reach it.

`CollectionSeq`:

A gap-free, monotonically increasing sequence number scoped to one
`CollectionUID`. It is the replay, dependency, and skip key.

`WALLSN`:

A globally monotonically increasing append position used for deterministic WAL
scan order, diagnostics, and segment cleanup accounting. It is not a dependency
ordering key and must not be used to skip a transaction for another collection.

`CollectionReadView`:

A pinned read snapshot covering backend snapshot state, collection catalog/root
metadata, collection-local pending state, derived index views, and every side ref
reachable from those states.

Close admission cut:

The point in `DB.Close` after which new collection mutations cannot be admitted
unless they are explicitly included in the close drain set. A racing mutation
must either fail with a closed error or be drained before close returns.

Clean WAL state:

No collection WAL transaction remains required for crash recovery. This is
stronger than "normal backend WAL has no files".

Column side-ref closure:

For any collection WAL transaction that publishes column-store descriptors,
locator roots, delete roots, filter roots, granule roots, count/visibility
metadata, compression metadata, or schema roots, the transaction's required side
refs are the complete transitive closure of every external file or byte range
referenced by root values added or modified by the transaction.

A root value must not reference a column file, manifest, delete bitmap, filter
file, dictionary file, or external compression metadata object unless that
object is listed as a required side ref in the same transaction or is already
reachable from a durable root visible to the transaction's base snapshot.

Optional or rebuildable filters may be omitted or rebuilt, but a published root
entry that names external filter bytes makes those bytes required. A side ref
marked `Required=false` must not be referenced by any published root value.

## 4. Goals

1. Make collection durability explicit and testable.
2. Provide a shared collection commit/recovery protocol for row-store and
   column-store collections.
3. Preserve existing collection visibility semantics for pending writes.
4. Preserve secondary index correctness across insert, update, delete, flush,
   async publish, recovery, and close.
5. Avoid re-running user update callbacks or JSON/template planning during
   recovery.
6. Reuse backend ordered root-group publish APIs for the final recovered commit.
7. Extend the existing WAL/RID fence idea to collection root deltas and side
   files.
8. Make `DB.Checkpoint`, `Collection.Flush`, `CollectionManager.FlushAll`, and
   `Close` coherent with collection WAL cleanup.
9. Provide a hard gate for production column-store collection implementation.
10. Add crash/fault tests that prove roots never point at missing side files.
11. Keep write overhead bounded with benchmarks before enabling stronger
    defaults broadly.
12. Define the boundary between future Raft logical command replication and
    local collection WAL/root-delta durability.

## 5. Non-Goals

- Do not replace the B-tree or ordered root-group publish APIs.
- Do not make WAL-off relaxed mode durable-at-ack.
- Do not promise fsync durability for non-sync collection APIs.
- Do not replay logical collection operations by re-parsing documents or
  re-running update callbacks.
- Do not persist in-memory helper structures such as unique-value probe maps as
  independent data. They must be reconstructable from WAL transactions or roots.
- Do not build a separate private WAL only for column-store collections.
- Do not require old pre-alpha directories to remain cross-version compatible.
- Do not use collection WAL transactions as Raft log entries. Raft entries are
  logical deterministic commands; collection WAL transactions are local storage
  apply artifacts derived from those commands.
- Do not make native-wire acknowledgement or response-shaping options part of
  recovered logical collection state.

## 6. Target Durability Contract

### 6.1 Mode Matrix

| Mode | Collection write acknowledgement after this spec |
|---|---|
| `DurabilityDurable` | For non-sync collection APIs, write success means the collection WAL transaction has reached the configured local WAL append/flush boundary needed for process-crash recovery. Non-sync APIs do not imply power-loss durability. Sync barriers additionally fsync according to durable mode. |
| `DurabilityWALOnRelaxed` | Write success means the collection WAL transaction has left process-local memory and is recoverable under the same process-crash assumptions as the existing WAL-on relaxed commit log. It does not claim power-loss durability. |
| `DurabilityWALOffRelaxed` | No durable-at-ack promise. Collection writes remain flush/checkpoint/close-boundary durable, and docs must say so directly. In this mode, `visible` acknowledgements are process-local visibility only. |

Recoverable means readable by a fresh process after process crash. For
`DurabilityWALOnRelaxed`, success requires that the complete collection WAL
frame and every required side ref have reached a file-writer flush boundary
sufficient for fresh-process reads. For `DurabilityDurable` sync barriers,
success additionally requires the configured fsync boundary for the WAL frame
and required side refs. Bytes accepted only into in-memory writer buffers are
not recoverable.

If the engine cannot append the collection WAL transaction in a WAL-on mode, the
collection write must fail before becoming visible to any reader, planner, or
pending-state merge. "Before success is returned" is not strong enough: a
concurrent read must not observe a write whose WAL transaction is not already
recoverable.

After the WAL commit marker reaches the durability boundary required by the
selected mode, the mutation is committed for recovery. The implementation must
not return an ordinary retryable mutation error after that point. If the process
crashes before the caller observes the response, recovery may expose the
mutation; this is the standard committed-before-response ambiguity. If
post-commit bookkeeping cannot complete, the implementation must report a
commit-ambiguous or fatal error, not a normal mutation error that invites a
blind retry.

### 6.2 API Boundaries

`Collection.Insert`, `InsertBatch`, `Update`, `UpdateBatch`, `Delete`, and
`DeleteBatch`:

- In WAL-on modes, successful return means the root-delta transaction is
  recoverable.
- The write may still be pending in the collection write domain for read
  performance and publish amortization.

`Collection.Flush`:

- Drains the collection write domain into backend roots.
- Advances the applied watermark for all published collection WAL transactions.
- Enables collection WAL cleanup after the backend checkpoint boundary that
  contains the watermark is durable.

`CollectionManager.FlushAll`:

- Applies the same rule across all known write domains.
- Must wait for in-flight async indexed publishing units.

`DB.Checkpoint`:

- Must become a full database durability/cleanup boundary for collection WAL.
- Must call a backend-owned collection WAL checkpoint service, or registered
  hooks that are coordinated by such a service.
- PR1 checkpoint handling must close admission for the checkpoint cut, wait for
  in-flight collection writes admitted before the cut, wait for in-flight async
  publish, drain all known write domains through `FlushAll`, publish root
  groups, advance applied watermarks in backend commits, sync the backend
  boundary required by the selected mode, and only then allow collection WAL
  cleanup.
- Writes admitted after the checkpoint cut may remain in retained collection WAL,
  but they are not part of the checkpoint-covered prefix.
- It must not report a clean WAL state while collection WAL transactions remain
  needed for recovery.
- If a future checkpoint mode chooses not to publish a transaction, it must keep
  the transaction and all required side refs protected and report nonzero
  collection WAL debt.

`DB.Close`:

- Must establish a close admission cut, reject or explicitly include every
  racing collection write, wait for admitted in-flight writers, drain in-flight
  async publishers, publish replayable collection WAL transactions, advance
  applied watermarks, reach the selected durability boundary, and only then allow
  cleanup before backend close.
- No collection write may return success after the close admission cut unless it
  is included in the close drain and is visible after reopen.
- Must not discard a collection WAL transaction before its applied watermark and
  cleanup boundary are safely published.
- Close must use the same backend-owned collection WAL publish/checkpoint path
  as `DB.Checkpoint`; manager-local close hooks may drain visibility domains,
  but they are not sufficient as the sole source of WAL recovery truth.

Read-only open after crash:

- Read-only open cannot run mutating collection WAL replay.
- If unapplied committed collection WAL is present, read-only open must fail
  with a clear recovery-required error unless a future explicit stale-read-only
  mode is added.
- A stale-read-only mode, if added, must be named as stale and must not claim
  durable-at-ack visibility for unapplied collection WAL writes.

Native-wire `ack_policy` is request/response policy only. It must not be
encoded into deterministic command entries, must not affect logical
state-machine results, and must not affect collection WAL replay.

- `visible` means the mutation is visible through the serving process or owning
  write domain. In WAL-on collection modes after this spec, `visible` still
  waits for the local collection WAL transaction to be recoverable because the
  underlying collection API is durable-at-ack. In WAL-off mode, `visible` is not
  crash-durable.
- `flushed` means affected collection state has been published into backend
  roots and the collection WAL applied watermark for those transactions has
  advanced in the same backend commit.
- `synced` means `flushed` plus the backend checkpoint/fsync boundary required
  by the configured local durability mode. It must not be silently downgraded.
- Collection WAL append and collection root publication must both expose
  explicit flush/sync options so these acknowledgement policies can be
  implemented without relying on implicit backend defaults.
- `raft_committed` is a cluster-mode policy, not a local WAL policy. It requires
  consensus commit plus the cluster's defined local apply durability rule. Local
  collection WAL alone cannot satisfy it.
- Local policies are ordered as `visible < flushed < synced`.
  `raft_committed` is a separate cluster policy and must not be implemented as a
  simple numeric extension of the local ordering unless a future cluster spec
  explicitly defines the implied local durability level.

### 6.3 PR1 Invariants

These invariants are normative for PR1 and are the acceptance criteria for
turning the design into code:

1. Visibility implies recoverability. In WAL-on modes, any collection mutation
   visible to a read, scan, uniqueness check, update planner, delete planner, or
   schema/index barrier must be recoverable from backend roots or from a
   committed collection WAL transaction.
2. Side refs precede WAL commit. A collection WAL transaction may reference a
   side ref only after the bytes are readable at the selected durability boundary
   and registered as protected against GC, rewrite, cleanup, and compaction.
3. Visible install follows WAL commit. Write-domain install is a short critical
   section after the WAL commit point. If WAL commit fails, the mutation leaves
   no read-visible pending state, uniqueness reservation, queued unit, publishing
   unit, or schema/index barrier state.
4. `CollectionSeq` is gap-free per collection. Recovery and publication process
   transactions for a collection in `CollectionSeq` order. PR1 has no
   same-collection independence skip.
5. Watermark coverage is contiguous and atomic. A backend publish may advance
   `applied_collection_seq[CollectionUID]` only over the next contiguous prefix,
   and descriptor updates plus watermark updates must be one backend commit.
6. Cleanup requires applied plus checkpointed. A collection WAL segment or side
   ref is cleanable only after every referencing transaction is covered by a
   durable applied watermark, the backend checkpoint/meta boundary containing the
   watermark is durable, and side-ref reachability has incorporated the published
   roots.
7. `DB.Checkpoint` is collection-aware. PR1 checkpoint drains and publishes
   checkpoint-admitted collection writes before reporting a clean collection WAL
   state. A future non-publishing checkpoint must report collection WAL debt.
8. `DB.Close` is an admission barrier. A write racing with close either fails
   before visible install or is included in the close drain and survives reopen.
9. Snapshot readers pin pending state. A collection read view must pin backend
   snapshot state, collection-local pending units, side refs, and derived indexes
   for the lifetime of the read.
10. No waits under state locks. The implementation must not wait for backend
    publish, checkpoint, WAL I/O, side-ref GC, async publish completion, or
    value-log rewrite while holding a lock that those operations need to make
    progress.

### 6.4 Column-Store Gate

Production column-store collection implementation is blocked until this
contract is implemented and tested. Allowed pre-gate work:

- docs and design review;
- benchmark fixtures;
- pure compression codecs;
- reusable fast filter/search packages;
- isolated `TCS1` encode/decode tests that do not publish collection roots.

Blocked pre-gate work:

- persistent column-store collection APIs;
- column part descriptor roots;
- secondary indexes pointing at column-store rows;
- column-file side refs in published roots;
- any crash/reopen safety claim for column-store writes.

## 7. Architecture

### 7.1 Chosen Model: Root-Delta Transaction WAL

The collection WAL should log root deltas, not logical operations.

Reasons:

- Recovery must not re-run user callbacks.
- Recovery must not depend on current JSON/template extraction code producing
  identical root deltas after a future code change.
- Secondary index updates are already planned as root-local mutations.
- The same transaction shape works for row-store roots and future column-store
  roots.
- Root-delta replay can reuse existing backend publish APIs.

### 7.2 Transaction Shape

Local physical collection WAL record:

A collection WAL transaction is a replayable physical root-group transaction.
It is not a logical insert/update/delete command. Recovery must not rerun user
callbacks, predicates, JSON extraction, template extraction, secondary-index
planning, or future column-store planning.

The normative byte encoding for collection WAL segments, frames, transaction
payloads, side-ref sections, cleanup records, and required feature gates lives
in `storage-format.md`. The struct below is the semantic payload model used by
the write-path and recovery contract, not permission to encode fields in ad hoc
Go-struct order. No implementation PR may write collection WAL bytes until
`storage-format.md` contains the exact v1 wire format and golden fixture
expectations.

```text
CollectionWALTransaction {
    Version                  uint16

    // Identity and ordering.
    WALLSN                   uint64
    CollectionUID            uuid128
    CollectionName           string diagnostic
    CollectionGeneration     uint64
    CollectionSeq            uint64
    DependsOnCollectionSeq   uint64

    // Catalog guard.
    SchemaEpoch              uint64
    SchemaVersion            uint32 optional for row-store, required for column-store
    BaseCommitSeq            uint64
    BaseSystemRootID         uint64
    BaseCatalogDigest        bytes
    CatalogDigest            bytes
    RootDescriptorEpochs     map[root-name]uint64
    MutationClass            uint8

    // Physical mutation.
    RootDeltas               []CollectionRootDelta
    SystemDeltaTemplate      CollectionSystemDeltaTemplate

    // Required external bytes named by RootDeltas or SystemDeltaTemplate.
    SideRefs                 []CollectionSideRef

    // Local observability only; excluded from replay identity.
    CreatedUnixNanos         int64
    Stats                    CollectionWALStats

    ReplayDigest             bytes
    RecordChecksumCRC32C     uint32
}

CollectionRootDelta {
    RootName                 string
    RootKind                 uint16
    RootDeltaOrdinal         uint32

    BaseRootID               uint64
    BaseRootGeneration       uint64
    BaseRootDescriptorEpoch  uint64
    BaseCollectionSeq        uint64

    StoragePolicy            uint8
    DeltaEncoding            uint8
    IncludeDeletedOnColdBuild bool

    InlineDelta              bytes optional
    DeltaPayloadRef          CollectionSideRefRef optional

    EntryCount               uint32
    KeyBytes                 uint64
    ValueBytes               uint64
    DeltaDigest              bytes
}

CollectionSystemDeltaTemplate {
    BaseSystemRootID         uint64
    BaseCommitSeq            uint64
    Preconditions            []SystemPrecondition
    DescriptorOps            []DescriptorOp
    WatermarkOp              AppliedWatermarkOp
    MetaOps                  []SystemMetaOp optional
}

DescriptorOp {
    Op                       ReplaceRoot | AppendOverlayRoot | ClearOverlayRoots |
                             ReplaceRootList | PutMeta | DeleteMeta
    Key                      bytes
    ExpectedValueDigest      bytes optional
    RootDeltaOrdinal         uint32 optional
    RootIDPlaceholder        uint32 optional
    RootListPlaceholders     []uint32 optional
}

AppliedWatermarkOp {
    CollectionUID            uuid128
    AdvanceCollectionSeqTo   uint64
    OptionalWALLSN           uint64
}

CollectionSideRef {
    RefClass                 uint8
    ClassVersion             uint16
    Critical                 bool
    RefID                    uint64
    RelativePath             string optional
    Offset                   uint64
    Size                     uint64
    ChecksumKind             uint8
    Checksum                 bytes
    Required                 bool
}

CollectionWALAppendOptions {
    Flush                    bool
    Sync                     bool
}

OrderedRootPublishOptions {
    Sync                     bool
    WALLSNRange              [2]uint64
    CollectionSeqRange       [2]uint64
    WatermarkOp              AppliedWatermarkOp
}
```

This record is not portable across replicas. `CollectionUID` is the durable
collection identity; `CollectionID` is reserved for transient in-memory handles
or legacy text and must not name a separate durable identity. `CollectionName`
is diagnostic only and must not be used as the replay identity. `WALLSN`,
`CollectionSeq`, `BaseSystemRootID`,
`BaseRootID`, root names as resolved by local storage, side-ref paths/RIDs,
timestamps, stats, and other local observability metadata are meaningful only in
the database directory that wrote the record. A follower applying the same
deterministic command entry may produce different local root ids, side refs,
file names, and collection WAL bytes.

All collection WAL transactions must carry enough catalog identity to make
replay schema-stable. `CollectionGeneration` guards collection incarnation and
drop/recreate. `SchemaEpoch` guards catalog-level schema/index evolution.
`SchemaVersion` guards physical column decode for column-store roots.
`BaseCatalogDigest`, `CatalogDigest`, and `RootDescriptorEpochs` are mandatory
replay guards. The
`MutationClass` values are versioned and include at least `row_insert`,
`row_update`, `row_delete`, `column_insert`, `column_update_delta`,
`column_delete`, `column_compaction`, `column_recompression`, and
`schema_change`.

A transaction is replayable only when:

1. the record checksum is valid;
2. every required side ref is present and passes integrity checks;
3. `CollectionUID`, `CollectionGeneration`, `SchemaEpoch`, `CatalogDigest`,
   base root ids, base root generations, and root descriptor epochs match the
   current replay state or an explicit replay accumulator state;
4. `DependsOnCollectionSeq` is covered by the collection applied watermark or by
   the active accumulator;
5. the `SystemDeltaTemplate` can be instantiated with the produced root ids and
   its preconditions hold.

The same backend commit that writes descriptor operations from
`SystemDeltaTemplate` must also advance the collection applied watermark. A
descriptor update without the watermark, or a watermark update without the
descriptor update, is not a valid collection WAL publish.

`WALLSN` is a global append position, not the replay dependency key. The
collection-local `CollectionSeq` and `DependsOnCollectionSeq` fields define the
dependency domain. Recovery may use `WALLSN` to scan and diagnose records, but
it must not use a higher `WALLSN` from another collection to skip a lower
unapplied `CollectionSeq`.

`CreatedUnixNanos` and `Stats` are local observability metadata. If an
implementation retains an acknowledgement-related field, it must be encoded as
an ignored observability field, not as replay input. Replay, idempotency,
catalog guards, Raft command digests, and logical state digests must not depend
on it.

Two digests are required:

- `RecordChecksumCRC32C`: covers the whole encoded record for corruption
  detection;
- `ReplayDigest`: covers replay-critical fields only: identity, dependencies,
  root deltas, side refs, system delta template, and preconditions.

### 7.3 Collection Identity and Root Generations

Every collection must have a stable `CollectionUID` persisted in collection
metadata. `CollectionID` may be used only for transient in-memory handles or
legacy explanatory text; it must not introduce a second durable identifier.
`CollectionName` is diagnostic and must not be the replay identity. Dropping and
recreating a collection with the same name creates a new `CollectionUID` and a
new `CollectionGeneration`. Renaming a collection, if supported, preserves
`CollectionUID` and increments only the catalog metadata required by the rename.

Every schema or index metadata change increments `SchemaEpoch`. Every root
descriptor change increments that root's `RootGeneration` and descriptor epoch.
Overlay descriptor changes and overlay compaction also increment the affected
root generation and descriptor epoch.

A collection WAL transaction records `CollectionUID`, `CollectionGeneration`,
`SchemaEpoch`, `CatalogDigest`, `RootDescriptorEpochs`, `BaseRootID`, and
`BaseRootGeneration` for every root it mutates. Recovery must reject or block a
transaction whose identity or generation guard does not match the replay state,
unless the mismatch is explicitly explained by the active replay accumulator. An
unapplied transaction whose `CollectionUID` is absent from the recovered catalog
must not be replayed into a collection with the same name; PR1 must stop open or
quarantine according to a documented recovery-admin procedure rather than
silently applying the transaction to a later incarnation.

Maintenance that rewrites root ids or descriptor values must not run for a
collection with unapplied WAL transactions unless the maintenance operation is
itself encoded as a collection WAL transaction.

Schema/index changes are collection-sequenced operations. Before a schema change
becomes visible, all lower `CollectionSeq` transactions must either be published
and watermarked, or the schema change itself must be a collection WAL
transaction that depends on the previous `CollectionSeq` and carries schema and
root descriptor changes atomically.

### 7.4 Root Delta Encoding

The WAL format must not depend on transient in-memory `batch.Batch` internals.
The backend may convert WAL deltas into `batch.Batch` at replay or publish time,
but the durable encoding is versioned independently.

```text
RootDeltaEncodingV1:
  entries sorted by bytewise key;
  keys unique per root delta after intra-transaction merge;
  stable EntryOrdinal for fold/replay diagnostics;
  entry op: PutInline | PutValuePtr | DeleteTombstone;
  key length + key bytes;
  value length + value bytes OR stable ValuePtr encoding;
  per-entry flags;
  IncludeDeletedOnColdBuild flag recorded on the root delta.
```

`IncludeDeletedOnColdBuild` must be true for overlay and cold-build roots whose
tombstones suppress older roots. Delete/tombstone entries are durable logical
entries in the root delta, not an artifact of the current builder.

Small deltas may be inline in the WAL transaction. Large deltas should use a
`RootDeltaPayload` side ref. The WAL encoder must preserve deterministic merge
rules: if two accumulated deltas affect the same key, the later
`CollectionSeq` wins, and deletes suppress older values according to normal
collection visibility rules.

A collection WAL transaction has a configured maximum encoded byte size,
maximum root-delta count, and maximum decoded entry count. Larger root deltas
must spill to `RootDeltaPayload` side refs with the same side-ref preparation
and protection rules as value-log or column-file refs. Recovery must be able to
decode and fold root deltas in bounded memory; a transaction that exceeds the
configured limits fails before acknowledgement or uses side-payload mode.

Root kinds are explicit, versioned values. PR1 row-store root kinds include
`primary`, `template`, `index_state`, `secondary`, `overlay`,
`overlay_descriptor`, `delete`, and `metadata`.

Column-store root kinds are part of the production gate and include at least:

- `primary_locator`: authoritative `id -> row_locator` primary root;
- `column_parts`: immutable part descriptor root;
- `column_granules`: optional part/granule mark root;
- `column_locator`: optional auxiliary primary-id locator root;
- `column_deletes`: tombstone and delete-bitmap descriptor root;
- `column_schema`: physical column schema and schema-evolution root;
- `column_filter`: persistent filter descriptor root;
- `column_count_visibility`: exact count and visibility aggregate metadata root;
- `column_compression_metadata`: external compression metadata or dictionary
  descriptor root when not represented in part descriptors;
- `secondary`: existing secondary index roots, column-store aware;
- `template` and `index_state`: existing template/index-state roots when the
  collection format requires them.

Recommended enum names are `RootKindPrimaryLocator`,
`RootKindColumnParts`, `RootKindColumnGranules`, `RootKindColumnLocator`,
`RootKindColumnDeletes`, `RootKindColumnSchema`, `RootKindColumnFilter`,
`RootKindColumnCountVisibility`, and `RootKindColumnCompressionMetadata`.

Every locator, part descriptor, granule/mark, delete, filter, count/visibility,
schema, compression metadata, primary, and secondary-index change for one
column-store mutation must be represented in the same collection WAL transaction
and the same backend root-group publish.

### 7.5 File Class

Use a dedicated logical collection WAL class. The implementation may reuse
commit-log framing utilities, but the replay semantics are different from
normal user-root commit records.

The canonical file layout, segment magic, frame header, transaction payload
header, section table, commit trailer, compression fields, checksum coverage,
feature gates, and decoder outcome taxonomy are defined in `storage-format.md`.
The collection WAL reader must be a dedicated reader with those outcomes; it
must not inherit cached commit-log RID skip behavior or the commit-log
length/CRC-only frame semantics.

Canonical main-DB layout:

```text
Dir/maindb/wal/
    collection-l<lane>-<seq>.log
```

Semantic segment rules:

- Each segment contains framed `CollectionWALTransaction` records.
- Each transaction has exactly one `WALLSN` and one collection-local
  `CollectionSeq`.
- Transactions are sorted by `WALLSN` during segment scanning, with file
  order as a deterministic tie breaker only for malformed legacy segments.
- A short read or EOF is accepted only for the terminal frame of the active
  non-cleaned segment in that lane.
- A complete frame with a bad frame checksum or transaction checksum is hard
  corruption, even when it is the final frame.
- A short read before a later complete frame, before a later non-cleaned segment,
  or in a sealed segment is hard corruption.
- Hard corruption before the terminal tail fails recovery.

Every sealed collection WAL segment must have durable segment metadata:

```text
CollectionWALSegmentMeta {
    Version                       uint16
    Lane                          uint16
    SegmentSeq                    uint64
    MinWALLSN                     uint64
    MaxWALLSN                     uint64
    ParticipantCollectionUIDs     []uuid128
    FirstFrameOffset              uint64
    LastCompleteFrameEndOffset    uint64
    Sealed                        bool
    MetadataChecksumCRC32C        uint32
}
```

Cleanup requires durable cleanup metadata before a missing segment can be
accepted during startup:

```text
CollectionWALCleanupRecord {
    Version                       uint16
    CleanupEpoch                  uint64
    Lane                          uint16
    SegmentSeq                    uint64
    MinWALLSN                     uint64
    MaxWALLSN                     uint64
    ParticipantCollectionUIDs     []uuid128
    State                         planned | unlinked | dirsynced
    RecordChecksumCRC32C          uint32
}
```

Startup treats a missing collection WAL segment as valid only when a durable
cleanup record proves the segment's whole transaction range was safely cleaned.
A missing non-cleaned segment is recovery corruption and must stop open. Mixed
collection segments are cleanable only when every transaction in the segment is
covered by durable per-collection watermarks, or after a future segment-splitting
protocol rewrites the remaining live transactions into a new crash-tested
segment.

### 7.6 Applied Progress and Skip Rules

The system root stores applied progress by `CollectionUID`:

```text
treedb/collection-wal/global-contiguous-cleanup-wallsn -> uint64 optional
treedb/collection-wal/collection/<collection-id>/applied-collection-seq -> uint64
```

`WALLSN` is the globally monotonic append position. It is useful for segment
ordering, diagnostics, and cleanup scans. `CollectionSeq` is the replay and skip
key. Recovery may skip a transaction only when
`txn.CollectionSeq <= applied-collection-seq` for the same `CollectionUID` and
the transaction's `CollectionUID`/`SchemaEpoch` guard matches the catalog
history covered by that watermark.

A global contiguous `WALLSN` cleanup marker may be used only as a cleanup-scan
optimization when every lower `WALLSN` is known applied or intentionally absent.
It must never cause recovery to skip an unapplied lower `CollectionSeq` for any
collection, and PR1 cleanup must still verify every transaction in a segment
against the relevant per-collection applied watermark before deleting the
segment.

Watermarks advance only in the same backend commit that updates collection root
descriptors for the root group. A watermark value means every collection
transaction up to that `CollectionSeq` is fully represented in persisted roots
and descriptor metadata.

PR1 uses per-write WAL transactions with per-collection dependency chains:

```text
WALLSN                   uint64  // global append position for diagnostics/cleanup
CollectionSeq            uint64  // strictly increasing per CollectionUID
DependsOnCollectionSeq   uint64  // previous collection transaction observed
```

Recovery must process each collection in contiguous `CollectionSeq` order. A
transaction may be replayed only if all lower collection sequences are applied,
present in the active replay accumulator, or explicitly known not to exist for a
new collection. A missing complete transaction blocks later transactions for the
same collection.

PR1 does not support true multi-collection collection WAL transactions. Any API
or internal planner that would need to mutate multiple collections atomically
must reject the operation before preparing side refs or appending collection WAL.
A future multi-collection format must declare all participant collections,
validate every participant dependency, and publish all participant root
descriptor changes plus all participant watermarks in one backend meta/system
commit.

### 7.7 Side-Ref Fences

`CollectionSideRef` names external bytes that must exist before a collection WAL
transaction can be replayed. Side refs are input dependencies of the
transaction, not output files produced by backend root publication.

Side-ref classes:

- `ValueLogRecord`: value-log bytes embedded in root-delta values.
- `LeafLogRecord`: pre-existing outer-leaf or child-record bytes embedded in a
  root-delta value or descriptor.
- `RootDeltaPayload`: serialized root-delta payload stored outside the WAL
  record.
- `ColumnManifest`: manifest or prepare record naming a column part's external
  files.
- `ColumnSubstreamFile`: immutable column payload or packed update-delta
  substream file.
- `ColumnFilterFile`: persistent filter file.
- `ColumnDeleteBitmapFile`: delete/tombstone bitmap file.
- `ColumnDictionaryFile`: compression or encoding dictionary file.
- `ColumnMetadataFile`: external compression metadata, mark metadata, or future
  side metadata file.

Leaf-log or page-log records produced while replay builds new B-tree roots are
not `CollectionSideRefs`. They are publish outputs and must be flushed or synced
by the backend commit path before descriptors are published. A pre-existing
leaf-log child ref embedded in a root delta is a `LeafLogRecord` input side ref
and must be validated like a value-log ref.

The WAL encoder must derive required side refs from the final root-delta payload
and from column descriptors. A caller-provided `SideRefs` list is insufficient
unless the encoder verifies it against the payload.

Declared side refs are not trusted. During WAL append and recovery, TreeDB must
derive the canonical embedded required side-ref set by decoding root deltas,
root descriptors, column part descriptors, filter descriptors, delete bitmap
references, dictionary references, value-log pointer payloads, and leaf-log
pointer payloads. The declared required side-ref set must match the canonical
set. Missing embedded refs, undeclared embedded refs, and declared refs that are
not embedded are invalid unless a future format version defines a precise
optional-ref class.

Column-store descriptor validation is transitive. The canonical embedded
side-ref set must include every external file or byte range reachable from added
or modified column descriptors, manifests, substream block directories, filter
descriptors, delete bitmap descriptors, dictionary ids, compression metadata,
granule/mark roots, locator roots, and count/visibility metadata. Recovery must
reject a transaction whose root delta publishes any column descriptor graph that
is not side-ref closed.

Side refs must be sorted and unique by `(RefClass, RefID, RelativePath, Offset,
Size)`. Recovery must reject duplicate required refs with conflicting checksums
or metadata. External dictionaries and compression metadata are required side
refs whenever a published payload cannot decode without them. Codec ids,
parameters, dictionary ids, dictionary registry/version, and codec registry
version must be persisted in descriptors or manifests and protected until every
dependent payload is unreachable.

Side-ref compatibility rule:

Each side ref encodes `RefClass`, `ClassVersion`, `Critical`, `FileID`,
`Offset`, `Length`, `ChecksumKind`, `Checksum`, and optional advisory
`RelativePath`. Unknown critical `RefClass` or `ClassVersion` makes the
transaction unsupported-required and recovery fails closed. Unknown noncritical
refs may be ignored only if no replay-critical section references them and the
transaction feature bits explicitly allow skipping them. Cleanup, GC, rewrite,
and quarantine must treat unknown refs from complete unwatermarked transactions
as protected until the transaction is watermarked and checkpointed or an
operator performs a destructive rebuild.

Side refs are resolved by `RefClass` and `RefID`/`FileID` through a file-class
registry. `RelativePath`, when encoded, is advisory validation data. It must
normalize to a relative path with no `..` components, no absolute prefix, no
empty path elements after cleaning, and no symlink traversal. It must remain
confined to the owning file-class root and match the `RefID`/`FileID` registry
entry. Cleanup and quarantine operations must refuse paths outside the owning
file-class root, symlinks, and path/FileID mismatches.

Prepared, readable, and integrity-passing means the file-class writer has made
the referenced range visible to a fresh file reader, and class-specific
validation passes:

- `ValueLogRecord`: segment exists, offset/length are in bounds, RID or grouped
  sub-index matches when present, and record checksum passes.
- `LeafLogRecord`: leaf segment exists, referenced child bytes are in bounds,
  and checksum or page/record validation passes.
- `RootDeltaPayload`: payload file or value-log record exists, length and
  checksum match, and decoding yields the `DeltaDigest` named by the root delta.
- `ColumnManifest`: final manifest exists, checksum matches, and the manifest's
  collection id, part id, schema version, file list, and side-ref digest match
  the root descriptor.
- `ColumnSubstreamFile`: final file path exists, size/checksum match the
  descriptor or manifest, and compression/dictionary metadata validates.
- `ColumnFilterFile`: filter file exists, checksum matches, and the filter
  header names the expected column/part generation.
- `ColumnDeleteBitmapFile`: bitmap file exists, checksum matches, and row-range
  and generation metadata match the descriptor.
- `ColumnDictionaryFile`: dictionary bytes exist, checksum matches, and
  codec/dict id match every dependent payload.
- `ColumnMetadataFile`: metadata bytes exist, checksum matches, and the owning
  descriptor names the expected metadata class and generation.

Collection WAL append must not start until every required side ref in the
transaction is prepared under this definition. In-memory append buffers,
unflushed value-log buffers, temp files that are not fresh-process-readable, and
side files whose checksums have not been verified do not satisfy the boundary.

Column part prepare protocol:

A column part builder must write files into a prepare namespace before the WAL
transaction is appended:

```text
Dir/maindb/columns/.prepare/txn-<wallsn>/part-<part-id>/
```

The prepare group must contain a manifest or prepare record that names every
file/range, final relative path, size, checksum, `PartID`, `FileID`,
`SchemaVersion`, compression pipeline, dictionary ids, and owning
`CollectionUID`.

Before a WAL-on transaction can be acknowledged:

1. all required column files and ranges have been written;
2. checksums and lengths have been computed;
3. the manifest or prepare record has been written and included in the side-ref
   closure;
4. durable mode has fsynced required files and containing directories according
   to the configured durability boundary;
5. the implementation has atomically renamed or otherwise finalized prepared
   files to their final paths and fsynced containing directories when final-path
   descriptors are used;
6. the WAL transaction containing the matching canonical side-ref set has
   reached the configured WAL boundary.

A root descriptor must not be published before the referenced final bytes are
readable. Temp-only files with no committed WAL/root reference are
orphan-prepared and may be quarantined or deleted after recovery.

A complete, unwatermarked transaction with a missing or checksum-invalid
required side ref is a recovery error in WAL-on durable/recoverable modes and
must either fail the database open or quarantine the affected collection as
unavailable under an explicit recovery-admin path. Only a clearly incomplete
tail transaction whose frame was not made recoverable may be ignored. Recovery
must not skip a complete collection transaction and continue applying later
transactions for the same `CollectionUID`.

Collection WAL side refs are also retention roots. Value-log GC, value-log
rewrite, column-file cleanup, and future side-file maintenance must treat every
required side ref in an unapplied or not-yet-cleanable collection WAL
transaction as reachable until the transaction is covered by a safe applied
watermark and the checkpoint/meta boundary needed for cleanup is durable.

PR1 maintains a protected side-ref index updated atomically with WAL append and
WAL cleanup. GC/rewrite must consult this index plus persisted roots. Recovery
rebuilds the index by scanning collection WAL before any maintenance starts. If
a value-log segment or column file is referenced only by pending collection WAL,
maintenance must keep it in place or abort. A future rewrite mode may rewrite
protected refs only if it publishes replacement side refs and collection WAL
metadata atomically under an explicit crash-tested protocol.

Collection WAL append and side-ref protection registration are one critical
section with respect to value-log GC, value-log rewrite, leaf-log GC, column-file
GC, and side-file cleanup. A maintenance pass must either see the transaction's
side refs or be ordered before the transaction can be acknowledged.

A writer preparing side refs must hold a side-ref prepare guard from before the
first side-ref append/write until either the collection WAL frame referencing
those side refs is durable, or the operation aborts and the prepared refs are
deleted or recorded as orphan-prepared. GC, value-log rewrite, leaf-log GC,
column cleanup, checkpoint cleanup, and full storage compaction must acquire the
conflicting maintenance side of this guard before computing reclaimable refs.
This prevents in-process maintenance from deleting side refs in the window after
side-ref creation but before WAL commit.

For every required side ref, the WAL-on write path order is:

1. write side-ref bytes;
2. reach the selected side-ref durability boundary: process-crash-readable for
   WAL-on relaxed mode, and the configured flush/sync boundary for durable sync
   modes;
3. register the side ref in the protected collection WAL retention set;
4. append the collection WAL transaction and commit marker that reference it;
5. install the transaction into read-visible pending state.

The protected-retention registration must happen before the commit marker can be
considered replayable. If the WAL append aborts before the commit marker, the
prepared refs are orphan-prepared and may be quarantined or deleted only after
the guard proves no committed WAL transaction or published root references them.

### 7.8 Commit Marker and Ack Boundary

A collection WAL record is replayable only after its commit marker is complete.
The implementation must complete validation, side-ref preparation, side-ref
flush/sync required by the selected durability mode, memory reservation, and
hidden write-domain staging before appending the commit marker.

Hidden staging is private. It must not be reachable from any collection read,
scan, uniqueness check, update/delete planner, schema/index barrier, queued unit,
publishing unit, or pending-state merge before the commit marker is complete.

After the commit marker reaches the required durability boundary, the complete
collection WAL transaction is the logical commit point. The implementation must
not return an ordinary mutation error after that point. It must either stage and
return success, return an explicit committed/ambiguous status that tells the
caller the mutation may recover after restart, or terminate before a response is
observed. If the process crashes before the client observes the response,
recovery may expose the mutation; this is committed-before-response ambiguity,
not an incomplete transaction.

The visible in-memory staging step after the commit marker must be non-failing.
If an implementation cannot guarantee that, it must use a two-phase in-memory
reservation protocol or expose a commit-ambiguous/fatal error instead of a
normal retryable mutation error.

### 7.9 Native Wire and Raft Layering

The collection WAL is below native-wire and Raft:

```text
native-wire request
    -> deterministic command entry for replicated writes
    -> Raft log entry in cluster mode
    -> local TreeDB state-machine apply
    -> local collection WAL root-delta transaction
    -> backend root publish and applied watermark
```

The boundary is strict:

- Native-wire deterministic entries replicate logical command input.
- Raft may store those deterministic entries directly or wrap them in
  consensus metadata.
- Collection WAL transactions record local root deltas and side refs generated
  by applying the logical command on one node.
- Collection WAL transactions must not be sent through Raft or compared
  byte-for-byte across replicas.
- Replica equality must be judged by logical catalog, collection contents,
  index definitions, idempotency records, and declared logical state digests,
  not by local value-log offsets, column-file names, root ids, flush timing, or
  WAL transaction bytes.

Collection WAL golden files test the local storage format only. They are not
cross-replica fixtures and must not require byte-identical apply output across
nodes.

On a future follower, applying a committed deterministic entry should derive the
same logical mutation outcome but may produce different local physical files and
root ids. That is acceptable only if the local collection WAL makes the derived
root group recoverable before the node advertises the Raft entry as durably
applied under the cluster policy.

A committed Raft entry has three distinct states:

- `consensus_committed`: the Raft algorithm has committed the deterministic
  command-entry bytes.
- `locally_applied`: this node has evaluated the command against local TreeDB
  state.
- `locally_recoverable`: after restart, this node can reconstruct the applied
  logical outcome before serving reads or advertising the applied index.

Raft commit alone is insufficient for `locally_recoverable`.

After collection WAL is enabled, collection catalog metadata, root descriptor
metadata, idempotency records, catalog guard outcomes, and Raft/local
applied-index metadata that affect collection mutation replay must be advanced
in the same collection root-group commit or in a metadata transaction with an
explicit total order against collection WAL `CollectionSeq`/`WALLSN` records.
Normal cached commit-log replay must not silently move those metadata values
past a collection WAL transaction that was planned against an earlier catalog or
idempotency state.

## 8. Write Path

Every WAL-on collection mutation has three phases:

1. Private planning: root deltas, side refs, uniqueness reservations, publish
   inputs, and schema/index barrier state are built in objects unreachable from
   any collection read or planner.
2. Durable commit: required side refs are prepared/protected, `CollectionSeq`
   and `WALLSN` are assigned, and a complete collection WAL transaction reaches
   the selected recovery boundary.
3. Visible install: the already-committed transaction is installed into the
   collection write domain under a short state lock. This step must be
   non-failing or commit-ambiguous/fatal; it must not return an ordinary
   retryable mutation error.

For every WAL-on collection mutation:

1. Validate catalog identity, schema epoch, root descriptor generations,
   uniqueness, and mutation-specific preconditions.
2. Build final physical root deltas for every affected root. The deltas must
   already contain the exact inline values, stable `ValuePtr` encodings,
   tombstones, secondary-index deletes/puts, template/index-state changes,
   overlay changes, delete-root changes, and future column descriptor entries
   that recovery will publish.
3. Prepare and verify every required side ref. Flush or sync side refs according
   to the selected durability boundary before the WAL commit marker is
   considered replayable.
4. Reserve hidden write-domain state using the exact serialized root deltas or
   immutable decoded delta objects. Hidden means no collection read path,
   uniqueness helper, update/delete planner, queued flush unit, publishing unit,
   or schema/index barrier can reach it.
5. Append the collection WAL transaction and commit marker.
6. Mark the reserved write-domain state visible without further fallible work
   while holding the collection write-domain state lock only for the install.
7. Return success.

Flush, async publish, checkpoint, and close are publication and cleanup paths.
They must publish the exact WAL-backed root deltas and advance watermarks; they
must not re-plan, re-pointerize, or otherwise transform the acknowledged
transaction.

Direct synchronous publish paths are not exempt from the WAL-on transaction
model. In WAL-on modes they must either append a collection WAL transaction and
then publish/watermark it through the same collection-specific publish wrapper,
or be limited to `DurabilityWALOffRelaxed`/debug paths whose weaker contract is
explicitly documented. Uniform WAL-on behavior should log first, then publish.
This no-bypass rule applies to every collection write API path, including
`Insert`, `InsertBatch`, `Update`, `UpdateBatch`, `Delete`, `DeleteBatch`,
disabled-memtable paths, large-batch direct publish paths, direct update paths,
and delete-batch paths. An implementation may claim an equivalent durable
root-group/side-ref fence only if it is tested at the same crash points as the
collection WAL path.

### 8.1 No-Index Inserts

Current no-index inserts can remain buffered in `domain.table`. After this
spec, a WAL-on no-index insert must canonicalize the final primary-root entry
before acknowledgement. If the value is stored as a value-log pointer, the WAL
transaction records the stable `ValuePtr` bytes and the required
`ValueLogRecord` side ref; flush must not later pointerize the raw document into
a different physical value.

No-index buffered writes must enter the same collection mutation serialization
path as indexed writes before allocating `CollectionSeq` or making pending state
visible. Bypassing the mutation serialization path makes per-collection ordering
and committed-before-visible guarantees too hard to verify.

### 8.2 Indexed Inserts

Indexed insert planning already produces root runs for:

- primary document root;
- template/index-state root when needed;
- secondary index roots.

After this spec, the planner must convert those root runs into one collection
WAL transaction before adding them to the durable pending write domain. Unique
probe helpers remain in memory, but they must be rebuildable from the root
deltas and persisted roots during recovery. The WAL transaction must contain the
primary put, template/index-state changes, and every secondary-index put needed
for the insert.

### 8.3 Updates and Deletes

Updates and deletes must use the same root-delta transaction model:

- primary root put/delete;
- old secondary index deletes;
- new secondary index puts;
- template/index-state changes when document format requires them;
- delete/tombstone roots where applicable.

The WAL records planned deltas, not the callback or predicate that produced
them.

Updates that change indexed values must include old secondary-index deletes and
new secondary-index puts in the same transaction as the primary mutation.
Deletes must include primary tombstones or deletes, secondary deletes, and any
delete-root or overlay tombstone entries required to suppress older roots.

Column-store update-delta writes are a named mutation class. A
`column_update_delta` transaction must include new update-delta part descriptor
puts, all required column side refs for those parts, primary locator puts for
replacement rows, old-locator tombstones or delete bitmap updates, secondary
index deletes for old indexed values, secondary index puts for new indexed
values, count/visibility metadata deltas when present, and the applied
watermark update in one WAL transaction and one backend publish. The durable
crash-recovery representation must be the column update-delta part plus
visibility changes, not a permanent row-oriented overlay.

### 8.4 Async Indexed Flush

Async flush becomes a publication optimization, not a durability boundary.

Allowed states:

1. durable pending transaction exists in collection WAL;
2. transaction is visible through mutable/queued/publishing write-domain state;
3. async worker publishes root group;
4. system root advances applied watermark;
5. cleanup retires published in-memory units and eventually WAL bytes.

If async publish fails, in-memory units can be requeued as they are today. The
collection WAL transaction remains the recovery source until a successful
publish advances the watermark.

Every queued or publishing flush unit must carry the collection WAL coverage it
covers:

- `WALLSN` range for diagnostics and cleanup;
- contiguous `CollectionSeq` range for the owning collection;
- required side-ref ownership or references to the protected side-ref index;
- root names and root generations covered by the publish.

Async publish completion may advance the applied watermark only for whole
transactions covered by the successful root publish. Requeue must preserve WAL
transaction ownership and must not append replacement WAL records for the same
acknowledged mutation.

For any collection/root dependency chain, async publish and watermark
advancement must cover a contiguous `CollectionSeq` prefix. A newer transaction
may remain visible through pending state while an older publishing unit is
requeued, but it must not become backend-authoritative, watermarked, or
cleanable ahead of that older dependent transaction. PR1 may publish independent
roots out of order only when root independence is machine-checkable and the
collection watermark still advances over a contiguous transaction prefix; the
default rule is no out-of-prefix advancement.

### 8.5 Overlay Roots

Overlay-root mode should use the same transaction model. The WAL transaction
must identify whether the root ids are base roots or overlay roots, and the
system-root delta must atomically update overlay descriptors and the applied
watermark.

Overlay compaction is a separate collection WAL maintenance transaction when it
can race with user-visible collection writes. It uses `ReplaceRoot` and
`ClearOverlayRoots` descriptor operations, advances the collection sequence, and
updates the affected root generation. Root-rewriting overlay maintenance is
forbidden while unapplied collection WAL exists unless it is encoded this way.
Overlay compaction and other layout-only root rewrites are local physical
maintenance transactions. They must not be appended as deterministic Raft
command entries and must not change the logical collection digest except by
preserving equivalent contents. A future distributed compaction/checkpoint
barrier must be specified as a separate deterministic barrier command if
cluster-visible semantics are required.

Column-store compaction, delete-bitmap compaction, and recompression are always
collection WAL maintenance transactions when they create external files or
change active part descriptors. They must include new side refs, new descriptors,
descriptor supersession/deletion for source parts, obsolete delete/mark/filter
metadata removal, count/visibility metadata preservation, and unchanged logical
secondary-index state in one atomic root group. The optional overlay-compaction
maintenance shortcut does not apply to physical column parts.

Descriptor replacement, delete-bitmap rewrite, filter rewrite, and side-file
rewrite are also collection WAL maintenance transactions unless a separate
backend-only protocol is specified with equivalent side-ref, watermark,
ordering, and crash-test guarantees.

### 8.6 Collection-Specific Publish Wrapper

Collection WAL recovery, flush, async publish, checkpoint, close, and
maintenance must call a collection-specific transaction executor rather than
reconstructing ad hoc runtime system-builder closures.

```text
PublishCollectionWALTransaction(txn, options) {
    validate txn.CollectionUID, CollectionGeneration, SchemaEpoch,
             CatalogDigest, RootDescriptorEpochs, BaseRootGeneration,
             dependencies
    materialize root deltas from txn
    apply root deltas
    instantiate txn.SystemDeltaTemplate with produced root ids
    atomically publish system descriptors + applied watermark
    honor options.Sync
}
```

The underlying ordered-root publish APIs may remain the root executor, but the
collection WAL layer owns validation, template instantiation, watermark
advancement, sync policy, and side-ref protection.

The existing ordered-root APIs are useful executors, not transaction APIs. The
collection WAL layer must not expose arbitrary runtime system-builder closures
as the durable recovery contract. Recovery and flush should call a typed wrapper
that consumes the recorded transaction, validates its guards, and instantiates
the recorded `SystemDeltaTemplate`.

### 8.7 WAL-Off Mode

`DurabilityWALOffRelaxed` should not write collection WAL transactions. In that
mode:

- pending collection writes are visible in-process only until published;
- `Flush`, `FlushAll`, checkpoint, and close remain the durability boundaries;
- column-store mutable writes must not be production-enabled under WAL-off
  until explicit benchmark and safety gates are added.

### 8.8 Future Raft Apply Path

For future Raft write replication, the apply path must not replay native-wire
transport requests. It should consume committed deterministic command-entry
bytes, validate the command version and catalog guard against applied state, and
derive local collection root deltas through the same planner/state-machine logic
used by the single-node path.

A node must not advance persistent applied-index metadata, return a
`raft_committed` client success from that node, or advertise the command as
locally recoverable until one of these is true:

1. the local collection WAL transaction and any required idempotency/catalog
   result metadata are recoverable locally; or
2. the Raft stable-store recovery design explicitly guarantees that
   committed-but-not-locally-applied entries are replayed after every restart
   before the node serves reads or advertises the corresponding applied index.

The first implementation should use rule 1: committed entry, local root-delta
WAL append, write-domain visibility/publish, then applied-index and idempotency
metadata advancement. Any persistent idempotency record or catalog guard outcome
that affects retry/replay behavior must be advanced atomically with the logical
mutation outcome, either in the same backend root group or in an explicitly
ordered metadata transaction whose crash behavior is tested with the collection
WAL.

If local collection WAL append fails after Raft commit, the command must remain
committed-but-not-locally-applied on that node. The node must retry local apply
or fail/step down according to the future Raft recovery policy; it must not
report local recoverability.

`flush_collection`, `flush_all`, `checkpoint`, and physical maintenance commands
remain local barriers. Cluster mode must reject `ack_policy=raft_committed` for
those commands or define a separate deterministic distributed barrier command
with explicit consensus semantics.

### 8.9 Lock Ordering and Goroutine Lifecycle

PR1 must document and test lock ordering before product code depends on
collection WAL. Required ordering constraints:

- close/checkpoint admission gate is acquired before admitting new collection
  mutations or selecting a checkpoint drain cut;
- collection manager domain registry state is consulted before per-collection
  mutation locks;
- collection mutation serialization happens before `CollectionSeq` allocation
  and before pending visibility changes;
- side-ref prepare guard is acquired before writing side refs and is released
  only after committed WAL or abort/quarantine;
- collection WAL append lock must not be held while acquiring backend publish
  locks;
- backend write/commit locks must not be held while waiting for collection
  domain locks or async publisher condition variables;
- `domain.mu` must be held only for short visible-install or completion
  bookkeeping critical sections. It must not be held while waiting for async
  publish, performing collection WAL I/O, registering long-running GC work,
  calling backend publish, or calling backend checkpoint;
- async publishers publish already-logged transactions and never allocate new
  `WALLSN` or `CollectionSeq` values for the covered acknowledged writes;
- `DB.Checkpoint` must invoke collection drain/checkpoint hooks before acquiring
  backend locks that async publish needs, or must use a nonblocking protocol that
  cannot wait on a publisher needing those locks;
- close/checkpoint first stop or fence new writers, wait for admitted in-flight
  writers, drain async publishers, publish/replay WAL-backed transactions,
  advance durable watermarks, create the durable backend boundary, then clean;
- background goroutines must be owned by the backend collection WAL service or
  be registered with it so recovery/checkpoint/close can quiesce them.

The safe high-level sequence is:

```text
admit write through DB/collection admission gate
-> serialize mutation under collection mutationMu
-> prepare final deltas in private, non-visible objects
-> prepare/protect required side refs
-> append/sync collection WAL under collection WAL writer lock
-> mark pre-reserved pending state visible under a short domain.mu critical section
-> later publish logged deltas without holding the WAL append lock
-> atomically advance root descriptors and watermarks
-> cleanup only after durable checkpoint/meta boundary
```

Implementations may use finer-grained locks, but they must preserve the same
deadlock and visibility invariants.

### 8.10 Collection Read Views

Collection reads that span more than a single copied point value must acquire a
`CollectionReadView`. A point read may avoid a read view only when it copies the
returned value and does not retain references to pending tables, value-log
records, column files, filters, delete bitmaps, or publishing units after
dropping collection locks.

A `CollectionReadView` captures:

- a backend snapshot and collection catalog/root descriptor view;
- immutable or refcounted views of mutable, queued, and publishing collection
  write-domain units that were visible at view creation;
- side refs reachable from those units;
- generated secondary-index views and unique-helper state needed by the scan or
  planner;
- the `CollectionUID`, `CollectionSeq`, and `WALLSN` values that own pending
  visible units.

Flush, async publish completion, rollback, overlay compaction, root rewrite,
collection WAL cleanup, value-log GC/rewrite, leaf-log GC, and future column-file
cleanup must not reset, reuse, delete, rewrite, or unprotect any object reachable
from a live `CollectionReadView`. Mutable state must either be frozen into an
immutable unit before inclusion in a view or copied into the view.

## 9. Recovery Algorithm

### 9.1 Collection WAL Recovery States

A collection WAL transaction is in exactly one durable recovery state:

| State | Meaning | Recovery behavior |
|---|---|---|
| `S0 Absent` | No complete collection WAL frame exists. | Do not publish roots. Any side files not referenced by a committed WAL transaction or published root are orphan-prepared and may be quarantined or deleted. |
| `S1 PreparedSideRefs` | Side bytes may exist, but no complete committed WAL frame references them. | Do not publish roots. Reclaim only after proving no complete WAL transaction and no published root references the bytes. |
| `S2 CommittedWAL` | A complete collection WAL frame with valid checksums exists and is not covered by the durable applied watermark. | Validate all required side refs, identity, schema, generations, and dependencies; then replay in collection sequence order. Missing or corrupt required side refs stop open. |
| `S3 MaterializedUnpublished` | Recovery or live publish built root pages or publish-output files, but backend meta/system-root commit did not publish root descriptors. | Not externally visible. After crash, retry from `S2` or skip if `S4` is observed. |
| `S4 Applied` | One backend meta/system-root commit atomically published root descriptor changes and applied watermark entries covering the transaction. | Skip during replay. Reapplying an `S4` transaction is a bug. |
| `S5 Cleanable` | Transaction is `S4`, and a durable checkpoint/meta boundary exists such that descriptors, watermarks, and side-ref reachability tracking for published roots are durable. | WAL files and WAL-only side-ref protection may be cleaned idempotently. |
| `S6 Cleaned` | Durable cleanup metadata says the segment or transaction range is safely cleaned. | Missing segment files are acceptable only for ranges covered by this metadata. Missing uncleaned segments stop open. |

Root descriptors published without the matching applied watermark are not a
valid state. If an implementation bug or future format can produce that split,
recovery must stop open unless the format also provides a proven idempotent
repair protocol. PR1 must make the split unrepresentable by publishing
descriptor ops and watermark ops in one backend commit.

### 9.2 Startup Recovery

Startup recovery should extend the existing recovery order while preserving
the current invariant that side-store scans run before cached commit-log replay:

1. Acquire the exclusive recovery/maintenance lock. No GC, rewrite, column
   cleanup, collection writer, async publisher, checkpoint, or close cleanup may
   run concurrently with recovery.
2. Recover backend index metadata and choose the valid meta page.
3. Load durable collection WAL cleanup metadata.
4. Discover collection WAL segment files. For each lane, segments are ordered by
   segment sequence. A missing segment is valid only if covered by durable
   cleanup metadata. A missing segment not covered by cleanup metadata is
   corruption and recovery must stop open.
5. Scan value-log and leaf-log side-store segments used by existing commit-log
   RID fences, and build the `RID -> side ref` maps required for replay.
6. Replay normal cached commit logs as today, using the side-ref maps created
   before replay.
7. Scan any additional collection WAL side-file classes, such as root-delta
   payloads, column files, filters, delete bitmaps, and dictionaries, needed by
   collection WAL fences.
8. Scan collection WAL frames in segment order:
   - EOF or short read is allowed only at the terminal frame of the last
     non-cleaned segment for that lane;
   - short read before a later complete frame or later non-cleaned segment is
     corruption;
   - a complete frame with checksum mismatch is corruption;
   - unsupported version is corruption unless explicitly marked skippable by a
     forward-compatible feature flag.
9. Decode complete transactions and rebuild the protected side-ref index before
   any maintenance can run.
10. Load applied collection-sequence watermark metadata from the recovered system
   root.
11. Sort unapplied transactions by `CollectionUID`, `CollectionSeq`, and
   `WALLSN` for deterministic diagnostics.
12. For each uncovered transaction, validate declared side refs and the
   canonical embedded required side-ref set extracted from root deltas and
   descriptors. The sets must match.
13. For each unapplied collection in contiguous `CollectionSeq` order:
   - validate checksum and format version;
   - validate required side refs;
   - validate collection identity, schema epoch, root generation guards, and
     sequence dependency;
   - add the transaction to the per-collection replay accumulator;
   - materialize publish inputs from `RootDeltaEncodingV1`;
   - instantiate `SystemDeltaTemplate` with produced root ids;
   - publish descriptor updates and applied watermark in one backend commit.
14. After successful replay, clean collection WAL segments whose transactions are
   fully covered by the durable applied watermark.
15. Quarantine or delete prepared side files that are not referenced by any
   committed transaction or reachable root.

Recovery must be deterministic. Replaying the same directory twice must produce
the same collection roots and applied watermark.

No side-store cleanup, value-log rewrite, column-file cleanup, or collection WAL
segment cleanup may run until collection WAL recovery and protected side-ref
index reconstruction are complete.

The recovery implementation must be backend-owned. It must run before collection
managers or user collection handles can observe collection state, and it must not
depend on a caller constructing a `CollectionManager`. The intended insertion
point is after normal cached key/value WAL replay and side-store inventory, but
before recovered roots are exposed to user-visible state, snapshots, native-wire
servers, or collection managers.

PR1 recovery must publish all replayable transactions before collection APIs can
serve reads or writes. If a future implementation chooses to keep recoverable
transactions as a durable pending overlay after open, it must rebuild primary
visibility, secondary visibility, and unique-index helper state from WAL before
serving any collection API; otherwise duplicate unique values can be admitted
before pending durable state is published.

Read-only open cannot perform steps that mutate backend state. If unapplied
committed collection WAL exists, read-only open must fail with a clear
recovery-required error unless an explicit stale-read-only mode is requested.
Read-only open must scan enough collection WAL metadata to detect unapplied
collection WAL transactions. Production column-store read-only open must not
silently hide acknowledged WAL-on writes after crash.

### 9.2.1 Column Side-File Recovery

Before collection WAL replay, recovery scans column prepare directories and
final column file classes and builds a side-file availability map. For each
complete unapplied column-store transaction, recovery:

1. validates transaction checksum, `CollectionGeneration`, `SchemaVersion`, and
   catalog identity;
2. extracts column file refs from descriptor, manifest, filter, delete bitmap,
   granule/mark, count/visibility, schema, compression metadata, and locator root
   deltas;
3. verifies that extracted refs are covered by the transaction's required
   `SideRefs`;
4. verifies side-ref existence, size, checksum, manifest consistency, file
   class, codec registry version, and dictionary identity;
5. publishes the entire root group and applied watermark in one backend commit;
6. records every published side ref as reachable from roots after the commit.

Recovery classifies column prepare/final files after replay:

| Column file state | Meaning | Recovery behavior |
|---|---|---|
| `building` | Incomplete prepare group or missing manifest/prepare record. | Quarantine or delete after proving no committed WAL/root references it. |
| `prepared` | Complete prepare group exists, but no committed WAL transaction references it. | Quarantine or delete; allocator ids remain reserved until classification finishes. |
| `wal_committed` | Complete WAL transaction references the prepare/final group, but applied watermark does not cover it. | Keep, validate side-ref closure, replay, or stop open on missing/corrupt required bytes. |
| `published` | Reachable from active roots or live snapshots/read views. | Keep and represent in normal column reachability. |
| `orphan_final` | Final-path file is not referenced by roots, live snapshots, read views, or not-yet-cleanable WAL. | Quarantine or delete after cleanup manifest/checkpoint rules allow it. |

`PartID` and `FileID` allocators must be advanced during open from every
reachable root, pending WAL transaction, and prepared or orphaned column
directory. Reusing a `PartID` or `FileID` that appears in an unclassified
prepare group is forbidden.

### 9.3 Buffered Transaction Replay and Accumulation

PR1 uses per-collection replay-side accumulation. Merged flush-unit WAL
transactions are deferred because they are incompatible with durable-at-ack for
independently acknowledged buffered writes unless those writes wait for the
merged unit to become durable.

Recovery processes transactions in `CollectionUID`, `CollectionSeq` order. A
transaction may enter the accumulator only if its `DependsOnCollectionSeq` is
already applied or already present in that accumulator. A complete missing
transaction blocks later transactions for the same collection.

For each root, the accumulator records:

- the persisted base root id and generation;
- the collection sequence range covered;
- the merged ordered delta entries in transaction order;
- whether cold-build tombstones must be preserved.

The PR1 fold order is `(CollectionSeq, RootDeltaOrdinal, EntryOrdinal)`.
`CollectionSeq` is the transaction order key; if a future record also exposes a
diagnostic `TxnID`, it must not replace `CollectionSeq` for same-collection
dependency ordering. `RootDeltaOrdinal` is the order of root deltas within the
transaction, and `EntryOrdinal` is the stable operation order exposed by the
root-delta codec. If two accumulated deltas affect the same key, the last
operation in fold order wins. Deletes/tombstones are first-class operations and
must suppress older values from the same accumulator and from older persisted or
overlay roots according to normal collection visibility rules.

A successful accumulated publish must cover whole transactions, not individual
roots. A transaction is covered only when every root delta in that transaction,
every catalog/root descriptor update, every required side ref, and the applied
watermark update have committed in one backend root-group commit. The
system-root commit must update every affected root descriptor and advance the
collection watermark to the highest contiguous `CollectionSeq` covered by the
publish. If any root in a transaction cannot be applied, no root from that
transaction may be considered applied.

The applied watermark may advance from `N` to `M` only when the publish covers
every transaction with `CollectionSeq` in `(N, M]` for that collection, with no
gaps. A publish that contains merged root deltas, replay accumulators, or async
flush units must record the covered contiguous range before cleanup can consider
any transaction in that range applied. Scalar watermark metadata is valid in PR1
only because every publish is constrained to this contiguous-prefix rule.

This rule applies to no-index buffered writes, indexed mutable runs, queued
flush units, async publishing states, overlay compaction, schema/index changes,
and future column-store delta-part descriptor updates.

### 9.4 Failure Behavior

| Failure point | Required behavior |
|---|---|
| Crash or ordinary failure during private planning before side refs or WAL | Return/observe no write. No read-visible pending state, uniqueness reservation, queued unit, or publishing unit exists. |
| Side-ref preparation fails before WAL commit marker | Return ordinary error. Do not expose mutation. Delete or quarantine prepared side files. |
| Crash before any side ref or WAL write | `S0 Absent`. No recovery action is required beyond ordinary orphan scan. |
| WAL append fails before commit marker | Return ordinary error. Do not expose mutation. Side refs remain uncommitted and are deleted or quarantined. |
| Crash after side refs prepared before commit marker | Recovery ignores transaction. Unreferenced side files are deleted or quarantined. |
| Concurrent read or planner races before WAL commit marker | Must not observe the private mutation. WAL-on visibility before recoverability is a correctness bug. |
| Crash after commit marker before response | Recovery may expose transaction. This is committed-before-response ambiguity. |
| Crash after visible staging before API response | WAL-on modes recover the committed transaction. Visible-without-committed-WAL is not permitted. |
| In-memory visible staging fails after commit marker | Not permitted as an ordinary error. Implementation must make this step non-failing or report commit-ambiguous/fatal. |
| Checkpoint races with admitted writer | Writer is either before the checkpoint cut and must be drained/published/watermarked by the checkpoint, or after the cut and must remain protected in retained WAL. |
| Close races with admitted writer | Writer either fails with closed before visible install or is included in the close drain and visible after reopen. |
| Root publish fails before backend meta commit | WAL transaction remains unapplied and protected. Retry on flush or recovery. |
| Root pages are built but backend meta commit fails | Built pages are unreachable. WAL transaction remains source of truth. |
| Descriptor update succeeds without watermark | Not permitted. Descriptor updates and watermark update are the same system-root commit. |
| Watermark update succeeds without descriptor update | Not permitted. Descriptor updates and watermark update are the same system-root commit. |
| Cleanup fails after watermark/checkpoint | Safe leak. Retry cleanup later. |
| WAL segment unlink/rename fails during cleanup | Cleanup remains retryable. Startup accepts missing segments only with durable cleanup metadata. |
| GC/rewrite wants a side ref protected by unapplied WAL | Must keep it in place or abort maintenance. Rewrite requires an explicit crash-tested WAL metadata protocol. |

## 10. Cleanup and Retention

Collection WAL cleanup is safe only when both are true:

1. every transaction in the segment is covered by the safe per-collection
   applied sequence watermark;
2. the backend checkpoint/meta boundary containing that watermark is durable for
   the selected mode.

Segment cleanup must decode the candidate segment and prove coverage for every
complete transaction in that segment. A segment maximum transaction id,
maximum `WALLSN`, or per-collection high value is not sufficient unless it is
paired with proof that every complete transaction in the segment is covered by
that transaction's collection watermark or by a valid global contiguous
watermark.

Prepared side files use a two-state lifecycle:

```text
prepared side file -> referenced by committed transaction -> reachable from root
prepared side file -> no committed transaction -> quarantine or delete
```

Column-store files should use the same lifecycle when they are introduced. A
root descriptor must never be published before the referenced file bytes are
readable at the required durability boundary.

Value-log and side-file GC must include collection WAL side refs in the
reachability set before deleting, truncating, rewriting, or moving any protected
bytes. Cleanup may stop protecting a side ref only after the transaction is
covered by a safe applied watermark and the durable checkpoint/meta boundary
makes the published roots or the discarded incomplete transaction authoritative.

PR1 uses a protected side-ref index:

- WAL append registers every required side ref before the commit marker becomes
  replayable.
- Recovery rebuilds the index by scanning collection WAL before maintenance
  starts.
- Cleanup unregisters side refs only after the applied watermark and durable
  checkpoint/meta boundary make the published roots authoritative.
- Value-log rewrite and column-file rewrite refuse protected refs in PR1 rather
  than trying to patch WAL records in place.

WAL-only side-ref protection may be released only after all are true:

1. the transaction is covered by a durable applied watermark;
2. the root descriptors containing the refs are durable in the backend
   meta/system root;
3. the relevant side-ref reachability tracker has incorporated that published
   root set, or a full reachability scan has completed;
4. old snapshots and active iterators that could reference either old or new
   roots are represented in the normal retention system;
5. durable cleanup metadata records that WAL protection for the segment/range is
   no longer required.

Column files, filters, delete bitmaps, and dictionaries follow the same
lifecycle as value-log refs: WAL protection before publish, root/snapshot
protection after publish, and quarantine/delete only after neither WAL nor any
published snapshot/iterator can reference them.

Column file GC/rewrite must treat these as reachability roots:

- active backend roots and all live snapshot roots;
- live `CollectionReadView` roots and pinned pending units;
- collection WAL transactions not covered by a safe applied watermark and
  durable checkpoint/meta boundary;
- protected prepare groups whose committed/uncommitted status has not been
  classified;
- external dictionaries and compression metadata referenced by reachable column
  files, reachable descriptors, or pending WAL transactions.

Column file rewrite must not rewrite refs that are protected solely by
collection WAL in PR1. A future rewrite protocol may update column WAL refs only
with its own crash-tested atomic redirect mechanism.

Column `PartID` and `FileID` allocators are recovery state, not in-memory
counters. Open must advance allocator high watermarks from active roots, old
snapshot roots that remain retained, unapplied or not-yet-cleanable WAL
transactions, and prepared/orphan column directories before accepting new column
part writes.

Cleanup is idempotent. A cleanup implementation must be safe across crashes
after cleanup record write, after unlink/rename, and before/after directory
fsync. Missing segments are acceptable only for `S6 Cleaned` ranges covered by
durable cleanup metadata.

`DB.Checkpoint` must coordinate with collection WAL before reporting a clean
collection WAL state. PR1 checkpoint hooks freeze collection WAL admission for
the checkpoint cut, force publication of replayable collection WAL admitted
before the cut, wait for in-flight async publish, drain known write domains,
advance watermarks, persist the backend checkpoint/meta durability boundary,
and include collection WAL side refs in value-log and side-file reachability
before any prune, rewrite, or deletion. If a future checkpoint mode leaves
replayable collection WAL unpublished, it must report collection WAL debt and
preserve all side-ref protections; it is not a clean collection-WAL boundary.

## 11. Test Plan

### 11.1 Current Contract Tests

Keep and expand tests that document current behavior:

- checkpoint does not flush pending collection-local no-index inserts before
  this spec changes that contract;
- close drains queued indexed flush units;
- `FlushAll` drains queued indexed flush units;
- async flush backpressure and requeue behavior remains visible and bounded.

### 11.2 Format Tests

- exact-byte `CollectionWALTransaction` v1 golden files;
- corrupt `RecordChecksumCRC32C` rejection;
- `ReplayDigest` changes when replay-critical fields change and does not change
  when observability-only fields change;
- truncated tail without commit marker is ignored;
- complete transaction with missing side ref fails recovery in WAL-on modes;
- hard corruption before the tail fails recovery;
- mixed transaction sequence rejection;
- large root-delta `RootDeltaPayload` side-ref round trips;
- unknown future version rejection with clear error;
- tombstone/cold-build encoding preserves `IncludeDeletedOnColdBuild`;
- `SystemDeltaTemplate` placeholder instantiation golden tests;
- descriptor-op precondition failures are reported before any publish is marked
  applied;
- segment metadata golden tests for min/max `WALLSN`, participant
  collections, sealed status, and checksum;
- cleanup record golden tests for `planned`, `unlinked`, and `dirsynced` states.

### 11.3 Recovery Tests

Crash/fault points:

1. after side-file append before side-file flush;
2. after side-file flush before collection WAL append;
3. before collection WAL append;
4. after WAL commit marker before visible staging;
5. after visible staging before response;
6. after acknowledgement before root publish;
7. during async publish;
8. after root pages are built but before the system-root commit advances the
   applied watermark;
9. after applied watermark before WAL cleanup;
10. during overlay compaction;
11. during column-file prepare once column store exists;
12. after two or more acknowledged buffered transactions with the same
    persisted `BaseRootID`;
13. after a higher `WALLSN` from another collection publishes before a
    lower `WALLSN`;
14. after value-log GC/rewrite is requested while an unapplied collection WAL
    transaction is the only owner of a side ref;
15. after a schema/index change is planned while lower `CollectionSeq`
    transactions are durable but unpublished;
16. after a drop/recreate under the same collection name;
17. after a root descriptor generation rewrite attempt with unapplied WAL;
18. after collection WAL cleanup record write before unlink;
19. after collection WAL segment unlink before directory fsync;
20. after read-only open observes unapplied committed collection WAL;
21. after a multi-collection operation reaches the collection WAL planner;
22. after direct `InsertBatch`, `UpdateBatch`, `DeleteBatch`, disabled
    indexed-memtable path, or large-batch path enters WAL-on execution;
23. after an oversized inline root delta chooses fail-before-ack or side-payload
    mode;
24. after recovery decodes a side ref with `../`, absolute, symlinked, or
    path/FileID-mismatched `RelativePath`;
25. after Raft commit before local collection WAL append once Raft apply exists;
26. after local collection WAL append before Raft applied-index/idempotency
    metadata advancement once Raft apply exists.
27. midway through a column substream file;
28. after all column files are written but before manifest write;
29. after manifest write before final rename;
30. during update-delta publish with secondary unique/nonunique index changes;
31. during delete bitmap or filter publish;
32. during column compaction/recompression after output files are prepared but
    before descriptor supersession;
33. during column compaction/recompression after descriptor supersession before
    cleanup;
34. during column file GC while an old snapshot or `CollectionReadView` still
    references source parts.

Required assertions:

- no acknowledged WAL-on collection write is lost after process crash;
- incomplete/no-marker transactions are not exposed;
- a committed-before-response transaction may be recovered and is not treated as
  a normal retryable failure;
- post-commit ordinary mutation errors are not returned;
- primary and secondary roots recover together;
- primary, template/index-state, overlay, delete, and descriptor roots recover
  with their watermark in the same logical publish;
- unique indexes reject duplicates after recovery;
- reads do not need stale in-memory write domains after recovery;
- roots never reference missing value-log or column-file bytes;
- side refs with unsafe relative paths, symlinks, or path/FileID mismatches are
  rejected before recovery or cleanup touches files;
- collection WAL files are cleaned only after safe watermark/checkpoint
  boundaries;
- buffered transactions with shared persisted bases replay by accumulation or
  rebasing without losing acknowledged writes;
- a higher published or cleaned `WALLSN` never causes recovery to skip a lower
  unapplied transaction from another collection;
- value-log GC/rewrite cannot remove or move bytes referenced only by pending
  collection WAL side refs;
- leaf-log GC/rewrite and future column cleanup cannot remove bytes referenced
  only by pending collection WAL side refs;
- declared side refs and embedded side refs match the canonical required set;
- a missing complete transaction `N` blocks `N+1` for the same collection;
- same-collection independence skips are rejected in PR1;
- multi-collection transactions are rejected before side refs or WAL are
  written in PR1;
- drop/recreate with the same collection name does not replay old WAL into the
  new collection;
- schema epoch and root generation mismatches block stale transactions;
- `DB.Checkpoint` either publishes replayable collection WAL or reports debt
  while preserving side refs;
- read-only open fails with recovery-required when unapplied committed
  collection WAL exists;
- missing cleaned segments open only when covered by durable cleanup metadata;
- missing uncleaned segments stop open;
- root descriptors published without matching watermarks are unrepresentable or
  stop open under fault injection;
- direct-publish, disabled-memtable, direct update, delete batch, and large-batch
  paths pass the same WAL-on crash matrix or are rejected under WAL-on modes;
- oversized inline transactions fail before ack or spill to side-payload refs
  with the same side-ref fence;
- future Raft apply cannot report an applied index whose logical mutation is
  neither locally recoverable nor guaranteed to be replayed from the Raft log.
- descriptor side-ref closure is complete for column manifests, substreams,
  filters, delete bitmaps, dictionaries, compression metadata, and granule roots;
- missing or corrupt required column side refs fail recovery for complete
  transactions;
- prepared but uncommitted parts and final-path orphan files are quarantined or
  deleted only after no committed WAL/root/read view references them;
- `PartID` and `FileID` allocators do not reuse pending, reachable, or
  unclassified orphan ids;
- primary locators, part descriptors, delete bitmaps, filters, exact
  count/visibility metadata, and secondary indexes recover atomically;
- update-delta rows are stored in column files after reopen, with no permanent
  row overlay;
- compaction does not expose duplicate rows from source and compacted parts;
- GC/rewrite never deletes root-reachable, snapshot-reachable,
  read-view-reachable, or WAL-pending column side refs.

### 11.4 Concurrency, Admission, and Snapshot Tests

Visibility and WAL append:

- no-index insert WAL append failure leaves no visible document and no unique or
  planner-observable state;
- indexed insert WAL append failure leaves no primary, secondary, unique-helper,
  queued, or publishing visibility;
- update/delete WAL append failure leaves no visible old-secondary delete or
  new-secondary put;
- race a writer blocked between private planning and WAL commit against reads,
  unique checks, update planners, delete planners, and pending-state merges;
  none may observe the write;
- crash after WAL commit before visible install replays the write; an in-process
  reader before the crash must not have depended on uncommitted private state.

Checkpoint and close:

- no-index insert -> `DB.Checkpoint` -> crash/reopen shows the document under
  WAL-on PR1 semantics;
- indexed buffered insert with async publish disabled -> `DB.Checkpoint` ->
  crash/reopen has primary and secondary roots;
- checkpoint racing with new writes covers writes before the admission cut and
  retains protected WAL for writes after the cut;
- checkpoint with async publishing in flight waits, publishes, or safely retries
  without deadlock;
- close racing no-index insert, indexed insert batch, update, delete,
  schema/index mutation, and async flush scheduling either returns closed or
  drains every successful mutation before close returns.

Ordering and async publish:

- two collections with interleaved `WALLSN` values can publish one collection
  first without causing recovery to skip the other's lower unapplied
  `CollectionSeq`;
- a segment containing applied collection B transactions and blocked collection A
  transactions is retained until every transaction in the segment is cleanable;
- async publish failure for older transaction `N` blocks watermark advancement
  for later dependent transaction `N+1`;
- replay accumulation publishes only contiguous `CollectionSeq` prefixes and
  blocks on gaps.

Read views and retention:

- long-running index range iterator remains valid while async publish completes
  and publishing units are reset after the iterator closes;
- `CollectionReadView` pins mutable, queued, and publishing units during
  `Flush`, `FlushAll`, overlay compaction, value-log GC/rewrite, and future
  column-file cleanup;
- value-log, leaf-log, filter, delete-bitmap, dictionary, and column side refs
  reachable from a live read view remain protected until the view is released;
- debug lock-order assertions or stress tests cover concurrent writes,
  checkpoint, flush, async publish, close, value-log GC, side-ref cleanup, and
  WAL append under timeouts.

Read-only open:

- crash after WAL commit before publish, then read-only open, returns the PR1
  recovery-required error;
- after read-write recovery completes, read-only reopen sees the recovered write
  without needing collection WAL replay.

### 11.5 Fuzz Tests

- transaction decoder fuzzing;
- recovery ordering fuzzing with multiple collections and duplicate keys;
- side-ref fence fuzzing with missing and truncated payloads;
- root-delta replay fuzzing against an in-memory oracle.

### 11.6 Required Named Acceptance Tests

These tests or equivalent subtests must exist before implementation can be
considered complete. Names are normative so CI, verification docs, and
acceptance artifacts can point to stable evidence.

| Test | Harness shape | Must prove |
|---|---|---|
| `TestCollectionWALOnRelaxedNoIndexAckBeforeFlushRecovers` | Child process opens with `DurabilityWALOnRelaxed`, inserts a no-index document, exits without `Flush`/`Close`; parent reopens. | WAL-on relaxed is durable-at-ack for process crash. |
| `TestCollectionWALOffRelaxedNoIndexAckBeforeFlushDoesNotClaimRecovery` | Same fixture under `DurabilityWALOffRelaxed`, with a second subtest that calls `Flush` first. | WAL-off remains relaxed before flush; explicit flush remains a persistence boundary. |
| `TestCollectionWALAppendFailureRejectsWriteBeforeVisibility` | Fault hook fails collection WAL append after private planning; assert API error, `Get` misses, reopen misses. | WAL append precedes visibility and acknowledgement. |
| `TestCollectionWALIndexedInsertUpdateDeleteRecoverAtomically` | WAL-on crash helper performs indexed insert, update changing indexed values, and delete, then exits before flush. | Primary, template/index-state, unique secondary, and nonunique secondary roots recover as one group. |
| `TestCollectionWALUniqueNonUniqueSecondaryIndexesAfterRecovery` | Insert two documents with one unique index and one shared nonunique value; crash/reopen; attempt duplicate unique insert and nonunique lookup. | Unique helpers are rebuildable and nonunique indexes preserve multiplicity. |
| `TestCollectionWALBufferedSameBaseRootTransactionsReplayByAccumulator` | Force multiple acknowledged buffered transactions with the same persisted `BaseRootID`; crash before publish. | Recovery accumulates/rebases rather than losing later transactions or failing on root mismatch. |
| `TestCollectionWALPartialFrameAndMissingSideRefNoPhantomRoots` | Corrupt/truncate collection WAL tail and remove a required value-log or leaf-log side ref. | Tail truncation is ignored only when safe; missing required side refs never publish phantom roots. |
| `TestCollectionWALWatermarkOutOfOrderTxnDoesNotSkipLowerUnapplied` | Two collections; force higher `WALLSN` publish/watermark before a lower `WALLSN`; crash/reopen. | Per-collection watermark logic cannot skip lower unapplied transactions. |
| `TestCollectionWALRecoveryCrashAndCleanupAreIdempotent` | Fault hook exits during recovery after N transactions, after watermark commit, and during segment cleanup; reopen repeatedly. | No double-apply, no lost writes, cleanup eventually converges. |
| `TestCollectionWALGCRewriteCompactionSnapshotsProtectPendingSideRefs` | Create pending WAL transaction whose side ref has no published root owner; pin snapshot/read view; run GC/rewrite/compaction; crash/reopen. | Side refs are protected until watermark plus checkpoint handoff, and snapshots do not observe dangling roots. |

### 11.7 Required Fault Points

The crash harness must expose these named fault points. Each point must support
both "return injected error" and "process exit" modes when the code path can
observe both outcomes.

```text
before_side_file_prepare
after_side_file_prepare_before_wal_append
before_collection_wal_append
after_collection_wal_append_before_visibility
after_ack_before_root_publish
queued_flush_unit_before_publish
publishing_unit_after_state_move_before_publish
after_root_pages_built_before_system_root_commit
after_system_root_commit_before_watermark_visible
after_watermark_commit_before_wal_cleanup
during_wal_segment_cleanup
during_prepared_side_file_quarantine
during_recovery_after_n_transactions
during_value_log_gc_scan_with_pending_collection_wal
during_value_log_rewrite_with_pending_collection_wal
during_overlay_compaction_publish
```

`after_system_root_commit_before_watermark_visible` should be untriggerable in a
correct PR1 implementation because descriptor changes and watermark advancement
are one backend commit. The fault point exists to prove the implementation does
not expose or test a two-phase publish path.

## 12. Benchmark Plan

Benchmarks must compare baseline and new implementation on the same host with
`benchstat`. Use the current `ProfileWALOnFast` / `wal_on_fast` collection
profile as the primary baseline when measuring WAL-on collection overhead, with
the same storage-policy cell, document format, batch size, and fixture data.
Durable-mode benchmarks are smoke gates for sync behavior, not the primary
regression baseline.

Required storage-policy cells:

- `data_outer=true,index_outer=false` (production-mainline priority);
- `data_outer=true,index_outer=true` (fully compressed);
- `data_outer=false,index_outer=false` (fast/control);
- `data_outer=false,index_outer=true` (low-priority compatibility cell).

Required collection WAL microbenchmark command:

```bash
for cell in \
  "true false" \
  "true true" \
  "false false" \
  "false true"
do
  set -- $cell
  export TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG=$1
  export TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG=$2
  export TREEDB_COLLECTION_BENCH_ENGINE=wal_on_fast
  export TREEDB_COLLECTION_BENCH_BATCH_SIZE=8000

  go test ./TreeDB/collections \
    -run '^$' \
    -bench '^(BenchmarkCollectionWALNoIndexInsertBatch|BenchmarkCollectionWALOneUniqueIndexInsertBatch|BenchmarkCollectionWALThreeIndexInsertBatch|BenchmarkCollectionWALUpdateBatchUnchangedIndexedValues|BenchmarkCollectionWALUpdateBatchChangedIndexedValues|BenchmarkCollectionWALDeleteHeavy|BenchmarkCollectionWALAsyncIndexedFlushStalled|BenchmarkCollectionWALFlushPending)$' \
    -benchmem -count=10 \
    > "artifacts/collection-wal/bench-${1}-${2}.txt"
done
```

Required recovery benchmark command:

```bash
TREEDB_COLLECTION_BENCH_ENGINE=wal_on_fast \
TREEDB_COLLECTION_WAL_RECOVERY_DOCS=1000,100000,1000000 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkCollectionWALRecoveryReplayPendingDocs$' \
  -benchmem -benchtime=1x -count=5 \
  > artifacts/collection-wal/recovery.txt
```

Required durable-mode smoke benchmark command:

```bash
TREEDB_COLLECTION_BENCH_ENGINE=durable \
TREEDB_COLLECTION_BENCH_BATCH_SIZE=8000 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^(BenchmarkCollectionWALNoIndexInsertBatch|BenchmarkCollectionWALOneUniqueIndexInsertBatch|BenchmarkCollectionWALFlushPending)$' \
  -benchmem -count=5 \
  > artifacts/collection-wal/durable-smoke.txt
```

Required format and recovery-scan benchmark command:

```bash
go test ./TreeDB/internal/collectionwal \
  -run '^$' \
  -bench '^(BenchmarkCollectionWALEncodeNoIndexInlineRootDelta|BenchmarkCollectionWALDecodeNoIndexInlineRootDelta|BenchmarkCollectionWALEncodeIndexedThreeRoots|BenchmarkCollectionWALDecodeIndexedThreeRoots|BenchmarkCollectionWALReplayDigest|BenchmarkCollectionWALScanEmptySegments|BenchmarkCollectionWALScanWatermarkedTransactions|BenchmarkCollectionWALScanPendingTransactions|BenchmarkCollectionWALRebuildSideRefIndex|BenchmarkCollectionWALLargeInlineRootDelta|BenchmarkCollectionWALLargeSidePayloadRootDelta|BenchmarkCollectionWALZstdLargeRootDelta|BenchmarkCollectionWALSideRefValidation)$' \
  -benchmem -count=10 \
  > artifacts/collection-wal/format.txt
```

Format benchmarks must report bytes/transaction, allocations, encode ns/op,
decode ns/op, CRC cost, compression cost, segments/sec, frames/sec, bytes/sec,
time to first recovery error, inline versus side-payload crossover, compression
ratio, fsync count when applicable, and side-ref verification time. Inline
thresholds, side-payload thresholds, and compression defaults are not eligible
for stronger-default status until these artifacts exist.

Every benchmark row must report these metrics when the harness can measure them:

| Metric | Purpose |
|---|---|
| `docs/sec`, `ns/doc` | Primary throughput. |
| `ack_ns/doc` | Acknowledgement latency cost. |
| `collection_wal_append_ns/doc` | WAL append overhead. |
| `collection_wal_bytes/doc` | Write amplification. |
| `root_delta_entries/doc` | Workload normalization. |
| `side_refs/doc` | Side-file pressure. |
| `pending_collection_wal_bytes` | Cleanup debt. |
| `applied_watermark_lag_txns` | Recovery debt. |
| `recovery_docs/sec` | Reopen speed. |
| `recovery_root_delta_entries/sec` | Replay throughput. |
| `recovery_peak_rss_bytes` or `recovery_peak_heap_bytes` | Replay memory bound. |
| `gc_protected_side_ref_bytes` | GC/rewrite retention cost. |
| `cleanup_ns/segment` | Segment cleanup cost. |
| `allocs/doc`, `bytes_allocated/doc` | Heap regression. |

Gate thresholds:

| Metric | Initial implementation gate | Stronger-default gate |
|---|---:|---:|
| No-index WAL-on insert regression | <=25 percent | <=10 percent |
| One-unique-index insert regression | <=30 percent | <=15 percent |
| Three-index insert regression | <=35 percent | <=20 percent |
| Update unchanged indexed values | <=20 percent | <=10 percent |
| Update changed indexed values | <=30 percent | <=20 percent |
| Delete-heavy workload | <=30 percent | <=20 percent |
| Flush pending WAL-backed deltas | <=30 percent | <=20 percent |
| Recovery, no-index | >=50K docs/sec | >=100K docs/sec target |
| Recovery, indexed | >=20K docs/sec and >=50K root-delta entries/sec | >=50K docs/sec target |
| WAL bytes/doc | Reported and below chosen formula threshold | Required; exceeding threshold blocks stronger default |
| Async stalled memory | Numeric cap reported; queue bounds enforced | Same, plus no steady-state leak across repeated cycles |
| Cleanup | No unapplied segment removed; fully applied segments cleaned after safe checkpoint | Same |

Use `benchstat -count=10` for microbenchmarks. A performance gate fails only
when both the mean regression crosses the threshold and `benchstat` reports a
statistically significant change. Correctness gates do not depend on benchmark
variance and must pass before any performance gate can be considered.

The stronger-default gate is required before enabling WAL-on collection
durable-at-ack as a default profile. The initial implementation gate is enough
to merge guarded/internal code paths when all correctness gates are green and
all metrics above are emitted.

Column-store gates added before persistent column writes:

- column part prepare plus WAL append overhead reported as `ns/row` and
  `bytes/row`;
- side-ref closure validation throughput reported on deterministic descriptors
  with 1K, 100K, and 1M file refs;
- recovery throughput reported by row count, root-delta entry count, and file
  count;
- recovery of many small update-delta parts has numeric peak memory bounds;
- file count per default update micro-batch stays under the configured policy;
- column WAL bytes/row and side-ref metadata bytes/row are reported;
- orphan cleanup of prepared files has a bounded benchmark target;
- GC protected-byte debt is reported by payload, filter, delete bitmap,
  dictionary, manifest, and compression metadata class;
- compaction publish crash/recover benchmark reports duplicate-row checks and
  reclaimed bytes;
- read-only recovery-required scan has bounded latency and does not parse full
  column payloads.

If inline root-delta WAL exceeds 2.5x serialized root-delta payload plus fixed
frame overhead, side-payload mode must be implemented before the
stronger-default gate can pass.

## 13. Roadmap and Gates

Traceability matrix:

| Design area | Sections | Milestones | Required evidence |
|---|---|---|---|
| Current contract | 2, 6 | M0 | existing behavior tests and docs |
| Testability decisions | 15 | M0.5 | closed PR1 choices and acceptance artifact |
| WAL format | `storage-format.md` Section 9 plus this doc 7.2, 7.4, 7.5 | M1 | exact-byte golden, fuzz, corrupt-tail, feature-gate, and migration-state tests |
| Recovery state machine | 9.1, 9.2, 9.4 | M1, M5 | crash-state and stop-open tests |
| Identity and sequencing | 7.3, 7.6, 9.3 | M1, M5 | collection-seq, epoch, generation tests |
| System-root template | 7.2, 8.6 | M1, M5 | descriptor-op and watermark atomicity tests |
| Side-file fences | 7.7, 10 | M2 | missing-ref and GC protection tests |
| Segment cleanup metadata | 7.5, 10 | M1, M5 | missing-segment and cleanup-manifest tests |
| Write-domain integration | 8 | M3, M4 | insert/update/delete recovery tests |
| Visibility/admission barriers | 6.2, 6.3, 8, 8.9 | M3, M6 | read-race, checkpoint-cut, and close-race tests |
| Collection read views | 3, 6.3, 8.10, 10 | M4, M6 | long-iterator and side-ref pinning tests |
| Recovery replay | 9 | M4, M4.5 | crash matrix, accumulation, and deterministic replay tests |
| Checkpoint/cleanup | 10 | M5 | watermark and segment cleanup tests |
| Lock ordering | 8.9, 11.4 | M0, M6 | debug lock assertions and deadlock stress |
| Performance | 12 | M6.5 | benchstat gates and artifacts |
| Column-store unblock | 6.4, 10 | M7 | side-file fence and root-group recovery proof |
| Native-wire/Raft layering | 2.4, 6.2, 7.9, 8.8 | M8 | deterministic-entry apply and local-WAL durability tests |

Each milestone must produce a machine-readable acceptance artifact at
`artifacts/collection-wal/<milestone>/acceptance.json` with:

- git commit;
- test command;
- test result;
- benchmark command when applicable;
- benchmark result path when applicable;
- enabled durability mode;
- storage-policy cell;
- collection document format;
- fault points exercised;
- pass/fail decision.

Acceptance matrix:

| Gate | Required evidence | Pass criteria |
|---|---|---|
| M0 Contract and harness freeze | Existing current-contract tests and crash helper skeleton. | Current flush-boundary behavior remains documented; named fault hooks can stop at side prepare, WAL append, staging, publish, watermark, cleanup, and recovery replay. |
| M0.5 Testability decisions | Closed PR1 decisions in Section 15. | File class, transaction identity, checkpoint behavior, missing side-ref policy, buffered replay model, GC protection, and overlay compaction rule are testable. |
| M1 WAL format | Golden fixtures, checksum/corruption/truncation tests, decoder fuzzing. | Decoder is deterministic; safe terminal truncation is accepted; hard corruption fails. |
| M2 Side-ref fences | Missing-ref, side-payload, and GC/rewrite protection tests. | No root points at missing bytes; WAL-pending side refs are retained. |
| M3 No-index WAL integration | WAL-on/WAL-off no-index crash tests and append-failure tests. | WAL-on crash recovers acknowledged writes; WAL-off expectations stay relaxed; append failure has no visibility. |
| M4 Indexed integration | Indexed insert/update/delete atomic recovery tests. | Primary, template/index-state, unique, and nonunique roots agree after reopen. |
| M4.5 Buffered/async states | Mutable, queued, publishing, requeue, and accumulator tests. | No acknowledged write is lost in any write-domain state. |
| M5 Watermark/recovery/cleanup | Repeated reopen, watermark, missing segment, and cleanup-crash tests. | Replay is idempotent; no double-apply; no premature cleanup. |
| M6 Barriers and modes | Flush, FlushAll, checkpoint, close, and read-only open tests. | Chosen barrier semantics are deterministic and documented. |
| M6.5 Performance | Required benchmark commands and `benchstat` artifacts. | Correctness is green; metrics are emitted; thresholds pass for the selected gate. |
| M7 Column-store sign-off | M1-M6 artifacts, synthetic side-file tests, benchmark artifacts, and column PR checklist links. | Persistent column roots and side refs stay blocked until the sign-off artifact is green. |

### Milestone 0: Contract Freeze

Deliverables:

- document current collection flush-boundary behavior;
- identify every API that can acknowledge collection writes;
- freeze benchmark fixtures and crash-test harness shape.
- document PR1 visibility, checkpoint admission, close admission, read-view, and
  lock-order invariants before product code relies on them.

Tests:

- current checkpoint boundary test;
- close drains queued indexed flush unit;
- `FlushAll` drains queued indexed flush unit;
- failpoint hooks compile and can stop execution at side prepare, WAL append,
  staging, publish, watermark, cleanup, and recovery replay boundaries.
- debug lock-order hooks can identify inverted acquisition for collection
  `mutationMu`, `domain.mu`, WAL lane locks, side-ref protection locks, backend
  write/commit locks, checkpoint locks, and close/checkpoint admission gates.

Gate:

- no production column-store persistent work starts before this milestone is
  complete.

### Milestone 0.5: Testability Decisions

The PR1 decisions in Section 15 are prerequisites for executable tests. Before
M1 starts, the implementation owner must confirm the exact testable choices for:

- collection WAL segment format and filename class;
- storage-format feature gates, migration states, and byte envelope;
- `CollectionSeq` and `WALLSN` allocation and persistence;
- missing side-ref behavior by durability mode;
- checkpoint behavior for PR1;
- buffered replay model;
- GC/rewrite side-ref protection mechanism;
- overlay compaction durability rule.

Gate:

- no collection-WAL implementation PR may claim milestone completion while an
  applicable testability decision remains open or has no acceptance artifact.

### Milestone 1: Collection WAL Format Package

Deliverables:

- internal transaction structs;
- encoder/decoder;
- segment reader/writer;
- WALLSN and per-collection sequence allocator interfaces;
- persisted `CollectionUID`, `SchemaEpoch`, and root generation descriptor
  encoding;
- `SystemDeltaTemplate` and descriptor-op encoding;
- segment metadata and cleanup record encoding;
- exact byte-format documentation in `storage-format.md`;
- stable root-delta encode/decode APIs independent of transient in-memory
  `batch.Batch` layout.

Tests:

- golden files;
- unsupported required version and unknown critical section fail-closed tests;
- malformed length rejection before allocation;
- feature-gate and migration-state fixtures;
- downgrade detection fixtures;
- corrupt checksum;
- truncated tail;
- decoder fuzzing;
- replay digest tests;
- descriptor-op placeholder instantiation tests;
- cleanup metadata and missing-segment classification tests.

Gate:

- format package has no dependency on collection planners or backend DB.

### Milestone 2: Root Delta and Side-Ref Fences

Deliverables:

- root-delta batch serialization;
- inline and side-payload modes;
- side-ref availability checker;
- value-log side-ref integration;
- leaf-log side-ref validation for pre-existing embedded leaf refs;
- protected side-ref index updated by WAL append/cleanup and rebuilt during
  recovery;
- side-ref prepare guard that excludes GC/rewrite/cleanup during preparation;
- value-log GC/rewrite protection for unapplied collection WAL side refs;
- fail-hard recovery behavior for complete transactions with missing required
  side refs.

Tests:

- missing side refs fail recovery for complete WAL-on collection transactions;
- declared and embedded side-ref set mismatch fails recovery;
- no root can be published with missing side bytes;
- large delta side-payload round trips;
- GC/rewrite cannot delete or move bytes referenced only by unapplied
  collection WAL side refs.

Gate:

- collection side-ref fence behavior is stricter than existing RID-fenced cached
  WAL skip behavior: complete missing-side-ref transactions fail recovery rather
  than being skipped.

### Milestone 3: No-Index Collection Integration

Deliverables:

- WAL-on no-index inserts append collection WAL before acknowledgement;
- no-index buffered inserts enter collection mutation serialization before
  `CollectionSeq` allocation and pending visibility;
- private planning, durable commit, and visible install are separate phases;
- no collection read, planner, uniqueness check, queued unit, or publishing unit
  can observe a no-index write before its WAL commit marker is complete;
- final physical primary-root deltas are materialized before the commit marker;
- pending write-domain visibility remains unchanged;
- direct publish paths log first under WAL-on modes or remain explicitly
  WAL-off/debug-only;
- `Flush` publishes WAL-backed no-index deltas and advances watermark.

Tests:

- `TestCollectionWALOnRelaxedNoIndexAckBeforeFlushRecovers`;
- `TestCollectionWALDurableNoIndexAckBeforeFlushRecovers`;
- `TestCollectionWALOffRelaxedNoIndexAckBeforeFlushDoesNotClaimRecovery`;
- `TestCollectionWALAppendFailureRejectsWriteBeforeVisibility`;
- `TestCollectionWALNoIndexInsertBatchAckBeforeFlushRecovers`;
- no-index `Update`, `UpdateBatch`, `Delete`, and `DeleteBatch` either publish
  synchronously under the documented current rule or are WAL-backed before ack;
  whichever rule is chosen must have crash/reopen tests;
- crash before WAL append does not expose document;
- concurrent reads and planners blocked around WAL append cannot observe the
  private mutation;
- checkpoint behavior updated to the new contract;
- WAL-off mode creates no collection WAL files for unflushed writes.

Gate:

- no-index insert benchmark meets the M6 initial implementation gate and emits
  all required metrics.

### Milestone 4: Indexed Insert/Update/Delete Integration

Deliverables:

- indexed insert root-group WAL;
- update/delete root-group WAL;
- async flush publish uses existing durable transactions;
- queued/publishing flush units carry `WALLSN` and contiguous
  `CollectionSeq` ranges;
- queued/publishing flush units cannot advance watermark outside a contiguous
  per-collection prefix;
- collection read views pin pending mutable, queued, and publishing units plus
  reachable side refs;
- unique helper rebuild after recovery;
- replay accumulator for multiple buffered transactions that share one persisted
  root base;
- PR1 rejection of true multi-collection transactions before side refs or WAL
  are written;
- schema/index changes ordered by collection sequence;
- overlay compaction encoded as a collection WAL maintenance transaction when it
  can race with user-visible writes.

Tests:

- `TestCollectionWALIndexedInsertRecoverAtomically`;
- `TestCollectionWALIndexedUpdateChangedSecondaryRecoverAtomically`;
- `TestCollectionWALIndexedUpdateUnchangedSecondarySkipsSecondaryRootsAfterRecovery`;
- `TestCollectionWALIndexedDeleteRecoverAtomically`;
- `TestCollectionWALUniqueReuseAfterDeleteRecovery`;
- `TestCollectionWALIndexedInsertUpdateDeleteRecoverAtomically`;
- crash after ack before async publish;
- crash during async publish;
- WAL append failure for indexed `Insert`, `InsertBatch`, `Update`,
  `UpdateBatch`, `Delete`, and `DeleteBatch` leaves no visible primary,
  secondary, unique-helper, queued, or publishing state;
- long-running iterators remain valid while async publish completes and cleanup
  waits for read-view release;
- unique and nonunique secondary correctness after recovery;
- unchanged indexed-value update, changed unique value, changed nonunique value,
  delete followed by unique-value reuse, and duplicate conflict after recovery;
- two acknowledged buffered writes against one persisted base both survive
  crash/reopen;
- per-collection ordering gaps block later same-collection transactions;
- multi-collection transaction attempts are rejected before side effects;
- insert/update/delete chains preserve secondary deletes and puts after
  recovery;
- schema epoch and root generation mismatches block stale transactions.

Gate:

- one-index and three-index insert benchmarks meet the M6 initial implementation
  gate and emit all required metrics;
- async flush cannot lose acknowledged documents under process crash.

### Milestone 4.5: Buffered and Async State Recovery

Deliverables:

- mutable, queued, and publishing units carry WAL ownership and contiguous
  `CollectionSeq` ranges;
- replay-side accumulator handles multiple root deltas sharing the same
  persisted base root;
- async publish failure requeues without changing WAL identity or cleanup
  eligibility;
- memory and queue bounds remain enforced when async publish is stalled.

Tests:

- `TestCollectionWALBufferedSameBaseRootTransactionsReplayByAccumulator`;
- `TestCollectionWALQueuedFlushUnitCrashRecovers`;
- `TestCollectionWALPublishingUnitCrashRecovers`;
- `TestCollectionWALAsyncPublishFailureRequeueKeepsWAL`;
- `TestCollectionWALAsyncPublishNoWatermarkOutOfPrefixOrder`;
- async stalled benchmark reports numeric memory and queue caps.

Gate:

- no acknowledged write is lost from mutable, queued, or publishing state under
  the required crash harness.

### Milestone 5: Recovery, Watermark, and Cleanup

Deliverables:

- applied watermark in system root;
- per-collection applied sequence watermark plus optional global-contiguous
  `WALLSN` cleanup marker;
- collection WAL replay in open path;
- backend-owned collection WAL recovery service independent of
  `CollectionManager` construction;
- collection-specific publish wrapper around ordered-root publish APIs;
- segment cleanup after safe checkpoint;
- prepared side-file quarantine/delete;
- read-only open recovery-required error when unapplied committed collection WAL
  exists.

Tests:

- `TestCollectionWALWatermarkOutOfOrderTxnDoesNotSkipLowerUnapplied`;
- `TestCollectionWALRecoveryCrashAfterNTransactionsIdempotent`;
- `TestCollectionWALCleanupCrashIdempotent`;
- `TestCollectionWALPreparedUncommittedSideFilesQuarantined`;
- crash after root page build or watermark commit before cleanup does not
  double-apply;
- crash during recovery after publishing a subset of unapplied transactions is
  idempotent after repeated reopen;
- crash after watermark commit before WAL cleanup leaves the transaction
  skipped by watermark and cleanup retryable;
- crash during WAL segment cleanup is idempotent;
- crash during prepared side-file quarantine/delete is idempotent;
- out-of-order async publish cannot advance a watermark that hides a lower
  unapplied collection sequence;
- accumulated publishes advance only contiguous `CollectionSeq` prefixes;
- descriptor update and watermark update cannot split across commits;
- older segments remain when watermark is not durable;
- cleanup removes only fully applied segments;
- prepared side files without committed transactions do not become visible;
- missing cleaned segments are accepted only with durable cleanup records;
- missing uncleaned segments stop open;
- read-only open cannot silently serve stale state when committed collection WAL
  is unapplied.

Gate:

- repeated reopen after crash is idempotent.

### Milestone 6: Checkpoint and Close Semantics

Deliverables:

- `DB.Checkpoint` coordinates with collection WAL;
- checkpoint establishes an admission cut, waits for in-flight writers admitted
  before the cut, drains async publish and write domains, publishes/watermarks,
  creates the backend durable boundary, and only then reports clean collection
  WAL state;
- close establishes an admission cut and rejects or drains every racing
  collection mutator before returning;
- backend-owned checkpoint service or checkpoint hook registry coordinated by
  that service;
- `CollectionManager.FlushAll` and close hooks use the same publish path;
- API docs distinguish process-crash recovery from fsync durability.

Tests:

- checkpoint forces publication of replayable collection WAL in PR1 or reports
  nonzero collection WAL debt in future modes;
- checkpoint-cut tests prove writes before the cut are covered and writes after
  the cut remain protected in retained WAL;
- close-race tests prove every successful racing write survives reopen and every
  non-surviving write returns closed;
- concurrent checkpoint/flush/async publish/value-log GC stress tests do not
  deadlock;
- close with in-flight async publish is recoverable;
- WAL-off mode preserves its relaxed contract.

Gate:

- no stale collection WAL files after clean close and checkpoint.

### Milestone 6.5: Performance Acceptance

Deliverables:

- required collection WAL microbenchmarks implemented under
  `TreeDB/collections`;
- required recovery benchmarks implemented;
- benchmark runner writes artifacts under `artifacts/collection-wal/`;
- metrics from Section 12 are emitted for each benchmark row;
- baseline/new `benchstat` output is attached to the acceptance artifact.

Gate:

- correctness gates M1 through M6 are green;
- initial implementation thresholds from Section 12 pass for guarded/internal
  enablement;
- stronger-default thresholds from Section 12 pass before WAL-on
  durable-at-ack becomes a default profile.

### Milestone 7: Column-Store Persistent Write Unblock Sign-Off

M7 is a release gate, not a standalone implementation milestone. It cannot pass
unless M1 through M6, including the required benchmark artifacts, are complete.

Prerequisites:

- M1 through M6 acceptance artifacts are present and green;
- the verification matrix lists every passing collection-WAL test by exact name;
- baseline/new `benchstat` output exists for the required benchmark rows and
  storage cells;
- synthetic external side-file tests pass before real column-file writes are
  exposed;
- the column-store PR checklist links to the exact collection-WAL acceptance
  artifact.

Additional column-store evidence:

- explicit column root-kind inventory for parts, granules, locators, deletes,
  schema, filters, count/visibility metadata, compression metadata, primary, and
  secondary roots;
- side-ref closure validator for descriptors, manifests, substreams, filters,
  delete bitmaps, dictionaries, compression metadata, granule roots, and
  root-delta payloads;
- column prepare-group scanner and orphan cleanup;
- `PartID` and `FileID` allocator recovery from roots, WAL, snapshots/read
  views, and prepare directories;
- read-only recovery-required behavior for unapplied collection WAL;
- GC/rewrite protection for WAL-pending and read-view-pinned column side refs;
- synthetic column-file class or minimal real column-file class exercising
  manifest, substream, filter, delete bitmap, dictionary, and compression
  metadata refs;
- column compaction, delete-bitmap compaction, and recompression modeled as
  collection WAL transactions.

Gate:

- no production or public-facing column-store collection API may publish
  persistent roots, descriptor roots, secondary indexes to column rows, or
  column-file side refs until this sign-off gate is green.

### Milestone 8: Native-Wire and Raft Apply Coordination

This milestone is required before any R3-style `ack_policy=raft_committed`
collection write is exposed. It is not a prerequisite for the local collection
WAL or column-store unblock gates unless that work also exposes cluster apply.

Deliverables:

- deterministic-entry apply adapter derives local collection root deltas without
  reconstructing native-wire transport requests;
- local collection WAL append is integrated into follower/leader apply before
  the node advertises the Raft index as locally durable;
- apply state distinguishes `consensus_committed`, `locally_applied`, and
  `locally_recoverable`;
- applied-index, catalog guard outcome, and idempotency metadata are advanced
  atomically with the logical mutation outcome or are replayed from Raft after
  restart by a documented rule;
- cluster `raft_committed` acknowledgement waits for consensus commit plus the
  selected local apply durability boundary;
- local-only flush/checkpoint/maintenance commands are rejected for
  `raft_committed` or replaced by explicit distributed barrier commands.

Tests:

- same committed deterministic entry sequence applied in separate fresh
  databases produces the same logical catalog/content/index/idempotency digest;
- deterministic-entry fixtures prove that changing `ack_policy`,
  `consistency_policy`, deadlines, trace context, compression choices, request
  ids, and response-shaping flags does not change canonical command bytes or
  command digest;
- crash after Raft commit before local WAL append recovers by replaying the
  committed entry or never advertises it as applied;
- crash after local WAL append before applied-index metadata advancement does
  not double-apply after restart;
- duplicate idempotency identity with the same digest returns the prior outcome
  after restart;
- duplicate idempotency identity with a different digest fails deterministically
  after restart;
- `raft_committed` is unavailable for local-only flush, checkpoint, and physical
  maintenance commands.

Gate:

- no cluster write path may report `raft_committed` until local collection WAL,
  applied-index metadata, and idempotency replay pass the crash matrix.

## 14. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Root-delta WAL duplicates data already in memtables. | Start with simple encoding, then move large deltas to side payloads when benchmarks require it. |
| Replaying old transactions double-applies after crash. | Store applied collection-sequence watermark in the same system-root commit as root descriptor updates. |
| Recovery cannot reconstruct descriptor or watermark updates. | Persist `SystemDeltaTemplate` with descriptor ops, preconditions, root-id placeholders, and watermark op in the WAL transaction. |
| Side-file ordering creates dangling pointers. | Require side refs to be readable before WAL commit is replayable; never publish roots until side refs pass; fail recovery on missing side refs for complete WAL-on transactions. |
| Value-log GC/rewrite removes bytes referenced only by pending collection WAL. | Treat required collection WAL side refs as GC/rewrite roots through a protected side-ref index until safe applied watermark and checkpoint cleanup. |
| A global `WALLSN` marker skips lower unapplied transactions. | Use per-collection `CollectionSeq` watermarks as the replay key; keep any global `WALLSN` marker cleanup-only and segment-verified. |
| Buffered transactions share an old persisted `BaseRootID`. | Use per-collection dependency chains plus replay-side accumulation in PR1. |
| A drop/recreate or schema change makes an old WAL transaction look applicable. | Persist `CollectionUID`, `CollectionGeneration`, `SchemaEpoch`, `CatalogDigest`, root descriptor epochs, and per-root `RootGeneration` guards and block mismatches. |
| API returns a normal error after the WAL commit marker. | Reserve fallible state before commit; after commit marker, allow only success, process death, or commit-ambiguous/fatal reporting. |
| Async flush races with WAL cleanup. | Treat async flush as publish-only; cleanup requires applied watermark and checkpoint boundary. |
| Unique index helpers are lost on crash. | Rebuild helpers from durable root deltas or persisted roots during recovery. |
| WAL-on throughput regresses too much. | Benchmark each milestone, use side payloads, batching, and async publish before relaxing durability. |
| WAL-off users assume stronger guarantees. | Keep WAL-off relaxed docs explicit and add tests that preserve the relaxed contract. |
| Raft log entries are confused with collection WAL transactions. | Keep deterministic entries as logical command input and collection WAL as node-local root-delta durability; test logical digests instead of byte-identical physical layout. |
| A node reports a Raft entry applied before its local collection mutation is recoverable. | Tie applied-index/idempotency metadata to local collection WAL durability or replay unapplied committed entries from Raft before serving. |
| Native-wire acknowledgement policy leaks into recovered logical state. | Treat acknowledgement policy as response/local durability control unless a future deterministic command version explicitly makes it logical state. |

## 15. Closed PR1 Decisions and Future Questions

PR1 closes the correctness-critical questions that block implementation:

1. Collection WAL uses a dedicated logical file class. It may reuse commit-log
   framing utilities, but it has separate record kinds, commit-marker semantics,
   replay rules, and side-ref behavior.
2. Transactions use both `WALLSN` and per-collection `CollectionSeq`.
   `WALLSN` is the global append position for scan order, diagnostics, and
   cleanup accounting; `CollectionSeq` is the dependency, replay, and skip key.
3. Complete WAL-on collection transactions with missing required side refs fail
   recovery. They are not skipped like current RID-fenced cached WAL batches.
4. PR1 uses replay-side accumulation for buffered writes that share a persisted
   base root.
5. PR1 uses a protected side-ref index, rebuilt from WAL during recovery, for
   GC/rewrite safety.
6. PR1 `DB.Checkpoint` calls collection checkpoint hooks and forces publication
   of replayable collection WAL before reporting a clean collection WAL state.
7. Overlay compaction that can race with user-visible collection writes is
   encoded as a collection WAL maintenance transaction.
8. True multi-collection collection WAL transactions are unsupported in PR1 and
   must be rejected before side refs or WAL are written.
9. Read-only open with unapplied committed collection WAL fails with a
   recovery-required error unless a future explicitly stale mode is requested.
10. Missing collection WAL segments are valid only when covered by durable
    cleanup metadata.
11. WAL-on visibility implies recoverability: private planning state is never
    reachable by readers, planners, uniqueness checks, or pending-state merges
    before the WAL commit point.
12. PR1 checkpoint and close establish admission cuts. A racing write is either
    outside the cut and protected in retained WAL, inside the cut and drained, or
    rejected before visible install.
13. Long-running collection reads use `CollectionReadView` pinning for backend
    snapshots, pending collection units, derived index views, and reachable side
    refs.
14. Column-store persistent writes are blocked until the column side-ref closure
    validator, prepare/finalize protocol, allocator recovery, and revised M7
    crash tests pass.
15. Column compaction, delete-bitmap compaction, and recompression that create
    external files are collection WAL transactions, not optional backend-only
    maintenance commits.
16. `CollectionUID`, `CollectionGeneration`, `SchemaEpoch`, `CatalogDigest`,
    and root descriptor epochs are mandatory replay guards.
17. Side-ref cleanup resolves files through a class registry by ref class and
    file id; relative paths are confined validation data and cannot authorize
    cleanup outside the class root.
18. Direct publish and disabled-memtable write paths are not bypasses under
    WAL-on modes; they must emit collection WAL or prove an equivalent crash
    fence.
19. Metadata that affects collection replay must share the collection root-group
    commit or have an explicit total order against collection WAL records.

Future questions that do not weaken PR1 correctness:

1. Should `Collection.Flush` gain a sync variant, or should callers use
   `DB.Checkpoint` for fsync-style barriers?
2. How large can inline root deltas be before side-payload mode becomes
   mandatory beyond the Section 12 stronger-default threshold?
3. For Raft R3, should applied-index/idempotency metadata live in TreeDB system
   roots, in a Raft library stable store, or in both with an explicit ordering
   rule?

Resolved PR1 decision: native-wire `ack_policy` remains request/local
durability control and is stripped before deterministic-entry construction. A
future cluster-visible barrier must be a separate deterministic command or a
new deterministic command field with explicit state-machine semantics.

## 16. Required Runtime Assertions

The implementation should enforce these assertions in debug, fault-injection, or
always-on validation paths where practical.

Write path:

- Before acknowledging a WAL-on collection write: `CollectionSeq != 0`,
  `WALLSN != 0`, `CollectionUID != 0`, `CollectionGeneration` and `SchemaEpoch`
  are present, `CatalogDigest` is present, root delta count is nonzero, and all
  root names are valid for the catalog epoch.
- Before appending a collection WAL transaction: every required side ref has
  reached the fresh-process-readable boundary and checksum verification has
  succeeded in debug/fault-injection builds.
- Before staging visible pending state: the WAL transaction is recoverable. In
  WAL-off mode, the code path is explicitly marked relaxed.
- After WAL append succeeds: ordinary failure returns are disallowed unless the
  return status is explicitly committed/ambiguous.
- Direct insert/update/delete paths assert either `collectionWALTxn != nil` or
  `equivalentDurableRootFence == true`, and the latter must be covered by the
  same crash matrix.

Recovery:

- Duplicate `WALLSN` or duplicate `(CollectionUID, CollectionSeq)` is fatal
  unless covered by an explicit legacy quarantine path.
- A transaction is skipped only if its own collection watermark or a valid
  global contiguous watermark covers it.
- Missing required side ref for a complete unwatermarked transaction is
  fatal/quarantine, never silent skip.
- A watermark advance includes every root delta and metadata delta in the
  covered transaction set.
- Base root mismatch is allowed only through formal accumulator state;
  otherwise recovery fails.
- Replaying the same directory twice produces the same collection root
  descriptor digest and applied watermark.

Cleanup and GC:

- Before deleting a WAL segment, decode it and prove every complete transaction
  is covered.
- Before deleting, moving, or rewriting value-log, leaf-log, column, filter,
  delete-bitmap, dictionary, or metadata bytes, prove the range is not reachable
  from current roots, snapshots, live `CollectionReadView`s, unapplied
  collection WAL, not-yet-cleanable collection WAL, or the protected side-ref
  index.
- Cleanup may stop protecting side refs only after applied watermark plus
  durable checkpoint/meta boundary.
- Side-file cleanup refuses paths outside the class root, symlinks, and
  path/FileID mismatches.

Serving:

- No collection API serves after write-open recovery while replayable collection
  WAL remains unapplied, unless a durable replay overlay with rebuilt primary,
  secondary, and unique-helper state is installed.
- Read-only open with pending collection WAL fails or installs a read-only
  replay overlay.
- Unique-index checks assert that pending WAL/replay state has rebuilt unique
  helpers or has already been published.

Column store:

- Every published column part descriptor references readable files with matching
  checksums, row counts, block directories, and schema epoch.
- Primary locator rows, part descriptor row counts, delete bitmap counts, and
  secondary index deltas agree before root-group publish.
- Column compaction asserts that source descriptors are removed/superseded and
  target descriptors are added in one covered maintenance transaction.
- GC asserts old column files remain reachable while any snapshot root,
  `CollectionReadView`, or unapplied WAL transaction can reference them.

## 17. Production Observability

Expose metrics and structured logs for:

- collection WAL appended transactions, bytes, flush/sync latency, and failed
  appends;
- oldest unapplied transaction age and bytes by `CollectionUID`;
- written `WALLSN`, per-collection applied watermark, and global contiguous
  cleanup watermark when present;
- recovery replay count, skipped-tail count, missing side-ref errors, checksum
  failures, duplicate `WALLSN` failures, and duplicate `(CollectionUID,
  CollectionSeq)` failures;
- side refs protected by class, count, bytes, and oldest age;
- GC bytes blocked by collection WAL side refs;
- checkpoint blocked time due to pending collection WAL;
- committed-but-not-staged return count;
- direct-path writes that used collection WAL versus an equivalent durable
  fence;
- recovery accumulator groups, transactions per group, and base-root mismatch
  failures;
- read-only opens refused due to pending collection WAL;
- column side-file prepared, referenced, reachable, quarantined, and deleted
  counts;
- overlay and column maintenance WAL transactions and cleanup lag.

Structured log fields should include `wallsn`, `collection_uid`,
`collection_name`, `collection_generation`, `schema_epoch`, `root_names`,
`base_root_ids`, `new_root_ids`, `side_ref_class`, `file_id`, `offset`, `size`,
`checksum`, `durability_mode`, and `watermark_after`.

## 18. Implementation Notes

- Prefer adding the collection WAL package below `TreeDB/internal` until the
  format stabilizes.
- Keep the transaction encoder independent from collection planner internals.
- Make root-delta serialization deterministic from the beginning.
- Add fault-injection hooks before building the full product path; otherwise
  crash coverage will be too shallow.
- Preserve existing write-domain read precedence while adding durable backing.
- Treat side-file fences as part of the storage format, not as cleanup policy.
- Keep future deterministic-entry apply code above the collection WAL package:
  it should derive root deltas through a state-machine adapter and then hand the
  resulting local transaction to the collection WAL layer.
- Keep Raft applied-index/idempotency metadata and collection WAL watermark
  updates ordered by a documented crash-recovery rule before enabling
  `ack_policy=raft_committed`.
- Update `collections-write-domain.md`, `write-path-and-durability.md`,
  `recovery.md`, `verification.md`, `native-wire-protocol.md`, and
  `native-query-raft-roadmap.md` as implementation milestones land.

Module-specific constraints:

- `TreeDB/internal/collectionwal` or equivalent owns versioned framing, commit
  markers, `WALLSN`, per-collection sequence allocation, side-ref
  verification, segment metadata, cleanup records, protected side-ref index
  rebuild, missing-segment classification, and segment cleanup after the durable
  applied watermark/checkpoint boundary. It may reuse or extract generic frame
  encoding from `internal/commitlog`, but it must not reuse the key/value
  `commitlog.Record` schema as the collection transaction schema.
- `TreeDB/internal/collectionwal` decoders must follow the `storage-format.md`
  envelope order: segment magic, segment version/min-reader, segment header CRC,
  frame magic, frame version/min-reader, length caps before allocation, frame
  header CRC, stored payload CRC, commit trailer, transaction fixed-header CRC,
  section CRCs, replay digest, and side-ref closure. Unsupported required
  versions, malicious lengths, mixed-version segments, and unknown critical
  sections or side-ref classes fail closed.
- The collection WAL transaction builder must require `CollectionUID`,
  `CollectionGeneration`, `SchemaEpoch`, `CatalogDigest`,
  `RootDescriptorEpochs`, `CollectionSeq`, `WALLSN`, root-delta ordinals, and
  canonical side refs before it can encode a replayable record.
- Required storage-feature gates for `collection_wal_v1` must be written before
  any WAL-on collection write can be acknowledged. `format.json` is the
  early-open gate, the system root is the authoritative recovered gate, and WAL
  headers are the decoder gate. Runtime overrides such as `IgnoreFormatConfig`
  must not bypass required features except through explicit destructive rebuild
  tooling.
- Feature gates, segment metadata, cleanup records, and migration manifests need
  a durable manifest writer with file fsync, rename, parent-directory fsync, and
  WAL-directory fsync after segment unlink. Atomic rename without those fsync
  steps is not enough for missing-segment proof.
- `TreeDB/collections` owns final physical delta planning before
  acknowledgement. WAL-on collection planners must serialize primary,
  template/index-state, secondary, overlay, delete, schema, and future
  column-store descriptor deltas before the commit marker and must stage the
  same immutable deltas for visibility. No-index buffered inserts must use the
  mutation serialization path before pending state becomes visible.
- `TreeDB/db` owns a collection-specific publish wrapper around ordered-root
  executors. The wrapper validates identity/generation/dependency guards,
  materializes root deltas, instantiates `SystemDeltaTemplate`, publishes
  descriptors and watermark atomically, honors explicit sync options, and makes
  read-only open fail when unapplied committed collection WAL requires mutating
  recovery. Collection WAL replay and cleanup are backend-owned services, not
  manager-local behaviors.
- `TreeDB/caching` and checkpoint code must track collection WAL debt through
  the backend-owned service. Automatic checkpoint and close paths must not
  report clean WAL state or prune required bytes while collection WAL
  transactions remain needed for recovery.
- `TreeDB/internal/valuelog` and future column-file maintenance must consult the
  protected side-ref index and side-ref prepare guard. PR1 rewrite refuses
  protected refs rather than moving them and patching WAL records.
- Column-store implementation may not publish persistent column parts until
  part files, descriptor deltas, primary locator deltas, delete bitmap roots,
  secondary roots, filter roots, and schema roots can be committed as one
  collection WAL transaction. Column files must have side-ref closure
  extraction, temp/final naming, file fsync, rename, directory fsync,
  manifest/checksum validation, `PartID`/`FileID` allocator recovery, dictionary
  and compression metadata protection, and cleanup tests before production
  enablement.

Recommended implementation sequence:

1. Add failpoint/crash harness plumbing around side-ref prepare, WAL append,
   pending visibility staging, root publish, watermark update, cleanup, and
   recovery replay. These tests may initially document expected failures before
   product behavior is implemented.
2. Add `internal/collectionwal` plus stable root-delta encode/decode golden
   tests. Keep this package independent from collection planner internals.
3. Add file-class side-ref preparation APIs for prepare, flush-readable,
   verify, protect, and quarantine; wire value-log pointerization to flush or
   sync side refs to the required boundary before collection WAL append.
4. Refactor collection write paths into prepare -> WAL append -> stage, starting
   with no-index buffered inserts and then indexed insert/update/delete.
5. Add backend-owned collection WAL recovery and per-collection watermarks in
   the backend open path before collection managers or native-wire servers can
   observe state.
6. Integrate async flush ownership, checkpoint, close, cleanup metadata, and
   protected side-ref GC/rewrite behavior.
7. Add performance gates and tune side-payload thresholds only after correctness
   crash tests pass.
8. Unblock persistent column-store writes only after the row/template collection
   WAL path proves the shared recovery and side-ref protocol.
