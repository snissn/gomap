# SPEC: Collection WAL Durability and Root-Group Recovery

Status: draft proposal. PR1 sections are normative for the planned collection
WAL implementation; explicitly marked future-work sections remain
non-normative.
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
- use a stable persisted `CollectionID`, `SchemaEpoch`, and per-root
  `RootGeneration` as replay guards;
- include a versioned `SystemDeltaTemplate` in every replayable transaction;
- fail recovery on any complete collection WAL transaction with a missing
  required side ref in WAL-on modes;
- maintain a protected side-ref index, rebuilt from WAL during recovery, for
  PR1 GC/rewrite safety;
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
published into backend roots for one `CollectionID`. Recovery uses the watermark
to avoid double-applying transactions after a crash that occurs after root
publish but before WAL cleanup.

Durable-at-ack:

Under WAL-on profiles, once a collection write API returns success, recovery can
make that write visible after process crash. This does not imply an fsync power
loss guarantee unless the caller used a sync barrier and the selected durability
mode requires fsync.

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
| `DurabilityDurable` | Write returns only after the collection WAL transaction reaches the configured durable boundary. Sync barriers fsync according to durable mode. |
| `DurabilityWALOnRelaxed` | Write returns after the collection WAL transaction is appended/flushed enough for process-crash recovery. It does not claim power-loss durability. |
| `DurabilityWALOffRelaxed` | No durable-at-ack promise. Collection writes remain flush/checkpoint/close-boundary durable, and docs must say so directly. |

If the engine cannot append the collection WAL transaction in a WAL-on mode, the
collection write must fail before becoming visible to the caller.

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
- Must call registered collection checkpoint hooks. PR1 checkpoint hooks must
  publish every replayable collection WAL transaction known to the manager,
  advance applied watermarks in backend commits, sync the backend boundary
  required by the selected mode, and only then allow collection WAL cleanup.
- It must not report a clean WAL state while collection WAL transactions remain
  needed for recovery.
- If a future checkpoint mode chooses not to publish a transaction, it must keep
  the transaction and all required side refs protected and report nonzero
  collection WAL debt.

`DB.Close`:

- Must drain collection write domains before backend close, as current tests
  already require.
- Must not discard a collection WAL transaction before its applied watermark is
  safely published.

Native-wire acknowledgement policies map onto these same local boundaries:

- `visible` may return after the write is visible through the serving process.
  In WAL-on collection modes after this spec, it still cannot return before the
  collection WAL transaction is recoverable.
- `flushed` must publish affected collection state into backend roots and
  advance the collection WAL applied watermark for those transactions.
- `synced` must also reach the backend checkpoint/sync boundary required by the
  configured local durability mode.
- Collection WAL append and collection root publication must both expose explicit
  flush/sync options so these acknowledgement policies can be implemented
  without relying on implicit backend defaults.
- `raft_committed` is a cluster-mode policy. It cannot be satisfied by local
  collection WAL alone; it requires consensus commit plus the cluster's defined
  local apply durability rule.

### 6.3 Column-Store Gate

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

A collection WAL transaction is a replayable physical root-group transaction.
It is not a logical insert/update/delete command. Recovery must not rerun user
callbacks, predicates, JSON extraction, template extraction, secondary-index
planning, or future column-store planning.

```text
CollectionWALTransaction {
    Version                  uint16

    // Identity and ordering.
    GlobalTxnID              uint64
    CollectionID             uuid128
    CollectionName           string diagnostic
    CollectionSeq            uint64
    DependsOnCollectionSeq   uint64

    // Catalog guard.
    SchemaEpoch              uint64
    BaseCommitSeq            uint64
    BaseSystemRootID         uint64
    BaseCatalogDigest        bytes optional

    // Physical mutation.
    RootDeltas               []CollectionRootDelta
    SystemDeltaTemplate      CollectionSystemDeltaTemplate

    // Required external bytes named by RootDeltas or SystemDeltaTemplate.
    SideRefs                 []CollectionSideRef

    // Local observability only; excluded from replay identity.
    AckMode                  uint8
    CreatedUnixNanos         int64
    Stats                    CollectionWALStats

    ReplayDigest             bytes
    RecordChecksumCRC32C     uint32
}

CollectionRootDelta {
    RootName                 string
    RootKind                 uint16
    RootDeltaIndex           uint32

    BaseRootID               uint64
    BaseRootGeneration       uint64
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
    RootDeltaIndex           uint32 optional
    RootIDPlaceholder        uint32 optional
    RootListPlaceholders     []uint32 optional
}

AppliedWatermarkOp {
    CollectionID             uuid128
    AdvanceCollectionSeqTo   uint64
    OptionalGlobalTxnID      uint64
}

CollectionSideRef {
    RefClass                 uint8
    RefID                    uint64
    RelativePath             string optional
    Offset                   uint64
    Size                     uint64
    ChecksumCRC32C           uint32
    Required                 bool
}

CollectionWALAppendOptions {
    Flush                    bool
    Sync                     bool
}

OrderedRootPublishOptions {
    Sync                     bool
    GlobalTxnIDsCovered      []uint64
    CollectionSeqRange       [2]uint64
    WatermarkOp              AppliedWatermarkOp
}
```

A transaction is replayable only when:

1. the record checksum is valid;
2. every required side ref is present and passes integrity checks;
3. `CollectionID`, `SchemaEpoch`, base root ids, and base root generations match
   the current replay state or an explicit replay accumulator state;
4. `DependsOnCollectionSeq` is covered by the collection applied watermark or by
   the active accumulator;
5. the `SystemDeltaTemplate` can be instantiated with the produced root ids and
   its preconditions hold.

The same backend commit that writes descriptor operations from
`SystemDeltaTemplate` must also advance the collection applied watermark. A
descriptor update without the watermark, or a watermark update without the
descriptor update, is not a valid collection WAL publish.

`AckMode`, `CreatedUnixNanos`, and `Stats` are local observability and policy
metadata. They must not change root-delta replay output, idempotency outcomes,
catalog guard outcomes, or future Raft state-machine results.

Two digests are required:

- `RecordChecksumCRC32C`: covers the whole encoded record for corruption
  detection;
- `ReplayDigest`: covers replay-critical fields only: identity, dependencies,
  root deltas, side refs, system delta template, and preconditions.

### 7.3 Collection Identity and Root Generations

Every collection must have a stable `CollectionID` persisted in collection
metadata. `CollectionName` is diagnostic and must not be the replay identity.
Dropping and recreating a collection with the same name creates a new
`CollectionID`.

Every schema or index metadata change increments `SchemaEpoch`. Every root
descriptor change increments that root's `RootGeneration`. Overlay descriptor
changes and overlay compaction also increment the affected root generation.

A collection WAL transaction records `CollectionID`, `SchemaEpoch`,
`BaseRootID`, and `BaseRootGeneration` for every root it mutates. Recovery must
reject or block a transaction whose identity or generation guard does not match
the replay state, unless the mismatch is explicitly explained by the active
replay accumulator.

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

Root kinds are explicit, versioned values. PR1 root kinds include primary,
template/index-state, secondary index, overlay, overlay descriptor, delete, and
metadata roots. Future column-store root kinds include part descriptor, locator,
column schema, filter descriptor, delete bitmap, and compression/dictionary
metadata roots.

### 7.5 File Class

Use a dedicated logical collection WAL class. The implementation may reuse
commit-log framing utilities, but the replay semantics are different from
normal user-root commit records.

Recommended layout:

```text
Dir/maindb/wal/
    collection-l<lane>-<seq>.log
```

Segment rules:

- Each segment contains framed `CollectionWALTransaction` records.
- Each transaction has exactly one `GlobalTxnID` and one collection-local
  `CollectionSeq`.
- Transactions are sorted by `GlobalTxnID` during segment scanning, with file
  order as a deterministic tie breaker only for malformed legacy segments.
- A truncated tail stops scanning that segment after the last complete record.
- Hard corruption before the tail fails recovery.

### 7.6 Applied Progress and Skip Rules

The system root stores applied progress by `CollectionID`:

```text
treedb/collection-wal/global-contiguous-applied-txn-id -> uint64 optional
treedb/collection-wal/collection/<collection-id>/applied-collection-seq -> uint64
```

`GlobalTxnID` is a unique transaction identity and diagnostic ordering key.
`CollectionSeq` is the replay and skip key. Recovery may skip a transaction only
when `txn.CollectionSeq <= applied-collection-seq` for the same `CollectionID`
and the transaction's `CollectionID`/`SchemaEpoch` guard matches the catalog
history covered by that watermark.

A global contiguous watermark may be used only as an optimization when every
lower `GlobalTxnID` is known applied or intentionally absent. It must never
cause recovery to skip an unapplied lower `CollectionSeq` for any collection.

Watermarks advance only in the same backend commit that updates collection root
descriptors for the root group. A watermark value means every collection
transaction up to that `CollectionSeq` is fully represented in persisted roots
and descriptor metadata.

PR1 uses per-write WAL transactions with per-collection dependency chains:

```text
GlobalTxnID              uint64  // unique identity, diagnostics, segment order
CollectionSeq            uint64  // strictly increasing per CollectionID
DependsOnCollectionSeq   uint64  // previous collection transaction observed
```

Recovery must process each collection in contiguous `CollectionSeq` order. A
transaction may be replayed only if all lower collection sequences are applied,
present in the active replay accumulator, or explicitly known not to exist for a
new collection. A missing complete transaction blocks later transactions for the
same collection.

### 7.7 Side-Ref Fences

`CollectionSideRef` names external bytes that must exist before a collection WAL
transaction can be replayed. Side refs are input dependencies of the
transaction, not output files produced by backend root publication.

Side-ref classes:

- `ValueLogRecord`: value-log bytes embedded in root-delta values.
- `RootDeltaPayload`: serialized root-delta payload stored outside the WAL
  record.
- `ColumnPartFile`: immutable column payload file.
- `ColumnFilterFile`: filter file.
- `DeleteBitmapFile`: delete/tombstone bitmap file.
- `DictionaryFile`: compression or encoding dictionary file.

Leaf-log or page-log records produced while replay builds new B-tree roots are
not `CollectionSideRefs`. They are publish outputs and must be flushed or synced
by the backend commit path before descriptors are published.

The WAL encoder must derive required side refs from the final root-delta payload
and from column descriptors. A caller-provided `SideRefs` list is insufficient
unless the encoder verifies it against the payload.

A complete transaction with a missing required side ref is a recovery error in
WAL-on durable/recoverable modes, except for an incomplete tail transaction that
lacks a valid commit marker. Recovery must not skip a complete collection
transaction and continue applying later transactions for the same
`CollectionID`.

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

### 7.8 Commit Marker and Ack Boundary

A collection WAL record is replayable only after its commit marker is complete.
The implementation must complete validation, side-ref preparation, side-ref
flush/sync required by the selected durability mode, memory reservation, and
hidden write-domain staging before appending the commit marker.

After the commit marker reaches the required durability boundary, the mutation is
committed for recovery. The implementation must not return an ordinary mutation
error after that point. If the process crashes before the client observes the
response, recovery may expose the mutation; this is committed-before-response
ambiguity, not an incomplete transaction.

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

On a future follower, applying a committed deterministic entry should derive the
same logical mutation outcome but may produce different local physical files and
root ids. That is acceptable only if the local collection WAL makes the derived
root group recoverable before the node advertises the Raft entry as durably
applied under the cluster policy.

## 8. Write Path

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
   immutable decoded delta objects.
5. Append the collection WAL transaction and commit marker.
6. Mark the reserved write-domain state visible without further fallible work.
7. Return success.

Flush, async publish, checkpoint, and close are publication and cleanup paths.
They must publish the exact WAL-backed root deltas and advance watermarks; they
must not re-plan, re-pointerize, or otherwise transform the acknowledged
transaction.

### 8.1 No-Index Inserts

Current no-index inserts can remain buffered in `domain.table`. After this
spec, a WAL-on no-index insert must canonicalize the final primary-root entry
before acknowledgement. If the value is stored as a value-log pointer, the WAL
transaction records the stable `ValuePtr` bytes and the required
`ValueLogRecord` side ref; flush must not later pointerize the raw document into
a different physical value.

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

### 8.6 Collection-Specific Publish Wrapper

Collection WAL recovery, flush, async publish, checkpoint, close, and
maintenance must call a collection-specific transaction executor rather than
reconstructing ad hoc runtime system-builder closures.

```text
PublishCollectionWALTransaction(txn, options) {
    validate txn.CollectionID, SchemaEpoch, BaseRootGeneration, dependencies
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

A committed Raft entry must not be marked locally durable/applied under a
cluster durability policy until one of these is true:

1. the derived collection WAL transaction is recoverable locally; or
2. the Raft recovery design explicitly guarantees that unapplied committed log
   entries are replayed after every restart before the node serves reads or
   advertises the corresponding applied index.

The first implementation should prefer the first rule: committed entry, local
root-delta WAL append, write-domain visibility/publish, then applied-index and
idempotency metadata advancement. Any persistent idempotency record or catalog
guard outcome that affects retry/replay behavior must be advanced atomically
with the logical mutation outcome, either in the same backend root group or in
an explicitly ordered metadata transaction whose crash behavior is tested with
the collection WAL.

`flush_collection`, `flush_all`, `checkpoint`, and physical maintenance commands
remain local barriers. Cluster mode must reject `ack_policy=raft_committed` for
those commands or define a separate deterministic distributed barrier command
with explicit consensus semantics.

## 9. Recovery Algorithm

Startup recovery should extend the existing recovery order while preserving
the current invariant that side-store scans run before cached commit-log replay:

1. Recover backend index metadata and choose the valid meta page.
2. Scan value-log and leaf-log side-store segments used by existing commit-log
   RID fences, and build the `RID -> side ref` maps required for replay.
3. Replay normal cached commit logs as today, using the side-ref maps created
   before replay.
4. Scan any additional collection WAL side-file classes, such as root-delta
   payloads, column files, filters, delete bitmaps, and dictionaries, needed by
   collection WAL fences.
5. Scan collection WAL segments, decode complete transactions, distinguish
   incomplete tails from hard corruption, and rebuild the protected side-ref
   index before any maintenance can run.
6. Load applied collection-sequence watermark metadata from the recovered system
   root.
7. Sort unapplied transactions by `CollectionID`, `CollectionSeq`, and
   `GlobalTxnID` for deterministic diagnostics.
8. For each unapplied collection in contiguous `CollectionSeq` order:
   - validate checksum and format version;
   - validate required side refs;
   - validate collection identity, schema epoch, root generation guards, and
     sequence dependency;
   - add the transaction to the per-collection replay accumulator;
   - materialize publish inputs from `RootDeltaEncodingV1`;
   - instantiate `SystemDeltaTemplate` with produced root ids;
   - publish descriptor updates and applied watermark in one backend commit.
9. After successful replay, clean collection WAL segments whose transactions are
   fully covered by the durable applied watermark.
10. Quarantine or delete prepared side files that are not referenced by any
   committed transaction or reachable root.

Recovery must be deterministic. Replaying the same directory twice must produce
the same collection roots and applied watermark.

No side-store cleanup, value-log rewrite, column-file cleanup, or collection WAL
segment cleanup may run until collection WAL recovery and protected side-ref
index reconstruction are complete.

### 9.1 Buffered Transaction Replay and Accumulation

PR1 uses per-collection replay-side accumulation. Merged flush-unit WAL
transactions are deferred because they are incompatible with durable-at-ack for
independently acknowledged buffered writes unless those writes wait for the
merged unit to become durable.

Recovery processes transactions in `CollectionID`, `CollectionSeq` order. A
transaction may enter the accumulator only if its `DependsOnCollectionSeq` is
already applied or already present in that accumulator. A complete missing
transaction blocks later transactions for the same collection.

For each root, the accumulator records:

- the persisted base root id and generation;
- the collection sequence range covered;
- the merged ordered delta entries in transaction order;
- whether cold-build tombstones must be preserved.

If two accumulated deltas affect the same key, later `CollectionSeq` wins
according to root-delta merge rules. Deletes/tombstones must suppress older
values from the same accumulator and from older persisted or overlay roots
according to normal collection visibility rules.

A successful accumulated publish must cover whole transactions, not individual
roots. The system-root commit must update every affected root descriptor and
advance the collection watermark to the highest contiguous `CollectionSeq`
covered by the publish. If any root in a transaction cannot be applied, no root
from that transaction may be considered applied.

This rule applies to no-index buffered writes, indexed mutable runs, queued
flush units, async publishing states, overlay compaction, schema/index changes,
and future column-store delta-part descriptor updates.

### 9.2 Failure Behavior

| Failure point | Required behavior |
|---|---|
| Side-ref preparation fails before WAL commit marker | Return ordinary error. Do not expose mutation. Delete or quarantine prepared side files. |
| WAL append fails before commit marker | Return ordinary error. Do not expose mutation. Side refs remain uncommitted and are deleted or quarantined. |
| Crash after side refs prepared before commit marker | Recovery ignores transaction. Unreferenced side files are deleted or quarantined. |
| Crash after commit marker before response | Recovery may expose transaction. This is committed-before-response ambiguity. |
| In-memory visible staging fails after commit marker | Not permitted as an ordinary error. Implementation must make this step non-failing or report commit-ambiguous/fatal. |
| Root publish fails before backend meta commit | WAL transaction remains unapplied and protected. Retry on flush or recovery. |
| Root pages are built but backend meta commit fails | Built pages are unreachable. WAL transaction remains source of truth. |
| Descriptor update succeeds without watermark | Not permitted. Descriptor updates and watermark update are the same system-root commit. |
| Watermark update succeeds without descriptor update | Not permitted. Descriptor updates and watermark update are the same system-root commit. |
| Cleanup fails after watermark/checkpoint | Safe leak. Retry cleanup later. |
| GC/rewrite wants a side ref protected by unapplied WAL | Must keep it in place or abort maintenance. Rewrite requires an explicit crash-tested WAL metadata protocol. |

## 10. Cleanup and Retention

Collection WAL cleanup is safe only when both are true:

1. every transaction in the segment is covered by the safe per-collection
   applied sequence watermark;
2. the backend checkpoint/meta boundary containing that watermark is durable for
   the selected mode.

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

`DB.Checkpoint` must call registered collection checkpoint hooks before
reporting a clean collection WAL state. PR1 checkpoint hooks force publication
of replayable collection WAL, advance watermarks, and then allow collection WAL
cleanup. If a future checkpoint mode leaves replayable collection WAL
unpublished, it must report collection WAL debt and preserve all side-ref
protections.

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
  applied.

### 11.3 Recovery Tests

Crash/fault points:

1. before collection WAL append;
2. after side-file prepare before collection WAL append;
3. after WAL commit marker before visible staging;
4. after visible staging before response;
5. after acknowledgement before root publish;
6. during async publish;
7. after root pages are built but before the system-root commit advances the
   applied watermark;
8. after applied watermark before WAL cleanup;
9. during overlay compaction;
10. during column-file prepare once column store exists;
11. after two or more acknowledged buffered transactions with the same
    persisted `BaseRootID`;
12. after a higher `GlobalTxnID` from another collection publishes before a
    lower `GlobalTxnID`;
13. after value-log GC/rewrite is requested while an unapplied collection WAL
    transaction is the only owner of a side ref;
14. after a schema/index change is planned while lower `CollectionSeq`
    transactions are durable but unpublished;
15. after a drop/recreate under the same collection name;
16. after a root descriptor generation rewrite attempt with unapplied WAL;
17. after Raft commit before local collection WAL append once Raft apply exists;
18. after local collection WAL append before Raft applied-index/idempotency
    metadata advancement once Raft apply exists.

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
- collection WAL files are cleaned only after safe watermark/checkpoint
  boundaries.
- buffered transactions with shared persisted bases replay by accumulation or
  rebasing without losing acknowledged writes;
- a higher applied `GlobalTxnID` never causes recovery to skip a lower unapplied
  transaction from another collection;
- value-log GC/rewrite cannot remove or move bytes referenced only by pending
  collection WAL side refs;
- a missing complete transaction `N` blocks `N+1` for the same collection;
- drop/recreate with the same collection name does not replay old WAL into the
  new collection;
- schema epoch and root generation mismatches block stale transactions;
- `DB.Checkpoint` either publishes replayable collection WAL or reports debt
  while preserving side refs;
- future Raft apply cannot report an applied index whose logical mutation is
  neither locally recoverable nor guaranteed to be replayed from the Raft log.

### 11.4 Fuzz Tests

- transaction decoder fuzzing;
- recovery ordering fuzzing with multiple collections and duplicate keys;
- side-ref fence fuzzing with missing and truncated payloads;
- root-delta replay fuzzing against an in-memory oracle.

## 12. Benchmark Plan

Benchmarks must compare before/after on the same host with `benchstat`.

Core rows:

- no-index `InsertBatch`;
- one unique secondary index `InsertBatch`;
- three secondary indexes `InsertBatch`;
- `UpdateBatch` with unchanged indexed values;
- `UpdateBatch` with changed indexed values;
- delete-heavy workload;
- async indexed flush workload;
- `Flush`;
- crash-recovery replay of 1K, 100K, and 1M pending documents.

Initial gates:

- WAL-on no-index insert throughput regression <= 10 percent after warmup;
- WAL-on one-index insert throughput regression <= 15 percent;
- WAL-on three-index insert throughput regression <= 20 percent;
- recovery throughput >= 50K root-delta entries/sec on the deterministic
  fixture before optimization;
- collection WAL bytes/doc reported for every benchmark;
- no unbounded memory growth when async publish is stalled.

These gates are provisional. If the current write path cannot meet them with
inline root deltas, PR2 should move large deltas to side payloads before
weakening durability.

## 13. Roadmap and Gates

Traceability matrix:

| Design area | Sections | Milestones | Required evidence |
|---|---|---|---|
| Current contract | 2, 6 | M0 | existing behavior tests and docs |
| WAL format | 7.2, 7.4, 7.5 | M1 | golden, fuzz, corrupt-tail tests |
| Identity and sequencing | 7.3, 7.6, 9.1 | M1, M5 | collection-seq, epoch, generation tests |
| System-root template | 7.2, 8.6 | M1, M5 | descriptor-op and watermark atomicity tests |
| Side-file fences | 7.7, 10 | M2 | missing-ref and GC protection tests |
| Write-domain integration | 8 | M3, M4 | insert/update/delete recovery tests |
| Recovery replay | 9 | M4 | crash matrix, accumulation, and deterministic replay tests |
| Checkpoint/cleanup | 10 | M5 | watermark and segment cleanup tests |
| Performance | 12 | M6 | benchstat gates and artifacts |
| Column-store unblock | 6.3, 10 | M7 | side-file fence and root-group recovery proof |
| Native-wire/Raft layering | 2.4, 6.2, 7.9, 8.8 | M8 | deterministic-entry apply and local-WAL durability tests |

### Milestone 0: Contract Freeze

Deliverables:

- document current collection flush-boundary behavior;
- identify every API that can acknowledge collection writes;
- freeze benchmark fixtures and crash-test harness shape.

Tests:

- current checkpoint boundary test;
- close drains queued indexed flush unit;
- `FlushAll` drains queued indexed flush unit.

Gate:

- no production column-store persistent work starts before this milestone is
  complete.

### Milestone 1: Collection WAL Format Package

Deliverables:

- internal transaction structs;
- encoder/decoder;
- segment reader/writer;
- global transaction id and per-collection sequence allocator interfaces;
- persisted `CollectionID`, `SchemaEpoch`, and root generation descriptor
  encoding;
- `SystemDeltaTemplate` and descriptor-op encoding;
- exact format documentation in this file.

Tests:

- golden files;
- corrupt checksum;
- truncated tail;
- decoder fuzzing;
- replay digest tests;
- descriptor-op placeholder instantiation tests.

Gate:

- format package has no dependency on collection planners or backend DB.

### Milestone 2: Root Delta and Side-Ref Fences

Deliverables:

- root-delta batch serialization;
- inline and side-payload modes;
- side-ref availability checker;
- value-log side-ref integration;
- protected side-ref index updated by WAL append/cleanup and rebuilt during
  recovery;
- value-log GC/rewrite protection for unapplied collection WAL side refs;
- fail-hard recovery behavior for complete transactions with missing required
  side refs.

Tests:

- missing side refs fail recovery for complete WAL-on collection transactions;
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
- final physical primary-root deltas are materialized before the commit marker;
- pending write-domain visibility remains unchanged;
- `Flush` publishes WAL-backed no-index deltas and advances watermark.

Tests:

- crash after ack before flush recovers document;
- crash before WAL append does not expose document;
- checkpoint behavior updated to the new contract.

Gate:

- no-index insert regression <= 10 percent in WAL-on benchmark fixture.

### Milestone 4: Indexed Insert/Update/Delete Integration

Deliverables:

- indexed insert root-group WAL;
- update/delete root-group WAL;
- async flush publish uses existing durable transactions;
- unique helper rebuild after recovery;
- replay accumulator for multiple buffered transactions that share one persisted
  root base;
- schema/index changes ordered by collection sequence;
- overlay compaction encoded as a collection WAL maintenance transaction when it
  can race with user-visible writes.

Tests:

- crash after ack before async publish;
- crash during async publish;
- unique secondary correctness after recovery;
- update/delete secondary roots recover atomically;
- two acknowledged buffered writes against one persisted base both survive
  crash/reopen;
- insert/update/delete chains preserve secondary deletes and puts after
  recovery;
- schema epoch and root generation mismatches block stale transactions.

Gate:

- one-index insert regression <= 15 percent;
- three-index insert regression <= 20 percent;
- async flush cannot lose acknowledged documents under process crash.

### Milestone 5: Recovery, Watermark, and Cleanup

Deliverables:

- applied watermark in system root;
- per-collection applied sequence watermark plus optional global-contiguous
  diagnostic watermark;
- collection WAL replay in open path;
- collection-specific publish wrapper around ordered-root publish APIs;
- segment cleanup after safe checkpoint;
- prepared side-file quarantine/delete.

Tests:

- crash after root page build or watermark commit before cleanup does not
  double-apply;
- out-of-order async publish cannot advance a watermark that hides a lower
  unapplied collection sequence;
- descriptor update and watermark update cannot split across commits;
- older segments remain when watermark is not durable;
- cleanup removes only fully applied segments;
- prepared side files without committed transactions do not become visible.

Gate:

- repeated reopen after crash is idempotent.

### Milestone 6: Checkpoint and Close Semantics

Deliverables:

- `DB.Checkpoint` coordinates with collection WAL;
- checkpoint hook registry analogous to close hooks;
- `CollectionManager.FlushAll` and close hooks use the same publish path;
- API docs distinguish process-crash recovery from fsync durability.

Tests:

- checkpoint forces publication of replayable collection WAL in PR1 or reports
  nonzero collection WAL debt in future modes;
- close with in-flight async publish is recoverable;
- WAL-off mode preserves its relaxed contract.

Gate:

- no stale collection WAL files after clean close and checkpoint.

### Milestone 7: Column-Store Unblock Gate

Deliverables:

- side refs support future column file classes;
- root-group WAL accepts column-store roots without a separate WAL path;
- column-store PR checklist points to this milestone.

Tests:

- synthetic external side file fence test;
- recovery refuses roots with missing external side files;
- cleanup of prepared but unpublished external files.

Gate:

- production column-store persistent writes may start only after this milestone.

### Milestone 8: Native-Wire and Raft Apply Coordination

This milestone is required before any R3-style `ack_policy=raft_committed`
collection write is exposed. It is not a prerequisite for the local collection
WAL or column-store unblock gates unless that work also exposes cluster apply.

Deliverables:

- deterministic-entry apply adapter derives local collection root deltas without
  reconstructing native-wire transport requests;
- local collection WAL append is integrated into follower/leader apply before
  the node advertises the Raft index as locally durable;
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
| A global applied watermark skips lower unapplied transactions. | Use per-collection `CollectionSeq` watermarks as the replay key; keep global watermarks optional and contiguous only. |
| Buffered transactions share an old persisted `BaseRootID`. | Use per-collection dependency chains plus replay-side accumulation in PR1. |
| A drop/recreate or schema change makes an old WAL transaction look applicable. | Persist `CollectionID`, `SchemaEpoch`, and per-root `RootGeneration` guards and block mismatches. |
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
2. Transactions use both `GlobalTxnID` and per-collection `CollectionSeq`.
   `GlobalTxnID` is identity/diagnostics; `CollectionSeq` is the replay and
   skip key.
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

Future questions that do not weaken PR1 correctness:

1. Should `Collection.Flush` gain a sync variant, or should callers use
   `DB.Checkpoint` for fsync-style barriers?
2. How large can inline root deltas be before side-payload mode becomes
   mandatory?
3. What metric names should expose pending collection WAL bytes, applied
   collection-sequence lag, replay count, fenced transactions, protected side
   refs, and cleanup debt?
4. For Raft R3, should applied-index/idempotency metadata live in TreeDB system
   roots, in a Raft library stable store, or in both with an explicit ordering
   rule?
5. Should native-wire `ack_policy` remain purely transport/local durability
   control for deterministic entries, or should a future command version define
   an explicit logical barrier flag? This must be resolved before
   `raft_committed` writes are exposed.

## 16. Implementation Notes

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
  markers, `GlobalTxnID`, per-collection sequence allocation, side-ref
  verification, protected side-ref index rebuild, and segment cleanup after the
  durable applied watermark/checkpoint boundary.
- `TreeDB/collections` owns final physical delta planning before
  acknowledgement. WAL-on collection planners must serialize primary,
  template/index-state, secondary, overlay, delete, schema, and future
  column-store descriptor deltas before the commit marker and must stage the
  same immutable deltas for visibility.
- `TreeDB/db` owns a collection-specific publish wrapper around ordered-root
  executors. The wrapper validates identity/generation/dependency guards,
  materializes root deltas, instantiates `SystemDeltaTemplate`, publishes
  descriptors and watermark atomically, and honors explicit sync options.
- `TreeDB/caching` and checkpoint code must track collection WAL debt. Automatic
  checkpoint and close paths must not report clean WAL state or prune required
  bytes while collection WAL transactions remain needed for recovery.
- `TreeDB/internal/valuelog` and future column-file maintenance must consult the
  protected side-ref index. PR1 rewrite refuses protected refs rather than
  moving them and patching WAL records.
- Column-store implementation may not publish persistent column parts until
  part files, descriptor deltas, primary locator deltas, delete bitmap roots,
  secondary roots, filter roots, and schema roots can be committed as one
  collection WAL transaction.
