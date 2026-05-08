# SPEC: Collection WAL Durability and Root-Group Recovery

Status: draft proposal, non-normative.
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
3. The collection WAL transaction records root names, base root ids, ordered
   root deltas, side-file references, and a transaction sequence.
4. Recovery replays complete transactions by publishing the same root group and
   advancing an applied watermark in the system root.

This is a hard prerequisite for production column-store collection writes.
Before this milestone passes, column-store work may proceed only for docs,
benchmarks, pure codecs, filters, and isolated format encode/decode tests that
do not publish persistent collection roots.

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
leaf-log records. Future examples include column part files, filter files, and
delete bitmap files.

Applied watermark:

System-root metadata that records the latest collection WAL transaction that
has been published into backend roots. Recovery uses the watermark to avoid
double-applying transactions after a crash that occurs after root publish but
before WAL cleanup.

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
- It may either force `FlushAll` through the registered collection close/flush
  hook or publish all durable collection WAL transactions before pruning related
  log segments.
- It must not report a clean WAL state while collection WAL transactions remain
  needed for recovery.

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

Logical transaction:

```text
CollectionWALTransaction {
    Version              uint8
    TxnID                uint64
    CollectionID         uint64 or stable collection name
    CollectionName       string
    BaseCommitSeq        uint64
    BaseSystemRoot       uint64
    PlannerEpoch         uint64 optional
    RootDeltas           []CollectionRootDelta
    SideRefs             []CollectionSideRef
    AckMode              uint8
    CreatedUnixNanos     int64
    Stats                CollectionWALStats
    ChecksumCRC32C       uint32
}

CollectionRootDelta {
    RootName             string
    RootKind             uint8
    BaseRootID           uint64
    StoragePolicy        uint8
    DeltaEncoding        uint8
    InlineDelta          bytes optional
    DeltaRID             uint64 optional
    EntryCount           uint32
    KeyBytes             uint64
    ValueBytes           uint64
}

CollectionSideRef {
    RefClass             uint8
    FileID               uint64
    RelativePath         string optional
    Offset               uint64
    Size                 uint64
    ChecksumCRC32C       uint32
    Required             bool
}
```

Root delta payloads should use the same canonical sorted delta encoding as
`OrderedRootDeltaBatchFromIterator`. Small deltas may be inline in the WAL
transaction. Large deltas should use a side payload referenced by RID or by a
dedicated root-delta payload class.

`AckMode`, `CreatedUnixNanos`, and `Stats` are local observability and policy
metadata. They must not change root-delta replay output, idempotency outcomes,
catalog guard outcomes, or future Raft state-machine results.

### 7.3 File Class

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
- Each transaction has exactly one `TxnID`.
- Transactions are sorted by `TxnID` during recovery, with file order as a
  deterministic tie breaker only for malformed legacy segments.
- A truncated tail stops scanning that segment after the last complete record.
- Hard corruption before the tail fails recovery.

### 7.4 Applied Watermark

The system root must store collection WAL progress:

```text
treedb/collection-wal/global-contiguous-applied-txn-id -> uint64 optional
treedb/collection-wal/collection/<collection-id>/applied-txn-id -> uint64
```

PR1 can use one global monotonic transaction id for transaction identity. It
must not use a single global applied high-watermark unless publication and
watermark advancement are strictly contiguous in `TxnID` order across all
collection write domains.

Safe choices:

1. Enforce one ordered global publisher, so transaction `N+1` cannot advance an
   applied watermark before transaction `N` is applied.
2. Track per-collection applied watermarks and advance a separate global
   contiguous watermark only when every lower `TxnID` is known applied.

The second choice is preferred if async publishers can run independently per
collection or write domain. Recovery may skip a transaction only when that
transaction is covered by its collection watermark or by a global contiguous
watermark. It must never skip transaction `N` merely because transaction `N+1`
from another collection was published first.

When a collection root group is published, the same backend commit that updates
collection root descriptors must also advance the applied watermark. This is the
atomic marker that makes WAL cleanup safe.

Recovery skips transactions covered by a safe applied watermark. Transactions
not covered by a safe watermark are candidates for replay.

### 7.5 Side-File Fences

A collection transaction is replayable only when every required side reference
is readable and passes its integrity check.

Current side refs:

- value-log records for large collection document values;
- leaf-log records when ordered roots store outer leaves in the value log.

Future side refs:

- column part files;
- column filter files;
- delete bitmap files;
- dictionary files.

If a required side ref is missing during recovery:

- no root group from that transaction may be published;
- recovery must not produce roots pointing to missing bytes;
- later transactions for the same collection should be blocked unless the
  implementation can prove they do not depend on the skipped transaction;
- durable mode should surface an error unless the missing ref is in a clearly
  incomplete tail transaction that can be ignored by the same rules as existing
  commit-log tails.

Collection WAL side refs are also retention roots. Value-log GC, value-log
rewrite, column-file cleanup, and future side-file maintenance must treat every
required side ref in an unapplied or not-yet-cleanable collection WAL
transaction as reachable until the transaction is covered by a safe applied
watermark and the checkpoint/meta boundary needed for cleanup is durable.

Implementation options:

1. GC/rewrite scans collection WAL segments and includes required `SideRefs` in
   its reachability set.
2. WAL append/update maintains a protected side-ref index that GC/rewrite scans
   with the normal root reachability set.

PR1 should prefer protection over rewrite: if a value-log segment or column file
is referenced only by pending collection WAL, maintenance should keep it in
place rather than moving it and trying to patch WAL records. A future rewrite
mode may rewrite protected refs only if it publishes replacement side refs and
collection WAL metadata atomically under an explicit crash-tested protocol.

### 7.6 Native Wire and Raft Layering

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

### 8.1 No-Index Inserts

Current no-index inserts can remain buffered in `domain.table`. After this
spec, a WAL-on insert must:

1. validate the collection catalog and primary uniqueness;
2. build the root-local primary delta entry;
3. pointerize large values if configured;
4. append a collection WAL transaction containing the primary delta and side
   refs;
5. stage the same delta in the collection write domain for immediate reads;
6. optionally publish later through `Flush`, threshold, checkpoint, or close.

The key change is that step 4 happens before success is returned to the caller.

### 8.2 Indexed Inserts

Indexed insert planning already produces root runs for:

- primary document root;
- template/index-state root when needed;
- secondary index roots.

After this spec, the planner should convert those root runs into a collection
WAL transaction before adding them to the durable pending write domain. Unique
probe helpers remain in memory, but they must be rebuildable from the root
deltas during recovery.

### 8.3 Updates and Deletes

Updates and deletes must use the same root-delta transaction model:

- primary root put/delete;
- old secondary index deletes;
- new secondary index puts;
- template/index-state changes when document format requires them;
- delete/tombstone roots where applicable.

The WAL records planned deltas, not the callback or predicate that produced
them.

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

Overlay compaction is a separate root-group transaction. It can be WAL logged
like any other collection root-group publish or treated as a maintenance commit
that is replayed through backend metadata if it reaches the backend commit
boundary. The implementation must choose one rule and test crash points around
it.

### 8.6 WAL-Off Mode

`DurabilityWALOffRelaxed` should not write collection WAL transactions. In that
mode:

- pending collection writes are visible in-process only until published;
- `Flush`, `FlushAll`, checkpoint, and close remain the durability boundaries;
- column-store mutable writes must not be production-enabled under WAL-off
  until explicit benchmark and safety gates are added.

### 8.7 Future Raft Apply Path

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
4. Scan any additional collection WAL side-file classes, such as column-file
   and collection-root-delta side files, needed by collection WAL fences.
5. Scan collection WAL segments and decode complete transactions.
6. Load applied watermark metadata from the recovered system root.
7. Sort unapplied transactions by `TxnID`.
8. For each unapplied transaction:
   - validate checksum and format version;
   - validate required side refs;
   - validate collection catalog identity and schema epoch;
   - verify each named root's current id matches the transaction's `BaseRootID`,
     unless the transaction is covered by a replay accumulator as described
     below;
   - materialize `OrderedRootDeltaBatchPublishInput` values;
   - publish the root group with a system-root delta that updates collection
     root descriptors and advances the applied watermark.
9. After successful replay, clean collection WAL segments whose transactions are
   fully covered by the durable applied watermark.
10. Quarantine or delete prepared side files that are not referenced by any
   committed transaction or reachable root.

Recovery must be deterministic. Replaying the same directory twice must produce
the same collection roots and applied watermark.

### 9.1 Buffered Transaction Replay and Rebasing

Buffered collection writes can create several acknowledged WAL transactions
whose root deltas all name the same persisted `BaseRootID`. That is valid if
the write domain recorded each pending mutation relative to the persisted
catalog root and planned to publish the merged flush unit later. It is not safe
for recovery to publish transaction 1, update the root id, and then reject
transaction 2 solely because transaction 2 still names the old persisted base.

The implementation must choose one of these replay-safe models:

1. Log only merged flush-unit transactions, so every transaction's `BaseRootID`
   is the backend root id expected at replay time.
2. Give each later WAL transaction a logical base that includes earlier pending
   WAL deltas for that root.
3. Rebase/accumulate during recovery.

The recommended PR1 model is replay-side accumulation. Recovery should process
unapplied transactions in `TxnID` order, grouped by collection and root. For
each root, it maintains a virtual replay base seeded from the current catalog
root and a pending delta accumulator:

- if a transaction's `BaseRootID` equals the current persisted root, recovery
  may start or extend the accumulator for that root;
- if a transaction's `BaseRootID` equals the original buffered base already
  covered by the accumulator, recovery appends the transaction's root delta to
  the accumulator instead of treating the root-id mismatch as corruption;
- if a transaction's `BaseRootID` is neither the current persisted root nor a
  known buffered base for that accumulator, recovery must block or fail rather
  than publish an ambiguous delta;
- recovery publishes accumulated root deltas in one deterministic root group or
  in a sequence whose later `BaseRootID` values are explicitly rebased to the
  roots produced by earlier publishes;
- applied watermarks may advance only for the exact transactions covered by the
  successful accumulated publish.

This rule applies to no-index buffered writes, indexed mutable runs, queued
flush units, and async publishing states. It is also the rule future
column-store delta-part descriptors must follow if several acknowledged deltas
share one persisted base part-descriptor root.

## 10. Cleanup and Retention

Collection WAL cleanup is safe only when both are true:

1. every transaction in the segment is covered by a safe applied watermark;
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

## 11. Test Plan

### 11.1 Current Contract Tests

Keep and expand tests that document current behavior:

- checkpoint does not flush pending collection-local no-index inserts before
  this spec changes that contract;
- close drains queued indexed flush units;
- `FlushAll` drains queued indexed flush units;
- async flush backpressure and requeue behavior remains visible and bounded.

### 11.2 Format Tests

- exact-byte collection WAL transaction golden files;
- corrupt checksum rejection;
- truncated tail acceptance;
- mixed transaction sequence rejection;
- large root delta side-payload round trips;
- unknown future version rejection with clear error.

### 11.3 Recovery Tests

Crash/fault points:

1. before collection WAL append;
2. after side-file prepare before collection WAL append;
3. after collection WAL append before in-memory staging;
4. after acknowledgement before root publish;
5. during async publish;
6. after root pages are built but before the system-root commit advances the
   applied watermark;
7. after applied watermark before WAL cleanup;
8. during overlay compaction;
9. during column-file prepare once column store exists.
10. after two or more acknowledged buffered transactions with the same
    persisted `BaseRootID`;
11. after a higher `TxnID` from another collection publishes before a lower
    `TxnID`;
12. after value-log GC/rewrite is requested while an unapplied collection WAL
    transaction is the only owner of a side ref;
13. after Raft commit before local collection WAL append once Raft apply exists.
14. after local collection WAL append before Raft applied-index/idempotency
    metadata advancement once Raft apply exists.

Required assertions:

- no acknowledged WAL-on collection write is lost after process crash;
- no unacknowledged incomplete transaction is exposed;
- primary and secondary roots recover together;
- unique indexes reject duplicates after recovery;
- reads do not need stale in-memory write domains after recovery;
- roots never reference missing value-log or column-file bytes;
- collection WAL files are cleaned only after safe watermark/checkpoint
  boundaries.
- buffered transactions with shared persisted bases replay by accumulation or
  rebasing without losing acknowledged writes;
- a higher applied `TxnID` never causes recovery to skip a lower unapplied
  transaction from another collection;
- value-log GC/rewrite cannot remove or move bytes referenced only by pending
  collection WAL side refs;
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
| WAL format | 7.2, 7.3 | M1 | golden, fuzz, corrupt-tail tests |
| Side-file fences | 7.5, 10 | M2 | missing-ref and GC protection tests |
| Write-domain integration | 8 | M3, M4 | insert/update/delete recovery tests |
| Recovery replay | 9 | M4 | crash matrix, rebasing, and deterministic replay tests |
| Checkpoint/cleanup | 10 | M5 | watermark and segment cleanup tests |
| Performance | 12 | M6 | benchstat gates and artifacts |
| Column-store unblock | 6.3, 10 | M7 | side-file fence and root-group recovery proof |
| Native-wire/Raft layering | 2.4, 6.2, 7.6, 8.7 | M8 | deterministic-entry apply and local-WAL durability tests |

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
- transaction id allocator interface;
- exact format documentation in this file.

Tests:

- golden files;
- corrupt checksum;
- truncated tail;
- decoder fuzzing.

Gate:

- format package has no dependency on collection planners or backend DB.

### Milestone 2: Root Delta and Side-Ref Fences

Deliverables:

- root-delta batch serialization;
- inline and side-payload modes;
- side-ref availability checker;
- value-log and leaf-log side-ref integration;
- value-log GC/rewrite protection for unapplied collection WAL side refs.

Tests:

- missing side refs skip or fail safely;
- no root can be published with missing side bytes;
- large delta side-payload round trips;
- GC/rewrite cannot delete or move bytes referenced only by unapplied
  collection WAL side refs.

Gate:

- side-ref fence behavior matches existing RID fence safety or is stricter.

### Milestone 3: No-Index Collection Integration

Deliverables:

- WAL-on no-index inserts append collection WAL before acknowledgement;
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
- replay accumulator or merged-transaction path for multiple buffered
  transactions that share one persisted root base.

Tests:

- crash after ack before async publish;
- crash during async publish;
- unique secondary correctness after recovery;
- update/delete secondary roots recover atomically;
- two acknowledged buffered writes against one persisted base both survive
  crash/reopen.

Gate:

- one-index insert regression <= 15 percent;
- three-index insert regression <= 20 percent;
- async flush cannot lose acknowledged documents under process crash.

### Milestone 5: Recovery, Watermark, and Cleanup

Deliverables:

- applied watermark in system root;
- per-collection or global-contiguous watermark safety for out-of-order async
  publishers;
- collection WAL replay in open path;
- segment cleanup after safe checkpoint;
- prepared side-file quarantine/delete.

Tests:

- crash after root page build or watermark commit before cleanup does not
  double-apply;
- out-of-order async publish cannot advance a watermark that hides a lower
  unapplied transaction;
- older segments remain when watermark is not durable;
- cleanup removes only fully applied segments;
- prepared side files without committed transactions do not become visible.

Gate:

- repeated reopen after crash is idempotent.

### Milestone 6: Checkpoint and Close Semantics

Deliverables:

- `DB.Checkpoint` coordinates with collection WAL;
- `CollectionManager.FlushAll` and close hooks use the same publish path;
- API docs distinguish process-crash recovery from fsync durability.

Tests:

- checkpoint drains or checkpoints collection WAL according to the chosen rule;
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
| Replaying old transactions double-applies after crash. | Store applied watermark in the same system-root commit as root descriptor updates. |
| Side-file ordering creates dangling pointers. | Require side refs to be readable before WAL commit is replayable; never publish roots until side refs pass. |
| Value-log GC/rewrite removes bytes referenced only by pending collection WAL. | Treat required collection WAL side refs as GC/rewrite roots until safe applied watermark and checkpoint cleanup. |
| A global applied watermark skips lower unapplied transactions. | Enforce contiguous global publication or use per-collection watermarks plus a global contiguous watermark. |
| Buffered transactions share an old persisted `BaseRootID`. | Log merged flush units, make later transactions depend on earlier pending deltas, or rebase/accumulate deltas during recovery. |
| Async flush races with WAL cleanup. | Treat async flush as publish-only; cleanup requires applied watermark and checkpoint boundary. |
| Unique index helpers are lost on crash. | Rebuild helpers from durable root deltas or persisted roots during recovery. |
| WAL-on throughput regresses too much. | Benchmark each milestone, use side payloads, batching, and async publish before relaxing durability. |
| WAL-off users assume stronger guarantees. | Keep WAL-off relaxed docs explicit and add tests that preserve the relaxed contract. |
| Raft log entries are confused with collection WAL transactions. | Keep deterministic entries as logical command input and collection WAL as node-local root-delta durability; test logical digests instead of byte-identical physical layout. |
| A node reports a Raft entry applied before its local collection mutation is recoverable. | Tie applied-index/idempotency metadata to local collection WAL durability or replay unapplied committed entries from Raft before serving. |
| Native-wire acknowledgement policy leaks into recovered logical state. | Treat acknowledgement policy as response/local durability control unless a future deterministic command version explicitly makes it logical state. |

## 15. Open Questions

1. Should collection WAL share the existing commit-log segment writer with a new
   record kind, or use a separate `collection-l<lane>-<seq>.log` file class?
2. Should the first transaction id be global or per collection?
3. Should `Collection.Flush` gain a sync variant, or should callers use
   `DB.Checkpoint` for fsync-style barriers?
4. Should recovery skip missing side-ref transactions like existing RID fences,
   or fail hard in durable mode?
5. How large can inline root deltas be before side-payload mode becomes
   mandatory?
6. Can overlay compaction use ordinary backend commit durability, or should it
   also be encoded in collection WAL for uniform recovery?
7. Should `DB.Checkpoint` always call `CollectionManager.FlushAll`, or can it
   checkpoint collection WAL without forcing root publication?
8. What metric names should expose pending collection WAL bytes, applied
   watermark lag, replay count, skipped/fenced transactions, and cleanup debt?
9. Should PR1 use merged flush-unit WAL records or replay-side accumulation for
   buffered writes that share one persisted base root?
10. Should side-ref GC protection scan collection WAL segments directly or
    maintain a separate protected side-ref index?
11. For Raft R3, should applied-index/idempotency metadata live in TreeDB system
   roots, in a Raft library stable store, or in both with an explicit ordering
   rule?
12. Should native-wire `ack_policy` remain purely transport/local durability
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
