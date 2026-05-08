# SPEC: Collection WAL Durability and Root-Group Recovery

Status: proposal  
Target repository studied: `/Users/michaelseiler/dev/snissn/gomap`  
Primary code paths studied: `TreeDB/collections`, `TreeDB/db`,
`TreeDB/caching`, `TreeDB/internal/commitlog`, `TreeDB/internal/valuelog`  
Related specs:

- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/recovery.md`
- `TreeDB/docs/spec/collections-write-domain.md`

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
treedb/collection-wal/global-applied-txn-id -> uint64
treedb/collection-wal/collection/<collection-id>/applied-txn-id -> uint64 optional
```

PR1 can use one global monotonic transaction id. Per-collection watermarks can
be added later if recovery or cleanup needs more concurrency.

When a collection root group is published, the same backend commit that updates
collection root descriptors must also advance the applied watermark. This is the
atomic marker that makes WAL cleanup safe.

Recovery skips transactions at or below the applied watermark. Transactions
above the watermark are candidates for replay.

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

## 9. Recovery Algorithm

Startup recovery should extend the existing recovery order:

1. Recover backend index metadata and choose the valid meta page.
2. Replay normal cached commit logs as today.
3. Scan value-log, leaf-log, column-file, and collection-root-delta side files
   needed by collection WAL fences.
4. Scan collection WAL segments and decode complete transactions.
5. Load applied watermark metadata from the recovered system root.
6. Sort unapplied transactions by `TxnID`.
7. For each unapplied transaction:
   - validate checksum and format version;
   - validate required side refs;
   - validate collection catalog identity and schema epoch;
   - verify each named root's current id matches the transaction's `BaseRootID`,
     unless the transaction is already covered by the applied watermark;
   - materialize `OrderedRootDeltaBatchPublishInput` values;
   - publish the root group with a system-root delta that updates collection
     root descriptors and advances the applied watermark.
8. After successful replay, clean collection WAL segments whose transactions are
   fully covered by the durable applied watermark.
9. Quarantine or delete prepared side files that are not referenced by any
   committed transaction or reachable root.

Recovery must be deterministic. Replaying the same directory twice must produce
the same collection roots and applied watermark.

## 10. Cleanup and Retention

Collection WAL cleanup is safe only when both are true:

1. every transaction in the segment is at or below the applied watermark;
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

Required assertions:

- no acknowledged WAL-on collection write is lost after process crash;
- no unacknowledged incomplete transaction is exposed;
- primary and secondary roots recover together;
- unique indexes reject duplicates after recovery;
- reads do not need stale in-memory write domains after recovery;
- roots never reference missing value-log or column-file bytes;
- collection WAL files are cleaned only after safe watermark/checkpoint
  boundaries.

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
| Side-file fences | 7.5, 10 | M2 | missing-ref recovery tests |
| Write-domain integration | 8 | M3, M4 | insert/update/delete recovery tests |
| Recovery replay | 9 | M4 | crash matrix and deterministic replay tests |
| Checkpoint/cleanup | 10 | M5 | watermark and segment cleanup tests |
| Performance | 12 | M6 | benchstat gates and artifacts |
| Column-store unblock | 6.3, 10 | M7 | side-file fence and root-group recovery proof |

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
- value-log and leaf-log side-ref integration.

Tests:

- missing side refs skip or fail safely;
- no root can be published with missing side bytes;
- large delta side-payload round trips.

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
- unique helper rebuild after recovery.

Tests:

- crash after ack before async publish;
- crash during async publish;
- unique secondary correctness after recovery;
- update/delete secondary roots recover atomically.

Gate:

- one-index insert regression <= 15 percent;
- three-index insert regression <= 20 percent;
- async flush cannot lose acknowledged documents under process crash.

### Milestone 5: Recovery, Watermark, and Cleanup

Deliverables:

- applied watermark in system root;
- collection WAL replay in open path;
- segment cleanup after safe checkpoint;
- prepared side-file quarantine/delete.

Tests:

- crash after root page build or watermark commit before cleanup does not
  double-apply;
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

## 14. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Root-delta WAL duplicates data already in memtables. | Start with simple encoding, then move large deltas to side payloads when benchmarks require it. |
| Replaying old transactions double-applies after crash. | Store applied watermark in the same system-root commit as root descriptor updates. |
| Side-file ordering creates dangling pointers. | Require side refs to be readable before WAL commit is replayable; never publish roots until side refs pass. |
| Async flush races with WAL cleanup. | Treat async flush as publish-only; cleanup requires applied watermark and checkpoint boundary. |
| Unique index helpers are lost on crash. | Rebuild helpers from durable root deltas or persisted roots during recovery. |
| WAL-on throughput regresses too much. | Benchmark each milestone, use side payloads, batching, and async publish before relaxing durability. |
| WAL-off users assume stronger guarantees. | Keep WAL-off relaxed docs explicit and add tests that preserve the relaxed contract. |

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

## 16. Implementation Notes

- Prefer adding the collection WAL package below `TreeDB/internal` until the
  format stabilizes.
- Keep the transaction encoder independent from collection planner internals.
- Make root-delta serialization deterministic from the beginning.
- Add fault-injection hooks before building the full product path; otherwise
  crash coverage will be too shallow.
- Preserve existing write-domain read precedence while adding durable backing.
- Treat side-file fences as part of the storage format, not as cleanup policy.
- Update `collections-write-domain.md`, `write-path-and-durability.md`,
  `recovery.md`, and `verification.md` as implementation milestones land.
