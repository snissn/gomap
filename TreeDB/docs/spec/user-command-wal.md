# SPEC: User Command WAL and Applied-LSN Recovery

Status: normative target contract; not current behavior until the milestone
evidence below is accepted. The detailed implementation plan lives in issue
https://github.com/snissn/gomap/issues/1529. This document supersedes the
collection-specific physical/root-delta WAL target in
`collection-wal-durability-plan.md` for future implementation work.

Tracker: https://github.com/snissn/gomap/issues/1529

TreeDB is pre-alpha. On-disk formats and public APIs may change. This freedom
must not create fail-open recovery behavior: once a directory advertises a
required command-WAL feature, unsupported binaries must refuse to serve, clean,
compact, or rewrite that directory.

Related specs:

- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/recovery.md`
- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/docs/spec/collections-write-domain.md`
- `TreeDB/docs/spec/native-wire-protocol.md`
- `TreeDB/docs/spec/native-query-raft-roadmap.md`

## 1. Summary

The active WAL direction is a user-command WAL: TreeDB records accepted,
deterministic user-level mutation commands, then recovery replays unapplied
commands in log sequence order until the checkpointed state catches up.

The WAL should be write-once/read-rarely. In the normal case, committed state is
covered by checkpointed roots and the WAL is deleted without ever being replayed.
The WAL exists to recover acknowledged commands that were accepted but not yet
covered by a durable checkpoint boundary.

The core model is:

```text
accepted command stream
  -> typed frames in the existing commit-log WAL ordered by LSN
  -> normal command executor publishes TreeDB roots
  -> checkpoint records AppliedLSN with durable roots
  -> recovery replays WAL frames with LSN > AppliedLSN
```

This is intentionally closer to the existing raw commit-log model than to a
collection-specific physical root-delta WAL. The durable record is a canonical
command envelope plus payload, not transient collection planner state and not
physical page/root changes.

There is one WAL substrate. User-command WAL extends the existing TreeDB
commit-log implementation; it does not introduce a second collection-specific
WAL system. Command frames use the same `wal/commit-l*.log` segment family and reuse the
current WAL subsystem's lifecycle concepts: append-before-visibility ordering,
checksums, optional compression, lane ordering, recovery discovery, checkpoint
cleanup boundaries, and value-log RID/external-reference fences. The on-disk
command WAL format is allowed to break compatibility with the current raw
`commitlog.Record` batch format; reducing implementation churn and keeping one
command format is preferred over preserving old WAL payload readers.

## 2. Design Goals

1. Keep the WAL simple: append ordered command frames, replay only after crash.
2. Cover the mutation commands TreeDB already exposes when they can be lowered to
   deterministic replay input.
3. Fail closed for commands whose replay semantics are not defined.
4. Make checkpointed roots plus `AppliedLSN` the cleanup boundary.
5. Align local WAL, native-wire deterministic command entries, and future Raft
   around shared canonical command payload schemas while keeping their
   storage/consensus responsibilities separate.
6. Avoid encoding JSON/BSON/template/index internals in the WAL unless those
   bytes are the canonical user payload for the command.
7. Avoid a separate collection-only WAL semantics stack: no
   `wal/collection-l*.log`, no active `internal/collectionwal` appender, and no
   collection-only cleanup/watermark system.
8. Support compact declarative update APIs without trusting arbitrary Go
   callback purity: nondeterministic helper values must be resolved to literals
   before WAL append.

## 3. Non-Goals for V1

The first user-command WAL does not implement:

- arbitrary query-wide mutations such as `update users where age > 30`;
- MongoDB multi-document transactions;
- cross-collection atomic transactions;
- Raft replication;
- a consensus state root;
- physical root-delta replay;
- page-level redo/undo logging;
- WAL-backed durable pending overlays, mutable overlays, queued flush units, or
  publishing overlays that become visible before root publication;
- replay of arbitrary Go callbacks;
- replay-time calls to nondeterministic helpers such as wall clock, random, UUID,
  sequence, or process-local resolver functions.

Callback-based APIs may be WAL-covered only after the callback has run and the
accepted mutation is lowered to replayable final replacements or another stable
command payload. The WAL must never attempt to serialize or rerun user callback
code.

Declarative update APIs may expose convenience helpers for values such as
server time, UUIDs, or monotonic sequences, but those helpers are an admission
path feature only. The helper must resolve to a canonical literal value before
the WAL frame is appended; recovery and future followers replay the literal, not
the helper call.

## 4. Definitions

| Term | Meaning |
|---|---|
| `CommandEnvelope` | Versioned durable wrapper for one replayable user mutation command. |
| `CommandPayload` | Canonical bytes for the command kind. Payload bytes must be stable for replay and any command-specific assertion validation. |
| `LSN` | Monotonic local WAL sequence number assigned by the shared command WAL journal before command visibility/durable acknowledgement. Raw KV, collection, catalog, and future native-wire commands share this one sequence stream. |
| `AppliedLSN` | Highest contiguous command LSN covered by the durable checkpointed TreeDB state. This value is selected atomically with the roots that contain those command effects. |
| `Checkpoint` | Durable root boundary that syncs required files and records `AppliedLSN`. |
| `Publish boundary` | One backend commit/meta selection that makes user roots, system root metadata, value-log/leaf-log reachability, target raw KV revision state, and `AppliedLSN` visible together. |
| `ExternalRef` | Reserved typed reference to bytes outside the command frame. The current V1 and active V2 codecs reject every non-empty `CommandEnvelope.ExternalRefs` section. V2 RawKV `SetRID` dependencies use the separate canonical RID fence and exact-handle debt protocol; the general typed section remains quarantined until its producer, sync, recovery, and deletion ownership land atomically. |
| `State root` | The TreeDB root state selected by meta/system-root recovery. In distributed mode this is not automatically a consensus digest. |
| `Replay executor` | The same semantic command executor used by the normal write path, constrained to deterministic replay mode. |
| `DeclarativeUpdateOp` | Versioned update operator whose canonical payload fully defines replay, for example set field to literal, unset field, or a future `$inc`-style operator. |
| `Resolver helper` | Admission-time helper that chooses nondeterministic values such as time, UUID, random, or sequence values before WAL append. The helper call is never part of replay identity. |
| `Resolved literal` | The concrete value produced by a resolver helper and embedded in a canonical command payload. |
| `WAL-supported` | Command has canonical encoding, deterministic replay rules, failure behavior, and crash/recovery tests. |
| `WAL-rejected` | Command exists but must be rejected in WAL-on durable-at-ack modes until replay semantics are defined. |
| `WAL-off-only` | Command may be allowed only when the selected durability profile makes no durable-at-ack claim. |

## 5. Command Envelope

The command envelope is the WAL payload format for all mutating surfaces. Raw
key/value writes are represented as `RawKVBatch` commands in this envelope, not
as a legacy `commitlog.Record` compatibility payload. Collection, catalog, and
native-wire command payloads use the same envelope family with their own
versioned command payload schemas.

Where a native-wire deterministic command-entry schema already exists for the
same logical mutation, the local command WAL payload must reuse that canonical
schema directly or wrap it without changing its deterministic bytes. TreeDB must
not grow separate local-WAL and native-wire encoders for the same command unless
the divergence is documented in the command matrix and covered by golden fixtures
that prove both encodings lower to the same command semantics.

### 5.1 Native-wire and Raft alignment

The V1 local command WAL and native-wire deterministic-entry encoder have the
same semantic owner but not the same durability role. Native-wire/Raft entries
describe deterministic user command input. Local command-WAL frames describe the
single-node recoverable command that TreeDB will replay after a crash. A
native-wire mutation that maps to a supported command-WAL kind must lower to a
local command-WAL frame and satisfy the requested local ack boundary before
reporting local recoverability.

`raft_committed` is not local WAL append. It is a future distributed consensus
policy that may wrap native-wire deterministic entry bytes with term/index
metadata. Applying a committed Raft entry still has to respect the local
recoverability boundary for the serving replica: the replica may not claim that
the command is locally recoverable until the local command-WAL frame, required
external refs, normal executor effects, and any requested root/`AppliedLSN`
barrier are complete.

The V1 relationship between native-wire deterministic fixtures and local
command-WAL fixtures is tracked in
`TreeDB/docs/spec/command-wal-nativewire-alignment.json`. Entries marked
`lowered_equivalent_v1` are reserved for matched logical inputs that prove
native-wire deterministic command bytes lower to the listed local command-WAL
payload schema before local acknowledgement. Entries marked
`lowered_kind_only_v1` pin only the shared command kind and supported surface:
their current native-wire and local command-WAL fixtures intentionally use
different logical inputs and must not be treated as payload-equivalence proof.
Entries marked `future_rejected_v1` have pinned native-wire deterministic bytes
but remain explicitly rejected by local command WAL until the matching command
kind and recovery tests land.

This is a compatibility-breaking WAL format transition. TreeDB is pre-alpha, so
the command WAL implementation may require old directories to be cleanly
checkpointed with the previous binary or rebuilt. Once a directory advertises
the command WAL feature, readers must fail closed on old raw batch payloads
instead of silently replaying them.

The target envelope is:

```go
type CommandEnvelope struct {
    Version          uint16
    DurabilityClass  CommandDurabilityClass
    LSN              uint64
    Kind             CommandKind
    Scope            CommandScope
    FeatureFlags     uint64
    CatalogEpoch     uint64
    SchemaEpoch      uint64
    BaseAppliedLSN   uint64
    PayloadFormat    PayloadFormat
    Payload          []byte
    ExternalRefs     []ExternalRef
    Preconditions    []Precondition
    ResultAssertions []ResultAssertion
}
```

`AckPolicy`, request timeout, tracing fields, and transport details are not part
of deterministic command identity. They may influence when an API returns, but
must not change replay bytes.

### 5.2 Required Envelope Properties

- `LSN` is assigned by the shared commit-log journal service before a complete
  frame can become recoverable.
- `Kind` selects a versioned payload schema.
- `Scope` identifies raw KV, one collection, catalog/DDL, or a future explicit
  multi-scope command.
- The segment frame CRC covers the encoded command frame bytes for physical
  corruption and truncation detection.
- `ExternalRefs` is a reserved wire section for required bytes outside a
  command frame. Current V1 and V2 encode/decode reject every non-empty
  section. V2 RawKV `SetRID` uses `ExternalRefFenceV1` and exact-handle
  dependency debt instead of this general section.
- `Preconditions` make stale or incompatible replay fail closed.
- `ResultAssertions` allow recovery to detect semantic drift, for example
  expected matched/modified counts or optional document digests.
- Raw KV command apply assigns and stores target `EntryRevision` metadata as
  part of the same command effect as the value or tombstone. Replay must derive
  the same revision for the same accepted command input as live apply.
  Non-Raft local command-WAL raw KV frames use the command frame `LSN` as the
  mutation revision for all raw keys touched by the frame only when the shared
  LSN allocator is seeded above the durable raw-KV revision floor. Otherwise the
  accepted command must carry one effective mutation revision from the same
  persisted domain. Future Raft-applied raw KV frames must carry or
  deterministically derive the Raft apply identity used as the mutation revision
  before appending/applying through the local command-WAL durability boundary.
- Every command kind declares whether replay is strict or has an explicit
  idempotent-skip rule. Strict commands fail closed if replay observes evidence
  that the command effect already exists while `AppliedLSN` does not cover it.

### 5.3 Compatibility-Breaking Command WAL Format

The first implementation should modify `TreeDB/internal/commitlog` into the
command WAL format rather than layering command frames beside legacy raw batch
payloads. Reuse implementation pieces where useful, but do not preserve
`ReadBatch`/`AppendBatch` as a compatibility requirement for command WAL
directories.

Required integration points:

- define one command WAL frame payload that can represent `RawKVBatch`,
  collection-command, and catalog-command families;
- replace raw WAL writes with `RawKVBatch` command frames;
- make old raw `commitlog.Record` payloads unsupported once the command WAL
  required feature is present;
- require activation to start from a clean WAL state, or require the operator to
  rebuild the DB directory;
- keep the `wal/commit-l<lane>-<seq>.log` segment family and single journal
  ownership model;
- preserve the correctness properties of the current WAL implementation:
  append-before-visibility, checksum validation, terminal truncated-tail
  handling, rotation, flush/sync ordering, and cleanup only after durable proof;
- make raw KV entry revision metadata part of the `RawKVBatch` command effect
  instead of a physical sidecar record with a separate durability boundary;
- fail closed on unknown required frame versions or command kinds.

### 5.3.1 Active Command-Frame V2 Boundary

The `command_wal_v2` required feature activates V2 append, strict scan,
durable-frontier classification, suffix repair, replay, publication, and cleanup
as one format boundary:

- production command-WAL journals append only V2 frames and strict open rejects
  V1 with a pre-alpha rebuild-required error;
- strict V2 scan rejects commit-log segment compression with the typed
  `ErrCommandWALV2CompressedRecordUnsupported`, because a torn compressed
  record does not expose an authenticated LSN or durability class;
- each relaxed frame retains ordered exact-handle dependency debt without
  issuing a stable sync;
- a durable command or empty barrier syncs the coalesced dependency prefix and
  successor namespaces before appending, then syncs the WAL before releasing
  debt;
- when an already-appended relaxed staged intent is published through a sync
  API, its publication covers the contiguous range from the mutation LSN
  through the appended durable barrier LSN; the mutation keeps its original LSN
  identity while `AppliedCommandLSN` advances through the barrier;
- a successful synced checkpoint cleanup releases debt through its durable
  `AppliedCommandLSN`, so a later barrier cannot reopen a covered segment that
  cleanup deleted; and
- pre-append failures are retryable, while ambiguous post-append/sync failures
  poison the handle and require recovery.

V2 persists `CommandDurabilityClass` in header bytes `[54,56)` (`Durable=1`,
`Relaxed=2`). It adds the empty durable `DurablePrefixBarrierV1` and requires
one canonical `ExternalRefFenceV1` precondition on every raw-KV frame containing
`SetRID`; a frame without `SetRID` has no RID fence. The fence is the unique RID
count plus SHA-256 over sorted unique little-endian RIDs and is verified from
decoded payload bytes before external dependency lookup.

The `ExternalRefFenceV1` precondition is distinct from the dormant typed
`CommandEnvelope.ExternalRefs` section. It fences the RID set already encoded
by RawKV `SetRID` payload operations and remains supported while typed
`ExternalRefs` are rejected.

Recovery first derives the highest valid durable frontier and validates the
entire prefix through it without mutation. Relaxed frames above that frontier
are not replayable; they form the physical suffix that is repaired in
reverse global-LSN order, with later file sync/removal and WAL-directory sync
completed before the anchor truncate/sync. Read-only inspection returns the
same planned stages in structured `ErrRecoveryRequired` diagnostics without
changing files.

### 5.4 Single Journal Owner

Exactly one journal owner may open mutable WAL writers for a database directory.
The implementation should extract or reuse the existing cached WAL lane writer
and lifecycle code as a shared journal service instead of adding a backend-only
or collection-only writer that opens a second segment family.

The shared journal service owns:

- `wal/commit-l<lane>-<seq>.log` file creation;
- WAL sequence allocation;
- lane selection and ordering metadata;
- segment rotation;
- append buffering;
- flush and sync policy;
- checkpoint cleanup eligibility.

Raw cached writes, collection commands, catalog commands, and future native-wire
command entries must append through this shared service as typed commands. If cached raw mode and
collections are active in the same process, they must coordinate through the
same journal owner rather than opening independent writers into the same WAL
directory.

## 6. V1 Command Support Matrix

V1 should support only commands that are already exposed and can be made
replayable without adding query-wide mutation semantics.

| Surface | Current user-facing shape | V1 WAL command | Status policy |
|---|---|---|---|
| Raw KV set/delete/batch | `Set`, `Delete`, `Batch.Write` | `RawKVBatch` | PR1 has gated typed bytes and fixtures; production raw writes become `WAL-supported` after PR3 recovery dispatch and `AppliedCommandLSN` plumbing. |
| Raw KV conditional transaction | target optimistic point-read transaction API | `RawKVBatch` with preconditions or a future `RawKVConditionalBatch` if payload extension is required | `future`; must reject before LSN assignment until point-read preconditions, entry revisions, and replay result assertions are implemented together. |
| Collection insert batch | explicit IDs plus stored documents | `CollectionInsertBatchByID` | PR4 implementation: `WAL-supported` for JSON, BSON, and template-v1 stored documents through the normal collection executor. |
| Collection delete | explicit document ID or ID batch | `CollectionDeleteBatchByID` | PR4 implementation: `WAL-supported`; missing IDs are explicit no-ops and recovery still advances `AppliedCommandLSN`. |
| Collection declarative update | explicit document ID plus canonical update ops over resolved literal values | `CollectionUpdateByIDOps` or final replacement | PR5 implementation: callback and BSON-set update paths are `WAL-supported` by logging final accepted replacements; operator-native payloads remain future work. |
| Resolver-backed update helpers | helpers such as server-now, UUID, random, or sequence values used by declarative update APIs | not a replay command; lowers to resolved literals inside `CollectionUpdateByIDOps` | `WAL-supported` only when resolved before WAL append. Recovery must not invoke helper functions. |
| Collection update callback | explicit document ID plus Go callback | `CollectionUpdateBatchByID` after callback execution | PR5 implementation: callback itself is not replayed. WAL logs final accepted replacements; missing/no-op updates do not append production frames. |
| Mongo `updateOne` | `_id` equality plus accepted `$set` subset | `CollectionUpdateByIDSet` or final replacement | `WAL-supported` only after canonical lowering and result assertions. |
| Mongo `deleteOne` | `_id` equality | `CollectionDeleteBatchByID` | Same as native explicit-ID delete. |
| Collection/catalog metadata | create collection | `CatalogCreateCollection` | PR6 implementation: `WAL-supported`; payload carries canonical collection metadata, replay is idempotent for matching metadata and fail-closed for incompatible metadata. |
| Collection/index metadata | create/drop index | future catalog command | `WAL-rejected` in PR6; public index DDL fails before frame append or `AppliedCommandLSN` advancement until index catalog commands land. |
| Collection vector index rebuild | explicit index name through `Collection.RebuildVectorIndex` | `CollectionRebuildVectorIndex` | `WAL-supported` for `native_runtime` and explicit `column_graph` rebuild/publish maintenance. Payload carries only collection and index names. Replay publishes a complete native graph root for `native_runtime`; for `column_graph`, it rebuilds physical assets or publishes a defined no-op status boundary. |
| Query-wide update/delete | predicate/range matched mutation | none | `WAL-rejected`; future command kind required. |
| User-defined callback replay | arbitrary function | none | `WAL-rejected`; lower to final replacement first. |
| Column-store file publish | external side-file refs | future command plus external-ref classes | Deferred until external-file prepare/recovery is specified. |

The matrix is normative for planning: adding or broadening a mutating API also
requires a matrix update in the same PR. PR7 makes this executable through
`TreeDB/docs/spec/command-wal-support-matrix.json`; docs tests require current
collection APIs, Mongo gateway mutating commands, and native-wire mutating
commands to have explicit `WAL-supported`, `WAL-rejected`, `WAL-off-only`, or
`future` status.

Collection command replay handlers live in the `TreeDB/collections` package
because replay must re-enter the normal collection executor. Binaries that may
open `command_wal_v2` directories containing collection frames must import that
package before `db.Open` recovery runs, or call
`collections.RegisterCommandWALReplayHandlers()` during startup. A backend-only
binary without those handlers must fail closed on collection command kinds
rather than skipping frames.

### 6.1 Batch Atomicity

V1 batch commands are atomic at the command-frame level. `RawKVBatch`,
`CollectionInsertBatchByID`, `CollectionDeleteBatchByID`, and
`CollectionReplaceBatchByID` are each represented as one command frame with one
LSN, frame-level CRC integrity, and all-or-nothing replay semantics.

If a batch is too large for the configured command-frame limits, the V1 WAL-on
durable mode must reject it before assigning an LSN. The implementation must not
silently split one user-visible batch into multiple WAL commands while still
claiming the original API call was atomic. A caller may explicitly submit smaller
batches, but each smaller batch then has its own LSN and its own independent
atomicity boundary. Multi-frame atomic batches require a future `CommandGroup`
protocol with explicit group identity, ordering, result assertions, and
root/`AppliedLSN` publish rules.

Batch preconditions and result assertions are evaluated for the whole batch. On
recovery, any item-level mismatch that is not covered by a command-kind-specific
idempotent-skip proof fails the whole command closed.

Target conditional raw KV transaction commits use the same atomic command-frame
rule. User-level point-read conflicts, unsupported range guards, and unsupported
range operations must reject before LSN assignment so ordinary conflicts never
create a command-WAL gap. If a future implementation needs to persist a failed
conditional attempt after LSN assignment, it must log an explicit deterministic
no-op/failure command that can advance `AppliedLSN` contiguously. Replay
preconditions and result assertions are for deterministic drift/corruption
detection, not for routine stale-transaction conflicts. Point-read preconditions
and replay result assertions should use the existing
`CommandEnvelope.Preconditions` and `CommandEnvelope.ResultAssertions` extension
areas unless #3423 proves a new raw KV payload version is required.

### 6.2 Update API Categories

Update support should be split by replay contract:

| Category | API shape | WAL representation |
|---|---|---|
| Declarative literal update | set/unset/replace by explicit document ID using already-resolved values | canonical update operator payload or final replacement |
| Resolver-backed declarative update | helpers such as `SetNow`, `SetUUID`, or sequence assignment | canonical update operator payload containing resolved literals, not helper calls |
| Opaque Go callback | `Update(id, func(current []byte) ...)` or `UpdateBatch` callback item | final accepted replacement/no-op result after callback execution |
| Predicate/query-wide update | update/delete by filter, range, or scan predicate | rejected until it lowers to ordered explicit document IDs plus per-document commands |

The resolver boundary is strict: if command execution needs time, randomness,
UUID generation, process state, or an external service, the value must be chosen
before WAL append and embedded as data in the payload. The command WAL must not
contain an instruction to call a resolver during recovery.

This gives TreeDB three implementation choices without weakening durability:

- declarative APIs remain compact and semantic when their operators are
  versioned and canonical;
- nondeterministic convenience APIs remain safe because they resolve to literals
  before WAL append;
- arbitrary callback APIs remain safe by lowering to full replacements instead
  of claiming callback purity.

## 7. Normal Write Path

For a WAL-supported command, the required ordering is:

```text
1. parse and validate user request
2. resolve nondeterministic helper inputs to canonical literal values
3. run user callbacks if any, then discard callback identity
4. lower the request or callback result to a canonical replayable command payload
5. prepare/write required external refs, then sync and protect them according to
   the selected durability mode
6. compute preconditions, result assertions, and the canonical `ExternalRefs`
   set that the command payload requires
7. assign LSN
8. append the complete command WAL frame with payload, assertions, and
   `ExternalRefs`
9. flush/sync WAL according to durability mode
10. apply the command through the normal executor
11. install owner-visible process state only after the command frame is
    recoverable
12. return ordinary WAL-on success once the command is recoverable and the
    normal executor has accepted or installed the command according to the API
    visibility contract
13. later checkpoint, flush, close, or synced ack boundaries publish roots,
    system metadata, target raw KV revision state, external-ref reachability,
    and `AppliedLSN` in one durable publish boundary before WAL cleanup
```

For WAL-on collection writes, no owner-visible state may be installed before the
complete command frame and required external refs are recoverable. Ordinary
WAL-on success does not require a checkpointed root plus `AppliedLSN`; if a crash
happens first, recovery replays the complete frame from the previous durable
`AppliedLSN`. Durable pending overlays that survive without replay are a later
feature, not part of V1.

Failure to prepare, sync, protect, or declare a required external ref fails the
command before it becomes recoverable and before any owner-visible state or
uniqueness reservation is installed. A complete command frame that references a
missing or corrupt required external ref is a recovery error, not a skipped
write.

WAL-off relaxed modes may preserve existing flush-boundary behavior, but must
document that the write is not crash-durable until a publish/checkpoint boundary
covers it.

## 8. Checkpoint and Cleanup

A checkpoint, flush, close, synced ack, or recovery publish that records durable
command effects in roots must also record the highest contiguous `AppliedLSN`
covered by those roots and any target raw KV revision state selected by those
roots. `AppliedLSN` is not ordinary asynchronous checkpoint metadata; every
durable root publish that makes command effects independent of WAL replay must
make the corresponding `AppliedLSN` durable in the same publish boundary.

`AppliedLSN` is metadata for the same command WAL sequence stream, not a
separate collection watermark. It is required because commands are not always
safely idempotent if a crash happens after root publication but before obsolete
WAL segments are deleted. All command frames, including raw `RawKVBatch` frames,
with `LSN <= AppliedLSN` must be skipped during recovery.

Required publish/checkpoint ordering:

```text
1. stop or fence admission for commands that must be included in this checkpoint
2. wait for admitted command apply/publish through the chosen AppliedLSN
3. sync value-log, leaf-log, side-file, and pager bytes needed by published roots
4. write durable root metadata, target raw KV revision state, and AppliedLSN in
   the same meta-selected state
5. sync the metadata boundary
6. delete or mark clean WAL segments whose max LSN <= AppliedLSN
```

Runtime command-WAL segment growth is bounded by rotating the active segment
before an append would exceed `CommandWALSegmentTargetBytes`. That target is
separate from `WALMaxSegmentBytes`, which remains a per-frame safety cap. A
checkpoint boundary should rotate a non-empty active command-WAL segment before
publishing `AppliedLSN`, then clean only segments whose max LSN is covered by
the durable checkpointed state. In cached command-WAL mode, `MaxWALBytes`
participates in the same auto-checkpoint pressure loop using total command-WAL
segment bytes instead of the disabled legacy cached redo journal.

The V1 implementation target is an in-page-marked meta-page field named
`AppliedCommandLSN`. It must be selected by the same meta-page choice as the
roots, and the same selected page body must contain the command-WAL V1 marker
before the field is authoritative. PR1 may document a blocking reason to change
this decision before PR2 starts, but PR2 must not proceed with both meta-page
and system-root storage as live implementation options.

A sidecar file, format-config marker, post-commit manifest, async stats record,
or post-work callback must not be the authoritative `AppliedLSN` source because
it would allow split states after crash.

### 8.1 Crash-Correctness Requirements

The command WAL must preserve the correctness shape TreeDB already relies on:
copy-on-write root publication plus durable meta/root selection, with WAL cleanup
after durable proof.

Required crash cases:

- Crash after WAL append but before command visibility: no root state may expose
  the command yet; recovery sees `LSN > AppliedLSN` and replays it.
- Crash during command apply before root/meta publication: copy-on-write pages or
  partial root state are unreachable from the selected meta/root; recovery
  replays the command again from WAL.
- Crash after durable root publication but before `AppliedLSN` publication is
  forbidden as a selected recovery state. Command effects and `AppliedLSN`
  advancement must be in the same backend durability boundary, so recovery
  either sees neither or sees both.
- Crash after root plus `AppliedLSN` publication but before WAL cleanup: recovery
  selects the new root state, skips frames with `LSN <= AppliedLSN`, and cleanup
  resumes idempotently.
- Crash after WAL cleanup: cleanup is valid only for segments whose max LSN is
  covered by durable `AppliedLSN`; missing cleaned segments must not be needed
  for recovery.
- Crash during recovery replay: each replay publish must atomically commit the
  command effects and advance `AppliedLSN` through the replayed command or
  command group. On restart, recovery resumes from the selected durable
  `AppliedLSN` and must not double-apply commands above it.

A command implementation must not publish durable user roots first and advance
`AppliedLSN` later. It also must not advance `AppliedLSN` before the root state
and all required value-log/leaf-log/external-ref bytes are durable enough for the
selected durability profile.

For atomic command groups, the group effects and the resulting `AppliedLSN` must
publish in one backend commit, or the group must have explicit per-command
idempotency/result assertions that make replay after partial progress safe.

### 8.2 Publish Boundary Contract

The publish boundary must be implemented as one backend finalize/meta selection,
matching the current TreeDB correctness model where copy-on-write roots are not
visible until a meta page selects them. For command WAL, the selected state tuple
is:

```text
(UserRootPageID, SystemRootPageID, AppliedLSN, CommitSeq, EntryRevision state, required value-log/leaf-log reachability)
```

That tuple is indivisible for recovery. If any element is missing or stale, open
must select the previous durable tuple or fail closed. In particular:

- root descriptors must not point at command effects unless `AppliedLSN` covers
  those effects;
- `AppliedLSN` must not advance beyond roots that actually contain the command
  effects;
- `AppliedLSN` advances only over a contiguous LSN prefix;
- publishing command LSN `N` requires every lower LSN to already be covered or
  to be applied in the same publish boundary;
- post-commit work such as pruning, metrics, manifest maintenance, or cleanup
  cannot be required to reconstruct the authoritative applied state.

A command WAL implementation should add a dedicated publish helper rather than
allowing callers to compose `PublishOrderedRoot*` plus a later metadata update.
The helper should take the command LSN range being published and reject attempts
to publish roots without the matching `AppliedLSN` advancement.

### 8.3 Replay Idempotency Contract

Replay idempotency is primarily provided by the publish boundary, not by making
every command naturally idempotent. Recovery must run commands only from a state
whose durable `AppliedLSN` is lower than the command LSN. If recovery observes
that a strict command's effect already exists while `AppliedLSN` does not cover
it, that is a split-state/corruption signal and must fail closed.

Command kinds may define explicit idempotent-skip rules only when they can prove
the existing state is exactly the same command result. Examples of acceptable
proof include stable command IDs, document digests, matched/modified assertions,
and catalog/schema epoch guards. A generic duplicate-key or already-exists error
is not enough to skip replay.

Non-crash errors after a complete WAL frame is durable are commit-outcome
errors, not normal user rejections. The implementation must either retry/publish
to completion, close and require recovery, or return a public
recovery-required/commit-unknown error. User-level validation and deterministic
precondition failures must happen before the complete WAL frame is made
recoverable.

## 9. Recovery

Recovery starts from the durable checkpointed state selected by normal TreeDB
meta/root recovery.

Recovery algorithm:

```text
1. load durable AppliedLSN
2. discover command WAL segments
3. decode frames in LSN order
4. ignore or clean frames with LSN <= AppliedLSN only after cleanup proof
5. validate every frame with LSN > AppliedLSN
6. stop at a terminal incomplete tail only when no complete commit marker exists
7. replay complete commands in strict LSN order through deterministic executor
8. publish recovered roots, system metadata, and AppliedLSN in the same backend durability boundary
9. checkpoint recovered AppliedLSN
10. clean replayed WAL only after durable checkpoint success
```

Recovery must fail closed on:

- duplicate LSN;
- missing non-cleaned segment in a required range;
- corrupt complete frame;
- unsupported command version;
- unsupported command kind;
- failed precondition that is not explicitly classified as an idempotent skip with proof;
- missing required external ref;
- result assertion mismatch;
- observed command effect without matching durable `AppliedLSN` for a strict command.

## 10. Relationship to Existing Raw WAL

Raw key/value writes become first-class `RawKVBatch` commands in the command
WAL. The old raw `commitlog.Record` batch format is not a compatibility target
for command WAL directories.

The implementation must preserve current raw WAL correctness strengths:

- append-before-visibility ordering;
- checksums;
- optional compression;
- truncated-tail handling;
- RID/value-log fence ideas;
- replay-through-normal-command-executor discipline;
- post-replay cleanup only after success.

The implementation must not add `wal/collection-l*.log`,
`internal/collectionwal` as the active appender/decoder, or a collection-only
cleanup/watermark path. Any existing collection root-delta WAL scaffolding is
historical unless a later ticket explicitly revives it.

The implementation should remove or replace raw-record-specific writer and
reader paths as part of the command WAL conversion. Tests should move from
legacy raw record replay to `RawKVBatch` command replay.

Target raw KV entry revisions are part of the logical `RawKVBatch` effect and
must be recoverable through the selected root tuple. They must not be represented
as an additional raw-record sidecar whose replay, cleanup, or checkpoint
ordering can diverge from the user value.

## 11. Relationship to Raft and Distributed State

Raft and local WAL should share command semantics but not the same durability
responsibility.

```text
Raft log: replicated consensus ordering
Local command WAL: single-node crash recovery ordering
Checkpoint/snapshot: compacted root state plus applied index/LSN
```

A future Raft entry should carry the same canonical command payload or a strict
superset. A Raft node may not advertise a command as locally recoverable until
its local apply/durability rule is satisfied. Local page IDs/root IDs are not a
portable consensus state root; a future cluster state digest must be defined
separately if consensus requires byte-independent state equality.

Native-wire deterministic command entries are the canonical schema source for
wire-exposed mutations. The local command WAL should reuse those deterministic
payload schemas and golden fixtures wherever the same command exists. If a
single-node local API lands before the native-wire surface, the native-wire
schema added later must either adopt the local canonical bytes or explicitly
record why a wrapper is required.

For the R3a local apply harness tracked by #1654, the deterministic entry is not
the local WAL record itself. The harness decodes committed `CommandEntryV1`
bytes, validates digest, target, idempotency, and allowlist metadata, lowers
supported entries to local `CommandEnvelope` payloads, appends/applies through
the user-command WAL and normal executor, and advances future `ApplyProgress` or
applied-index metadata only after the selected local recoverability boundary is
satisfied. If the selected boundary is root-recoverable, that means roots and
`AppliedLSN` have been published atomically.

If future Raft apply identity becomes the authority for target raw KV
`EntryRevision` assignment, single-node command WAL apply and recovery must
derive revisions from the same deterministic input. A replica may not advance
future applied-index metadata past a revision-bearing raw KV mutation until the
value, revision, root tuple, and local recoverability boundary are durable
together.

## 12. Future Command Admission Policy

Every PR that adds or broadens a mutating user-facing command must update the
command WAL matrix and choose one status:

- `WAL-supported`: command has canonical encoding, deterministic replay,
  recovery tests, and result assertions where needed.
- `WAL-rejected`: command is rejected in WAL-on durable-at-ack modes until its
  replay contract lands.
- `WAL-off-only`: command is allowed only under durability profiles that make no
  durable-at-ack claim.

A command may not silently inherit WAL support from a similar command. Examples:

- Adding `$inc` is not covered by `$set`.
- Adding `SetNow`, `SetUUID`, or another resolver-backed helper is not covered
  unless the helper resolves to a literal before WAL append.
- Adding `multi: true` is not covered by `_id` updateOne.
- Adding range/predicate delete is not covered by explicit ID delete.
- Adding a user callback API is not covered unless it lowers to final replayable
  replacements before WAL append.

## 13. Query-Wide Mutation Gate

Query-wide mutation is deferred until the query planner/executor can prove:

- deterministic predicate semantics;
- deterministic match ordering;
- stable collation and type comparison;
- bounded replay work;
- stable index/scan choice or scan-choice-independent result semantics;
- versioned update operator semantics;
- result assertions, at minimum expected matched/modified counts;
- crash tests for replay after catalog/index changes.

Until then, commands such as:

```text
update users where age > 30 and city = "NYC" set active = true
```

must be rejected in WAL-on durable-at-ack modes or lowered by the caller into an
explicit ID batch before WAL append.

## 14. Implementation Ticket, Assertions, and Evidence Gates

The detailed test-driven execution plan for this WAL track lives in issue
https://github.com/snissn/gomap/issues/1529. This spec owns the durable
correctness contract, command matrix, required assertion categories, and
acceptance evidence shape. The issue may reorganize sequencing as implementation
facts change, but it must not weaken the invariants in this spec.

Implementation PRs should reference the tracker issue and the relevant milestone
below. If the issue plan and this spec disagree on a correctness requirement,
the stricter requirement applies until the spec is updated in the same PR as the
implementation change.

### 14.1 Required Assertion Hooks

The production implementation should include explicit internal assertion hooks
for command WAL invariants. Cheap corruption-prevention checks should run in all
builds and return typed errors. Expensive structural checks may be guarded by a
debug/test option, but every guard must have a unit test that demonstrates it can
catch the intended bug.

Required assertion categories:

- `assertCommandWALFeatureGate`: command WAL directories cannot be served,
  compacted, or cleaned by binaries that do not support the required feature.
- `assertSingleJournalOwner`: one mutable journal owner per DB directory and one
  shared LSN stream for raw, collection, catalog, and native command writes.
- `assertNoVisibilityBeforeRecoverability`: WAL-on command state cannot become
  visible before its complete command frame and required external refs are
  recoverable under the selected durability mode.
- `assertPublishTuple`: root IDs, system metadata, value-log/leaf-log
  reachability, and `AppliedCommandLSN` are selected in one backend durability
  boundary.
- `assertContiguousAppliedLSN`: `AppliedCommandLSN` advances only over a
  contiguous command LSN prefix.
- `assertRecoveryStartsFromSelectedTuple`: recovery starts from the selected
  root/meta tuple and never guesses through split root/LSN states.
- `assertExternalRefClosure`: every required external ref in a complete command
  frame is present, checksummed, class-valid, and protected until root
  reachability or command abort takes ownership.
- `assertCleanupProof`: WAL segment deletion requires durable proof that the
  segment's max LSN is covered by `AppliedCommandLSN`.
- `assertStrictReplayNoGenericSkip`: strict commands cannot skip replay on
  generic already-exists or duplicate-key evidence.
- `assertNoCallbackOrResolverReplay`: recovery cannot call Go callbacks, wall
  clock, random, UUID, sequence, or external resolver functions.

These hooks are implementation aids, not replacement tests. Every assertion
must have a test that intentionally violates the invariant and observes the
typed error or recovery failure.

### 14.2 Crash Harness Requirements

The command WAL implementation should introduce a deterministic fault-injection
harness before command breadth. The harness should expose named cut points, not
sleep-based races. Every command kind that becomes `WAL-supported` must run the
same cut-point suite.

Required harness behavior:

- cut points are named stable constants and appear in test output;
- each cut point can fail once, fail always, or fail by deterministic seed;
- injected failure reports whether a frame was assigned, written, synced,
  applied, published, and cleaned;
- reopen validation compares user-visible state, system metadata,
  `AppliedCommandLSN`, WAL segment debt, and external-ref reachability;
- read-write recovery and read-only open are tested separately;
- fault injection never masks ordinary Go race detector failures.

The initial harness should target raw `RawKVBatch` and explicit-ID collection
insert first. Later command kinds inherit the same crash matrix rather than
creating bespoke recovery tests.

### 14.3 Per-PR Evidence Gate

Each implementation PR must include a short evidence block in the PR description
and a machine-readable artifact under `artifacts/command-wal/<milestone>/`. The
evidence block must list:

- command kinds touched and their matrix status;
- red tests or fixture drift tests added before implementation;
- crash cut points exercised;
- model/fuzz targets added or extended;
- runtime assertions added or extended;
- unsupported modes and their exact public errors;
- local commands run, including race/fuzz/crash-harness duration where relevant;
- CI status and any excluded tests with rationale.

No milestone is complete until the same evidence is reflected in
`verification.md`. Test names in the spec should be exact names from code once
the implementation PR exists. Planned names are acceptable only in this planning
PR.

### 14.4 Existing Test Migration Gate

The compatibility-breaking command WAL rewrite must preserve existing WAL and
recovery invariants even when old raw payload readers are removed. Existing
tests should be migrated by invariant, not by implementation file.

No existing WAL, recovery, checkpoint, value-log fence, corruption, or
read-only-open test may be deleted unless the implementation PR records one of:

- a direct command-WAL equivalent test;
- a renamed test that asserts the same invariant under typed command frames;
- a documented reason the old test applied only to unsupported legacy raw
  payload compatibility after `command_wal_v2` activation.

PR 1 must include a test inventory that maps legacy WAL tests to command-WAL
coverage buckets. PRs 2 and 3 must keep that inventory current as publish,
checkpoint, cleanup, and raw KV replay move to `AppliedCommandLSN`.

Required migration buckets:

| Existing coverage | Command-WAL replacement |
|---|---|
| Legacy raw frame encoding/decoding | typed command frame golden fixtures and decoder hardening tests |
| Raw KV WAL replay | `RawKVBatch` command replay through the normal executor |
| RID/value-log fence behavior | command external-ref or equivalent payload fence tests |
| Checkpoint cleanup | `AppliedCommandLSN` cleanup proof and crash-before-cleanup tests |
| Truncated tail and corruption handling | typed frame decoder outcome tests |
| Read-only open/recovery-required behavior | unapplied command frame read-only-open rejection |
| Collection flush-boundary durability | current behavior tests plus command-WAL variants only when the command kind becomes `WAL-supported` |
| `internal/collectionwal` tests | historical/deprecated tests only, or removal when no active code depends on them |

## 15. PR Milestones

### PR 0: Spec, issue, and deprecation cleanup

Deliverables:

- add this spec;
- create the user-command WAL tracker issue;
- mark the older collection root-delta WAL tracker and PR as superseded;
- add pivot notes to transaction, update-staging, and column-store roadmaps.

Acceptance:

- docs describe user-command WAL as the active target;
- old collection WAL/root-delta work is preserved as historical context only.

### PR 1: Typed commit-log frames and feature gate

Deliverables:

- convert `TreeDB/internal/commitlog` to the compatibility-breaking command WAL
  frame format;
- versioned command frame encoder/decoder for `RawKVBatch`, collection command,
  and catalog command payload families;
- `CommandEnvelope` types and golden fixtures;
- `RawKVBatch` command payload encoding and fixtures, while production raw KV
  writes remain on the legacy raw payload path until PR3 adds recovery
  dispatch, `AppliedCommandLSN`, and cleanup plumbing;
- fail-closed rejection of legacy raw `commitlog.Record` payloads once the
  command WAL feature is present;
- feature gate / required storage feature;
- reader classification for tail, corruption, unsupported version, duplicate LSN;
- tests independent of collection internals.

Acceptance:

- no new `collection-l*.log` segment family or active `internal/collectionwal`
  appender is introduced;
- legacy raw frame tests are inventoried and mapped to typed-frame replacements;
  production raw replay tests remain active until PR3 converts raw KV writes to
  `RawKVBatch`;
- malformed or unsupported command WAL fails closed;
- truncated terminal tail handling is tested;
- command payload fixtures state whether they reuse native-wire deterministic
  schemas or intentionally define a local-only schema;
- batch command fixtures prove one frame, one LSN, and all-or-nothing decoding
  for supported V1 batch commands;
- docs-check and drift tests cover the command matrix.

### PR 2: Shared journal ownership and AppliedLSN checkpoint plumbing

Deliverables:

- extract or reuse the existing cached WAL lane writer/lifecycle as a shared
  journal service for raw, collection, and catalog command frames;
- single mutable journal owner per DB directory;
- typed command appends use `wal/commit-l<lane>-<seq>.log`;
- WAL sequence allocation comes from the shared journal service;
- durable `AppliedCommandLSN` storage in the in-page-marked meta-page format,
  selected by the same meta-page boundary as command roots;
- checkpoint and publish-boundary integration;
- segment cleanup rules;
- read-only dirty-WAL detection;
- backup/restore manifest updates.

Acceptance:

- cached raw writes and collection command writes cannot open independent WAL
  writers for the same directory;
- checkpointed WAL ranges are skipped/cleaned only after durable proof;
- roots and `AppliedLSN` cannot be published as split commits;
- typed command frames with `LSN <= AppliedLSN` are skipped during recovery;
- read-only open fails when mutating recovery would be required.

Implementation evidence expected for this milestone:

- the cached write path and command journal service acquire the same mutable
  journal-owner lock before opening commit-log writers;
- the command journal service assigns contiguous LSNs before typed frame append;
- `AppliedCommandLSN` is stored in the alternating meta page body and published
  with roots through a dedicated command-WAL publish helper;
- read-only opens scan typed command frames and return `ErrRecoveryRequired`
  when any complete frame has `LSN > AppliedCommandLSN`;
- read-write opens keep typed command frames with `LSN <= AppliedCommandLSN`
  out of legacy raw batch replay and fail closed on higher typed LSNs until the
  PR3 typed replay dispatcher is present;
- cleanup follows `durable-wal-cleanup-proof-3682.md`: it removes only exact
  non-active segment identities whose complete LSN range is covered by every
  recovery-selectable durable root, whose lineage and journal namespace still
  revalidate, and whose physical identity is outside every active/retry pin;
- benchmark evidence records shared journal allocation/append overhead and
  root/meta publication overhead with `AppliedCommandLSN`.

### PR 3: Recovery dispatcher and raw KV command conversion

Deliverables:

- extend the existing backend WAL recovery pass to dispatch typed frames;
- represent current raw set/delete/batch writes only as `RawKVBatch` command
  frames;
- preserve RID/value-log fence semantics through command `ExternalRefs` or an
  equivalent command payload fence;
- recovery tests cover restart during replay and crash before/during/after root
  plus `AppliedLSN` publication;
- replay idempotency tests prove strict commands fail closed on split-state
  evidence and idempotent-skip commands require digest/assertion proof.

Acceptance:

- unsupported required command kinds fail closed before serving reads;
- command WAL tests prove `RawKVBatch` replay, contiguous `AppliedLSN`
  advancement, root/`AppliedLSN` atomicity, and cleanup.

### PR 4: Collection insert/delete by explicit ID

Deliverables:

- `CollectionInsertBatchByID` and `CollectionDeleteBatchByID` canonical payloads;
- one-frame/one-LSN all-or-nothing semantics for insert/delete batches, with
  oversized WAL-on batches rejected unless the caller explicitly submits smaller
  independent batches;
- JSON/BSON/template payload policy for stored documents;
- recovery through normal collection executor;
- explicit unsupported-mode failures for uncovered mutation kinds.

Acceptance:

- crash after acknowledged insert/delete and before checkpoint recovers correctly;
- duplicate/missing IDs replay idempotency behavior is explicit and tested.

PR4 evidence:

- canonical frame fixtures:
  `command_wal_v1_collection_insert_by_id.hex` and
  `command_wal_v1_collection_delete_by_id.hex`;
- normal path: public `InsertBatch` appends one collection command frame and
  publishes roots with `AppliedCommandLSN`;
- recovery path: unapplied insert/delete frames replay through collection
  executors, including template-v1 stored documents and missing-only delete
  no-ops;
- unsupported index DDL fails with `ErrCommandWALUnsupported` until PR6+ index
  catalog commands;
- performance artifacts:
  `artifacts/command-wal/pr4/collection-insert-delete-microbench.txt` and
  `artifacts/command-wal/pr4/collection-insert-delete-microbench-benchstat.txt`.

### PR 5: Collection update by explicit ID

Deliverables:

- `CollectionUpdateBatchByID` canonical payload for callback/BSON-set APIs after
  final replacement is known;
- public `Update`, `UpdateBatch`, and structured BSON `$set` update paths lower
  to accepted replacement documents before WAL append;
- declarative operator and resolver-backed helper payloads stay explicitly
  future work rather than replaying helper functions;
- one-frame/one-LSN all-or-nothing semantics for replacement/update batches, with
  no implicit atomic split without a future `CommandGroup` protocol;
- tests for public publish, open-time replay, indexed secondary-root updates,
  no-op frame advancement, and corrupt-payload count bounds.

Acceptance:

- no Go callback is replayed;
- no resolver/helper code is invoked during recovery for implemented update
  paths because the WAL payload is the resolved stored document;
- recovered primary and secondary index state matches normal execution;
- unsupported future update operators remain outside the support matrix until a
  canonical operator payload lands.

PR5 evidence:

- canonical frame fixture:
  `command_wal_v1_collection_update_by_id.hex`;
- normal path: public `Update`, `UpdateBatch`, and BSON-set updates append one
  collection update command frame and publish roots with `AppliedCommandLSN`;
- recovery path: unapplied update frames replay through the collection executor
  without invoking the original callback;
- indexed public update coverage verifies secondary index delete/set state under
  command WAL;
- performance artifacts:
  `artifacts/command-wal/pr5/collection-update-microbench.txt` and
  `artifacts/command-wal/pr5/collection-update-microbench-benchstat.txt`.

### PR 6: Catalog mutation commands

Deliverables:

- `CatalogCreateCollection` command frame and payload fixture;
- public `CreateCollection` appends one catalog command frame and publishes the
  system catalog root plus `AppliedCommandLSN` in one backend tuple;
- open-time replay creates missing collections, treats same-metadata replay as
  idempotent, and fails closed on incompatible metadata without publishing the
  replay LSN;
- create/drop index commands remain explicit WAL-on pre-frame rejections until
  index catalog payloads land;
- lower-LSN drain evidence uses the shared command journal: recovered lower raw
  KV frames advance `AppliedCommandLSN` before a catalog create receives the next
  contiguous LSN.

Acceptance:

- GUI/Mongo-compatible `create` maps to the same `CreateCollection` command-WAL
  path: acknowledged success means the catalog command frame is recoverable and
  the catalog root plus `AppliedCommandLSN` were published together; it is not an
  fsync guarantee unless the caller uses a sync-capable durability barrier;
- schema/index changes cannot race lower unapplied command LSNs because catalog
  create uses the shared command journal and contiguous `AppliedCommandLSN`
  validation;
- unsupported index DDL is cheap and non-mutating in command-WAL mode.

PR6 evidence:

- canonical frame fixture:
  `command_wal_v1_catalog_create_collection.hex`;
- normal path: public `CreateCollection` appends a `CatalogCreateCollection`
  frame and advances `AppliedCommandLSN`;
- recovery path: unapplied catalog create frames replay through the collection
  catalog executor, including same-metadata idempotent replay;
- negative path: incompatible replay fails closed, and rejected index DDL leaves
  `AppliedCommandLSN` and frame count unchanged;
- performance artifact:
  `artifacts/command-wal/pr6/bench.txt`.

### PR 6.5: Collection/catalog command-WAL performance polish

Deliverables:

- consolidate the PR4, PR5, and PR6 collection/catalog command-WAL performance
  artifacts before matrix hardening and public raw KV default cutover;
- apply the strict default-ready throughput gate to collection/catalog lanes:
  command-WAL throughput divided by the relevant WAL-off baseline throughput
  must be greater than `1.01x`; anything not strictly greater than `1.01x`
  fails;
- explicitly prevent PR9 public raw KV performance evidence from being reused as
  a collection command-WAL default-readiness claim;
- convert any below-gate collection overhead into a follow-up issue with exact
  lane thresholds.

Acceptance:

- catalog create remains default-ready on performance evidence because the PR6
  artifact shows command-WAL create throughput above the WAL-off baseline;
- collection insert/delete/update command-WAL lanes remain supported command
  kinds but are not default-ready performance lanes until the follow-up issue
  clears strict `>1.01x` throughput for every supported lane;
- PR9 must restrict default-readiness claims to public raw KV unless it adds
  new collection/catalog performance artifacts that clear the same strict gate.

PR6.5 evidence:

- consolidated performance summary:
  `artifacts/command-wal/pr6_5/collection-catalog-performance-summary.md`;
- acceptance artifact:
  `artifacts/command-wal/pr6_5/acceptance.json`;
- follow-up issue for the below-gate collection lanes:
  `https://github.com/snissn/gomap/issues/1584`.

### PR 7: Matrix enforcement and future-command guardrails

Deliverables:

- machine-readable command WAL support matrix;
- CI/doc drift test requiring matrix updates for mutating command registry changes;
- CI/doc drift test rejecting active `collection-l*.log` or
  `internal/collectionwal` implementation references outside deprecated docs;
- public error types for unsupported WAL mode commands.

Acceptance:

- adding a new mutating command without a WAL status fails tests;
- current unsupported query-wide mutation remains explicitly rejected.

PR7 evidence:

- machine-readable matrix:
  `TreeDB/docs/spec/command-wal-support-matrix.json`;
- docs tests require rows for current collection mutators, Mongo gateway
  mutation handlers, and native-wire mutation commands;
- docs drift test rejects active collection-WAL segment references outside the
  deprecated implementation/docs allowance;
- public sentinel aliases:
  `db.ErrCommandWALRejected` and `treedb.ErrCommandWALRejected`.

### PR 8: Raft alignment design closeout

Deliverables:

- define how Raft entries map to `CommandEnvelope` payloads;
- define applied-index vs local `AppliedLSN` ordering;
- define idempotency metadata durability ordering;
- add non-implementation tests/fixtures if useful for deterministic bytes.

Acceptance:

- future Raft work can reuse the command payload contract without depending on
  local WAL segment layout.

### PR 9: Default command-WAL public raw KV cutover

Deliverables:

- public `treedb.Open` read-write handles no longer fail closed solely because
  `CommandWAL` or persisted `command_wal_v2` is active;
- public raw KV `Set`, `Delete`, and `Batch.Write` calls use `RawKVBatch`
  command frames through the cached public command-WAL path;
- the public command-WAL write path uses cached visibility while disabling the
  cached legacy redo journal and appending typed command frames through the
  shared backend journal owner;
- stats expose mode proof with `treedb.write_path.mode=command_wal_cached`,
  `treedb.command_wal.required_feature=true`, live accepted/covered command
  frame counters, and max LSN. Diagnostic WAL segment scans are optional
  inventory proof, not required for normal benchmark mode proof.

Acceptance:

- public raw KV command-WAL writes reopen through `treedb.Open` without explicit
  backend-only APIs;
- recovery preserves visible state and deletes after public batch writes;
- the support matrix classifies public raw KV writes as `WAL-supported` and
  points at executable evidence;
- the historical PR9 point `Set`, focused `Batch.Write`, `unified_bench`
  batch-write, and incompressible value-log auto/off throughput gates each
  require candidate throughput strictly greater than `1.01x` of the relevant
  baseline. Any required lane in that immutable acceptance artifact at or
  below `1.01x`, including sub-parity evidence such as `0.80x`, is failing
  rather than accepted evidence;
- all historical required command-WAL acceptance performance gates use the
  same strict parity-plus policy: passing evidence includes `>` semantics,
  explicit `1.01x` minimum ratio thresholds, and comparative throughput ratios
  above the threshold;
- the current hosted incompressible value-log gate is the #3861/#3863 settled
  CPU-efficiency/storage contract. It uses `batch_write_steady`, captures a
  CPU profile around each exact timed row, and retains every raw wall-throughput
  ratio as diagnostic evidence. The blocking ratio for each pair is off CPU
  sample seconds divided by auto CPU sample seconds. The gate records the CPU
  model and selected threshold and applies a strict `>0.94x` threshold on AMD
  EPYC 7763 runners, a strict `>0.93x` threshold on AMD EPYC 9V74 runners, or
  a strict `>0.95x` threshold on all other or unknown CPU models to the
  geometric mean of every fixed, order-balanced CPU-efficiency pair. Missing,
  ambiguous, or shorter-than-`0.25s` CPU profiles fail closed;
  no row is retried, selected, or discarded. The gate keeps the sum of each
  pair's `total=` fields for `maindb/value_vlog` plus `maindb/leaf_vlog` at or
  below `1.02x`. One favorable sample cannot override a mostly failing sample
  set. Independent mode checks require raw user values in both rows,
  block-compressed leaves in auto, and uncompressed leaves in off.

PR9 initial evidence:

- `TestPublicCommandWALRawKVWritesUseTypedFrames` opens with public
  `treedb.Open`, writes `Set` plus batch set/delete operations, proves command
  frame/max-LSN stats are non-zero, closes, reopens from persisted
  `command_wal_v2`, and verifies the final public state;
- this PR intentionally routes public command-WAL writes through cached
  visibility plus typed command-WAL frames rather than re-enabling the cached
  legacy redo journal.

## 16. Deprecation of the Collection Root-Delta WAL Target

The collection-specific physical/root-delta WAL target is deprecated for new
implementation work. Its documents and PRs remain useful as a record of crash
safety risks, external-ref concerns, and recovery failure modes, but future work
should not expand that approach feature-by-feature.

Superseded concepts:

- collection-only WAL record families;
- physical root-delta as the primary durable user mutation record;
- per-feature collection WAL support expansion by JSON/BSON/index path;
- recovery that must understand collection planner internals.

Concepts to preserve:

- fail-closed recovery;
- external-ref validation before serving reads;
- no visibility before recoverability in durable-at-ack modes;
- checkpoint cleanup only after durable proof;
- clear separation between local WAL and Raft consensus semantics.

## 17. Open Questions

1. Should Mongo `$set` replay as structured `$set` or as final replacement bytes?
2. What is the minimum idempotency metadata needed for retryable native-wire or
   future Raft commands?
3. Which catalog/DDL commands must be V1 WAL-supported versus WAL-rejected?
4. How should large command payloads reference value-log bytes without making
   the value log an ephemeral WAL again?
5. What exact public error should be returned for `WAL-rejected` commands?
6. Which resolver helpers should be first-class convenience APIs versus caller
   responsibility to resolve before invoking declarative update ops?
